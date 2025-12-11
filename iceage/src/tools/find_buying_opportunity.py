# iceage/src/tools/find_buying_opportunity.py
import pandas as pd
import numpy as np
import glob
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / "iceage" / "data"

def _normalize_code(x):
    try:
        return str(int(float(x))).zfill(6)
    except:
        return str(x).strip().zfill(6)

def run_bottom_fishing_test():
    print("🎣 [Signalist 2.0] 전략 고도화 테스트 (Smart Entry + Multi-Horizon)")
    
    files = sorted(glob.glob(str(DATA_DIR / "processed" / "volume_anomaly_v2_*.csv")))
    
    if not files:
        print("❌ 데이터 파일이 부족합니다.")
        return

    results = []
    print(f"📂 분석 대상 파일: 총 {len(files)}개")
    print(f"   ({os.path.basename(files[0])} ~ {os.path.basename(files[-1])})")
    print("⏳ 데이터 로딩 및 시뮬레이션 중... (잠시만 기다려 주세요)")

    # 시세 데이터 미리 로드 (I/O 최적화는 생략하고 직관적으로 처리)
    # 전체 날짜의 시세 파일 경로 캐싱
    price_files_map = {}
    for f in os.listdir(DATA_DIR / "raw"):
        if f.startswith("kr_prices_") and f.endswith(".csv"):
            d_str = f.replace("kr_prices_", "").replace(".csv", "")
            price_files_map[d_str] = DATA_DIR / "raw" / f

    for fpath in files:
        try:
            filename = os.path.basename(fpath)
            date_str = filename.replace("volume_anomaly_v2_", "").replace(".csv", "")
            current_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            df = pd.read_csv(fpath)
            
            if 'code' in df.columns:
                df['code'] = df['code'].apply(_normalize_code)
            if 'size_bucket' not in df.columns:
                df['size_bucket'] = 'unknown'
            
            # 컬럼명 통일
            if 'tv_z' in df.columns: df['sigma'] = df['tv_z']
            elif 'vol_sigma' in df.columns: df['sigma'] = df['vol_sigma']
            else: continue

            # -----------------------------------------------------------
            # 🎯 [전략 수정: 은밀한 매집 (Smart Entry)]
            # -----------------------------------------------------------
            # 1. 적당한 거래량 폭발 (2~6배)
            cond_sigma = (df['sigma'] >= 2.0) & (df['sigma'] <= 6.0)
            
            # 2. [NEW] "너무 뜨겁지 않은" 양봉 (3% ~ 9% 상승)
            # 상한가 따라잡기가 아니라, 바닥에서 고개를 드는 종목 타겟팅
            if 'change_rate' in df.columns:
                chg = pd.to_numeric(df['change_rate'], errors='coerce')
                # 0% 초과(양봉) AND 12% 이하 (급등주 추격 자제)
                cond_smart = (chg > 0.0) & (chg <= 12.0)
            else:
                continue # 등락률 없으면 패스
                
            candidates = df[cond_sigma & cond_smart].copy()
            if candidates.empty: continue

            # -----------------------------------------------------------
            # 🔮 [미래 보기] 5일, 10일, 20일 뒤 수익률 추적
            # -----------------------------------------------------------
            horizons = [5, 10, 20]
            future_prices = {} # {days: {code: price}}
            
            for h in horizons:
                target_d = current_date + timedelta(days=h + 2) # 주말 보정 대략
                # 근처 날짜 찾기
                found_f = None
                for i in range(5):
                    chk_s = (target_d + timedelta(days=i)).strftime("%Y-%m-%d")
                    if chk_s in price_files_map:
                        found_f = price_files_map[chk_s]
                        break
                
                if found_f:
                    tmp = pd.read_csv(found_f)
                    tmp['code'] = tmp['code'].apply(_normalize_code)
                    future_prices[h] = tmp.set_index('code')['close'].to_dict()

            # 결과 기록
            for _, row in candidates.iterrows():
                code = row['code']
                close = float(row['close'])
                bucket = row.get('size_bucket', 'unknown')
                
                record = {
                    'date': date_str,
                    'bucket': bucket,
                    'sigma': row['sigma'],
                    'chg': row['change_rate']
                }
                
                # 각 기간별 수익률 계산
                has_future = False
                for h in horizons:
                    if h in future_prices and code in future_prices[h]:
                        f_close = float(future_prices[h][code])
                        ret = (f_close - close) / close * 100
                        record[f'ret_{h}d'] = ret
                        record[f'win_{h}d'] = 1 if ret > 0 else 0
                        has_future = True
                    else:
                        record[f'ret_{h}d'] = np.nan
                        record[f'win_{h}d'] = np.nan
                
                if has_future:
                    results.append(record)

        except Exception:
            continue

    if not results:
        print("❌ 데이터 부족.")
        return

    res_df = pd.DataFrame(results)

    print("\n" + "="*60)
    print("🧪 [전략 분석 결과] '은밀한 매집' (상승폭 0~12% 제한)")
    print(f"   분석 대상: 총 {len(res_df)} 건")
    print("="*60)

    # 1. 기간별 전체 승률
    print(f"\n📅 기간별 보유 성과 (Holding Period)")
    for h in [5, 10, 20]:
        win_col = f'win_{h}d'
        ret_col = f'ret_{h}d'
        if win_col not in res_df.columns: continue
        
        win_rate = res_df[win_col].mean() * 100
        avg_ret = res_df[ret_col].mean()
        print(f"   [D+{h}일] 승률: {win_rate:.1f}%  |  평균수익: {avg_ret:+.2f}%")

    # 2. 체급별 심층 분석 (D+20일 기준)
    target_h = 20
    print(f"\n⚖️ 체급별 성과 (D+{target_h}일 기준)")
    print("-" * 50)
    if f'ret_{target_h}d' in res_df.columns:
        summary = res_df.groupby('bucket').agg(
            count=('date', 'count'),
            win_rate=(f'win_{target_h}d', lambda x: x.mean() * 100),
            avg_return=(f'ret_{target_h}d', 'mean')
        ).sort_values('avg_return', ascending=False)
        print(summary.round(2))
    
    # 3. 결론
    print("\n💡 [젬공의 제언]")
    best_bucket = summary.index[0]
    if summary.iloc[0]['win_rate'] > 50:
        print(f"   👉 '{best_bucket.upper()}' 종목을 D+{target_h}일 들고 가는 전략이 유효합니다!")
    else:
        print("   👉 여전히 시장 평균을 이기기 어렵습니다. '시장 지수(Beta)'를 고려해야 할 때입니다.")

if __name__ == "__main__":
    run_bottom_fishing_test()