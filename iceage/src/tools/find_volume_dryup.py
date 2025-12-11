# iceage/src/tools/find_volume_dryup.py
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

def run_dryup_test():
    print("🤫 [Signalist 3.0] '폭풍 전야(Volume Dry-up)' 전략 테스트 시작...")
    print("   조건: 거래량 급감(Sigma < -1.0) + 주가 횡보")
    
    files = sorted(glob.glob(str(DATA_DIR / "processed" / "volume_anomaly_v2_*.csv")))
    
    if not files:
        print("❌ 데이터 파일이 부족합니다.")
        return

    results = []
    
    # 시세 데이터 미리 로드 (매번 로드하면 느리므로 캐싱)
    price_files_map = {}
    for f in os.listdir(DATA_DIR / "raw"):
        if f.startswith("kr_prices_") and f.endswith(".csv"):
            d_str = f.replace("kr_prices_", "").replace(".csv", "")
            price_files_map[d_str] = DATA_DIR / "raw" / f

    print(f"📂 분석 대상: 총 {len(files)}개 파일")

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
            # 🎯 [전략: 거래량 급감 (눌림목)]
            # -----------------------------------------------------------
            # 1. 거래량/거래대금이 평소보다 조용함 (Z-score < -0.5 ~ -1.0)
            cond_quiet = df['sigma'] < -0.5 
            
            # 2. 가격 변동폭이 작음 (횡보, 도지 캔들)
            # change_rate가 -3% ~ +3% 사이
            if 'change_rate' in df.columns:
                chg = pd.to_numeric(df['change_rate'], errors='coerce')
                cond_flat = (chg > -3.0) & (chg < 3.0)
            else:
                continue
                
            candidates = df[cond_quiet & cond_flat].copy()
            
            if candidates.empty: continue

            # -----------------------------------------------------------
            # 🔮 [미래 보기]
            # -----------------------------------------------------------
            # 단순히 내일 오르는지 보는 게 아니라, '모았다가 터지는' 걸 기대하므로
            # 5일, 10일, 20일 뒤를 봅니다.
            horizons = [5, 10, 20]
            future_prices = {} 
            
            for h in horizons:
                target_d = current_date + timedelta(days=h + 2)
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

            for _, row in candidates.iterrows():
                code = row['code']
                close = float(row['close'])
                bucket = row.get('size_bucket', 'unknown')
                
                record = {
                    'date': date_str,
                    'bucket': bucket,
                    'sigma': row['sigma']
                }
                
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
                
                if has_future:
                    results.append(record)

        except Exception:
            continue

    if not results:
        print("❌ 데이터 부족.")
        return

    res_df = pd.DataFrame(results)

    print("\n" + "="*60)
    print("🧪 [전략 분석] '폭풍 전야(Dry-up)' (거래급감 + 횡보)")
    print(f"   분석 대상: 총 {len(res_df)} 건")
    print("="*60)

    # 1. 기간별 전체 승률
    print(f"\n📅 기간별 보유 성과")
    for h in [5, 10, 20]:
        win_col = f'win_{h}d'
        ret_col = f'ret_{h}d'
        if win_col in res_df.columns:
            win_rate = res_df[win_col].mean() * 100
            avg_ret = res_df[ret_col].mean()
            print(f"   [D+{h}일] 승률: {win_rate:.1f}%  |  평균수익: {avg_ret:+.2f}%")

    # 2. 체급별 (D+10일 기준)
    target_h = 10
    print(f"\n⚖️ 체급별 성과 (D+{target_h}일 기준)")
    print("-" * 50)
    if f'ret_{target_h}d' in res_df.columns:
        summary = res_df.groupby('bucket').agg(
            count=('date', 'count'),
            win_rate=(f'win_{target_h}d', lambda x: x.mean() * 100),
            avg_return=(f'ret_{target_h}d', 'mean')
        ).sort_values('avg_return', ascending=False)
        print(summary.round(2))

if __name__ == "__main__":
    run_dryup_test()