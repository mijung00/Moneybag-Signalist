# iceage/src/tools/find_panic_bottom.py
import pandas as pd
import numpy as np
import glob
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / "iceage" / "data"

def _normalize_code(x):
    try:
        return str(int(float(x))).zfill(6)
    except:
        return str(x).strip().zfill(6)

def run_panic_test():
    print("😱 [Signalist 4.0] '공포에 사라(Selling Climax)' 전략 테스트 시작...")
    print("   조건: 거래량 폭발(Sigma >= 2.5) + 주가 급락(-3% 이하)")
    
    files = sorted(glob.glob(str(DATA_DIR / "processed" / "volume_anomaly_v2_*.csv")))
    
    if not files:
        print("❌ 데이터 파일이 부족합니다.")
        return

    results = []
    
    # 시세 데이터 매핑 로드 (파일명만 캐싱)
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
            
            # 전처리
            if 'code' in df.columns:
                df['code'] = df['code'].apply(_normalize_code)
            if 'size_bucket' not in df.columns:
                df['size_bucket'] = 'unknown'
            if 'tv_z' in df.columns: df['sigma'] = df['tv_z']
            elif 'vol_sigma' in df.columns: df['sigma'] = df['vol_sigma']
            else: continue
            
            # 등락률 체크
            if 'change_rate' not in df.columns: continue
            df['chg'] = pd.to_numeric(df['change_rate'], errors='coerce')

            # -----------------------------------------------------------
            # 🎯 [전략: 패닉 바잉]
            # -----------------------------------------------------------
            # 1. 거래량은 터져야 함 (누군가 받아냄)
            cond_vol = df['sigma'] >= 2.5
            
            # 2. 가격은 떨어져야 함 (공포 심리)
            # -3% 이상 하락
            cond_panic = df['chg'] <= -3.0
            
            candidates = df[cond_vol & cond_panic].copy()
            
            if candidates.empty: continue

            # -----------------------------------------------------------
            # 🔮 [미래 보기] D+5, D+10, D+20
            # -----------------------------------------------------------
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
                name = row['name']
                close = float(row['close'])
                bucket = row.get('size_bucket', 'unknown')
                
                record = {
                    'date': date_str,
                    'code': code,
                    'bucket': bucket,
                    'sigma': row['sigma'],
                    'chg': row['chg']
                }
                
                has_data = False
                for h in horizons:
                    if h in future_prices and code in future_prices[h]:
                        f_close = float(future_prices[h][code])
                        ret = (f_close - close) / close * 100
                        record[f'ret_{h}d'] = ret
                        record[f'win_{h}d'] = 1 if ret > 0 else 0
                        has_data = True
                    else:
                        record[f'ret_{h}d'] = np.nan
                        record[f'win_{h}d'] = np.nan
                
                if has_data:
                    results.append(record)

        except Exception:
            continue

    if not results:
        print("❌ 데이터 부족.")
        return

    res_df = pd.DataFrame(results)

    print("\n" + "="*60)
    print(f"🧪 [Signalist 4.0] '패닉 바잉' 전략 결과 (총 {len(res_df)}건)")
    print("   조건: 괴리율 2.5σ 이상 + 등락률 -3% 이하 (투매 잡기)")
    print("="*60)
    
    # 1. 기간별 성과
    print(f"\n📅 기간별 보유 성과")
    for h in [5, 10, 20]:
        win_col = f'win_{h}d'
        ret_col = f'ret_{h}d'
        if win_col in res_df.columns:
            win_rate = res_df[win_col].mean() * 100
            avg_ret = res_df[ret_col].mean()
            print(f"   [D+{h}일] 승률: {win_rate:.1f}%  |  평균수익: {avg_ret:+.2f}%")

    # 2. 체급별 성과 (D+10일 기준)
    print(f"\n⚖️ 체급별 성과 (D+10일 기준)")
    print("-" * 50)
    if 'win_10d' in res_df.columns:
        summary = res_df.groupby('bucket').agg(
            count=('date', 'count'),
            win_rate=('win_10d', lambda x: x.mean() * 100),
            avg_return=('ret_10d', 'mean')
        ).sort_values('avg_return', ascending=False)
        print(summary.round(2))

if __name__ == "__main__":
    run_panic_test()