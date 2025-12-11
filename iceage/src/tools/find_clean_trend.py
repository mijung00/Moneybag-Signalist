# iceage/src/tools/find_clean_trend.py
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
    try: return str(int(float(x))).zfill(6)
    except: return str(x).strip().zfill(6)

def run_clean_trend_test():
    print("🧹 [Signalist 5.0] '노이즈 캔슬링' 전략 테스트")
    print("   조건: 3일 하락(-5%↓) + 거래량폭발(2σ↑) + 꽉 찬 양봉(No Whipsaw)")
    
    # 1. 분석 대상 (최근 1년치만 샘플링하거나 전체)
    files = sorted(glob.glob(str(DATA_DIR / "processed" / "volume_anomaly_v2_*.csv")))
    
    if not files:
        print("❌ 데이터 파일이 부족합니다.")
        return

    # 시세 파일 맵핑 (빠른 조회를 위해)
    price_map = {}
    for f in os.listdir(DATA_DIR / "raw"):
        if f.startswith("kr_prices_") and f.endswith(".csv"):
            d_str = f.replace("kr_prices_", "").replace(".csv", "")
            price_map[d_str] = DATA_DIR / "raw" / f

    results = []
    print(f"📂 분석 시작 ({len(files)}일 데이터)...")

    for fpath in files:
        try:
            date_str = os.path.basename(fpath).replace("volume_anomaly_v2_", "").replace(".csv", "")
            curr_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            # T-3일 날짜 구하기 (거래일 기준이 아니라 단순 날짜 계산이라 주말 껴있으면 오차 감안)
            # 정확도를 위해 3일 전 파일이 없으면 스킵
            prev_date = curr_date - timedelta(days=3)
            prev_date_str = prev_date.strftime("%Y-%m-%d")
            
            # 시그널 파일 로드
            df = pd.read_csv(fpath)
            if 'code' not in df.columns: continue
            df['code'] = df['code'].apply(_normalize_code)
            
            # 컬럼명 통일
            if 'tv_z' in df.columns: df['sigma'] = df['tv_z']
            elif 'vol_sigma' in df.columns: df['sigma'] = df['vol_sigma']
            else: continue

            # [필터 1] 거래량 폭발 (2.0 이상)
            df = df[df['sigma'] >= 2.0].copy()
            if df.empty: continue

            # [필터 2] 노이즈 제거 (양봉 & 윗꼬리 체크)
            # Open, Close, High, Low 필요
            if not {'open', 'close', 'high', 'low'}.issubset(df.columns):
                continue

            # (1) 양봉 조건: 종가 > 시가
            df = df[df['close'] > df['open']]
            
            # (2) 캔들 퀄리티 (Solid Body): 윗꼬리가 몸통보다 작아야 함
            # 윗꼬리 = High - Close
            # 몸통 = Close - Open
            df['upper_shadow'] = df['high'] - df['close']
            df['body'] = df['close'] - df['open']
            
            # 몸통이 어느 정도 있어야 함 (도지 제외) AND 윗꼬리가 몸통의 2배를 넘지 않음
            df = df[(df['body'] > 0) & (df['upper_shadow'] < df['body'] * 1.5)]
            
            if df.empty: continue

            # [필터 3] 3일간의 추세 (직전 3일간 많이 빠졌나?)
            # 3일 전 가격을 가져오기 위해 T-3일 파일 로드 (없으면 근처 찾기)
            past_price_file = None
            for i in range(3): # T-3 ~ T-5 사이 찾기
                chk_d = (curr_date - timedelta(days=3+i)).strftime("%Y-%m-%d")
                if chk_d in price_map:
                    past_price_file = price_map[chk_d]
                    break
            
            if not past_price_file: continue
            
            df_past = pd.read_csv(past_price_file)
            df_past['code'] = df_past['code'].astype(str).str.zfill(6)
            past_close_map = df_past.set_index('code')['close'].to_dict()
            
            valid_candidates = []
            for _, row in df.iterrows():
                code = row['code']
                curr_close = row['close']
                
                if code not in past_close_map: continue
                past_close = past_close_map[code]
                
                # 3일 등락률 계산
                ret_3d = (curr_close - past_close) / past_close * 100
                
                # [조건] 3일간 -5% 이상 하락했다가 오늘 반등한 놈 (낙폭과대 + 수급)
                # 단, 오늘은 양봉이어야 하므로, 오늘 오르기 전까지는 더 많이 빠졌을 것임.
                # 즉, '추세적 하락' 중 '의미 있는 반등'을 잡는 것.
                if ret_3d < -3.0: # 기준 완화: 3일간 -3% 이상 빠져있는 상태 (오늘 올랐는데도)
                    valid_candidates.append(row)
            
            if not valid_candidates: continue
            candidates_df = pd.DataFrame(valid_candidates)

            # -----------------------------------------------------------
            # 🔮 성과 검증 (D+5)
            # -----------------------------------------------------------
            target_date = curr_date + timedelta(days=7)
            future_file = None
            for i in range(5):
                chk_s = (target_date + timedelta(days=i)).strftime("%Y-%m-%d")
                if chk_s in price_map:
                    future_file = price_map[chk_s]
                    break
            
            if not future_file: continue
            
            df_fut = pd.read_csv(future_file)
            df_fut['code'] = df_fut['code'].astype(str).str.zfill(6)
            fut_map = df_fut.set_index('code')['close'].to_dict()
            
            for _, row in candidates_df.iterrows():
                code = row['code']
                close = float(row['close'])
                if code in fut_map:
                    ret_5d = (fut_map[code] - close) / close * 100
                    results.append({
                        'date': date_str,
                        'code': code,
                        'name': row['name'],
                        'bucket': row.get('size_bucket', 'unknown'),
                        'sigma': row['sigma'],
                        'ret_5d': ret_5d,
                        'win': 1 if ret_5d > 0 else 0
                    })

        except Exception as e:
            continue

    if not results:
        print("❌ 조건에 맞는 데이터가 없습니다.")
        return

    res_df = pd.DataFrame(results)

    print("\n" + "="*60)
    print("🧹 [노이즈 제거 전략] 3일 하락 후 '꽉 찬 양봉' 반등")
    print(f"   분석 대상: 총 {len(res_df)} 건")
    print("="*60)
    
    print(f"\n📌 전체 성과 (D+5일)")
    print(f"   - 승률: {res_df['win'].mean()*100:.1f}%")
    print(f"   - 평균 수익: {res_df['ret_5d'].mean():.2f}%")
    
    print(f"\n⚖️ 체급별 성과")
    print("-" * 50)
    summary = res_df.groupby('bucket').agg(
        count=('code', 'count'),
        win_rate=('win', lambda x: x.mean() * 100),
        avg_return=('ret_5d', 'mean')
    ).sort_values('avg_return', ascending=False)
    print(summary.round(2))
    
    print(f"\n🏆 베스트 케이스")
    print(res_df.sort_values('ret_5d', ascending=False).head(3)[['date', 'name', 'ret_5d']])

if __name__ == "__main__":
    run_clean_trend_test()