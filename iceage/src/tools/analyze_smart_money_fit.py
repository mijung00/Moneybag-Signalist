# iceage/src/tools/analyze_smart_money_fit.py
import pandas as pd
import numpy as np
import glob
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / "iceage" / "data"

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def run_smart_money_test():
    print("🏦 [Smart Money Logic] 대형주 '기관 선호 패턴' 정밀 분석")
    print("   가설: 대형주는 '폭발'보다 '안정적 추세'에서 수익이 난다.")
    
    files = sorted(glob.glob(str(DATA_DIR / "processed" / "volume_anomaly_v2_*.csv")))
    if not files: return

    dfs = []
    print(f"⏳ 데이터 로딩 중... ({len(files)}일치)")
    for f in files:
        try:
            df = pd.read_csv(f)
            d_str = os.path.basename(f).replace("volume_anomaly_v2_", "").replace(".csv", "")
            df['date'] = pd.to_datetime(d_str)
            if 'code' in df.columns: df['code'] = df['code'].astype(str).str.zfill(6)
            
            # 필수 데이터 확인
            if not {'close', 'open', 'high', 'low', 'size_bucket'}.issubset(df.columns): continue
            
            # 거래량 (tv_z 없으면 vol_sigma 사용)
            if 'tv_z' not in df.columns:
                 df['tv_z'] = df.get('vol_sigma', 0)
            
            dfs.append(df[['date', 'code', 'name', 'close', 'open', 'high', 'low', 'tv_z', 'size_bucket']])
        except: continue
        
    full_df = pd.concat(dfs).sort_values(['code', 'date']).reset_index(drop=True)
    
    print("✅ 기관형 지표(Smart Factors) 계산 중...")
    
    grouped = full_df.groupby('code')
    
    # 1. Volatility (20일간 일일 등락폭의 표준편차) - 낮을수록 좋음
    full_df['daily_ret'] = grouped['close'].pct_change()
    full_df['volatility_20'] = grouped['daily_ret'].transform(lambda x: x.rolling(20).std() * 100)
    
    # 2. RSI (14일) - 50~70 사이가 건전한 상승, 70 이상은 과열
    full_df['rsi_14'] = grouped['close'].transform(lambda x: calculate_rsi(x, 14))
    
    # 3. Volume Ratio (당일 거래량 / 20일 평균 거래량) - 1.0 근처가 좋음 (폭발 금지)
    #    (데이터에 volume 컬럼이 없어서 tv_z를 역산하거나 tv_z 자체를 활용)
    #    여기서는 tv_z가 이미 Z-score(표준편차 배수)이므로 tv_z 자체가 안정성 지표.
    #    tv_z가 0.0 근처면 평소 거래량, 3.0이면 폭발.
    
    # 4. Target (20일 후 수익률)
    full_df['close_next_20d'] = grouped['close'].transform(lambda x: x.shift(-20))
    full_df['ret_20d'] = (full_df['close_next_20d'] - full_df['close']) / full_df['close'] * 100
    
    # 대형주 필터링
    df_large = full_df[
        (full_df['size_bucket'] == 'large') & 
        (full_df['ret_20d'].notnull()) &
        (full_df['volatility_20'].notnull()) &
        (full_df['rsi_14'].notnull())
    ].copy()

    print(f"\n📊 분석 모집단: 대형주 {len(df_large):,}건")
    
    # ----------------------------------------------------
    # 구간별 승률 테스트
    # ----------------------------------------------------
    
    # 1. 변동성(Volatility) 테스트
    print("\n1️⃣ [변동성 테스트] 주가가 얌전할수록(Low Vol) 돈을 벌까?")
    print("-" * 60)
    for v_max in [1.5, 2.0, 2.5, 3.0, 5.0]:
        subset = df_large[df_large['volatility_20'] <= v_max]
        win_rate = (subset['ret_20d'] > 0).mean() * 100
        avg_ret = subset['ret_20d'].mean()
        print(f"   - 변동성 <= {v_max}% (샘플 {len(subset):5,}개) : 승률 {win_rate:.1f}% | 평균수익 {avg_ret:+.2f}%")
    
    # 2. RSI 테스트
    print("\n2️⃣ [RSI 테스트] 과열되지 않은(RSI < 70) 놈이 더 갈까?")
    print("-" * 60)
    ranges = [(30, 50), (50, 60), (60, 70), (70, 80), (80, 100)]
    for r_min, r_max in ranges:
        subset = df_large[(df_large['rsi_14'] >= r_min) & (df_large['rsi_14'] < r_max)]
        win_rate = (subset['ret_20d'] > 0).mean() * 100
        avg_ret = subset['ret_20d'].mean()
        print(f"   - RSI {r_min}~{r_max} (샘플 {len(subset):5,}개) : 승률 {win_rate:.1f}% | 평균수익 {avg_ret:+.2f}%")
        
    # 3. 거래량 폭발(TV_Z) 테스트 (역발상)
    print("\n3️⃣ [거래량 테스트] 거래량이 터지면(TV_Z > 2.0) 정말 좋을까?")
    print("-" * 60)
    ranges_z = [(-1.0, 0.5), (0.5, 1.5), (1.5, 2.5), (2.5, 10.0)]
    for z_min, z_max in ranges_z:
        subset = df_large[(df_large['tv_z'] >= z_min) & (df_large['tv_z'] < z_max)]
        win_rate = (subset['ret_20d'] > 0).mean() * 100
        avg_ret = subset['ret_20d'].mean()
        print(f"   - TV_Z {z_min:>4}~{z_max:<4} (샘플 {len(subset):5,}개) : 승률 {win_rate:.1f}% | 평균수익 {avg_ret:+.2f}%")

    # 4. [종합] '기관형' 필터 적용 시뮬레이션
    print("\n🏆 [종합 시뮬레이션] 배신자 제거 필터 적용")
    print("   조건: 변동성 <= 2.5% + RSI 50~70 + TV_Z 0.0~2.0 (폭발 금지)")
    print("-" * 60)
    
    smart_mask = (
        (df_large['volatility_20'] <= 2.5) & 
        (df_large['rsi_14'] >= 50) & (df_large['rsi_14'] <= 70) &
        (df_large['tv_z'] >= 0.0) & (df_large['tv_z'] <= 2.0)
    )
    
    smart_picks = df_large[smart_mask]
    base_win = (df_large['ret_20d'] > 0).mean() * 100
    smart_win = (smart_picks['ret_20d'] > 0).mean() * 100
    
    print(f"   - 📉 전체 대형주 평균 승률: {base_win:.1f}% (수익 {df_large['ret_20d'].mean():.2f}%)")
    print(f"   - 📈 필터 적용 후 승률  : {smart_win:.1f}% (수익 {smart_picks['ret_20d'].mean():.2f}%)")
    print(f"   - ✨ 성능 개선폭        : 승률 +{smart_win - base_win:.1f}%p")

if __name__ == "__main__":
    run_smart_money_test()