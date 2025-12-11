# iceage/src/tools/simulate_final_portfolio_v4.py
import pandas as pd
import numpy as np
import glob
import os
import sys
from pathlib import Path

# 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / "iceage" / "data"

def _normalize_code(x):
    try: return str(int(float(x))).zfill(6)
    except: return str(x).strip().zfill(6)

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calc_bollinger_width(series, window=20, num_std=2):
    rolling_mean = series.rolling(window=window).mean()
    rolling_std = series.rolling(window=window).std()
    upper = rolling_mean + (rolling_std * num_std)
    lower = rolling_mean - (rolling_std * num_std)
    bw = (upper - lower) / rolling_mean
    return bw

def simulate_final_portfolio_v4():
    print("🏆 [Signalist Final 4.0] '안전제일(Safety First)' 시뮬레이션")
    print("   목표: ±25% 변동성 컷오프 적용 + 3대 전략 밸런스 유지")
    
    files = sorted(glob.glob(str(DATA_DIR / "processed" / "volume_anomaly_v2_*.csv")))
    if not files:
        print("❌ 데이터 파일이 없습니다.")
        return

    data_frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            date_str = os.path.basename(f).replace("volume_anomaly_v2_", "").replace(".csv", "")
            df['date'] = pd.to_datetime(date_str)
            
            # 필수 컬럼 체크
            if 'tv_z' not in df.columns: 
                if 'vol_sigma' in df.columns: df['tv_z'] = df['vol_sigma']
                else: continue
            
            if 'trading_value' not in df.columns:
                if 'close' in df.columns and 'volume' in df.columns:
                    df['trading_value'] = df['close'] * df['volume']
                else: continue

            if 'code' in df.columns: df['code'] = df['code'].apply(_normalize_code)
            if 'change_rate' in df.columns: df['chg'] = pd.to_numeric(df['change_rate'], errors='coerce')
            
            req = ['date', 'code', 'name', 'close', 'open', 'high', 'low', 'chg', 'tv_z', 'size_bucket', 'trading_value']
            if set(req).issubset(df.columns):
                data_frames.append(df[req])
        except: continue

    if not data_frames: return
    full_df = pd.concat(data_frames).sort_values(['code', 'date']).reset_index(drop=True)
    
    print(f"✅ 데이터 로드 완료 ({len(full_df)} rows). 지표 계산 중...")

    grouped = full_df.groupby('code')
    
    # 1. Whale Price (360일)
    def get_long_term_whale_price(sub_df):
        sub_df = sub_df.sort_values('date')
        closes = sub_df['close'].values
        tvs = sub_df['trading_value'].values
        n = len(sub_df)
        whale_prices = np.full(n, np.nan)
        window = 360
        for i in range(n):
            if i < 30: continue
            start = max(0, i - window + 1)
            end = i + 1
            window_tvs = tvs[start:end]
            if len(window_tvs) > 0:
                max_idx = np.argmax(window_tvs)
                whale_prices[i] = closes[start:end][max_idx]
        return pd.Series(whale_prices, index=sub_df.index)

    full_df['whale_price'] = grouped.apply(get_long_term_whale_price).reset_index(level=0, drop=True)

    # 2. Indicators
    full_df['rsi_14'] = grouped['close'].transform(lambda x: calculate_rsi(x, 14))
    full_df['ma20'] = grouped['close'].transform(lambda x: x.rolling(20, min_periods=15).mean())
    full_df['ma60'] = grouped['close'].transform(lambda x: x.rolling(60, min_periods=40).mean())
    full_df['disparity_20'] = full_df['close'] / full_df['ma20']
    
    full_df['is_spike'] = (full_df['tv_z'] >= 2.0).astype(int)
    full_df['spike_count_60d'] = grouped['is_spike'].transform(lambda x: x.rolling(60, min_periods=30).sum())
    full_df['bb_width'] = grouped['close'].transform(lambda x: calc_bollinger_width(x))
    full_df['whale_gap'] = full_df['close'] / full_df['whale_price']

    # Future Returns
    for h in [5, 10, 20]:
        full_df[f'close_next_{h}d'] = grouped['close'].transform(lambda x: x.shift(-h))
        full_df[f'ret_{h}d'] = (full_df[f'close_next_{h}d'] - full_df['close']) / full_df['close'] * 100
    
    # ---------------------------------------------------------
    # 전략별 필터링 (Safety First)
    # ---------------------------------------------------------
    
    # 0. Global Safety Filter: ±25% 초과 변동성 제외
    mask_safety = (full_df['chg'].abs() < 25.0)

    # 1. Panic Buying (Elite + Safety)
    #    - Vol >= 3.0
    #    - Drop <= -5.0 AND Drop > -25.0 (하한가 제외)
    mask_panic = (full_df['size_bucket'] == 'small') & \
                 (full_df['tv_z'] >= 3.0) & \
                 (full_df['chg'] <= -5.0) & \
                 mask_safety
                 
    # 2. Phoenix (Revived + Safety)
    #    - RSI < 28
    #    - Disparity < 0.88
    #    - Close >= Open
    #    - Chg > -15.0 (이미 안전장치 있음)
    mask_phoenix = (full_df['size_bucket'].isin(['large', 'mid'])) & \
                   (full_df['rsi_14'] < 28) & \
                   (full_df['disparity_20'] < 0.88) & \
                   (full_df['close'] >= full_df['open']) & \
                   (full_df['chg'] > -15.0) & \
                   mask_safety

    # 3. Goldilocks (Elite + Safety)
    #    - Chg <= 8.0 (이미 안전장치 있음)
    #    - But Safety filter applied just in case
    mask_goldilocks = (full_df['size_bucket'] == 'large') & \
                      (full_df['tv_z'] >= 0.0) & (full_df['tv_z'] <= 3.0) & \
                      (full_df['chg'] >= 0.0) & (full_df['chg'] <= 8.0) & \
                      (full_df['bb_width'] >= 0.12) & (full_df['bb_width'] <= 0.40) & \
                      (full_df['spike_count_60d'] >= 2) & (full_df['spike_count_60d'] <= 6) & \
                      (full_df['whale_gap'] >= 1.02) & (full_df['whale_gap'] <= 1.20) & \
                      (full_df['close'] > full_df['ma60']) & \
                      mask_safety

    # 통합
    full_df['strategy'] = None
    full_df.loc[mask_panic, 'strategy'] = 'Panic Buying'
    full_df.loc[mask_phoenix, 'strategy'] = 'Phoenix'
    full_df.loc[mask_goldilocks, 'strategy'] = 'Goldilocks'
    
    final_signals = full_df.dropna(subset=['strategy']).copy()
    
    print("\n" + "="*80)
    print(f"📊 [Final 4.0 Portfolio] 최종 성과 분석 (총 {len(final_signals)}건)")
    print("="*80)
    
    strategies = ['Panic Buying', 'Phoenix', 'Goldilocks']
    
    print(f"{'Strategy':<15} | {'Count':<6} | {'Win(20d)':<8} | {'Ret(20d)':<8} | {'Role'}")
    print("-" * 80)
    
    for strat in strategies:
        subset = final_signals[final_signals['strategy'] == strat]
        if subset.empty: continue
        
        valid = subset.dropna(subset=['ret_20d'])
        win_rate = (valid['ret_20d'] > 0).mean() * 100
        avg_ret = valid['ret_20d'].mean()
        
        role = ""
        if strat == 'Panic Buying': role = "High Volatility"
        elif strat == 'Phoenix': role = "Mean Reversion"
        elif strat == 'Goldilocks': role = "Trend Follow"
        
        print(f"{strat:<15} | {len(subset):<6} | {win_rate:5.1f}%   | {avg_ret:+6.2f}%  | {role}")
        
    print("-" * 80)
    
    valid_total = final_signals.dropna(subset=['ret_20d'])
    total_win = (valid_total['ret_20d'] > 0).mean() * 100
    total_ret = valid_total['ret_20d'].mean()
    
    daily_counts = final_signals.groupby('date').size()
    avg_daily = daily_counts.mean()
    
    print(f"🏆 [TOTAL] 전체 포트폴리오 요약")
    print(f"   - 총 승률 : {total_win:.1f}%")
    print(f"   - 총 수익 : {total_ret:+.2f}%")
    print(f"   - 일평균 포착 : {avg_daily:.1f}개")

if __name__ == "__main__":
    simulate_final_portfolio_v4()