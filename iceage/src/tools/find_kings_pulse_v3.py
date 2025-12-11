# iceage/src/tools/find_kings_pulse_v3.py
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

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def run_kings_pulse_v3_test():
    print("👑 [Signalist 9.5] '침묵의 거인(Silent Titan)' 전략 테스트")
    print("   타겟: Large Cap Only")
    print("   조건: 변동성 2.5%↓ + RSI 60↑ + 거래량 1.5σ↓ (조용한 상승)")
    
    files = sorted(glob.glob(str(DATA_DIR / "processed" / "volume_anomaly_v2_*.csv")))
    if not files:
        print("❌ 데이터 파일이 없습니다.")
        return

    print(f"⏳ 데이터 로딩 및 병합 중... ({len(files)}일치)")
    
    data_frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            date_str = os.path.basename(f).replace("volume_anomaly_v2_", "").replace(".csv", "")
            df['date'] = pd.to_datetime(date_str)
            
            if 'tv_z' not in df.columns: 
                if 'vol_sigma' in df.columns: df['tv_z'] = df['vol_sigma']
                else: df['tv_z'] = 0.0
            
            if 'code' in df.columns:
                 df['code'] = df['code'].apply(_normalize_code)
            
            # 수익률 계산용
            if 'close' not in df.columns: continue

            # 필수 컬럼만 가져오기
            cols = ['date', 'code', 'name', 'close', 'open', 'high', 'low', 'tv_z', 'size_bucket']
            # change_rate가 있으면 가져오고 없으면 계산
            if 'change_rate' in df.columns:
                df['chg'] = pd.to_numeric(df['change_rate'], errors='coerce')
                cols.append('chg')
            
            data_frames.append(df[cols])
            
        except Exception:
            continue

    if not data_frames:
        print("❌ 로드된 데이터가 없습니다.")
        return

    full_df = pd.concat(data_frames)
    full_df = full_df.sort_values(['code', 'date']).reset_index(drop=True)
    
    print(f"✅ 통합 완료. 핵심 지표(RSI, Volatility) 계산 시작...")

    # -----------------------------------------------------------
    # 2. 지표 계산 (Vectorized Operations for Speed)
    # -----------------------------------------------------------
    grouped = full_df.groupby('code')
    
    # (1) Volatility (20일 변동성)
    full_df['daily_ret'] = grouped['close'].pct_change()
    full_df['volatility_20'] = grouped['daily_ret'].transform(lambda x: x.rolling(20).std() * 100)
    
    # (2) RSI (14일)
    full_df['rsi_14'] = grouped['close'].transform(lambda x: calculate_rsi(x, 14))
    
    # (3) Future Returns (성과 분석용)
    for h in [5, 10, 20]:
        full_df[f'close_next_{h}d'] = grouped['close'].transform(lambda x: x.shift(-h))

    # -----------------------------------------------------------
    # 3. 필터링 (Silent Titan Logic)
    # -----------------------------------------------------------
    # A. 대형주
    mask_size = full_df['size_bucket'] == 'large'
    
    # B. 변동성: 2.5% 이하 (기관 수급의 특징: 조용함)
    mask_vol = full_df['volatility_20'] <= 2.5
    
    # C. 추세: RSI 60 이상 (확실한 상승 모멘텀)
    #    * RSI 50~60 구간은 수익률이 마이너스였으므로 제외
    mask_rsi = full_df['rsi_14'] >= 60
    
    # D. 거래량: 스파이크 금지 (0.0 ~ 1.5)
    #    * 거래량이 터지면(2.0 이상) 단기 고점 징후
    mask_vol_control = (full_df['tv_z'] >= 0.0) & (full_df['tv_z'] <= 1.5)
    
    # [Optional] 캔들 조건: 윗꼬리가 너무 길면 안됨 (Shadow Ratio < 2%)
    full_df['upper_shadow'] = full_df['high'] - full_df[['close', 'open']].max(axis=1)
    full_df['shadow_ratio'] = full_df['upper_shadow'] / full_df['close'] * 100
    mask_candle = full_df['shadow_ratio'] < 2.0

    signals = full_df[mask_size & mask_vol & mask_rsi & mask_vol_control & mask_candle].copy()
    
    if signals.empty:
        print("❌ 조건에 맞는 시그널이 없습니다.")
        return

    print("\n" + "="*60)
    print(f"🧪 [Silent Titan] '침묵의 거인' 결과 (총 {len(signals)}건)")
    print("="*60)

    # 성과 분석
    for h in [5, 10, 20]:
        target_col = f'close_next_{h}d'
        valid_signals = signals.dropna(subset=[target_col])
        if valid_signals.empty: continue
        
        # SettingWithCopyWarning 방지를 위해 명시적 복사
        valid_signals = valid_signals.copy()
        valid_signals[f'ret_{h}d'] = (valid_signals[target_col] - valid_signals['close']) / valid_signals['close'] * 100
        
        win_rate = (valid_signals[f'ret_{h}d'] > 0).mean() * 100
        avg_ret = valid_signals[f'ret_{h}d'].mean()
        
        print(f"\n📅 [D+{h}일] 보유 성과 (샘플 {len(valid_signals)}개)")
        print(f"   - 승률: {win_rate:.1f}%")
        print(f"   - 평균 수익: {avg_ret:+.2f}%")

    print(f"\n🏆 최근 시그널 (Top 5)")
    recent = signals.sort_values('date', ascending=False).head(5)
    for _, r in recent.iterrows():
        print(f"   - {r['date'].date()} {r['name']} (RSI: {r['rsi_14']:.1f}, Vol: {r['volatility_20']:.1f}%)")

if __name__ == "__main__":
    run_kings_pulse_v3_test()