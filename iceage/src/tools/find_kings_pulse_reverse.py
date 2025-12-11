# iceage/src/tools/find_kings_pulse_reverse.py
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

def run_kings_pulse_reverse_test():
    print("🔄 [Signalist 7.5-R] '왕의 맥박 (Reverse Shadow)' 테스트")
    print("   조건: 윗꼬리가 몸통의 50% 이상인 '역망치형' 캔들 선호")
    
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
                else: continue
            
            if 'code' in df.columns:
                 df['code'] = df['code'].apply(_normalize_code)
            
            if 'change_rate' not in df.columns: continue
            df['chg'] = pd.to_numeric(df['change_rate'], errors='coerce')

            if not {'high', 'low', 'open', 'close'}.issubset(df.columns):
                continue

            cols = ['date', 'code', 'name', 'close', 'open', 'high', 'low', 'chg', 'tv_z', 'size_bucket']
            data_frames.append(df[cols])
            
        except Exception:
            continue

    if not data_frames:
        print("❌ 로드된 데이터가 없습니다.")
        return

    full_df = pd.concat(data_frames)
    full_df = full_df.sort_values(['code', 'date']).reset_index(drop=True)
    
    print(f"✅ 통합 완료. 지표 계산 시작...")

    # 2. 지표 계산
    full_df['is_spike'] = (full_df['tv_z'] >= 2.0).astype(int)
    grouped = full_df.groupby('code')
    full_df['spike_count_60d'] = grouped['is_spike'].transform(lambda x: x.rolling(60, min_periods=30).sum())
    full_df['ma60'] = grouped['close'].transform(lambda x: x.rolling(60, min_periods=40).mean())
    
    for h in [5, 10, 20]:
        full_df[f'close_next_{h}d'] = grouped['close'].transform(lambda x: x.shift(-h))
    
    # 3. 필터링 (Reverse Shadow)
    mask_size = full_df['size_bucket'] == 'large'
    mask_energy = full_df['spike_count_60d'] >= 5
    mask_trend = full_df['close'] > full_df['ma60']
    
    # Trigger: 기본 조건은 유지하되, 등락률 상한을 조금 틉니다 (윗꼬리 달리려면 고가는 높았을 테니)
    # 종가는 0.5% ~ 5.0% 사이 양봉
    mask_trigger = (full_df['tv_z'] >= 1.5) & (full_df['chg'] >= 0.5) & (full_df['chg'] <= 5.0)
    
    # ★ [역발상] 윗꼬리가 몸통의 50%보다 커야 함 (Shadow > Body * 0.5)
    full_df['body'] = full_df['close'] - full_df['open']
    full_df['upper_shadow'] = full_df['high'] - full_df['close']
    
    # 몸통이 양수이면서, 윗꼬리가 몸통 절반 이상 (매물 소화 캔들)
    mask_candle_reverse = (full_df['body'] > 0) & (full_df['upper_shadow'] > full_df['body'] * 0.5)
    
    signals = full_df[mask_size & mask_energy & mask_trend & mask_trigger & mask_candle_reverse].copy()
    
    if signals.empty:
        print("❌ 조건에 맞는 시그널이 없습니다.")
        return

    print("\n" + "="*60)
    print(f"🧪 [Signalist 7.5-R] '윗꼬리 역발상' 결과 (총 {len(signals)}건)")
    print("="*60)

    # 성과 분석
    for h in [5, 10, 20]:
        target_col = f'close_next_{h}d'
        valid_signals = signals.dropna(subset=[target_col])
        if valid_signals.empty: continue
        
        valid_signals[f'ret_{h}d'] = (valid_signals[target_col] - valid_signals['close']) / valid_signals['close'] * 100
        win_rate = (valid_signals[f'ret_{h}d'] > 0).mean() * 100
        avg_ret = valid_signals[f'ret_{h}d'].mean()
        
        print(f"\n📅 [D+{h}일] 보유 성과 (샘플 {len(valid_signals)}개)")
        print(f"   - 승률: {win_rate:.1f}%")
        print(f"   - 평균 수익: {avg_ret:+.2f}%")

    print(f"\n🏆 베스트 케이스 (D+20일 기준)")
    if 'ret_20d' in valid_signals.columns:
        top5 = valid_signals.sort_values('ret_20d', ascending=False).head(5)
        for _, r in top5.iterrows():
            print(f"   - {r['date'].date()} {r['name']} -> +{r['ret_20d']:.1f}% (Body: {r['body']:.0f}, Wick: {r['upper_shadow']:.0f})")

if __name__ == "__main__":
    run_kings_pulse_reverse_test()