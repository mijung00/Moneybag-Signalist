# iceage/src/tools/find_kings_pulse.py
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

def run_kings_pulse_test():
    print("👑 [Signalist 7.0] 대형주 전용 '왕의 맥박(King's Pulse)' 테스트")
    print("   타겟: Large Only")
    print("   조건: 60일간 5회 이상 폭발(Energy) + MA60 위(Trend) + 4% 이하 상승(Calm)")
    
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
            
            # 컬럼 매핑
            if 'tv_z' not in df.columns: 
                if 'vol_sigma' in df.columns: df['tv_z'] = df['vol_sigma']
                else: continue
            
            if 'code' in df.columns:
                 df['code'] = df['code'].apply(_normalize_code)
            
            if 'change_rate' not in df.columns: continue
            df['chg'] = pd.to_numeric(df['change_rate'], errors='coerce')

            cols = ['date', 'code', 'name', 'close', 'chg', 'tv_z', 'size_bucket']
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
    # (1) Energy: Spike 빈도 (2.0 이상)
    full_df['is_spike'] = (full_df['tv_z'] >= 2.0).astype(int)
    
    grouped = full_df.groupby('code')
    full_df['spike_count_60d'] = grouped['is_spike'].transform(lambda x: x.rolling(60, min_periods=30).sum())
    
    # (2) Trend: 60일 이동평균선 (MA60)
    full_df['ma60'] = grouped['close'].transform(lambda x: x.rolling(60, min_periods=40).mean())
    
    # (3) Future: D+5, D+10, D+20
    for h in [5, 10, 20]:
        full_df[f'close_next_{h}d'] = grouped['close'].transform(lambda x: x.shift(-h))
    
    # 3. 필터링 (King's Pulse)
    # A. 대형주만
    mask_size = full_df['size_bucket'] == 'large'
    
    # B. 에너지: 5회 이상 (확 쪼임)
    mask_energy = full_df['spike_count_60d'] >= 5
    
    # C. 추세: 현재가가 60일선 위에 있음 (살아있는 추세)
    mask_trend = full_df['close'] > full_df['ma60']
    
    # D. 트리거: 
    # - 괴리율 1.5 이상 (돈 들어옴)
    # - 등락률 +0.5% ~ +4.0% (오버슈팅 자제)
    mask_trigger = (full_df['tv_z'] >= 1.5) & (full_df['chg'] >= 0.5) & (full_df['chg'] <= 4.0)
    
    signals = full_df[mask_size & mask_energy & mask_trend & mask_trigger].copy()
    
    if signals.empty:
        print("❌ 조건에 맞는 시그널이 없습니다.")
        return

    print("\n" + "="*60)
    print(f"🧪 대형주 필승 전략 'King's Pulse' 결과 (총 {len(signals)}건)")
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
    if 'close_next_20d' in signals.columns:
        signals['ret_20d'] = (signals['close_next_20d'] - signals['close']) / signals['close'] * 100
        top5 = signals.dropna(subset=['ret_20d']).sort_values('ret_20d', ascending=False).head(5)
        for _, r in top5.iterrows():
            print(f"   - {r['date'].date()} {r['name']} (Energy: {r['spike_count_60d']:.0f}회) -> {r['ret_20d']:.1f}%")

if __name__ == "__main__":
    run_kings_pulse_test()