# iceage/src/tools/find_hyper_active.py
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

def run_hyper_active_test():
    print("🔥 [Signalist 8.0] '광기 포착(Hyper-Active)' 전략 테스트")
    print("   컨셉: 추세 무시. 오직 '끼(Energy)'만 본다.")
    print("   조건: 60일간 괴리율 2σ+ 발생 빈도 10회 이상 + 오늘 양봉")
    
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
            
            # 등락률 확인 (양봉 체크용)
            if 'change_rate' not in df.columns: continue
            df['chg'] = pd.to_numeric(df['change_rate'], errors='coerce')

            # 필요한 컬럼만 (메모리 최적화)
            cols = ['date', 'code', 'name', 'close', 'chg', 'tv_z', 'size_bucket']
            available_cols = [c for c in cols if c in df.columns]
            data_frames.append(df[available_cols])
            
        except Exception:
            continue

    if not data_frames:
        print("❌ 로드된 데이터가 없습니다.")
        return

    full_df = pd.concat(data_frames)
    full_df = full_df.sort_values(['code', 'date']).reset_index(drop=True)
    
    print(f"✅ 통합 완료: {len(full_df)}행. '끼' 측정 시작...")

    # 2. '끼' 지표 계산 (Rolling Spike Count)
    # 2.0 sigma 이상 터진 날을 1로 표시
    full_df['is_spike'] = (full_df['tv_z'] >= 2.0).astype(int)
    
    grouped = full_df.groupby('code')
    
    # 최근 60일간 spike 횟수 합계
    full_df['spike_count_60d'] = grouped['is_spike'].transform(lambda x: x.rolling(60, min_periods=30).sum())
    
    # 5일 후 가격 (수익률 확인용)
    full_df['close_next_5d'] = grouped['close'].transform(lambda x: x.shift(-5))
    
    # 3. 필터링 (Hyper Active 조건)
    # A. 끼: 60일간 10회 이상 폭발 (주군의 명령대로 '확 쪼임')
    mask_energy = full_df['spike_count_60d'] >= 10
    
    # B. 트리거: 오늘도 폭발 (2.0 이상) + 양봉 (0% 초과)
    mask_trigger = (full_df['tv_z'] >= 2.0) & (full_df['chg'] > 0)
    
    signals = full_df[mask_energy & mask_trigger].copy()
    
    if signals.empty:
        print("❌ 조건에 맞는 시그널이 없습니다. (조건이 너무 빡빡할 수 있음)")
        return

    # 4. 성과 분석
    signals['ret_5d'] = (signals['close_next_5d'] - signals['close']) / signals['close'] * 100
    signals.dropna(subset=['ret_5d'], inplace=True)
    
    signals['win'] = (signals['ret_5d'] > 0).astype(int)

    print("\n" + "="*60)
    print(f"🧪 [Signalist 8.0] '광기 포착' 결과 (총 {len(signals)}건)")
    print("   (최근 60일 중 10일 이상 거래량 폭발한 종목)")
    print("="*60)
    
    print(f"\n📌 전체 성과 (D+5일)")
    print(f"   - 승률: {signals['win'].mean()*100:.1f}%")
    print(f"   - 평균 수익: {signals['ret_5d'].mean():.2f}%")
    
    print(f"\n⚖️ 체급별 성과")
    print("-" * 50)
    summary = signals.groupby('size_bucket').agg(
        count=('date', 'count'),
        win_rate=('win', lambda x: x.mean() * 100),
        avg_return=('ret_5d', 'mean')
    ).sort_values('avg_return', ascending=False)
    print(summary.round(2))

    # 추가 분석: 빈도가 높을수록 수익률이 좋은가? (10~15회 vs 15회 이상)
    signals['freq_group'] = pd.cut(signals['spike_count_60d'], bins=[10, 15, 20, 60], labels=['10-15회', '15-20회', '20회+'])
    print(f"\n🔥 폭발 빈도별 성과 (많이 터질수록 좋은가?)")
    print(signals.groupby('freq_group', observed=True)[['win', 'ret_5d']].mean().round(2))
    
    print(f"\n🏆 베스트 케이스")
    print(signals.sort_values('ret_5d', ascending=False).head(5)[['date', 'name', 'spike_count_60d', 'ret_5d']])

if __name__ == "__main__":
    run_hyper_active_test()