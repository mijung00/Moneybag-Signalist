# iceage/src/tools/find_active_momentum.py
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

def run_active_momentum_test():
    print("🐉 [Signalist 6.0] '잠룡 승천(Active Momentum)' 전략 테스트")
    print("   타겟: Large & Mid (중대형주)")
    print("   조건: 60일간 괴리율 2σ+ 발생 빈도 3회 이상 + 우상향 추세")
    
    # 1. 파일 로드 (전체 기간)
    # 시계열 분석을 위해 전체 데이터를 메모리에 로드해서 종목별로 정렬해야 합니다.
    files = sorted(glob.glob(str(DATA_DIR / "processed" / "volume_anomaly_v2_*.csv")))
    
    if not files:
        print("❌ 데이터 파일이 없습니다.")
        return

    print(f"⏳ 대규모 데이터 로딩 및 병합 중... ({len(files)}일치)")
    
    data_frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            # 날짜 추출
            date_str = os.path.basename(f).replace("volume_anomaly_v2_", "").replace(".csv", "")
            df['date'] = pd.to_datetime(date_str)
            
            # 필요한 컬럼만 추출 (메모리 절약)
            cols = ['date', 'code', 'name', 'close', 'open', 'vol_sigma', 'tv_z', 'size_bucket']
            # 파일 버전에 따라 컬럼명이 다를 수 있음 처리
            if 'tv_z' not in df.columns: 
                if 'vol_sigma' in df.columns: df['tv_z'] = df['vol_sigma']
                else: continue
            
            if 'code' in df.columns:
                 df['code'] = df['code'].apply(_normalize_code)
            
            # 필요한 컬럼만 선택
            available_cols = [c for c in cols if c in df.columns]
            data_frames.append(df[available_cols])
            
        except Exception:
            continue
            
    if not data_frames:
        print("❌ 로드된 데이터가 없습니다.")
        return

    # 전체 통합
    full_df = pd.concat(data_frames)
    full_df = full_df.sort_values(['code', 'date']).reset_index(drop=True)
    
    print(f"✅ 통합 완료: {len(full_df)}행. 지표 계산 시작...")

    # 2. 롤링 지표 계산 (종목별)
    # (1) Energy: 최근 60일간 sigma > 2.0 인 날의 횟수
    # (2) Momentum: 60일 전 대비 수익률
    
    full_df['is_spike'] = (full_df['tv_z'] >= 2.0).astype(int)
    
    # GroupBy Rolling은 느릴 수 있으므로, transform 사용하거나 loop 최소화
    # 여기서는 직관적인 group apply 사용 (속도 개선 필요시 변경 가능)
    grouped = full_df.groupby('code')
    
    # 60일간 스파이크 횟수 합계
    full_df['spike_count_60d'] = grouped['is_spike'].transform(lambda x: x.rolling(60, min_periods=30).sum())
    
    # 60일 전 가격 (Shift)
    full_df['price_60d_ago'] = grouped['close'].transform(lambda x: x.shift(60))
    
    # 5일 후 가격 (수익률 검증용)
    full_df['close_next_5d'] = grouped['close'].transform(lambda x: x.shift(-5))
    
    # 3. 전략 필터링
    # 조건 A: 대형주/중형주 만 (소형주 제외)
    mask_size = full_df['size_bucket'].isin(['large', 'mid'])
    
    # 조건 B: 에너지 (60일 내 2배수 폭발이 3번 이상 있었던 놈 = 끼 있는 놈)
    mask_energy = full_df['spike_count_60d'] >= 3
    
    # 조건 C: 추세 (60일 전보다 현재가가 높아야 함 = 우상향)
    mask_trend = full_df['close'] > full_df['price_60d_ago']
    
    # 조건 D: 오늘 매수 신호 (양봉 + 거래량 살짝 증가)
    # 너무 터지면(10배) 고점일 수 있으니 적당히(1.0 ~ 5.0)
    mask_trigger_vol = (full_df['tv_z'] >= 1.0) & (full_df['tv_z'] <= 5.0)
    mask_trigger_candle = full_df['close'] > full_df['open'] # 양봉
    
    # 최종 시그널
    signals = full_df[mask_size & mask_energy & mask_trend & mask_trigger_vol & mask_trigger_candle].copy()
    
    # 4. 성과 분석
    if signals.empty:
        print("❌ 조건에 맞는 시그널이 없습니다.")
        return

    # 수익률 계산 (D+5)
    signals['ret_5d'] = (signals['close_next_5d'] - signals['close']) / signals['close'] * 100
    signals.dropna(subset=['ret_5d'], inplace=True) # 미래 데이터 없는 최근일 제외
    
    signals['win'] = (signals['ret_5d'] > 0).astype(int)

    print("\n" + "="*60)
    print(f"🧪 [Signalist 6.0] 중/대형주 '맥박 매매' 결과 (총 {len(signals)}건)")
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
    
    print(f"\n🏆 베스트 케이스")
    print(signals.sort_values('ret_5d', ascending=False).head(5)[['date', 'name', 'size_bucket', 'ret_5d']])

if __name__ == "__main__":
    run_active_momentum_test()