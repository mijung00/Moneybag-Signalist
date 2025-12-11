# iceage/src/tools/find_fallen_angels.py
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

def run_fallen_angel_test():
    print("👼 [Signalist 6.5] '추락하는 천사(Fallen Angel)' 전략 테스트")
    print("   타겟: Large & Mid (중대형주)")
    print("   조건: 60일간 활발(Active) + 역배열(Downtrend) + 당일 하락(Drop)")
    
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
            
            # 컬럼 매핑 및 선택
            if 'tv_z' not in df.columns: 
                if 'vol_sigma' in df.columns: df['tv_z'] = df['vol_sigma']
                else: continue
            
            # 등락률 컬럼 확인
            if 'change_rate' not in df.columns: continue
            
            if 'code' in df.columns:
                 df['code'] = df['code'].apply(_normalize_code)
            
            cols = ['date', 'code', 'name', 'close', 'change_rate', 'tv_z', 'size_bucket']
            available_cols = [c for c in cols if c in df.columns]
            data_frames.append(df[available_cols])
            
        except Exception:
            continue
            
    if not data_frames:
        print("❌ 로드된 데이터가 없습니다.")
        return

    full_df = pd.concat(data_frames)
    full_df = full_df.sort_values(['code', 'date']).reset_index(drop=True)
    
    print(f"✅ 통합 완료: {len(full_df)}행. 지표 계산 시작...")

    # 2. 지표 계산
    # (1) Energy: 60일간 스파이크 빈도
    full_df['is_spike'] = (full_df['tv_z'] >= 2.0).astype(int)
    
    grouped = full_df.groupby('code')
    full_df['spike_count_60d'] = grouped['is_spike'].transform(lambda x: x.rolling(60, min_periods=30).sum())
    
    # (2) Trend: 60일 전 가격 비교
    full_df['price_60d_ago'] = grouped['close'].transform(lambda x: x.shift(60))
    
    # (3) Future Return
    full_df['close_next_5d'] = grouped['close'].transform(lambda x: x.shift(-5))
    
    # 3. 필터링 (Fallen Angel 조건)
    # A. 체급: Large/Mid
    mask_size = full_df['size_bucket'].isin(['large', 'mid'])
    
    # B. 에너지: 여전히 '끼'는 있어야 함 (거래량 죽은 건 제외)
    mask_energy = full_df['spike_count_60d'] >= 3
    
    # C. 추세: [반대] 역배열 (현재가 < 60일 전 가격) -> 하락 추세
    mask_downtrend = full_df['close'] < full_df['price_60d_ago']
    
    # D. 트리거: [반대] 하락 마감 (-2% 이하) + 거래량 존재
    # 너무 심한 투매(-15% 등)는 제외하고 적당한 하락 (-2% ~ -10%)
    mask_drop = (full_df['change_rate'] <= -2.0) & (full_df['change_rate'] >= -10.0)
    mask_vol = full_df['tv_z'] >= 1.0 # 거래량은 평소보다 조금 더 실림 (매도세 출현)

    signals = full_df[mask_size & mask_energy & mask_downtrend & mask_drop & mask_vol].copy()
    
    if signals.empty:
        print("❌ 조건에 맞는 시그널이 없습니다.")
        return

    # 4. 성과 분석
    signals['ret_5d'] = (signals['close_next_5d'] - signals['close']) / signals['close'] * 100
    signals.dropna(subset=['ret_5d'], inplace=True)
    
    signals['win'] = (signals['ret_5d'] > 0).astype(int)

    print("\n" + "="*60)
    print(f"🧪 [Signalist 6.5] 중/대형주 '역발상(Fallen Angel)' 결과 (총 {len(signals)}건)")
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
    run_fallen_angel_test()