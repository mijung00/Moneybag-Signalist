# iceage/src/tools/analyze_winning_factors.py
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

def run_analysis():
    print("🕵️ [Reverse Engineering] 대형주 승리 패턴 분석 시작")
    print("   목표: D+20일 수익률 10% 이상 기록한 '대형주'들의 공통점 찾기")
    
    files = sorted(glob.glob(str(DATA_DIR / "processed" / "volume_anomaly_v2_*.csv")))
    if not files:
        print("❌ 데이터 파일이 없습니다.")
        return

    print(f"⏳ 데이터 로딩 중... ({len(files)}일치)")
    
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            date_str = os.path.basename(f).replace("volume_anomaly_v2_", "").replace(".csv", "")
            df['date'] = pd.to_datetime(date_str)
            
            # 컬럼 통일
            if 'tv_z' not in df.columns and 'vol_sigma' in df.columns:
                df['tv_z'] = df['vol_sigma']
            
            if 'code' in df.columns:
                 df['code'] = df['code'].apply(_normalize_code)
            
            if 'change_rate' in df.columns:
                df['chg'] = pd.to_numeric(df['change_rate'], errors='coerce')
            
            # 필수 컬럼 체크
            required = {'close', 'open', 'high', 'low', 'tv_z', 'size_bucket', 'chg'}
            if not required.issubset(df.columns):
                continue
                
            dfs.append(df[['date', 'code', 'name', 'close', 'open', 'high', 'low', 'chg', 'tv_z', 'size_bucket']])
        except:
            continue
            
    if not dfs: return

    full_df = pd.concat(dfs).sort_values(['code', 'date']).reset_index(drop=True)
    
    # ---------------------------------------------------------
    # 1. 지표 계산 (우리가 의심하는 범인들)
    # ---------------------------------------------------------
    print("✅ 지표 계산 중...")
    
    # (1) 거래량 강도
    full_df['is_spike'] = (full_df['tv_z'] >= 2.0).astype(int)
    
    grouped = full_df.groupby('code')
    
    # (2) 에너지 누적 (60일간 스파이크 횟수)
    full_df['spike_cnt'] = grouped['is_spike'].transform(lambda x: x.rolling(60, min_periods=30).sum())
    
    # (3) 추세 괴리율 (현재가 / 60일 이평선)
    full_df['ma60'] = grouped['close'].transform(lambda x: x.rolling(60, min_periods=40).mean())
    full_df['dist_ma60'] = (full_df['close'] - full_df['ma60']) / full_df['ma60'] * 100
    
    # (4) 캔들 모양 (윗꼬리 비율)
    full_df['body'] = (full_df['close'] - full_df['open']).abs()
    full_df['upper_shadow'] = full_df['high'] - full_df[['close', 'open']].max(axis=1)
    full_df['shadow_ratio'] = full_df['upper_shadow'] / full_df['close'] * 100  # 주가 대비 윗꼬리 길이 %
    
    # (5) 미래 수익률 (Target)
    full_df['close_next_20d'] = grouped['close'].transform(lambda x: x.shift(-20))
    full_df['ret_20d'] = (full_df['close_next_20d'] - full_df['close']) / full_df['close'] * 100
    
    # ---------------------------------------------------------
    # 2. 승자 vs 패자 그룹 분리 (Large Only)
    # ---------------------------------------------------------
    target_df = full_df[
        (full_df['size_bucket'] == 'large') & 
        (full_df['close_next_20d'].notnull())
    ].copy()
    
    # 승자: 20일 뒤 10% 이상 상승
    winners = target_df[target_df['ret_20d'] >= 10.0]
    
    # 패자: 20일 뒤 -5% 이하 하락 (손실 그룹)
    losers = target_df[target_df['ret_20d'] <= -5.0]
    
    # 일반: 나머지
    others = target_df[(target_df['ret_20d'] > -5.0) & (target_df['ret_20d'] < 10.0)]

    print(f"\n📊 분석 대상: 대형주 총 {len(target_df):,}건")
    print(f"   - 🏆 승자 그룹 (수익 >= 10%): {len(winners):,}건")
    print(f"   - ☠️ 패자 그룹 (수익 <= -5%): {len(losers):,}건")
    
    # ---------------------------------------------------------
    # 3. 통계 비교
    # ---------------------------------------------------------
    metrics = ['tv_z', 'spike_cnt', 'dist_ma60', 'chg', 'shadow_ratio']
    
    print("\n🧐 [승자 vs 패자] 핵심 지표 평균 비교")
    print("="*60)
    print(f"{'지표 (Feature)':<20} | {'🏆 승자 평균':<15} | {'☠️ 패자 평균':<15} | {'차이':<10}")
    print("-" * 60)
    
    for m in metrics:
        w_mean = winners[m].mean()
        l_mean = losers[m].mean()
        diff = w_mean - l_mean
        print(f"{m:<20} | {w_mean:10.2f}      | {l_mean:10.2f}      | {diff:+10.2f}")
    print("="*60)
    
    # ---------------------------------------------------------
    # 4. 인사이트 도출
    # ---------------------------------------------------------
    print("\n💡 [자동 분석 코멘트]")
    
    # Spike Count 분석
    if winners['spike_cnt'].mean() < losers['spike_cnt'].mean():
        print("👉 스파이크 횟수: 승자가 더 적습니다. 너무 잦은 거래량 폭발은 '고점' 징후일 수 있습니다.")
    else:
        print("👉 스파이크 횟수: 승자가 더 많습니다. 에너지가 충분히 축적된 종목이 갑니다.")
        
    # TV_Z 분석
    if winners['tv_z'].mean() < 2.0:
        print("👉 당일 거래량: 승자들은 당일 거래량이 폭발적(2.0 이상)이지 않았을 수 있습니다. 은근한 상승이 더 무섭습니다.")
        
    # MA60 이격도
    if winners['dist_ma60'].mean() < 5.0:
        print("👉 이격도: 승자들은 60일선에 가깝게 붙어있었습니다. 너무 뜬 종목은 위험합니다.")
    else:
        print("👉 이격도: 승자들은 이미 추세가 터져서 이평선 위에 떠있는 상태였습니다.")

if __name__ == "__main__":
    run_analysis()