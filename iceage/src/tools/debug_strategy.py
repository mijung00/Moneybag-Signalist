# iceage/src/tools/debug_strategy.py
import pandas as pd
import glob
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "iceage" / "data"

def debug_one_file():
    print("🕵️‍♂️ [디버그 모드] 데이터 매칭 문제 진단 시작...\n")
    
    # 1. 파일 확인
    files = sorted(glob.glob(str(DATA_DIR / "processed" / "volume_anomaly_v2_*.csv")))
    if not files:
        print("❌ [치명적] 'processed' 폴더에 volume_anomaly 파일이 없습니다.")
        return

    # 가장 오래된 파일 하나만 테스트 (확실히 미래 데이터가 있을 법한)
    fpath = files[0] 
    print(f"📄 분석 대상 파일: {os.path.basename(fpath)}")

    # 2. 데이터 로드 & 컬럼 확인
    df = pd.read_csv(fpath)
    print(f"   - 로드된 행 수: {len(df)}개")
    print(f"   - 컬럼 목록: {list(df.columns)}")
    
    if 'code' not in df.columns:
        print("❌ [오류] 'code' 컬럼이 없습니다!")
        return
    
    # 코드 샘플 확인
    sample_code = df['code'].iloc[0]
    print(f"   - 종목코드 샘플(Raw): {sample_code} (Type: {type(sample_code)})")

    # 3. 조건 필터링 테스트
    # 양봉 & 괴리율 조건
    cond_sigma = (df['vol_sigma'] >= 2.0) & (df['vol_sigma'] <= 6.0)
    
    if 'open' in df.columns and 'close' in df.columns:
        df_valid = df[df['open'] > 0]
        cond_red = df_valid['close'] >= df_valid['open']
        candidates = df_valid[cond_sigma & cond_red]
        print(f"   - 조건(양봉+괴리율) 만족 행 수: {len(candidates)}개")
    else:
        print("⚠️ [경고] open/close 컬럼이 없어 change_rate로 대체합니다.")
        cond_red = pd.to_numeric(df['change_rate'], errors='coerce') > 0
        candidates = df[cond_sigma & cond_red]
        print(f"   - 조건(양봉+괴리율) 만족 행 수: {len(candidates)}개")

    if candidates.empty:
        print("❌ [원인 발견] 필터링 조건에 맞는 종목이 0개입니다. (조건이 너무 빡빡하거나 데이터 이상)")
        return

    # 4. 미래 시세 파일 찾기
    date_str = os.path.basename(fpath).replace("volume_anomaly_v2_", "").replace(".csv", "")
    curr_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    target_date = curr_date + timedelta(days=7) # D+5 (일주일 뒤)
    
    print(f"\n📅 기준일: {curr_date}")
    print(f"🔭 목표 미래일(D+5): {target_date} 근처")

    future_price_file = None
    found_date = None
    
    for i in range(5):
        check_d = (target_date + timedelta(days=i)).strftime("%Y-%m-%d")
        check_p = DATA_DIR / "raw" / f"kr_prices_{check_d}.csv"
        print(f"   - 탐색 중: {check_p} ... ", end="")
        if check_p.exists():
            print("✅ 발견!")
            future_price_file = check_p
            found_date = check_d
            break
        else:
            print("없음")
    
    if not future_price_file:
        print("\n❌ [원인 발견] 미래 시세 파일을 찾을 수 없습니다.")
        print("   - 혹시 'raw' 폴더에 kr_prices_YYYY-MM-DD.csv 파일들이 있나요?")
        return

    # 5. 종목 매칭 테스트
    print(f"\n🤝 매칭 테스트 (Target: {found_date})")
    df_future = pd.read_csv(future_price_file)
    
    # Future 코드 정규화
    df_future['code'] = df_future['code'].astype(str).str.zfill(6)
    future_codes = set(df_future['code'].unique())
    
    # Candidate 코드 정규화
    # (여기서 사용자가 겪은 숫자형 코드를 문자열로 변환하는 로직 적용)
    def _norm(x):
        try: return str(int(float(x))).zfill(6)
        except: return str(x).strip().zfill(6)
    
    sample_candidate = candidates.iloc[0]
    cand_code_raw = sample_candidate['code']
    cand_code_norm = _norm(cand_code_raw)
    
    print(f"   - 후보 종목 코드(Raw): {cand_code_raw}")
    print(f"   - 후보 종목 코드(Norm): {cand_code_norm}")
    
    if cand_code_norm in future_codes:
        print(f"   ✅ [매칭 성공] 미래 가격 데이터에 {cand_code_norm} 종목이 있습니다.")
        price = df_future[df_future['code'] == cand_code_norm]['close'].values[0]
        print(f"   - 미래 가격: {price}")
    else:
        print(f"   ❌ [원인 발견] 미래 가격 데이터에서 코드를 찾을 수 없습니다.")
        print(f"   - 미래 데이터 샘플 코드: {list(future_codes)[:3]}")
        print("   👉 코드 포맷(6자리/숫자 등)이 서로 다를 가능성이 큽니다.")

if __name__ == "__main__":
    debug_one_file()