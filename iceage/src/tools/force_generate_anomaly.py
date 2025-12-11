# iceage/src/tools/force_generate_anomaly.py
import sys
import subprocess
from datetime import date, timedelta, datetime
from pathlib import Path

# 경로 안전장치
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

def run_force_generation(start_date_str: str, end_date_str: str):
    """
    지정된 기간 동안 volume_anomaly_v2를 강제로 실행하여 파일을 생성합니다.
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    print(f"🚀 괴리율 데이터 강제 생성 시작: {start_date} ~ {end_date}")
    
    curr = start_date
    while curr <= end_date:
        # 주말 제외 (토/일)
        if curr.weekday() < 5:
            ymd = curr.isoformat()
            
            # 시세 파일 확인
            price_path = PROJECT_ROOT / "iceage" / "data" / "raw" / f"kr_prices_{ymd}.csv"
            if price_path.exists():
                print(f"[{ymd}] 괴리율 계산 실행...")
                try:
                    # volume_anomaly_v2 실행
                    cmd = [sys.executable, "-m", "iceage.src.analyzers.volume_anomaly_v2", ymd]
                    subprocess.run(cmd, check=True)
                except subprocess.CalledProcessError:
                    print(f"  ❌ {ymd} 계산 실패 (데이터 부족 등)")
                except Exception as e:
                    print(f"  ❌ {ymd} 에러: {e}")
            else:
                print(f"[{ymd}] ⚠️ 시세 파일 없음 (Skip)")
        
        curr += timedelta(days=1)

    print("✅ 작업 완료")

if __name__ == "__main__":
    # 2023-03-01부터 2023-10-04까지 빈 구간을 채웁니다.
    # (1월 데이터부터 있다면 60일 윈도우 고려해 3월부터 돌리는 게 안전)
    run_force_generation("2023-03-01", "2023-10-05")