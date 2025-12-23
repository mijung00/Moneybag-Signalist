# iceage/src/tools/force_generate_anomaly.py
import sys
from datetime import date, timedelta
from pathlib import Path
from tqdm import tqdm

# 프로젝트 루트 경로를 sys.path에 추가
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# 의존성 임포트
from iceage.src.analyzers.volume_anomaly_v2 import run_volume_anomaly_v2

def run_force_generate(days: int):
    """지정된 일수만큼 'volume_anomaly_v2' 파일을 강제로 다시 생성합니다."""
    print(f"🔄 [Force Generate] 최근 {days}일치 'volume_anomaly_v2' 파일을 생성합니다.")
    today = date.today()
    
    date_range = [today - timedelta(days=i) for i in range(1, days + 1)]
    
    for target_date in tqdm(date_range, desc="Generating Anomaly Files"):
        target_date_str = target_date.strftime("%Y-%m-%d")
        
        # 원본 시세 파일이 있는지 확인
        raw_price_file = PROJECT_ROOT / "iceage" / "data" / "raw" / f"kr_prices_{target_date_str}.csv"
        if not raw_price_file.exists():
            # tqdm 진행률 바에 직접 출력
            tqdm.write(f"⚠️ [Skip] {target_date_str}: 원본 시세 파일({raw_price_file.name})이 없어 건너뜁니다.")
            continue
            
        try:
            run_volume_anomaly_v2(target_date)
        except Exception as e:
            tqdm.write(f"❌ [ERROR] {target_date_str} 처리 중 오류 발생: {e}")

if __name__ == "__main__":
    days_to_run = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    run_force_generate(days_to_run)
    print("✅ 모든 작업이 완료되었습니다.")