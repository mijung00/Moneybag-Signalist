import sys
import os
import glob
from pathlib import Path

# 프로젝트 루트 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from common.s3_manager import S3Manager
from iceage.src.tools.backfill_signalist_today_v3 import run_backfill

def emergency_restore():
    s3 = S3Manager()
    local_processed_dir = PROJECT_ROOT / "iceage" / "data" / "processed"
    local_processed_dir.mkdir(parents=True, exist_ok=True)

    print("🚨 [Emergency] 로그 파일 복구 작업을 시작합니다.")

    # 1. S3에서 과거 괴리율 데이터(volume_anomaly_v2) 싹 긁어오기
    # (S3Manager에 list 기능이 없어서 boto3 client를 직접 씁니다)
    print("\n📥 [1/3] S3에서 원천 데이터(괴리율 파일) 확인 및 다운로드...")
    bucket_name = s3.bucket_name
    prefix = "iceage/data/processed/volume_anomaly_v2_"
    
    try:
        paginator = s3.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        count = 0
        for page in pages:
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                s3_key = obj['Key']
                # volume_anomaly_v2_*.csv 파일만 대상
                if not s3_key.endswith(".csv"):
                    continue
                
                filename = os.path.basename(s3_key)
                local_path = local_processed_dir / filename
                
                # 로컬에 없으면 다운로드
                if not local_path.exists():
                    s3.download_file(s3_key, str(local_path))
                    count += 1
        print(f"   -> {count}개의 누락된 괴리율 파일을 다운로드했습니다.")
        
    except Exception as e:
        print(f"❌ S3 목록 조회 실패: {e}")
        return

    # 2. 로컬에서 로그 재생성 (V3 로직 사용 - 최신 전략 반영)
    print("\n🔄 [2/3] 로그 파일 재생성 (Backfill V3)...")
    # 최근 180일치 복구 (필요하면 숫자 조절)
    try:
        run_backfill(days=180) 
    except Exception as e:
        print(f"❌ 백필 실행 중 오류: {e}")
        return

    # 3. 복구된 로그 파일 S3에 강제 업로드
    print("\n☁️ [3/3] 복구된 로그 S3 강제 업로드...")
    log_file_local = local_processed_dir / "signalist_today_log.csv"
    log_file_s3 = "iceage/data/processed/signalist_today_log.csv"

    if log_file_local.exists():
        success = s3.upload_file(str(log_file_local), log_file_s3)
        if success:
            print(f"✅ [SUCCESS] 로그 복구 및 S3 동기화 완료! ({log_file_s3})")
        else:
            print("❌ S3 업로드 실패")
    else:
        print("❌ 생성된 로그 파일이 없습니다.")

if __name__ == "__main__":
    emergency_restore()