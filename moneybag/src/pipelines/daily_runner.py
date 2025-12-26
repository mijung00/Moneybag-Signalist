import sys
import os
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
now = datetime.now(ZoneInfo("Asia/Seoul"))


import time
import subprocess

# 경로 설정
BASE_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(BASE_DIR))

from common.env_loader import load_env
load_env(BASE_DIR)

# 파이프라인 모듈 임포트
from moneybag.src.pipelines.daily_newsletter import DailyNewsletter
from moneybag.src.pipelines.generate_cardnews_assets import CardNewsFactory
from moneybag.src.pipelines.generate_summary_image import SummaryImageGenerator
from moneybag.src.pipelines.send_email import EmailSender
from moneybag.src.pipelines.report_postprocessor import ReportPostProcessor
from moneybag.src.utils.slack_notifier import SlackNotifier

# [추가] S3 매니저 가져오기
try:
    from common.s3_manager import S3Manager
except ImportError:
    print("⚠️ [Import Error] common.s3_manager를 찾을 수 없습니다. (로컬 테스트 중?)")
    S3Manager = None

def validate_markdown(text):
    """
    [검문소] 생성된 시크릿 노트가 정상인지 확인
    """
    if not text or len(text) < 1000:
        print("❌ [검증 실패] 내용이 너무 짧거나 비어있습니다.")
        return False
    
    if "# 🐋" not in text:
        print("❌ [검증 실패] 제목(# 🐋)이 없습니다.")
        return False
        
    if "최종 결론" not in text and "The Verdict" not in text:
        print("❌ [검증 실패] '최종 결론' 섹션이 없습니다. (생성 중단 의심)")
        return False
        
    return True

def run_routine(mode="morning"):
    print(f"\n🏃 [Runner] {mode.upper()} 루틴을 시작합니다...")
    routine_start_time = time.time()
    
    # ---------------------------------------------------------
    # [수정] 워커 환경 데이터 동기화 (S3에서 전체 데이터 폴더 다운로드)
    # ---------------------------------------------------------
    if S3Manager:
        try:
            print("\n☁️ [S3 Sync] 워커에 필요한 데이터 동기화 중 (moneybag/data)...")
            s3 = S3Manager()
            
            # 로컬 data 폴더 경로
            local_data_dir = BASE_DIR / "moneybag" / "data"
            local_data_dir.mkdir(parents=True, exist_ok=True)
            
            # S3 경로
            s3_data_dir = f"s3://{s3.bucket_name}/moneybag/data/"
            
            # aws-cli를 사용하여 S3와 로컬 디렉토리 동기화 (iceage 방식과 동일)
            # 이 명령은 whale_transactions.jsonl 및 PostProcessor에 필요한 과거 데이터 모두를 가져옵니다.
            sync_cmd = ["aws", "s3", "sync", s3_data_dir, str(local_data_dir), "--quiet"]
            subprocess.run(sync_cmd, check=True, timeout=300)
            print("   -> 동기화 완료.")
        except FileNotFoundError:
            # aws-cli가 설치되지 않은 로컬 환경 등에서 발생할 수 있음
            print(f"⚠️ [S3 Sync Warning] 'aws' 명령을 찾을 수 없습니다. aws-cli가 설치되어 있는지 확인하세요.")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ [S3 Sync Error] 데이터 동기화 실패 (aws s3 sync): {e}")
        except Exception as e:
            print(f"⚠️ [S3 Sync Warning] 데이터 동기화 중 예외 발생 (계속 진행): {e}")
    
    notifier = SlackNotifier()
    newsletter = DailyNewsletter()
    card_factory = CardNewsFactory()
    email_sender = EmailSender()
    post_processor = ReportPostProcessor()
    
    generated_md_path = None # [추가] 생성된 파일 경로를 저장할 변수
    all_strategies_from_newsletter = [] # [NEW] 생성된 전략 원본 리스트
    # 👇 [수정] 한국 시간(KST) 기준으로 날짜를 뽑도록 변경!
    now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
    today_str = now_kst.strftime("%Y.%m.%d")
    filename = f"SecretNote_{mode.capitalize()}_{today_str}.md"
    file_path = BASE_DIR / "moneybag" / "data" / "out" / filename
    
    # ---------------------------------------------------------
    # 1단계: 뉴스레터 생성 (재시도 로직 포함)
    # ---------------------------------------------------------
    max_retries = 3
    success = False
    step_start_time = time.time()
    
    for attempt in range(max_retries):
        try:
            print(f"\n1️⃣ 뉴스레터 생성 중... (시도 {attempt+1}/{max_retries})")
            # [수정] 생성된 파일 경로와 함께, 후처리에 필요한 원본 전략 리스트를 받음
            generated_md_path, all_strategies_from_newsletter = newsletter.generate(mode)
            
            # 🔍 검증
            if generated_md_path and generated_md_path.exists():
                md_content = generated_md_path.read_text(encoding='utf-8')
                if validate_markdown(md_content):
                    print(f"✅ [Runner] 시크릿 노트 검증 통과! ({generated_md_path.name})")
                    success = True
                    break
            else:
                print(f"⚠️ [Runner] 생성 결과가 불완전합니다. 재시도합니다...")
                time.sleep(5) 
                
        except Exception as e:
            print(f"❌ [Error] 생성 중 예외 발생: {e}")
            time.sleep(5)
    print(f"   -> ⏱️ 소요 시간: {time.time() - step_start_time:.2f}초")

    if not success:
        error_msg = f"🚨 [Moneybag 긴급] {mode.upper()} 시크릿 노트 생성 최종 실패!\n3회 재시도했으나 결과물이 불완전합니다."
        print(error_msg)
        try: notifier.send_message(error_msg) 
        except: pass
        return # 중단

    # ---------------------------------------------------------
    # 1.5단계: [NEW] 전략 다양성 확보를 위한 페널티 적용
    # ---------------------------------------------------------
    step_start_time = time.time()
    try:
        print("\n1️⃣-2️⃣ 리포트 후처리 및 전략 다양성 보정 중...")
        # [수정] 원본 전략 리스트를 후처리기에게 전달
        post_processor.run(generated_md_path, all_strategies_from_newsletter)
        print(f"   -> ⏱️ 소요 시간: {time.time() - step_start_time:.2f}초")
    except Exception as e:
        print(f"⚠️ [Warning] 페널티 적용 실패 (계속 진행): {e}")

    # ---------------------------------------------------------
    # 2단계: 카드뉴스 생성
    # ---------------------------------------------------------
    step_start_time = time.time()
    try:
        print("\n2️⃣ 카드뉴스 생성 중...")
        card_factory.run() # 최신 파일을 자동으로 읽어서 처리
        print(f"   -> ⏱️ 소요 시간: {time.time() - step_start_time:.2f}초")
    except Exception as e:
        print(f"⚠️ [Warning] 카드뉴스 생성 실패 (계속 진행): {e}")

    # ---------------------------------------------------------
    # 2.5단계: 커뮤니티용 요약 이미지 생성
    # ---------------------------------------------------------
    step_start_time = time.time()
    # [개선] iceage와 동일하게 환경변수로 제어할 수 있도록 기능 추가
    run_summary_image_output = os.getenv("RUN_SUMMARY_IMAGE_OUTPUT", "1") == "1"
    if run_summary_image_output:
        try:
            print("\n2️⃣-2️⃣ 커뮤니티용 요약 이미지 생성 중...")
            summary_image_generator = SummaryImageGenerator(mode=mode)
            summary_image_generator.run()
            print(f"   -> ⏱️ 소요 시간: {time.time() - step_start_time:.2f}초")
        except Exception as e:
            print(f"⚠️ [Warning] 요약 이미지 생성 실패 (계속 진행): {e}")
    else:
        print("[INFO] RUN_SUMMARY_IMAGE_OUTPUT!=1 이므로 요약 이미지 생성은 스킵합니다.")

    # ---------------------------------------------------------
    # 3단계: 이메일 발송 (경로 전달 필수!)
    # ---------------------------------------------------------
    step_start_time = time.time()
    try:
        print(f"\n3️⃣ 이메일 발송 중... (타겟: {generated_md_path.name})")
        
        # [수정] 더 이상 파일 경로를 추측하지 않고, 생성 단계에서 반환된 경로를 직접 사용
        if not generated_md_path or not generated_md_path.exists():
            raise FileNotFoundError(f"발송할 뉴스레터 파일을 찾을 수 없습니다: {generated_md_path}")
        email_sender.send(str(generated_md_path), mode=mode)
        print(f"   -> ⏱️ 소요 시간: {time.time() - step_start_time:.2f}초")
        
        print(f"✅ [Moneybag] **{mode.upper()}** 시크릿 노트 발송 완료! 📧")
        
        # 성공 슬랙 알림
        try: notifier.send_message(f"✅ [Moneybag] {mode.upper()} 리포트 발송 완료!")
        except: pass
        
    except Exception as e:
        print(f"❌ [Error] 이메일 발송 실패: {e}")
        try: notifier.send_message(f"🚨 [Moneybag] {mode.upper()} 이메일 발송 실패!\n에러: {e}")
        except: pass

# ... (위쪽 코드는 그대로 유지) ...

    # ---------------------------------------------------------
    # 4단계: S3 데이터 백업 (퇴근)
    # ---------------------------------------------------------
    step_start_time = time.time()
    if S3Manager:
        try:
            print("\n☁️ [S3 Sync] 머니백 데이터 및 결과물 전체 백업 중...")
            s3 = S3Manager()
            
            # [테스트용] recent_days=0 (오늘 파일만)
            BACKUP_DAYS = 0
            
            # moneybag 폴더 위치 찾기
            moneybag_root = BASE_DIR / "moneybag"
            
            # 1. moneybag/data 폴더
            data_dir = moneybag_root / "data"
            if data_dir.exists():
                s3.upload_directory(str(data_dir), "moneybag/data", recent_days=BACKUP_DAYS)
            print(f"   -> ⏱️ 소요 시간: {time.time() - step_start_time:.2f}초")
            
                
        except Exception as e:
            print(f"⚠️ [S3 Error] 백업 중 오류 발생: {e}")

    print(f"\n🏃 [Runner] {mode.upper()} 루틴 정상 종료! (총 소요 시간: {time.time() - routine_start_time:.2f}초)")


def main(mode="morning", *args, **kwargs):
    """
    머니백 데일리 루틴의 표준 진입점.
    runner.py에서 호출하기 위해 run_routine을 래핑합니다.
    """
    mode_arg = mode if mode in ("morning", "night") else "morning"
    mode_arg = (mode_arg or "morning").strip().lower()
    mode_arg = mode_arg.replace("\r", "")
    run_routine(mode_arg)

if __name__ == "__main__":
    import sys
    mode_from_cli = sys.argv[1] if len(sys.argv) > 1 else "morning"
    main(mode=mode_from_cli)
