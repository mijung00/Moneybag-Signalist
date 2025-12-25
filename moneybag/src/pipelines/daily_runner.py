import sys
import time
import os
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
now = datetime.now(ZoneInfo("Asia/Seoul"))



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
    
    notifier = SlackNotifier()
    newsletter = DailyNewsletter()
    card_factory = CardNewsFactory()
    email_sender = EmailSender()
    post_processor = ReportPostProcessor()
    
    generated_md_path = None # [추가] 생성된 파일 경로를 저장할 변수
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
    
    for attempt in range(max_retries):
        try:
            print(f"\n1️⃣ 뉴스레터 생성 중... (시도 {attempt+1}/{max_retries})")
            # [수정] 생성된 파일의 경로를 직접 받음
            generated_md_path = newsletter.generate(mode)
            
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

    if not success:
        error_msg = f"🚨 [Moneybag 긴급] {mode.upper()} 시크릿 노트 생성 최종 실패!\n3회 재시도했으나 결과물이 불완전합니다."
        print(error_msg)
        try: notifier.send_message(error_msg) 
        except: pass
        return # 중단

    # ---------------------------------------------------------
    # 1.5단계: [NEW] 전략 다양성 확보를 위한 페널티 적용
    # ---------------------------------------------------------
    try:
        print("\n1️⃣-2️⃣ 리포트 후처리 및 전략 다양성 보정 중...")
        post_processor.run(generated_md_path)
    except Exception as e:
        print(f"⚠️ [Warning] 페널티 적용 실패 (계속 진행): {e}")

    # ---------------------------------------------------------
    # 2단계: 카드뉴스 생성
    # ---------------------------------------------------------
    try:
        print("\n2️⃣ 카드뉴스 생성 중...")
        card_factory.run() # 최신 파일을 자동으로 읽어서 처리
    except Exception as e:
        print(f"⚠️ [Warning] 카드뉴스 생성 실패 (계속 진행): {e}")

    # ---------------------------------------------------------
    # 2.5단계: 커뮤니티용 요약 이미지 생성
    # ---------------------------------------------------------
    # [개선] iceage와 동일하게 환경변수로 제어할 수 있도록 기능 추가
    run_summary_image_output = os.getenv("RUN_SUMMARY_IMAGE_OUTPUT", "1") == "1"
    if run_summary_image_output:
        try:
            print("\n2️⃣-2️⃣ 커뮤니티용 요약 이미지 생성 중...")
            summary_image_generator = SummaryImageGenerator(mode=mode)
            summary_image_generator.run()
        except Exception as e:
            print(f"⚠️ [Warning] 요약 이미지 생성 실패 (계속 진행): {e}")
    else:
        print("[INFO] RUN_SUMMARY_IMAGE_OUTPUT!=1 이므로 요약 이미지 생성은 스킵합니다.")

    # ---------------------------------------------------------
    # 3단계: 이메일 발송 (경로 전달 필수!)
    # ---------------------------------------------------------
    try:
        print(f"\n3️⃣ 이메일 발송 중... (타겟: {generated_md_path.name})")
        
        # [수정] 더 이상 파일 경로를 추측하지 않고, 생성 단계에서 반환된 경로를 직접 사용
        if not generated_md_path or not generated_md_path.exists():
            raise FileNotFoundError(f"발송할 뉴스레터 파일을 찾을 수 없습니다: {generated_md_path}")
        email_sender.send(str(generated_md_path), mode=mode)
        
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
            
                
        except Exception as e:
            print(f"⚠️ [S3 Error] 백업 중 오류 발생: {e}")

    print(f"\n🏃 [Runner] {mode.upper()} 루틴 정상 종료!")


if __name__ == "__main__":
    import sys
    mode_arg = sys.argv[1] if len(sys.argv) > 1 else "morning"
    mode_arg = (mode_arg or "morning").strip().lower()
    mode_arg = mode_arg.replace("\r", "")

    if mode_arg not in ("morning", "night"):
        print(f"[Warning] invalid mode='{mode_arg}', fallback to morning")
        mode_arg = "morning"

    run_routine(mode_arg)
