import sys
import time
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# 경로 설정
BASE_DIR = Path(__file__).resolve().parents[3]
sys.path.append(str(BASE_DIR))
load_dotenv(BASE_DIR / ".env")

# 파이프라인 모듈 임포트
from moneybag.src.pipelines.daily_newsletter import DailyNewsletter
from moneybag.src.pipelines.generate_cardnews_assets import CardNewsFactory
from moneybag.src.pipelines.send_email import EmailSender
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
    
    # 파일 경로 미리 계산 (저장될 경로)
    today_str = datetime.now().strftime("%Y.%m.%d")
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
            md_content = newsletter.generate(mode) # 파일은 generate 내부에서 저장됨
            
            # 🔍 검증
            if validate_markdown(md_content):
                print("✅ [Runner] 시크릿 노트 검증 통과!")
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
    # 2단계: 카드뉴스 생성
    # ---------------------------------------------------------
    try:
        print("\n2️⃣ 카드뉴스 생성 중...")
        card_factory.run() # 최신 파일을 자동으로 읽어서 처리
    except Exception as e:
        print(f"⚠️ [Warning] 카드뉴스 생성 실패 (계속 진행): {e}")

    # ---------------------------------------------------------
    # 3단계: 이메일 발송 (경로 전달 필수!)
    # ---------------------------------------------------------
    try:
        print(f"\n3️⃣ 이메일 발송 중... (타겟: {filename})")
        
        # [수정] 파일 경로 존재 확인
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"발송할 파일을 찾을 수 없습니다: {file_path}")
            
        # [수정] 명확한 메서드 호출 (파일 경로 전달)
        # EmailSender의 메서드가 send()라고 가정합니다. 
        # 만약 send_email()이라면 그에 맞춰 수정해주세요.
        email_sender.send(str(file_path)) 
        
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
            
            # [테스트용] recent_days=2
            BACKUP_DAYS = 2
            
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
    run_routine(mode_arg)