# moneybag/src/pipelines/send_welcome_email.py
import os
import sys
from pathlib import Path

# 경로 설정
BASE_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(BASE_DIR))

from common.env_loader import load_env
load_env(BASE_DIR)

# 기존 EmailSender를 재활용
from moneybag.src.pipelines.send_email import EmailSender

def find_latest_report() -> Path | None:
    """가장 최신 SecretNote MD 파일을 찾습니다."""
    md_dir = BASE_DIR / "moneybag" / "data" / "out"
    files = list(md_dir.glob("SecretNote_*.md"))
    if not files:
        return None
    return max(files, key=os.path.getctime)

def send_welcome_email(recipient_email: str):
    """신규 구독자에게 최신 리포트를 발송합니다."""
    print(f"🚀 [Moneybag Welcome Email] 신규 구독자 환영 메일 발송 시작 -> {recipient_email}")

    latest_report_path = find_latest_report()
    if not latest_report_path:
        print("❌ 발송할 최신 리포트가 없습니다. 환영 메일 발송을 건너뜁니다.")
        return

    print(f"▶️ 발송 대상 파일: {latest_report_path.name}")

    try:
        sender = EmailSender()
        sender.to_emails = [recipient_email]

        mode = "morning"
        if "night" in latest_report_path.name.lower():
            mode = "night"
        
        sender.send(str(latest_report_path), mode=mode)
        print(f"✅ [Moneybag Welcome Email] 환영 메일 발송 성공 -> {recipient_email}")
    except Exception as e:
        print(f"❌ [Moneybag Welcome Email] 환영 메일 발송 중 오류 발생: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python -m moneybag.src.pipelines.send_welcome_email <recipient_email>")
        sys.exit(1)
    
    send_welcome_email(sys.argv[1])