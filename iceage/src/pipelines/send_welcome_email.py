# iceage/src/pipelines/send_welcome_email.py
import os
import sys
import re
from pathlib import Path

# 경로 설정
ICEAGE_DIR = Path(__file__).resolve().parents[2]
REPO_DIR = ICEAGE_DIR.parent
sys.path.insert(0, str(REPO_DIR))

from common.env_loader import load_env
load_env(REPO_DIR)

# 기존 send_newsletter의 함수들을 재활용
from iceage.src.pipelines.send_newsletter import (
    send_email_with_sendgrid,
    load_md_and_render_html,
    _extract_headline_from_html
)

def find_latest_report_date() -> str | None:
    """가장 최신 Signalist Daily MD 파일에서 날짜를 추출합니다."""
    md_dir = ICEAGE_DIR / "out"
    files = list(md_dir.glob("Signalist_Daily_*.md"))
    if not files:
        return None
    latest_file = max(files, key=os.path.getctime)
    match = re.search(r'(\d{4}-\d{2}-\d{2})', latest_file.name)
    return match.group(1) if match else None

def send_welcome_email(recipient_email: str):
    """신규 구독자에게 최신 리포트를 발송합니다."""
    print(f"🚀 [Iceage Welcome Email] 신규 구독자 환영 메일 발송 시작 -> {recipient_email}")

    ref_date = find_latest_report_date()
    if not ref_date:
        print("❌ 발송할 최신 리포트가 없습니다. 환영 메일 발송을 건너뜁니다.")
        return

    print(f"▶️ 발송 대상 리포트 날짜: {ref_date}")

    try:
        html_body = load_md_and_render_html(ref_date)
        headline = _extract_headline_from_html(html_body)
        
        subject_prefix = os.getenv("NEWSLETTER_SUBJECT_PREFIX", "[Signalist Daily]")
        subject = f"{subject_prefix} {ref_date} | {headline}"
        sender_name = os.getenv("SIGNALIST_SENDER_NAME", "Signalist Daily")
        sender_addr = os.getenv("SIGNALIST_SENDER_ADDRESS", "admin@fincore.co.kr")
        from_email = f"{sender_name} <{sender_addr}>"

        if send_email_with_sendgrid([recipient_email], subject, html_body, from_email):
            print(f"✅ [Iceage Welcome Email] 환영 메일 발송 성공 -> {recipient_email}")
        else:
            print(f"❌ [Iceage Welcome Email] 환영 메일 발송 실패 -> {recipient_email}")

    except Exception as e:
        print(f"❌ [Iceage Welcome Email] 환영 메일 발송 중 오류 발생: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python -m iceage.src.pipelines.send_welcome_email <recipient_email>")
        sys.exit(1)
    
    send_welcome_email(sys.argv[1])