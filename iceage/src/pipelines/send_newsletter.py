# iceage/src/pipelines/send_newsletter.py
# -*- coding: utf-8 -*-
import re
import html as html_lib
import os
import sys
import datetime as dt
import math
from pathlib import Path
from sendgrid import SendGridAPIClient
# 👇 [수정] Personalization 모듈 추가
from sendgrid.helpers.mail import Mail, To, Personalization

from dotenv import load_dotenv

load_dotenv()

# 경로 설정
ICEAGE_DIR = Path(__file__).resolve().parents[2]
REPO_DIR = ICEAGE_DIR.parent
OUT_DIR = ICEAGE_DIR / "out"
OUT_SOCIAL_DIR = OUT_DIR / "social"

def _get_newsletter_env_suffix() -> str:
    env = os.getenv("NEWSLETTER_ENV", "prod").strip().lower()
    if env in ("", "prod"): return ""
    return f"-{env}"

def load_html(ref_date: str) -> str:
    file_name = f"Signalist_Daily_{ref_date}{_get_newsletter_env_suffix()}.html"
    file_path = OUT_DIR / file_name
    
    # 1. 로컬 파일 먼저 시도 (Cron 실행 시)
    if file_path.exists():
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
            
    # 2. S3에서 가져오기 (웹 앱에서 호출 시)
    print(f"   -> 로컬 파일 없음. S3에서 '{file_name}' 다운로드 시도...")
    try:
        from common.s3_manager import S3Manager
        s3 = S3Manager(bucket_name="fincore-output-storage")
        s3_key = f"iceage/out/{file_name}"
        content = s3.get_text_content(s3_key)
        if content:
            return content
    except Exception as e:
        print(f"   -> S3 다운로드 실패: {e}")

    raise FileNotFoundError(f"HTML 파일이 로컬과 S3 모두에 존재하지 않습니다: {file_path}")

def load_sns_report_txt(ref_date: str) -> str:
    file_name = f"Signalist_Instagram_{ref_date}.txt"
    file_path = OUT_SOCIAL_DIR / file_name
    if not file_path.exists():
        return f"금일 SNS Instagram 보고서({file_name})는 생성되지 않았습니다."
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

def get_subscribers(env: str, test_recipient: str, is_auto_send: bool) -> list[str]:
    if not is_auto_send or env == 'dev':
        print(f"⚠️ [Mode: {env}] 테스트 수신자에게만 발송합니다.")
        return [test_recipient] if test_recipient else []

    # DB에서 실제 구독자 조회
    try:
        import pymysql
        conn = pymysql.connect(
            host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT", 3306)),
            user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
            db=os.getenv("DB_NAME"), charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            # 시그널리스트 구독자(is_signalist=1)만 조회
            cursor.execute("SELECT email FROM subscribers WHERE is_signalist=1 AND is_active=1")
            result = cursor.fetchall()
            emails = [row['email'] for row in result]
            print(f"✅ [DB Load] 시그널리스트 구독자 {len(emails)}명 조회 성공")
            return emails
    except Exception as e:
        print(f"⚠️ [DB Error] 구독자 조회 실패: {e}")
        return [os.getenv("ADMIN_EMAIL")] if os.getenv("ADMIN_EMAIL") else []

def _extract_headline_from_html(html_content: str) -> str:
    """HTML 콘텐츠에서 제목을 추출합니다."""
    # <title> 태그에서 추출
    title_match = re.search(r'<title>(.*?)</title>', html_content, re.DOTALL | re.IGNORECASE)
    if title_match:
        # "FINCORE | " 접두사 제거
        title = title_match.group(1).strip()
        if "FINCORE | " in title:
            title = title.split("FINCORE | ", 1)[1]
        return title
    
    # <h1> 태그에서 추출
    h1_match = re.search(r'<h1[^>]*>(.*?)</h1>', html_content, re.DOTALL | re.IGNORECASE)
    if h1_match:
        return h1_match.group(1).strip()
    
    return "새로운 리포트"

def send_email_with_sendgrid(to_emails: list[str], subject: str, html_body: str, from_email: str) -> bool:
    """
    [핵심 수정] SendGrid Personalization을 사용하여 개별 발송 효과 (BCC X, Loop X)
    """
    api_key = os.getenv("SENDGRID_API_KEY")
    if not api_key or not api_key.strip().startswith("SG."):
        print("❌ SendGrid API Key가 잘못되었습니다.")
        return False
    
    api_key = api_key.strip()
    sg = SendGridAPIClient(api_key)

    # Batch Process (1000명 제한 고려)
    batch_size = 1000
    total_batches = math.ceil(len(to_emails) / batch_size)
    all_success = True

    print(f"📧 총 {len(to_emails)}명에게 발송 (API Personalization)")

    for i in range(total_batches):
        batch = to_emails[i * batch_size : (i + 1) * batch_size]
        
        # 1. 메시지 생성
        message = Mail(
            from_email=from_email,
            subject=subject,
            html_content=html_body
        )

        # 2. 개인화 추가 (각자에게 'To'가 찍힘)
        for email in batch:
            p = Personalization()
            p.add_to(To(email))
            message.add_personalization(p)

        # 3. 전송
        try:
            response = sg.send(message)
            if 200 <= response.status_code < 300:
                print(f"✅ [Batch {i+1}] 전송 성공")
            else:
                print(f"❌ [Batch {i+1}] 전송 실패: {response.status_code}")
                all_success = False
        except Exception as e:
            print(f"❌ [Batch {i+1}] 예외 발생: {e}")
            all_success = False
            
    return all_success

if __name__ == '__main__':
    # (실행부 로직 기존과 동일)
    ref_date = None
    if len(sys.argv) > 1 and re.match(r"\d{4}-\d{2}-\d{2}", sys.argv[1]):
        ref_date = sys.argv[1]
    elif os.getenv("REF_DATE"):
        ref_date = os.getenv("REF_DATE")
    else:
        html_files = sorted(OUT_DIR.glob("Signalist_Daily_*.html"))
        if html_files:
            latest = html_files[-1]
            m = re.search(r"Signalist_Daily_(\d{4}-\d{2}-\d{2})\.html", latest.name)
            if m: ref_date = m.group(1)
            else: ref_date = dt.date.today().isoformat()
        else:
            ref_date = dt.date.today().isoformat()

    env = os.getenv("NEWSLETTER_ENV", "prod").strip().lower()
    
    sender_name = os.getenv("SIGNALIST_SENDER_NAME", "Signalist Daily")
    sender_addr = os.getenv("SIGNALIST_SENDER_ADDRESS", "admin@fincore.co.kr")
    from_email = f"{sender_name} <{sender_addr}>"
    
    admin_email = os.getenv("ADMIN_EMAIL", "admin@fincore.co.kr")
    test_recipient = os.getenv("TEST_RECIPIENT")
    subject_prefix = os.getenv("NEWSLETTER_SUBJECT_PREFIX", "[Signalist Daily]")
    is_send_on = os.getenv("NEWSLETTER_AUTO_SEND") == "1"
    
    print(f"📧 Pipeline start: {ref_date} (env={env})")
    
    try:
        html_body = load_html(ref_date) # [수정] load_html은 이미 전체 HTML을 반환
        
        # [수정] HTML 본문에서 제목 추출
        headline = _extract_headline_from_html(html_body)
        
        # [수정] 추출된 제목을 포함하여 subject 생성
        subject_newsletter = f"{subject_prefix} {ref_date} | {headline}" if headline != "새로운 리포트" else f"{subject_prefix} {ref_date} 리포트"
        
        subscribers = get_subscribers(env, test_recipient, is_send_on)
        
        if subscribers:
            success = send_email_with_sendgrid(subscribers, subject_newsletter, html_body, from_email)
            print(f"✅ 결과: {'성공' if success else '실패'}")
        else:
            print("⚠️ 발송 대상 없음")

    except FileNotFoundError as e:
        print(f"❌ {e}")
        
    # SNS Report to Admin
    if os.getenv("SEND_SNS_REPORT_TO_ADMIN") == "1" and admin_email:
        try:
            sns_body = load_sns_report_txt(ref_date)
            subject_sns = f"[ADMIN] SNS Report {ref_date}"
            html_rep = f"<html><body><pre>{html_lib.escape(sns_body)}</pre></body></html>"
            send_email_with_sendgrid([admin_email], subject_sns, html_rep, from_email)
        except Exception: pass