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
# 👇 [수정] Personalization 및 Substitution 모듈 추가
from sendgrid.helpers.mail import Mail, To, Personalization, Substitution
from itsdangerous import URLSafeTimedSerializer
from iceage.src.pipelines.render_newsletter_html import render_markdown_to_html

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

def load_md_and_render_html(ref_date: str) -> str:
    """
    [수정] HTML을 직접 읽는 대신, MD 파일을 읽고 실시간으로 HTML을 렌더링합니다.
    이렇게 하면 항상 최신 푸터와 제목 구조가 반영됩니다.
    """
    # render_markdown_to_html 함수는 HTML을 파일로 저장하고 그 경로를 반환합니다.
    # 이 함수를 호출하여 최신 MD 파일로부터 항상 새로운 HTML을 생성하도록 합니다.
    html_path = render_markdown_to_html(ref_date)
    
    if html_path.exists():
        return html_path.read_text(encoding="utf-8")
    
    raise FileNotFoundError(f"HTML 렌더링에 실패했거나 파일을 읽을 수 없습니다: {html_path}")

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
            db=os.getenv("DB_NAME"), charset='utf8mb4', cursorclass=pymysql.cursors.SSDictCursor
        )
        with conn.cursor() as cursor:
            # 시그널리스트 구독자(is_signalist=1)만 조회
            cursor.execute("SELECT email FROM subscribers WHERE is_signalist=1 AND is_active=1")
            # [성능 개선] SSDictCursor와 함께 사용하여, 모든 결과를 메모리에 올리지 않고 스트리밍
            emails = [row['email'] for row in cursor]
            print(f"✅ [DB Load] 시그널리스트 구독자 {len(emails)}명 조회 성공")
            return emails
    except Exception as e:
        print(f"⚠️ [DB Error] 구독자 조회 실패: {e}")
        return [os.getenv("ADMIN_EMAIL")] if os.getenv("ADMIN_EMAIL") else []

def _extract_headline_from_html(html_content: str) -> str:
    """HTML 콘텐츠에서 제목을 추출합니다."""
    # [수정] 1. <h1> 바로 뒤에 오는 이탤릭체 부제(kicker)를 최우선으로 찾습니다.
    em_match = re.search(r'</h1>\s*<p><em>(.*?)</em></p>', html_content, re.DOTALL | re.IGNORECASE)
    if em_match:
        kicker = em_match.group(1).strip()
        if kicker:
            return kicker

    # 2. <h1> 태그에서 추출 (폴백)
    h1_match = re.search(r'<h1[^>]*>(.*?)</h1>', html_content, re.DOTALL | re.IGNORECASE)
    if h1_match:
        return h1_match.group(1).strip()

    # 3. <title> 태그에서 추출 (최종 폴백)
    title_match = re.search(r'<title>(.*?)</title>', html_content, re.DOTALL | re.IGNORECASE)
    if title_match:
        # "FINCORE | " 또는 "Signalist Daily —" 같은 접두/접미사 제거
        title = title_match.group(1).strip()
        if "FINCORE | " in title:
            title = title.split("FINCORE | ", 1)[1]
        if "Signalist Daily — " in title:
            title = title.replace("Signalist Daily — ", "")
        return title
    
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

    # [추가] 구독 취소 토큰 생성을 위한 Serializer
    secret_key = os.getenv('SECRET_KEY', 'a-very-secret-key-that-is-secure')
    serializer = URLSafeTimedSerializer(secret_key)

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

            # [추가] 각 이메일별 개인화된 구독 취소 링크 생성
            try:
                unsubscribe_token = serializer.dumps(email, salt='email-unsubscribe')
                # [수정] 로컬 테스트를 위해 WEB_BASE_URL 환경변수 사용
                web_base_url = os.getenv("WEB_BASE_URL", "https://www.fincore.trade")
                # 서비스명을 'signalist'로 지정
                unsubscribe_url = f"{web_base_url}/unsubscribe/signalist/{unsubscribe_token}"
                
                p.add_substitution(Substitution("-email-", email))
                p.add_substitution(Substitution("-unsubscribe_url-", unsubscribe_url))
            except Exception as e:
                print(f"⚠️ 토큰 생성 실패: {email}, {e}")

            message.add_personalization(p)

        # 3. 전송
        try:
            response = sg.send(message)
            if 200 <= response.status_code < 300:
                print(f"✅ [Batch {i+1}] 전송 성공")
            else:
                print(f"❌ [Batch {i+1}] 전송 실패: {response.status_code}")
                print(f"   -> SendGrid Body: {response.body}")
                all_success = False
        except Exception as e:
            print(f"❌ [Batch {i+1}] 예외 발생: {e}")
            all_success = False
            
    return all_success

if __name__ == '__main__':
    # [단순화] 로컬 테스트를 위해 복잡한 날짜 계산 대신, 가장 최근 파일을 찾거나 인자를 사용합니다.
    if len(sys.argv) > 1 and re.match(r"^\d{4}-\d{2}-\d{2}$", sys.argv[1]):
        ref_date = sys.argv[1] # 인자로 날짜가 주어지면 사용
    else:
        # 인자가 없으면 가장 최근에 생성된 MD 파일을 찾습니다.
        md_files = sorted(OUT_DIR.glob("Signalist_Daily_*.md"))
        if not md_files:
            print("❌ 발송할 뉴스레터(.md) 파일이 없습니다.")
            sys.exit(1)
        latest_md = md_files[-1]
        match = re.search(r'(\d{4}-\d{2}-\d{2})', latest_md.name)
        if not match:
            print(f"❌ 파일명에서 날짜를 찾을 수 없습니다: {latest_md.name}")
            sys.exit(1)
        ref_date = match.group(1)
        print(f"▶️ 최신 파일 자동 선택: {latest_md.name}")
    
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
        html_body = load_md_and_render_html(ref_date) # [수정] MD파일로부터 실시간 렌더링
        
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