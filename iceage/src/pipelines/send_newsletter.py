# iceage/src/pipelines/send_newsletter.py
# -*- coding: utf-8 -*-
"""
Signalist_Daily_YYYY-MM-DD.html 을 읽어서 이메일로 발송하는 스크립트.
SendGrid API를 사용하여 뉴스레터와 SNS 리포트를 발송합니다.
"""
import re
import html as html_lib
import os
import sys
import datetime as dt
from pathlib import Path
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, To

from dotenv import load_dotenv

# .env 로드
load_dotenv()

# --- [경로 설정 수정] 핵심 변경 부분 ---
# 이 파일의 위치: project/iceage/src/pipelines/send_newsletter.py
# parents[0]=pipelines, parents[1]=src, parents[2]=iceage, parents[3]=project(루트)

ICEAGE_DIR = Path(__file__).resolve().parents[2] # iceage 폴더
REPO_DIR = ICEAGE_DIR.parent                     # project (루트) 폴더

OUT_DIR = ICEAGE_DIR / "out"
OUT_SOCIAL_DIR = OUT_DIR / "social"

# 구독자 파일은 이제 '루트 폴더'에서 찾습니다!
SUBSCRIBERS_FILE = REPO_DIR / os.getenv("SIGNALIST_SUBSCRIBERS_FILE", "subscribers_signalist.csv")

# ------------------------------------

def _get_newsletter_env_suffix() -> str:
    env = os.getenv("NEWSLETTER_ENV", "prod").strip().lower()
    if env in ("", "prod"):
        return ""
    return f"_{env}"

def load_html(ref_date: str) -> str:
    """HTML 파일 내용을 읽어옵니다."""
    file_name = f"Signalist_Daily_{ref_date}{_get_newsletter_env_suffix()}.html"
    file_path = OUT_DIR / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"HTML 파일이 존재하지 않습니다: {file_path}")
    
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

def load_sns_report_txt(ref_date: str) -> str:
    """SNS 리포트 텍스트 파일을 읽어옵니다."""
    file_name = f"Signalist_Instagram_{ref_date}.txt"
    file_path = OUT_SOCIAL_DIR / file_name
    if not file_path.exists():
        return f"금일 SNS Instagram 보고서({file_name})는 생성되지 않았습니다."
    
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

def get_subscribers(env: str, test_recipient: str, is_auto_send: bool) -> list[str]:
    """구독자 리스트를 가져옵니다. (CSV 파일 사용)"""
    if not is_auto_send or env == 'dev':
        print(f"⚠️ AUTO_SEND={is_auto_send} 또는 DEV 환경: 테스트 수신자 {test_recipient}에게만 발송됩니다.")
        return [test_recipient]
    
    # [구독자 관리] CSV 파일을 읽어 처리
    if SUBSCRIBERS_FILE.exists():
        try:
            df = pd.read_csv(SUBSCRIBERS_FILE, encoding='utf-8')
            if 'subscribed' in df.columns and 'email' in df.columns:
                subscribers = df[df['subscribed'] == True]['email'].tolist()
                subscribers = [e.strip() for e in subscribers if "@" in e and "." in e]
                return subscribers
            else:
                print(f"❌ 구독자 파일({SUBSCRIBERS_FILE.name}) 컬럼 형식 오류. admin에게만 발송합니다.")
                return [os.getenv("ADMIN_EMAIL")]
        except Exception as e:
            print(f"❌ 구독자 파일({SUBSCRIBERS_FILE.name}) 읽기 오류: {e}. admin에게만 발송합니다.")
            return [os.getenv("ADMIN_EMAIL")]
    else:
        print(f"❌ 구독자 파일이 없습니다: {SUBSCRIBERS_FILE}")
        print("   (Tip: subscribers_signalist.csv 파일이 프로젝트 루트에 있는지 확인하세요)")
        return [os.getenv("ADMIN_EMAIL")]

def send_email_with_sendgrid(to_emails: list[str], subject: str, html_body: str, from_email: str) -> bool:
    """SendGrid API를 이용하여 이메일을 발송"""
    api_key = os.getenv("SENDGRID_API_KEY")
    
    if not api_key or not api_key.strip().startswith("SG."):
        print("❌ SendGrid API Key가 잘못되었거나 설정되지 않았습니다.")
        return False
    
    api_key = api_key.strip()

    try:
        message = Mail(
            from_email=from_email,
            subject=subject,
            html_content=html_body
        )
        message.to = [To(email) for email in to_emails]
        
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        
        if 200 <= response.status_code < 300:
            return True
        else:
            print(f"❌ 메일 발송 실패 (상태 코드: {response.status_code})")
            print(f"   에러 응답: {response.body}")
            return False

    except Exception as e:
        print(f"❌ 메일 발송 실패 (예외 발생): {e}")
        return False

if __name__ == '__main__':
    # --- 1) 환경 변수 및 날짜 설정 ---
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
    
    # [수정] 이메일 이름과 주소를 환경 변수에서 각각 가져와서 조립
    sender_name = os.getenv("SIGNALIST_SENDER_NAME", "Signalist Daily")
    sender_addr = os.getenv("SIGNALIST_SENDER_ADDRESS", "admin@fincore.co.kr")
    from_email = f"{sender_name} <{sender_addr}>"
    
    admin_email = os.getenv("ADMIN_EMAIL", "admin@fincore.co.kr")
    test_recipient = os.getenv("TEST_RECIPIENT")
    subject_prefix = os.getenv("NEWSLETTER_SUBJECT_PREFIX", "[Signalist Daily]")
    is_send_on = os.getenv("NEWSLETTER_AUTO_SEND") == "1"
    
    print(f"📧 Sending pipeline initiated for ref_date={ref_date} (env={env})")
    print(f"   From: {from_email}, Auto Send: {is_send_on}")
    
    # --- 2) 구독자용 뉴스레터 발송 ---
    try:
        html_body = load_html(ref_date)
        subject_newsletter = f"{subject_prefix} {ref_date}"
        
        subscribers = get_subscribers(env, test_recipient, is_send_on)
        
        if subscribers:
            print(f"📬 구독자 {len(subscribers)}명에게 뉴스레터를 발송합니다.")
            success = send_email_with_sendgrid(
                to_emails=subscribers, 
                subject=subject_newsletter, 
                html_body=html_body, 
                from_email=from_email
            )
            print(f"✅ 뉴스레터 발송 {'성공' if success else '실패'}.")
        else:
            print("⚠️ 구독자가 없거나 파일 오류로 발송을 건너뜁니다.")

    except FileNotFoundError as e:
        print(f"❌ 뉴스레터 HTML 파일 에러: {e}")
        
    # --- 3) 관리자용 SNS 보고서 발송 ---
    if os.getenv("SEND_SNS_REPORT_TO_ADMIN") == "1" and admin_email:
        try:
            sns_report_body = load_sns_report_txt(ref_date)
            subject_sns = f"[ADMIN REPORT] SNS Asset Summary for {ref_date}"
            html_report = f"<html><body><pre style=\"white-space: pre-wrap;\">{html_lib.escape(sns_report_body)}</pre></body></html>"
            
            print(f"📬 관리자 {admin_email}에게 SNS 보고서를 발송합니다.")
            success = send_email_with_sendgrid(
                to_emails=[admin_email], 
                subject=subject_sns, 
                html_body=html_report, 
                from_email=from_email
            )
            print(f"✅ SNS 보고서 발송 {'성공' if success else '실패'}.")
            
        except Exception as e:
            print(f"❌ SNS 보고서 발송 중 에러: {e}")