import os
import sys
from pathlib import Path
from itsdangerous import URLSafeTimedSerializer
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# --- 경로 설정 ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
except IndexError:
    PROJECT_ROOT = Path.cwd()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# --- 환경 변수 로드 ---
from common.env_loader import load_env
load_env(PROJECT_ROOT)

class ConfigLoader:
    def get_env(self, key, default=None):
        return os.getenv(key, default)

config = ConfigLoader()

def send_welcome_email(recipient_email: str):
    sendgrid_api_key = config.get_env("SENDGRID_API_KEY")
    web_base_url = config.get_env("WEB_BASE_URL", "https://www.fincore.co.kr")
    secret_key = config.get_env('SECRET_KEY')

    if not sendgrid_api_key or not secret_key:
        print("❌ [Email Error] SENDGRID_API_KEY 또는 SECRET_KEY가 설정되지 않았습니다.")
        return False
    
    # 구독 취소 링크 생성
    s = URLSafeTimedSerializer(secret_key)
    signed_token = s.dumps(recipient_email, salt='email-unsubscribe')
    unsubscribe_link = f"{web_base_url}/unsubscribe/signalist/{signed_token}"
    privacy_policy_link = f"{web_base_url}/privacy"

    subject = "Fincore - The Signalist 구독을 환영합니다! 📈"
    html_content = f"""
    <p>안녕하세요, {recipient_email}님!</p>
    <p>Fincore의 The Signalist 뉴스레터 구독을 환영합니다.</p>
    <p>저희는 국내 주식 시장(KRX)의 수급 이상 징후와 변곡점을 포착하여, 감정에 휘둘리지 않는 객관적인 투자 시그널을 매일 제공합니다.</p>
    <p>매일 아침, 시장의 숨겨진 신호를 읽고, AI 기반의 정교한 분석을 담은 '데일리 브리핑'을 받아보세요.</p>
    <p>저희와 함께라면, 복잡한 주식 시장에서도 성공적인 투자를 이어갈 수 있을 것입니다.</p>
    <p>감사합니다.<br>Fincore 팀 드림</p>
    <hr>
    <p style="font-size: 0.8em; color: #888;">
        본 메일은 admin@fincore.co.kr 주소로 발송된 Fincore 뉴스레터입니다.<br>
        더 이상 수신을 원하지 않으시면 <a href="{unsubscribe_link}">여기를 눌러 구독을 취소해주세요</a>.
    </p>
    <p style="font-size: 0.8em; color: #888;">
        (주)비제이유앤아이 | <a href="{privacy_policy_link}">개인정보 처리방침</a><br>
        본 메일은 투자 참고용이며, 투자의 책임은 본인에게 있습니다.
    </p>
    """

    message = Mail(
        from_email="Fincore <admin@fincore.co.kr>",
        to_emails=recipient_email,
        subject=subject,
        html_content=html_content
    )

    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        sg.send(message)
        print(f"✅ [Iceage Welcome Email Sent] To: {recipient_email}")
        return True
    except Exception as e:
        print(f"❌ [Iceage Welcome Email Error] {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python -m iceage.src.pipelines.send_welcome_email [recipient_email]")
        sys.exit(1)
    
    send_welcome_email(sys.argv[1])