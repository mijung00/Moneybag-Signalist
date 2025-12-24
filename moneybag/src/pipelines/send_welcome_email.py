import os
import sys
from pathlib import Path
from itsdangerous import URLSafeTimedSerializer
from sendgrid import SendGridAPIClient
import json
from sendgrid.helpers.mail import Mail

# --- 경로 설정 ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
except IndexError:
    PROJECT_ROOT = Path.cwd()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# --- 환경 변수 로드 ---
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

# --- [NEW] Secrets Manager JSON Normalizer ---
def _normalize_json_env(env_key: str) -> None:
    raw = os.getenv(env_key, "")
    if not raw:
        return
    s = raw.strip()

    if not (s.startswith("{") and s.endswith("}")):
        return

    try:
        obj = json.loads(s)
        if not isinstance(obj, dict): return

        v = obj.get(env_key) or obj.get("value")
        if not v:
            for vv in obj.values():
                if isinstance(vv, str) and vv.strip():
                    v = vv.strip()
                    break

        if isinstance(v, str) and v.strip():
            os.environ[env_key] = v.strip()
    except Exception:
        pass

_normalize_json_env("SENDGRID_API_KEY")
_normalize_json_env("SECRET_KEY")

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
    unsubscribe_link = f"{web_base_url}/unsubscribe/moneybag/{signed_token}"
    privacy_policy_link = f"{web_base_url}/privacy"

    subject = "Fincore - The Whale Hunter 구독을 환영합니다! 🐋"
    html_content = f"""
    <p>안녕하세요, {recipient_email}님!</p>
    <p>Fincore의 The Whale Hunter 뉴스레터 구독을 환영합니다.</p>
    <p>저희는 글로벌 암호화폐 시장의 고래 움직임을 추적하여, 변동성 속에서도 기회를 포착할 수 있는 심층 분석 리포트를 매일 제공합니다.</p>
    <p>매일 아침/저녁, 고래 사냥꾼의 시선으로 시장을 분석하고, AI 트레이딩 봇의 최적 전략을 담은 '시크릿 노트'를 받아보세요.</p>
    <p>저희와 함께라면, 암호화폐 시장의 파도를 성공적으로 헤쳐나갈 수 있을 것입니다.</p>
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
        print(f"✅ [Moneybag Welcome Email Sent] To: {recipient_email}")
        return True
    except Exception as e:
        print(f"❌ [Moneybag Welcome Email Error] {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python -m moneybag.src.pipelines.send_welcome_email [recipient_email]")
        sys.exit(1)
    
    send_welcome_email(sys.argv[1])