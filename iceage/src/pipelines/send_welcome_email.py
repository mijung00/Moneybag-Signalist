import os
import sys
import re
import json
import subprocess
from pathlib import Path
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# [1] 경로 설정 및 환경변수 로드
# -----------------------------------------------------------
# 이 파일의 위치(iceage/src/pipelines)에서 4단계 상위 폴더가 프로젝트 루트
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

try:
    from common.env_loader import load_env
    load_env(BASE_DIR)
    print("✅ [Welcome Email] 환경 변수 로드 성공")
except ImportError:
    print("⚠️ 'common' 폴더를 찾을 수 없습니다. 실행 위치를 확인해주세요.")
    sys.exit(1)

# [2] 설정 로더 (AWS 통합)
# -----------------------------------------------------------
class ConfigLoader:
    def __init__(self):
        self.region = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
        self.secrets_client = None

    def _get_secrets_client(self):
        if not self.secrets_client:
            self.secrets_client = boto3.client("secretsmanager", region_name=self.region)
        return self.secrets_client

    def get_env(self, key, default=None):
        value = os.getenv(key, default)
        if not value or not value.startswith("arn:aws:secretsmanager"):
            return value
        try:
            client = self._get_secrets_client()
            resp = client.get_secret_value(SecretId=value)
            secret = resp.get("SecretString")
            if secret and secret.strip().startswith("{"):
                try:
                    data = json.loads(secret)
                    return data.get(key) or data.get("value") or secret
                except json.JSONDecodeError:
                    pass
            return secret
        except ClientError:
            return value

config = ConfigLoader()

# [3] S3 매니저 및 헬퍼 함수
# -----------------------------------------------------------
try:
    from common.s3_manager import S3Manager
except ImportError:
    print("⚠️ common/s3_manager.py를 찾을 수 없거나 임포트 실패.")
    S3Manager = None

s3_manager = None
if S3Manager:
    s3_manager = S3Manager(bucket_name="fincore-output-storage")

def get_latest_report_date(service_name: str) -> str | None:
    """S3에서 서비스별 최신 리포트 날짜를 찾아 반환합니다."""
    if not s3_manager: return None
    
    prefix = "iceage/out/" if service_name == 'signalist' else "moneybag/data/out/"
    latest_report_date_str = None
    try:
        latest_file = s3_manager.get_latest_file_in_prefix(prefix)
        if latest_file:
            match = re.search(r'(\d{4}-\d{2}-\d{2})', latest_file)
            if match: latest_report_date_str = match.group(1)
    except Exception as e:
        print(f"⚠️ [S3 Error] 최신 파일 조회 실패: {e}")
    return latest_report_date_str

def send_simple_welcome_email(recipient_email: str):
    """SendGrid를 사용하여 간단한 환영 이메일을 보냅니다."""
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail

    api_key = config.get_env("SENDGRID_API_KEY")
    if not api_key:
        print("❌ [Email Error] SendGrid API Key가 없습니다.")
        return

    subject = "[Fincore] The Signalist 구독해주셔서 감사합니다!"
    body = f"""
    <p>Fincore의 The Signalist 서비스를 구독해주셔서 감사합니다.</p>
    <p>매일 아침, 국내 주식 시장의 수급 이상 징후와 변곡점 리포트를 보내드립니다.</p>
    <p>곧 첫 번째 리포트가 발송될 예정입니다. 많은 기대 바랍니다!</p>
    <br>
    <p>감사합니다.</p>
    <p>Fincore 팀 드림</p>
    """
    
    message = Mail(from_email="Fincore <admin@fincore.co.kr>", to_emails=recipient_email, subject=subject, html_content=body)
    try:
        sg = SendGridAPIClient(api_key)
        sg.send(message)
        print(f"✅ [Welcome Email] 기본 환영 이메일 발송 완료: {recipient_email}")
    except Exception as e:
        print(f"❌ [Welcome Email Error] {e}")

# [4] 메인 실행 로직
# -----------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2: sys.exit(1)
    recipient_email = sys.argv[1]
    service_name = "signalist"
    print(f"📈 [Iceage Welcome Email] 신규 구독자 환영 메일 발송 시작 -> {recipient_email}")
    latest_date = get_latest_report_date(service_name)
    if latest_date:
        print(f"✅ 최신 리포트({latest_date})를 찾았습니다. 해당 리포트를 발송합니다.")
        env = os.environ.copy()
        env["NEWSLETTER_AUTO_SEND"] = "0"
        env["TEST_RECIPIENT"] = recipient_email
        subprocess.run([sys.executable, "-m", "iceage.src.pipelines.send_newsletter", latest_date], env=env)
    else:
        print("⚠️ 발송할 최신 리포트가 없습니다. 기본 환영 이메일을 발송합니다.")
        send_simple_welcome_email(recipient_email)