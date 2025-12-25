import os
import json
import logging
import boto3
from botocore.exceptions import ClientError

def initialize_environment():
    """
    모든 모듈의 최상단에서 호출될 환경 초기화 함수입니다.
    서버 환경(EB)인지 로컬인지 판별하여 로컬에서만 .env를 로드합니다.
    """
    is_server = os.path.exists('/opt/elasticbeanstalk/deployment/env')

    if is_server:
        try:
            import dotenv
            # 서버 환경에서는 load_dotenv 함수를 '아무 일도 안 하는 가짜'로 교체합니다.
            # 이제 어느 파일에서 이 함수를 호출하든 무시됩니다.
            dotenv.load_dotenv = lambda *args, **kwargs: None
            logging.info("Server environment: load_dotenv has been neutralized.")
        except ImportError:
            # dotenv가 설치되지 않았으면 아무것도 할 필요가 없습니다.
            pass
    else: # 로컬 환경
        try:
            from dotenv import load_dotenv
            load_dotenv()
            logging.info("Local environment: .env loaded.")
        except ImportError:
            logging.warning("dotenv module not found. Skipping .env load.")

# 이 모듈이 임포트되는 순간, 환경 초기화 함수를 단 한 번 실행합니다.
initialize_environment()

class ConfigLoader:
    def __init__(self):
        self.region = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
        self.secrets_client = None

    def _get_secrets_client(self):
        if not self.secrets_client:
            self.secrets_client = boto3.client("secretsmanager", region_name=self.region)
        return self.secrets_client

    def ensure_secret(self, key, default=None):
        """기존 셸의 ensure_secret_env 로직을 파이썬으로 완벽 구현"""
        value = os.getenv(key, default)
        
        if value and not value.startswith("arn:aws:secretsmanager"):
            return value

        sid = value if value and value.startswith("arn:aws:secretsmanager") else key
        try:
            client = self._get_secrets_client()
            resp = client.get_secret_value(SecretId=sid)
            secret_str = resp.get("SecretString")
            data = json.loads(secret_str) if secret_str and secret_str.strip().startswith('{') else {}
            final_val = data.get(key) or data.get("value") or secret_str
            os.environ[key] = final_val
            return final_val
        except Exception as e:
            logging.warning(f"Secret load failed for {key}: {e}")
            return value

# 전역 설정 객체 생성
config = ConfigLoader()