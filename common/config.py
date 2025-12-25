import os
import json
import logging
import boto3
from botocore.exceptions import ClientError

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
            data = json.loads(secret_str) if secret_str and secret_str.strip().startswith("{") else {}
            final_val = data.get(key) or data.get("value") or secret_str
            os.environ[key] = final_val
            return final_val
        except Exception as e:
            logging.warning(f"Secret load failed for {key}: {e}")
            return value

# 전역 설정 객체 생성
config = ConfigLoader()