from flask import Flask, render_template, request, flash, redirect, url_for
import pymysql
import secrets
import os
import sys
import json
import boto3
import re  # [추가] 정규표현식 사용
from pathlib import Path
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# [경로 설정] common 패키지 import용
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))

# S3Manager 가져오기
try:
    from common.s3_manager import S3Manager
except ImportError:
    print("⚠️ common/s3_manager.py를 찾을 수 없습니다.")
    S3Manager = None

# ----------------------------------------------------------------
# [설정 로더]
# ----------------------------------------------------------------
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

# ----------------------------------------------------------------
# [DB & S3 설정]
# ----------------------------------------------------------------
DB_HOST = config.get_env("DB_HOST")
DB_PORT = int(config.get_env("DB_PORT", "3306"))
DB_USER = config.get_env("DB_USER")
DB_PASSWORD = config.get_env("DB_PASSWORD")
DB_NAME = config.get_env("DB_NAME")
TARGET_BUCKET = "fincore-output-storage"

s3_manager = None
if S3Manager:
    s3_manager = S3Manager(bucket_name=TARGET_BUCKET)
    print(f"[INFO] S3 Manager initialized. Bucket: {TARGET_BUCKET}")

# ----------------------------------------------------------------
# [핵심] HTML 정제 함수 (수술 도구)
# ----------------------------------------------------------------
def clean_html_content(raw_html):
    if not raw_html: return None
    
    # 1. <body> 태그 안의 내용만 추출 (re.DOTALL로 줄바꿈 포함)
    body_match = re.search(r'<body[^>]*>(.*?)</body>', raw_html, re.DOTALL | re.IGNORECASE)
    if body_match:
        content = body_match.group(1)
    else:
        # body 태그가 없으면 전체 사용
        content = raw_html

    # 2. <style> 태그 제거 (이게 있으면 전체 레이아웃을 망가뜨림)
    #    (S3 리포트 내부 스타일이 꼭 필요하다면 이 부분은 주석 처리해야 하지만,
    #     지금처럼 레이아웃이 깨지는 걸 막으려면 제거하거나 격리해야 함. 
    #     우선 제거하고 archive_view.html의 .report-reset으로 제어하는 게 가장 깔끔함)
    # content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
    
    # [수정] 스타일을 완전히 제거하면 표 등이 깨질 수 있으니, 
    # body 추출만으로도 대부분의 margin/padding 문제는 해결됨.
    return content

# ----------------------------------------------------------------
# Flask 앱 시작
# ----------------------------------------------------------------
application = Flask(__name__)
app = application
app.secret_key = secrets.token_hex(16)

def get_db_connection():
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, db=DB_NAME,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # (구독 로직 생략 - 기존과 동일)
        pass
    return render_template('index.html')

@app.route('/archive/<service_name>')
def archive_latest(service_name):
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return redirect(url_for('archive_view', service_name=service_name, date_str=yesterday))

@app.route('/archive/<service_name>/<date_str>')
def archive_view(service_name, date_str):
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return redirect(url_for('archive_latest', service_name=service_name))

    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")
    
    prev_date = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
    next_date = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    is_locked = target_date.date() >= today.date()
    display_name = "The Signalist" if service_name == 'signalist' else "The Whale Hunter"

    content_html = None
    
    if not is_locked and s3_manager:
        if service_name == 'signalist':
            s3_key = f"iceage/out/Signalist_Daily_{date_str}.html"
            # [수정] clean_html_content 적용
            raw_html = s3_manager.get_text_content(s3_key)
            content_html = clean_html_content(raw_html)
            
        elif service_name == 'moneybag' or service_name == 'whalehunter':
            morning_key = f"moneybag/data/out/Moneybag_Letter_Morning_{date_str}.html"
            night_key = f"moneybag/data/out/Moneybag_Letter_Night_{date_str}.html"
            
            # [수정] clean_html_content 적용
            morning_html = clean_html_content(s3_manager.get_text_content(morning_key))
            night_html = clean_html_content(s3_manager.get_text_content(night_key))
            
            parts = []
            if morning_html: parts.append(morning_html)
            if night_html:
                if morning_html:
                    parts.append('<div style="margin: 60px 0; border-top: 2px dashed #e5e7eb;"></div>')
                parts.append(night_html)
            if parts:
                content_html = "".join(parts)

    return render_template(
        'archive_view.html',
        service_name=service_name,
        display_name=display_name,
        date_str=date_str,
        content_html=content_html,
        prev_date=prev_date,
        next_date=next_date,
        is_locked=is_locked,
        today_str=today_str 
    )

if __name__ == '__main__':
    app.run(debug=True, port=5000)