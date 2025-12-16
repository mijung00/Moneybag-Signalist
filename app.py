from flask import Flask, render_template, request, flash, redirect, url_for
import pymysql
import secrets
import os
import sys
import json
import boto3
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
# [설정 로더] AWS 환경 변수 & Secrets Manager 통합
# ----------------------------------------------------------------
class ConfigLoader:
    def __init__(self):
        # .env 파일 로드 없이, 시스템(AWS) 환경 변수를 직접 사용합니다.
        self.region = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
        self.secrets_client = None

    def _get_secrets_client(self):
        if not self.secrets_client:
            self.secrets_client = boto3.client("secretsmanager", region_name=self.region)
        return self.secrets_client

    def get_env(self, key, default=None):
        """
        1. 시스템 환경 변수(OS Env)를 읽음
        2. 값이 ARN(arn:aws:secretsmanager...)이면 Secrets Manager에서 실제 값 조회
        """
        value = os.getenv(key, default)

        # 값이 없거나 평문이면 그대로 반환
        if not value or not value.startswith("arn:aws:secretsmanager"):
            return value

        # ARN이면 Secrets Manager 조회
        try:
            client = self._get_secrets_client()
            resp = client.get_secret_value(SecretId=value)
            secret = resp.get("SecretString")
            
            # JSON 파싱 시도 (키=값 구조 대응)
            if secret and secret.strip().startswith("{"):
                try:
                    data = json.loads(secret)
                    return data.get(key) or data.get("value") or secret
                except json.JSONDecodeError:
                    pass
            
            return secret
        except ClientError as e:
            print(f"[WARN] Secret 로드 실패 ({key}): {e}")
            return value

config = ConfigLoader()

# ----------------------------------------------------------------
# [1] DB 설정 (RDS - 고객 정보 관리)
# ----------------------------------------------------------------
DB_HOST = config.get_env("DB_HOST")
DB_PORT = int(config.get_env("DB_PORT", 3306))
DB_USER = config.get_env("DB_USER")
DB_PASSWORD = config.get_env("DB_PASSWORD") # ARN일 경우 자동 변환됨
DB_NAME = config.get_env("DB_NAME")

def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# ----------------------------------------------------------------
# [2] S3 설정 (리포트 본문 - S3Manager 사용)
# ----------------------------------------------------------------
# 버킷 이름 하드코딩
TARGET_BUCKET = "fincore-output-storage"

s3_manager = None
if S3Manager:
    # AWS 인증 정보는 boto3가 환경변수에서 자동으로 찾음
    s3_manager = S3Manager(bucket_name=TARGET_BUCKET)
    print(f"[INFO] S3 Manager initialized. Bucket: {TARGET_BUCKET}")

# ----------------------------------------------------------------
# Flask 앱 시작
# ----------------------------------------------------------------
app = Flask(__name__)
app.secret_key = secrets.token_hex(16)

@app.route('/', methods=['GET', 'POST'])
def index():
    # [구독 로직: RDS 유지]
    if request.method == 'POST':
        email = request.form.get('email')
        name = request.form.get('name')
        selected_services = request.form.getlist('services') 
        agree_terms = request.form.get('agree_terms')

        if not email or not agree_terms:
            flash("이메일 입력 및 약관 동의는 필수입니다.", "error")
            return redirect(url_for('index'))

        sub_signalist = 1 if 'signalist' in selected_services else 0
        sub_moneybag = 1 if 'moneybag' in selected_services else 0 

        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM subscribers WHERE email = %s", (email,))
                if cursor.fetchone():
                    sql = "UPDATE subscribers SET is_signalist=%s, is_moneybag=%s, is_active=1 WHERE email=%s"
                    cursor.execute(sql, (sub_signalist, sub_moneybag, email))
                    flash("구독 정보가 업데이트되었습니다. ✅", "success")
                else:
                    token = secrets.token_urlsafe(16)
                    sql = "INSERT INTO subscribers (email, name, unsubscribe_token, is_signalist, is_moneybag) VALUES (%s, %s, %s, %s, %s)"
                    cursor.execute(sql, (email, name, token, sub_signalist, sub_moneybag))
                    flash(f"{name}님, 구독해주셔서 감사합니다! 🎉", "success")
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[DB Error] {e}")
            flash("일시적인 오류가 발생했습니다.", "error")
        return redirect(url_for('index'))

    return render_template('index.html')


@app.route('/archive/<service_name>')
def archive_latest(service_name):
    # 어제 날짜로 리다이렉트
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return redirect(url_for('archive_view', service_name=service_name, date_str=yesterday))

@app.route('/archive/<service_name>/<date_str>')
def archive_view(service_name, date_str):
    target_date = datetime.strptime(date_str, "%Y-%m-%d")
    today = datetime.now()
    
    # 네비게이션
    prev_date = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
    next_date = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    
    # 미래 데이터 잠금
    is_locked = target_date.date() >= today.date()

    display_name = "The Signalist" if service_name == 'signalist' else "The Whale Hunter"
    theme_color = "blue" if service_name == 'signalist' else "orange"

    content_html = None
    
    # S3 데이터 로드
    if not is_locked and s3_manager:
        if service_name == 'signalist':
            # 시그널리스트: 하루 1개
            s3_key = f"iceage/out/Signalist_Daily_{date_str}.html"
            content_html = s3_manager.get_text_content(s3_key)
            
        elif service_name == 'moneybag' or service_name == 'whalehunter':
            # [수정] 머니백: Morning & Night 두 개 다 체크
            morning_key = f"moneybag/data/out/Moneybag_Letter_Morning_{date_str}.html"
            night_key = f"moneybag/data/out/Moneybag_Letter_Night_{date_str}.html"
            
            morning_html = s3_manager.get_text_content(morning_key)
            night_html = s3_manager.get_text_content(night_key)
            
            # 두 내용을 하나로 합치기
            parts = []
            
            # 1. Morning
            if morning_html:
                parts.append(morning_html)
            
            # 2. Night (Morning이 있으면 구분선 추가)
            if night_html:
                if morning_html:
                    # 중간 구분선 (Morning과 Night 사이)
                    divider = """
                    <div style="margin: 40px 0; text-align: center; border-top: 1px dashed #ccc; position: relative;">
                        <span style="background: #fff; padding: 0 10px; position: relative; top: -12px; color: #888; font-weight: bold;">
                            🌙 Night Edition
                        </span>
                    </div>
                    """
                    parts.append(divider)
                parts.append(night_html)
            
            # 내용이 하나라도 있으면 합쳐서 저장
            if parts:
                content_html = "".join(parts)

    # 데이터 없음 처리
    if not content_html:
        if is_locked:
            msg_title = "🔒 오늘의 리포트는 준비 중입니다."
            msg_desc = "매일 아침 8시 / 저녁 9시에 발행됩니다."
        else:
            msg_title = "📭 해당 날짜의 리포트가 없습니다."
            msg_desc = f"({date_str} 데이터가 아직 S3에 없습니다)"
            
        content_html = f"""
        <div class="text-center py-12 bg-gray-50 rounded-lg border border-gray-200">
            <h3 class="text-xl text-gray-500 font-bold mb-2">{msg_title}</h3>
            <p class="text-gray-400">{msg_desc}</p>
        </div>
        """

    return render_template(
        'archive_view.html',
        service_name=service_name,
        display_name=display_name,
        theme_color=theme_color,
        date_str=date_str,
        content_html=content_html,
        prev_date=prev_date,
        next_date=next_date,
        is_locked=is_locked,
        today_str=today.strftime("%Y-%m-%d")
    )

if __name__ == '__main__':
    app.run(debug=True, port=5000)