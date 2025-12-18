import os
import sys
import json
import secrets
import pymysql
import boto3
import re
import subprocess
from flask import Flask, render_template, request, flash, redirect, url_for
from pathlib import Path
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from threading import Thread

# ----------------------------------------------------------------
# [1] 기본 설정 및 경로
# ----------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))

# S3Manager 가져오기 (배포 환경 고려)
try:
    from common.s3_manager import S3Manager
except ImportError:
    print("⚠️ common/s3_manager.py를 찾을 수 없거나 임포트 실패.")
    S3Manager = None

# [중요] AWS Elastic Beanstalk는 'application'이라는 변수를 찾습니다.
application = Flask(__name__)
app = application  # 로컬 실행 호환용 Alias
application.secret_key = secrets.token_hex(16)

# ----------------------------------------------------------------
# [2] 설정 로더 (AWS 환경변수 & Secrets Manager 통합)
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
        # 값이 없거나, 평문이면 그대로 반환
        if not value or not value.startswith("arn:aws:secretsmanager"):
            return value
        
        # ARN이면 Secrets Manager 조회
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

# DB & S3 설정 로드
DB_HOST = config.get_env("DB_HOST")
DB_PORT = int(config.get_env("DB_PORT", "3306"))
DB_USER = config.get_env("DB_USER")
DB_PASSWORD = config.get_env("DB_PASSWORD")
DB_NAME = config.get_env("DB_NAME")
TARGET_BUCKET = "fincore-output-storage" # [하드코딩]

# S3 Manager 초기화
s3_manager = None
if S3Manager:
    s3_manager = S3Manager(bucket_name=TARGET_BUCKET)
    print(f"[INFO] S3 Manager initialized. Bucket: {TARGET_BUCKET}")

# ----------------------------------------------------------------
# [2.5] S3 비용 절감을 위한 메모리 캐시
# ----------------------------------------------------------------
S3_CACHE = {}
CACHE_TTL = timedelta(hours=1) # 1시간 동안 캐시 유지 (아카이브는 정적 데이터이므로 길게 설정)

def get_s3_content_with_cache(s3_key: str) -> str | None:
    """S3 콘텐츠를 메모리 캐시와 함께 가져옵니다."""
    now = datetime.now()
    
    # 1. 캐시 확인 (유효 기간 내)
    if s3_key in S3_CACHE:
        content, timestamp = S3_CACHE[s3_key]
        if now - timestamp < CACHE_TTL:
            return content
            
    # 2. 캐시 없으면 S3에서 가져와서 저장
    try:
        content = s3_manager.get_text_content(s3_key)
        if content: S3_CACHE[s3_key] = (content, now)
        return content
    except Exception as e:
        print(f"⚠️ [S3 Read Error] {s3_key}: {e}")
        return None

# ----------------------------------------------------------------
# [3] 헬퍼 함수들 (DB연결, 스크립트 실행, HTML 정제)
# ----------------------------------------------------------------
def get_db_connection():
    """DB 연결 객체 반환"""
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, db=DB_NAME,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

def clean_html_content(raw_html):
    """S3 HTML에서 <body> 태그 내부만 추출 (스타일 격리용)"""
    if not raw_html: return None
    body_match = re.search(r'<body[^>]*>(.*?)</body>', raw_html, re.DOTALL | re.IGNORECASE)
    return body_match.group(1) if body_match else raw_html

def run_script(folder_name, module_path, args=[]):
    """
    [태스크 러너용] 특정 모듈을 서브프로세스로 실행
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    full_module_path = f"{folder_name}.{module_path}"
    cmd = [sys.executable, "-m", full_module_path] + args
    
    print(f"🚀 [Start Task] {full_module_path}")
    try:
        # cwd를 프로젝트 루트로 설정하여 실행
        result = subprocess.run(cmd, cwd=base_dir, capture_output=True, text=True, encoding='utf-8')
        print(f"✅ Output:\n{result.stdout}")
        if result.stderr:
            print(f"⚠️ Error Log:\n{result.stderr}")
        return "SUCCESS" if result.returncode == 0 else f"FAIL: {result.stderr}"
    except Exception as e:
        print(f"❌ Exception: {e}")
        return f"EXCEPTION: {str(e)}"

def send_report_email_async(service_name, date_str, recipient_email):
    """백그라운드에서 리포트 이메일을 발송하는 함수"""
    with app.app_context():
        module_name = "iceage.src.pipelines.send_newsletter" if service_name == 'signalist' else "moneybag.src.pipelines.send_email"
        
        # 환경변수를 통해 이메일과 날짜 전달
        env = os.environ.copy()
        env["NEWSLETTER_AUTO_SEND"] = "0" # 구독자 DB 무시하고 강제 발송
        env["NEWSLETTER_ENV"] = "dev" # 테스트 모드로 설정하여 TEST_RECIPIENT 사용
        env["TEST_RECIPIENT"] = recipient_email
        env["REF_DATE"] = date_str
        
        subprocess.run([sys.executable, "-m", module_name, date_str], env=env)

# ================================================================
# 🌐 [PART A] 태스크 러너 라우트 (AWS/Cron 호출용)
# ================================================================
@application.route('/run_moneybag_morning', methods=['GET', 'POST'])
def moneybag_morning():
    return run_script("moneybag", "src.pipelines.daily_runner", ["morning"]), 200

@application.route('/run_moneybag_night', methods=['GET', 'POST'])
def moneybag_night():
    return run_script("moneybag", "src.pipelines.daily_runner", ["night"]), 200

@application.route('/run_signalist', methods=['GET', 'POST'])
def signalist_morning():
    return run_script("iceage", "src.pipelines.daily_runner"), 200

@application.route('/update_stock_data', methods=['GET', 'POST'])
def update_stock_data():
    today = datetime.now()
    logs = []
    collectors = [
        "src.collectors.krx_listing_collector",
        "src.collectors.krx_index_collector",
        "src.collectors.krx_daily_price_collector"
    ]
    for i in range(3, 0, -1):
        target_date = today - timedelta(days=i)
        date_str = target_date.strftime("%Y%m%d")
        logs.append(f"Date: {date_str}")
        for module in collectors:
            msg = run_script("iceage", module, [date_str])
            logs.append(f" - {module}: {msg}")
    return "\n".join(logs), 200

# ================================================================
# 🌐 [PART B] 웹사이트 UI 라우트 (메인 & 아카이브)
# ================================================================
@application.route('/', methods=['GET', 'POST'])
def index():
    # POST 요청 통합 처리
    if request.method == 'POST':
        email = request.form.get('email')
        name = request.form.get('name')
        agree_terms = request.form.get('agree_terms')
        action = request.form.get('action') # 'unlock' 또는 None

        # [수정] 처리 후 돌아갈 페이지 주소 (기본값: 메인)
        redirect_url = request.referrer or url_for('index')

        # 1. 유효성 검사 (공통)
        if not email or not agree_terms:
            flash("이메일 입력 및 약관 동의는 필수입니다.", "error")
            return redirect(redirect_url)

        # 2. 구독자 DB 처리
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                # 기존 구독자 체크
                cursor.execute("SELECT id, is_signalist, is_moneybag FROM subscribers WHERE email = %s", (email,))
                existing_user = cursor.fetchone()

                # 구독할 서비스 결정
                sub_signalist = 0
                sub_moneybag = 0
                if action == 'unlock':
                    service_name = request.form.get('service_name')
                    if service_name == 'signalist':
                        sub_signalist = 1
                    else:
                        sub_moneybag = 1
                else: # 메인 폼
                    selected_services = request.form.getlist('services')
                    sub_signalist = 1 if 'signalist' in selected_services else 0
                    sub_moneybag = 1 if 'moneybag' in selected_services else 0

                if existing_user:
                    # 기존 유저: 구독 정보 업데이트 (기존 구독 유지하며 추가)
                    new_signalist = max(existing_user['is_signalist'], sub_signalist)
                    new_moneybag = max(existing_user['is_moneybag'], sub_moneybag)
                    sql = "UPDATE subscribers SET is_signalist=%s, is_moneybag=%s, is_active=1 WHERE id=%s"
                    cursor.execute(sql, (new_signalist, new_moneybag, existing_user['id']))
                    flash("구독 정보가 업데이트되었습니다. ✅", "success")
                else:
                    # 신규 유저: 새로 추가
                    token = secrets.token_urlsafe(16)
                    sql = "INSERT INTO subscribers (email, name, unsubscribe_token, is_signalist, is_moneybag) VALUES (%s, %s, %s, %s, %s)"
                    cursor.execute(sql, (email, name, token, sub_signalist, sub_moneybag))
                    flash(f"{name or '독자'}님, 구독해주셔서 감사합니다! 🎉", "success")
                
                # [중요] 이메일 발송 전에 먼저 커밋해서 구독 정보 저장 확실히 하기
                conn.commit()

                # [수정] 신규/기존 상관없이 구독 신청한 서비스의 최신 리포트 발송
                if sub_signalist:
                    latest_signalist_date = get_latest_report_date('signalist')
                    if latest_signalist_date:
                        Thread(target=send_report_email_async, args=('signalist', latest_signalist_date, email)).start()
                        flash("시그널리스트 최신 리포트를 메일로 보내드렸습니다.", "info")
                if sub_moneybag:
                    latest_moneybag_date = get_latest_report_date('moneybag')
                    if latest_moneybag_date:
                        Thread(target=send_report_email_async, args=('moneybag', latest_moneybag_date, email)).start()
                        flash("웨일헌터 최신 리포트를 메일로 보내드렸습니다.", "info")

        except Exception as e:
            print(f"[DB Error] {e}")
            flash("일시적인 오류가 발생했습니다.", "error")
            if conn and conn.open: conn.close()
            return redirect(redirect_url)
        finally:
            if conn and conn.open:
                conn.close()

        if action == 'unlock':
            # 잠금 해제 요청: 현재 보고 있는 리포트 발송
            service_name = request.form.get('service_name')
            date_str = request.form.get('date_str')
            Thread(target=send_report_email_async, args=(service_name, date_str, email)).start()
            flash(f"{email}으로 해당 리포트를 발송했습니다. 🚀", "info")
            return redirect(redirect_url)
        else:
            # 메인 폼 구독: 최신 리포트 발송
            if sub_signalist:
                latest_date = get_latest_report_date('signalist')
                if latest_date:
                    Thread(target=send_report_email_async, args=('signalist', latest_date, email)).start()
                    flash("시그널리스트 최신 리포트를 메일로 보내드렸습니다.", "info")
            if sub_moneybag:
                latest_date = get_latest_report_date('moneybag')
                if latest_date:
                    Thread(target=send_report_email_async, args=('moneybag', latest_date, email)).start()
                    flash("웨일헌터 최신 리포트를 메일로 보내드렸습니다.", "info")
            return redirect(redirect_url)

    # GET 요청
    return render_template('index.html')

def get_latest_report_date(service_name: str) -> str | None:
    """S3에서 서비스별 최신 리포트 날짜를 찾아 반환합니다."""
    if not s3_manager: return None
    
    prefix = "iceage/out/" if service_name == 'signalist' else "moneybag/data/out/"
    latest_report_date_str = None
    try:
        # 1. S3Manager 메서드 시도
        latest_file = s3_manager.get_latest_file_in_prefix(prefix)
        if latest_file:
            match = re.search(r'(\d{4}-\d{2}-\d{2})', latest_file)
            if match: latest_report_date_str = match.group(1)
    except Exception as e:
        print(f"⚠️ [S3 Error] 최신 파일 조회 실패: {e}")
    return latest_report_date_str

@application.route('/archive/<service_name>')
def archive_latest(service_name):
    # [수정] 무조건 어제가 아니라, 실제 S3에 있는 '가장 최신 날짜'로 이동
    latest_date = get_latest_report_date(service_name)
    if latest_date:
        return redirect(url_for('archive_view', service_name=service_name, date_str=latest_date))
    
    # 파일이 하나도 없으면 오늘 날짜로 이동 (가서 '없음' 메시지 띄움)
    today = datetime.now().strftime("%Y-%m-%d")
    return redirect(url_for('archive_view', service_name=service_name, date_str=today))

@application.route('/archive/<service_name>/<date_str>')
def archive_view(service_name, date_str):
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return redirect(url_for('archive_latest', service_name=service_name))

    # [수정] "가장 최신 리포트 1개"를 잠그는 로직
    # S3에서 해당 서비스의 가장 최신 파일 날짜를 가져옴
    latest_report_date_str = get_latest_report_date(service_name)
    
    prev_date = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
    next_date = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
    # [수정] 최신 리포트 날짜와 같거나 더 미래의 날짜(아직 안 온 날짜 포함)는 모두 잠금
    is_locked = (latest_report_date_str is not None) and (date_str >= latest_report_date_str)
    display_name = "The Signalist" if service_name == 'signalist' else "The Whale Hunter"

    content_html = None
    
    # [수정] 잠금 상태라도 블러 효과(배경)를 위해 데이터는 로드함
    if s3_manager:
        if service_name == 'signalist':
            s3_key = f"iceage/out/Signalist_Daily_{date_str}.html"
            raw_html = get_s3_content_with_cache(s3_key)
            content_html = clean_html_content(raw_html)
            
        elif service_name == 'moneybag' or service_name == 'whalehunter':
            # [수정] 머니백은 Morning/Night 리포트를 합쳐서 보여줌
            morning_key = f"moneybag/data/out/Moneybag_Letter_Morning_{date_str}.html"
            night_key = f"moneybag/data/out/Moneybag_Letter_Night_{date_str}.html"
            
            # 머니백은 Morning/Night 두 개를 합쳐서 보여줌
            morning_html = clean_html_content(get_s3_content_with_cache(morning_key))
            night_html = clean_html_content(get_s3_content_with_cache(night_key))
            
            parts = []
            if morning_html:
                parts.append('<h2>☀️ Morning Report</h2>')
                parts.append(morning_html)
            if night_html:
                if morning_html:
                    # 중간 구분선
                    parts.append('<div style="margin: 80px 0; border-top: 2px dashed #e5e7eb;"></div><h2>🌙 Night Report</h2>')
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
        today_str=datetime.now().strftime("%Y-%m-%d")
    )

@application.route('/health')
def health_check():
    return "OK", 200

if __name__ == '__main__':
    application.run(port=5000, debug=True)