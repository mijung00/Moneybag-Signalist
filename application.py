import os
import sys
import json
import logging
import secrets
import pymysql
import boto3
import re
from flask import Flask, render_template, request, flash, redirect, url_for, Response
import markdown
from pathlib import Path
from datetime import datetime, timedelta
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadTimeSignature
from botocore.exceptions import ClientError
from threading import Thread
from dotenv import load_dotenv

# 새로 분리된 공유 설정 로더를 임포트합니다.
from common.config import config

# [FIX] Load .env file only in local development, not on the server.
# The existence of the Beanstalk env file is a reliable indicator of the server environment.
if not os.path.exists('/opt/elasticbeanstalk/deployment/env'):
    load_dotenv()

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

# [수정] SECRET_KEY를 환경변수에서 가져오도록 변경 (서버 재시작 시에도 토큰 유지를 위함)
application.secret_key = os.getenv('SECRET_KEY', secrets.token_hex(16))
# 구독 취소 토큰을 위한 Serializer 초기화
s = URLSafeTimedSerializer(application.secret_key)

# ----------------------------------------------------------------
# [2] 설정 로더 (AWS 환경변수 & Secrets Manager 통합)
# ----------------------------------------------------------------
# DB & S3 설정 로드
DB_HOST = config.ensure_secret("DB_HOST")
DB_PORT = int(config.ensure_secret("DB_PORT", "3306"))
DB_USER = config.ensure_secret("DB_USER")
DB_PASSWORD = config.ensure_secret("DB_PASSWORD")
DB_NAME = config.ensure_secret("DB_NAME")
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
# [2.6] [NEW] 칼럼 데이터 로더 (JSON 기반)
# ----------------------------------------------------------------
COLUMN_DATA = []
COLUMN_DATA_BY_SLUG = {}

def load_column_data():
    """
    data/columns.json 파일에서 칼럼 메타데이터를 로드하고, 정렬 후 캐시합니다.
    """
    global COLUMN_DATA, COLUMN_DATA_BY_SLUG
    try:
        json_path = BASE_DIR / "data" / "columns.json"
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 날짜를 기준으로 최신순으로 정렬하고, 표시용 날짜 문자열 추가
        for item in data:
            dt = datetime.strptime(item['date'], '%Y-%m-%d')
            item['date_obj'] = dt
            item['date_str'] = dt.strftime('%Y년 %m월 %d일')

        data.sort(key=lambda x: x['date_obj'], reverse=True)
        
        COLUMN_DATA = data
        COLUMN_DATA_BY_SLUG = {item['slug']: item for item in data}
        print("✅ [Columns] 인사이트 칼럼 데이터 로드 완료.")
    except FileNotFoundError:
        print("⚠️ [Columns] data/columns.json 파일을 찾을 수 없습니다. 칼럼 기능이 비활성화됩니다.")
    except Exception as e:
        print(f"❌ [Columns] 칼럼 데이터 로드 중 오류 발생: {e}")

# ----------------------------------------------------------------
# [3] 헬퍼 함수들 (DB연결, 스크립트 실행, HTML 정제)
# ----------------------------------------------------------------
def get_db_connection():
    """DB 연결 객체 반환"""
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, db=DB_NAME,
        charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

def clean_html_content(raw_html: str) -> tuple[str, str]:
    """
    S3 HTML에서 스타일과 본문 내용을 분리, 가독성 보정 및 푸터 제거를 수행합니다.
    Returns: A tuple of (styles, body_content).
    """
    if not raw_html: return (None, None)

    # 1. 스타일 추출 및 폰트 보정
    head_match = re.search(r'<head[^>]*>(.*?)</head>', raw_html, re.DOTALL | re.IGNORECASE)
    style_tags = ''
    if head_match:
        original_styles = ''.join(re.findall(r'<style[^>]*>.*?</style>', head_match.group(1), re.DOTALL | re.IGNORECASE))
        # [가독성 개선] 이메일의 font-weight 스타일을 제거하여 브라우저 기본값(Pretendard)을 따르도록 함
        style_tags = re.sub(r'font-weight\s*:\s*[\d\w-]+\s*;?', '', original_styles, flags=re.IGNORECASE)

    # 2. <body>에서 내용 추출
    body_match = re.search(r'<body[^>]*>(.*?)</body>', raw_html, re.DOTALL | re.IGNORECASE)
    body_content = body_match.group(1) if body_match else raw_html

    # 3. 푸터 제거 로직 (주석 마커 방식 우선)
    # 3-1. [NEW] 가장 확실하고 안정적인 방법: 주석 마커를 찾아 제거
    comment_marker = '<!-- FINCORE_FOOTER_START -->'
    marker_pos = body_content.find(comment_marker)
    if marker_pos != -1:
        body_content = body_content[:marker_pos]
        return (style_tags, body_content.strip())

    # 3-2. [Fallback] 주석 마커가 없는 구형 템플릿을 위한 예비 로직
    markers = ["(주)비제이유앤아이", "더 이상 수신을 원하지 않으시면", "본 메일은 투자 참고용이며"]
    cut_pos = len(body_content)

    for marker in markers:
        pos = body_content.rfind(marker)
        if pos != -1:
            # 마커 바로 앞에 있는 푸터 컨테이너의 시작점을 찾습니다.
            # 우선순위: div.footer > hr > table 순으로 탐색
            footer_div_pos = body_content.rfind('<div class="footer"', 0, pos)
            footer_hr_pos = body_content.rfind('<hr', 0, pos)
            footer_table_pos = body_content.rfind('<table', 0, pos)
            
            possible_starts = [p for p in [footer_div_pos, footer_hr_pos, footer_table_pos] if p != -1]
            if possible_starts:
                # 발견된 시작점들 중 마커와 가장 가까운(가장 큰 값) 것을 선택
                cut_pos = min(cut_pos, max(possible_starts))

    if cut_pos < len(body_content):
        body_content = body_content[:cut_pos]

    return (style_tags, body_content.strip())

def send_report_email_async(service_name, date_str, recipient_email):
    """백그라운드에서 리포트 이메일을 발송하는 함수 (subprocess 제거 리팩토링)"""
    with app.app_context():
        try:
            # 시크릿 로드 보장
            config.ensure_secret("SENDGRID_API_KEY")
            
            # 환경 변수를 직접 설정하여 컨텍스트 전달
            os.environ["NEWSLETTER_AUTO_SEND"] = "0"
            os.environ["TEST_RECIPIENT"] = recipient_email

            if service_name == 'signalist':
                from iceage.src.pipelines import send_newsletter as iceage_sender
                logging.info(f"Sending Signalist report for {date_str} to {recipient_email}")
                iceage_sender.main(date_str)
            else: # moneybag or whalehunter
                from moneybag.src.pipelines import send_email as moneybag_sender
                logging.info(f"Sending Moneybag report for {date_str} to {recipient_email}")
                moneybag_sender.main(date_str)

        except Exception as e:
            logging.error(f"Failed to send report email: {e}", exc_info=True)

def send_welcome_email_async(service_name, recipient_email):
    """[NEW] 신규 구독자에게 환영 메일을 발송하는 전용 함수"""
    # [리팩토링] subprocess 대신 파이썬 함수를 직접 호출합니다.
    with app.app_context():
        try:
            if service_name == 'iceage':
                from iceage.src.pipelines import send_welcome_email as iceage_welcome
                iceage_welcome.main(recipient_email)
            elif service_name == 'moneybag':
                from moneybag.src.pipelines import send_welcome_email as moneybag_welcome
                moneybag_welcome.main(recipient_email)
            logging.info(f"Welcome email sent to {recipient_email} for {service_name}")
        except Exception as e:
            logging.error(f"Failed to send welcome email: {e}", exc_info=True)

def send_inquiry_email_async(to_email, subject, body, sender_email):
    """[NEW] 백그라운드에서 제휴문의 이메일을 발송하는 함수 (앱 컨텍스트 포함)"""
    with app.app_context():
        send_simple_email(to_email, subject, body, sender_email)

def send_simple_email(to_email, subject, body, sender_email):
    """SendGrid를 사용하여 간단한 텍스트 이메일을 보냅니다."""
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail

    api_key = config.ensure_secret("SENDGRID_API_KEY")
    if not api_key:
        print("❌ [Email Error] SendGrid API Key가 없습니다.")
        return False
    
    # [수정] 보내는 사람 이름을 Fincore로 고정, 답장 주소는 문의한 사람의 이메일로 설정
    from_email = "Fincore <admin@fincore.co.kr>"

    message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject=subject,
        html_content=f"<pre style='font-family: sans-serif; white-space: pre-wrap; font-size: 14px;'>{body}</pre>"
    )

    try:
        sg = SendGridAPIClient(api_key)
        sg.send(message)
        print(f"✅ [Inquiry Email Sent] To: {to_email}, Subject: {subject}")
        return True
    except Exception as e:
        print(f"❌ [Inquiry Email Error] {e}")
        return False

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
                    flash("구독 정보가 업데이트되었습니다. 확인 이메일을 곧 보내드립니다. 🚀", "success")
                else:
                    # 신규 유저: 새로 추가
                    token = secrets.token_urlsafe(16)
                    sql = "INSERT INTO subscribers (email, name, unsubscribe_token, is_signalist, is_moneybag) VALUES (%s, %s, %s, %s, %s)"
                    cursor.execute(sql, (email, name, token, sub_signalist, sub_moneybag))
                    flash(f"{name or '독자'}님, 구독해주셔서 감사합니다! 환영 이메일을 곧 보내드립니다. 🚀", "success")
                
                # [중요] 이메일 발송 전에 먼저 커밋해서 구독 정보 저장 확실히 하기
                conn.commit()

                # 신규 구독 서비스에 대한 환영 메일 발송
                if sub_signalist:
                    Thread(target=send_welcome_email_async, args=('iceage', email)).start()
                if sub_moneybag:
                    Thread(target=send_welcome_email_async, args=('moneybag', email)).start()

        except Exception as e:
            print(f"[DB Error] {e}")
            flash("일시적인 오류가 발생했습니다.", "error")
            if conn and conn.open: conn.close()
            return redirect(redirect_url)
        finally:
            if conn and conn.open:
                conn.close()

        if action == 'unlock':
            # [유지] 잠금 해제 요청: 현재 보고 있는 '특정 날짜' 리포트 발송 (기존 로직 유지)
            service_name = request.form.get('service_name')
            date_str = request.form.get('date_str')
            Thread(target=send_report_email_async, args=(service_name, date_str, email)).start()
            flash(f"{email}으로 해당 리포트를 발송했습니다. 🚀", "info")
        
        return redirect(redirect_url)

    # GET 요청
    # [수정] 최근 콘텐츠(리포트 + 칼럼) 정보 가져오기 및 정렬
    recent_items = []
    try:
        # 1. 시그널리스트 최신 리포트
        signalist_latest_date_str = get_latest_report_date('signalist')
        if signalist_latest_date_str:
            date_obj = datetime.strptime(signalist_latest_date_str, "%Y-%m-%d")
            recent_items.append({
                'display_name': 'The Signalist',
                'title': f"The Signalist 리포트",
                'date_obj': date_obj,
                'date_str': date_obj.strftime('%Y-%m-%d'),
                'service_name': 'signalist',
                'url': url_for('archive_view', service_name='signalist', date_str=signalist_latest_date_str)
            })
        
        # 2. 웨일헌터 최신 리포트
        moneybag_latest_date_str = get_latest_report_date('moneybag')
        if moneybag_latest_date_str:
            date_obj = datetime.strptime(moneybag_latest_date_str, "%Y-%m-%d")
            recent_items.append({
                'display_name': 'The Whale Hunter',
                'title': f"The Whale Hunter 리포트",
                'date_obj': date_obj,
                'date_str': date_obj.strftime('%Y-%m-%d'),
                'service_name': 'moneybag',
                'url': url_for('archive_view', service_name='moneybag', date_str=moneybag_latest_date_str)
            })

        # 3. 인사이트 최신 칼럼
        if COLUMN_DATA:
            latest_column = COLUMN_DATA[0] # 데이터 로드 시 이미 최신순으로 정렬됨
            recent_items.append({
                'display_name': '인사이트',
                'title': latest_column['title'],
                'date_obj': latest_column['date_obj'],
                'date_str': latest_column['date_obj'].strftime('%Y-%m-%d'), # 표시 형식 통일
                'service_name': 'insights',
                'url': url_for('column_view', slug=latest_column['slug'])
            })
        
        # 4. 모든 아이템을 날짜순으로 정렬 (최신순)
        recent_items.sort(key=lambda x: x['date_obj'], reverse=True)
    except Exception as e:
        print(f"⚠️ [Recent Items Error] {e}")

    page_title = "FINCORE | 데이터 기반 투자 분석"
    page_description = "Fincore는 데이터 기반의 투자 분석을 제공하여 감정에 휘둘리지 않는 객관적인 투자를 돕는 플랫폼입니다."
    return render_template('index.html', page_title=page_title, page_description=page_description, recent_reports=recent_items)


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
    
    # [수정] SEO를 위한 동적 메타 태그 생성 (용어 변경)
    page_title = f"{display_name} {date_str} 리포트 | FINCORE"
    page_description = f"{display_name}의 {date_str} 리포트입니다. 주요 시장 분석과 데이터를 확인하세요."
    if service_name == 'signalist':
        page_description = f"시그널리스트 {date_str} 리포트. 국내 주식 시장의 수급 데이터와 변곡점 분석을 제공합니다."
    elif service_name == 'moneybag':
        page_description = f"웨일헌터 {date_str} 리포트. 암호화폐 시장의 고래 움직임을 추적하여 변동성에 대응하는 데이터를 제공합니다."


    all_styles = []
    all_body_parts = []
    content_html = None    

    if s3_manager:
        if service_name == 'signalist':
            s3_key = f"iceage/out/Signalist_Daily_{date_str}.html"
            raw_html = get_s3_content_with_cache(s3_key)
            styles, body = clean_html_content(raw_html) if raw_html else (None, None)
            if styles: all_styles.append(styles)
            if body: all_body_parts.append(body)
            
        elif service_name == 'moneybag' or service_name == 'whalehunter':
            morning_key = f"moneybag/data/out/Moneybag_Letter_Morning_{date_str}.html"
            night_key = f"moneybag/data/out/Moneybag_Letter_Night_{date_str}.html"
            
            raw_morning_html = get_s3_content_with_cache(morning_key)
            morning_styles, morning_body = clean_html_content(raw_morning_html) if raw_morning_html else (None, None)
            
            raw_night_html = get_s3_content_with_cache(night_key)
            night_styles, night_body = clean_html_content(raw_night_html) if raw_night_html else (None, None)
            
            if morning_styles: all_styles.append(morning_styles)
            if night_styles: all_styles.append(night_styles)
            
            if morning_body:
                all_body_parts.append('<h2>☀️ Morning Report</h2>' + morning_body)
            if night_body:
                if morning_body: all_body_parts.append('<div style="margin: 60px 0; border-top: 2px dashed #e5e7eb;"></div>')
                all_body_parts.append('<h2>🌙 Night Report</h2>' + night_body)

    if all_body_parts:
        unique_styles = "".join(list(dict.fromkeys(all_styles)))
        
        # [수정] 공유/구독 버튼은 iframe 외부로 이동했으므로, 여기서는 본문만 생성
        full_body = "".join(all_body_parts)
        # [중요] iframe에서 사용할 것이므로, 완전한 HTML 구조를 만듭니다.
        # [수정] iframe 내부에서는 외부 스크립트가 동작하지 않으므로, 스크립트 태그는 제거
        content_html = f"<!DOCTYPE html><html><head><meta charset='UTF-8'><style>{unique_styles}</style></head><body>{full_body}</body></html>"

    return render_template(
        'archive_view.html',
        service_name=service_name,
        display_name=display_name,
        date_str=date_str,
        content_html=content_html,
        prev_date=prev_date,
        next_date=next_date,
        is_locked=is_locked,
        today_str=datetime.now().strftime("%Y-%m-%d"),
        page_title=page_title,
        page_description=page_description
    )


# ================================================================
# 🌐 [PART B-2] [NEW] 인사이트 칼럼 라우트
# ================================================================
@application.route('/insights')
def insights():
    """인사이트 칼럼 목록 페이지를 렌더링합니다."""
    # [추가] 데이터가 로드되지 않았을 경우를 위한 디버깅 메시지
    if not COLUMN_DATA:
        flash("칼럼 데이터를 불러오는 데 실패했습니다. 서버 로그를 확인하거나 관리자에게 문의해주세요.", "error")

    # 전역으로 로드된 데이터에 각 칼럼의 URL을 동적으로 추가합니다.
    columns_with_urls = []
    for col_data in COLUMN_DATA:
        col = col_data.copy() # 원본 수정을 방지하기 위해 복사
        col['url'] = url_for('column_view', slug=col['slug'])
        columns_with_urls.append(col)

    return render_template(
        'insights.html', 
        columns=columns_with_urls, 
        page_title="Fincore 인사이트",
        page_description="데이터와 시장에 대한 깊이 있는 분석과 전망을 공유합니다."
    )

@application.route('/column/<slug>')
def column_view(slug):
    """슬러그(slug)를 기반으로 개별 칼럼 상세 페이지를 렌더링합니다."""
    column = COLUMN_DATA_BY_SLUG.get(slug)
    
    if not column:
        flash("요청하신 칼럼을 찾을 수 없습니다.", "error")
        return redirect(url_for('insights'))

    page_title = f"{column['title']} | Fincore 인사이트"
    page_description = column.get('description', "Fincore의 데이터 기반 인사이트 칼럼입니다.")
    
    # [NEW] SEO를 위한 구조화된 데이터 (JSON-LD) 생성
    structured_data = {
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": column['title'],
        "description": page_description,
        "image": url_for('static', filename='images/og_image.png', _external=True), # 대표 OG 이미지 사용
        "datePublished": column['date'],
        "author": {
            "@type": "Organization",
            "name": "Fincore",
            "url": url_for('index', _external=True)
        },
        "publisher": {
            "@type": "Organization",
            "name": "Fincore",
            "logo": {
                "@type": "ImageObject",
                "url": url_for('static', filename='images/logo.png', _external=True)
            }
        }
    }

    # 템플릿 파일이 존재하는지 확인 (안정성 강화)
    template_path = BASE_DIR / "templates" / column['template']
    if not template_path.exists():
        print(f"❌ [Template Error] 칼럼 템플릿 파일을 찾을 수 없습니다: {column['template']}")
        flash("페이지를 표시하는 중 오류가 발생했습니다.", "error")
        return redirect(url_for('insights'))

    return render_template(
        column['template'],
        page_title=page_title,
        page_description=page_description,
        structured_data_json=json.dumps(structured_data, ensure_ascii=False)
    )

@application.route('/inquiry', methods=['POST'])
def inquiry():
    """제휴문의 처리 라우트"""
    sender_email = request.form.get('email')
    message = request.form.get('message')
    redirect_url = request.referrer or url_for('index')

    if not sender_email or not message:
        flash("이메일과 문의 내용을 모두 입력해주세요.", "error")
        return redirect(redirect_url)

    admin_email = os.getenv("ADMIN_EMAIL", "admin@fincore.co.kr")
    subject = f"[Fincore 제휴문의] {sender_email} 님으로부터"
    
    body = f"""
<b>보낸 사람:</b> {sender_email}
<b>문의 시각:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
--------------------------------------------------

{message}
    """
    
    Thread(target=send_inquiry_email_async, args=(admin_email, subject, body, sender_email)).start()
    flash("문의 내용이 성공적으로 전송되었습니다. 빠른 시일 내에 회신드리겠습니다. ✅", "success")
    return redirect(redirect_url)

@application.route('/robots.txt')
def robots_txt():
    """검색 로봇 제어 규칙 파일"""
    rules = [
        "User-agent: *",
        "Allow: /",
        "",
        "# Disallow admin/backend paths",
        "Disallow: /run_moneybag_morning",
        "Disallow: /run_moneybag_night",
        "Disallow: /run_signalist",
        "Disallow: /update_stock_data",
        "",
        f"Sitemap: {url_for('sitemap_xml', _external=True)}"
    ]
    return Response("\n".join(rules), mimetype='text/plain')

@application.route('/sitemap.xml')
def sitemap_xml():
    """사이트맵 동적 생성"""
    # 1. 정적 페이지 추가
    static_urls = [
        {'loc': url_for('index', _external=True)},
        {'loc': url_for('archive_latest', service_name='signalist', _external=True)},
        {'loc': url_for('archive_latest', service_name='moneybag', _external=True)},
    ]

    # 2. 동적 페이지 (S3 아카이브) 추가
    dynamic_urls = []
    if s3_manager:
        # Signalist 리포트
        signalist_files = s3_manager.list_all_files_in_prefix("iceage/out/")
        for key in signalist_files:
            match = re.search(r'(\d{4}-\d{2}-\d{2})', key)
            if match:
                dynamic_urls.append({
                    'loc': url_for('archive_view', service_name='signalist', date_str=match.group(1), _external=True)
                })
        
        # Moneybag 리포트 (날짜 중복 제거)
        moneybag_urls = set()
        moneybag_files = s3_manager.list_all_files_in_prefix("moneybag/data/out/")
        for key in moneybag_files:
            match = re.search(r'(\d{4}-\d{2}-\d{2})', key)
            if match:
                moneybag_urls.add(
                    url_for('archive_view', service_name='moneybag', date_str=match.group(1), _external=True)
                )
        
        for url in sorted(list(moneybag_urls), reverse=True):
            dynamic_urls.append({'loc': url})

    # 3. [NEW] 인사이트 칼럼 페이지 추가
    if COLUMN_DATA:
        for column in COLUMN_DATA:
            dynamic_urls.append({
                'loc': url_for('column_view', slug=column['slug'], _external=True)
            })

    all_urls = static_urls + dynamic_urls
    
    # 3. XML 템플릿 렌더링
    try:
        xml_sitemap = render_template('sitemap.xml', urls=all_urls)
        response = Response(xml_sitemap, mimetype='application/xml')
        return response
    except Exception as e:
        print(f"❌ [Sitemap Error] {e}")
        return Response(f"Sitemap generation error: {e}", status=500, mimetype='text/plain')

@application.route('/health')
def health_check():
    return "OK", 200

@application.route('/privacy')
def privacy_policy():
    """개인정보 처리방침 페이지 렌더링"""
    try:
        md_path = BASE_DIR / "templates" / "privacy.md"

        md_content = md_path.read_text(encoding='utf-8')
        
        # 마크다운을 HTML로 변환 (테이블 확장 기능 포함)
        html_content = markdown.markdown(md_content, extensions=['tables'])
        
        page_title = "개인정보 처리방침 | FINCORE"
        return render_template('privacy.html', content_html=html_content, page_title=page_title)
    except FileNotFoundError:
        flash("개인정보 처리방침 파일을 찾을 수 없습니다.", "error")
        return redirect(url_for('index'))
    except Exception as e:
        print(f"⚠️ [Privacy Page Error] {e}")
        flash("페이지를 표시하는 중 오류가 발생했습니다.", "error")
        return redirect(url_for('index'))

# ================================================================
# 🌐 [PART C] 구독 취소 라우트
# ================================================================
@application.route('/unsubscribe/<service_name>/<token>', methods=['GET', 'POST'])
def unsubscribe(service_name, token):
    if service_name not in ['signalist', 'moneybag']:
        flash('잘못된 접근입니다.', 'error')
        return redirect(url_for('index'))

    try:
        # 암호화된 토큰을 복호화하여 이메일 주소를 얻습니다. (유효시간: 30일)
        email = s.loads(token, salt='email-unsubscribe', max_age=2592000)
    except SignatureExpired:
        flash('구독 취소 링크가 만료되었습니다. 최신 이메일의 링크를 이용해주세요.', 'error')
        return redirect(url_for('index'))
    except (BadTimeSignature, Exception):
        flash('잘못된 접근입니다.', 'error')
        return redirect(url_for('index'))

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, is_active, is_signalist, is_moneybag FROM subscribers WHERE email = %s", (email,))
            subscriber = cursor.fetchone()

            if not subscriber or not subscriber['is_active']:
                flash('이미 구독이 취소되었거나 등록되지 않은 이메일입니다.', 'info')
                return redirect(url_for('index'))

            if request.method == 'POST':
                # POST 요청 시, 실제 DB에서 해당 서비스의 구독 상태를 비활성화합니다.
                update_col = 'is_signalist' if service_name == 'signalist' else 'is_moneybag'
                cursor.execute(f"UPDATE subscribers SET {update_col} = 0 WHERE id = %s", (subscriber['id'],))
                conn.commit()
                flash('뉴스레터 구독이 성공적으로 취소되었습니다.', 'success')
                return redirect(url_for('index'))
    except Exception as e:
        print(f"[DB Error] Unsubscribe failed: {e}")
        flash('구독 취소 처리 중 오류가 발생했습니다.', 'error')
        return redirect(url_for('index'))
    finally:
        if conn and conn.open: conn.close()

    display_name = "The Signalist" if service_name == 'signalist' else "The Whale Hunter"
    return render_template('unsubscribe.html', token=token, email=email, service_name=service_name, display_name=display_name)

# ================================================================
# 🌐 [PART D] [NEW] 작업자(Worker) 전용 라우트
# ================================================================
from tasks.runner import run_iceage_task, run_moneybag_task, run_krx_batch_task, run_iceage_weekly_task, run_iceage_monthly_task

@application.route('/worker/newsletter', methods=['POST'])
def worker_newsletter():
    """모닝 리포트 및 뉴스레터 발송 태스크 (시그널리스트)"""
    try:
        # run_iceage.sh newsletter 와 동일
        run_iceage_task("newsletter")
        return Response("Newsletter Task Success", status=200)
    except Exception as e:
        logging.error(f"Worker task /worker/newsletter failed: {e}", exc_info=True)
        return Response(str(e), status=500)

@application.route('/worker/moneybag-morning', methods=['POST'])
def worker_moneybag_morning():
    """머니백 모닝 리포트 발송 태스크"""
    try:
        run_moneybag_task("morning")
        return Response("Moneybag Morning Task Success", status=200)
    except Exception as e:
        logging.error(f"Worker task /worker/moneybag-morning failed: {e}", exc_info=True)
        return Response(str(e), status=500)

@application.route('/worker/krx', methods=['POST'])
def worker_krx_batch():
    """KRX 데이터 수집 배치 태스크"""
    try:
        msg = run_krx_batch_task(days=3)
        return Response(msg, status=200)
    except Exception as e:
        logging.error(f"Worker task /worker/krx failed: {e}", exc_info=True)
        return Response(str(e), status=500)

@application.route('/worker/iceage-weekly', methods=['POST'])
def worker_iceage_weekly():
    """시그널리스트 주간 리포트 발송 태스크"""
    try:
        run_iceage_weekly_task()
        return Response("IceAge Weekly Task Success", status=200)
    except Exception as e:
        logging.error(f"Worker task /worker/iceage-weekly failed: {e}", exc_info=True)
        return Response(str(e), status=500)

@application.route('/worker/iceage-monthly', methods=['POST'])
def worker_iceage_monthly():
    """시그널리스트 월간 리포트 발송 태스크"""
    try:
        run_iceage_monthly_task()
        return Response("IceAge Monthly Task Success", status=200)
    except Exception as e:
        logging.error(f"Worker task /worker/iceage-monthly failed: {e}", exc_info=True)
        return Response(str(e), status=500)

# 애플리케이션 시작 시 칼럼 데이터 로드 (모듈 임포트 시점에 실행)
load_column_data()

if __name__ == '__main__':
    application.run(port=5000, debug=True)