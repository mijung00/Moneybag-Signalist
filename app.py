from flask import Flask, render_template, request, flash, redirect, url_for
import pymysql
import secrets
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# [1] 환경 설정
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))

try:
    from common.env_loader import load_env
    load_env(BASE_DIR)
except ImportError:
    pass # 로컬 테스트용 예외처리

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)

# [2] DB 연결
def get_db_connection():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        db=os.getenv("DB_NAME"),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# [NEW] 아카이브 데이터 가져오기 (지금은 테스트용 더미 데이터)
def get_recent_archives():
    # 나중에는 여기서 S3 파일 목록을 읽어오거나 DB에 저장된 발송 이력을 가져오면 됩니다.
    today = datetime.now()
    return [
        {
            "date": (today - timedelta(days=1)).strftime("%Y-%m-%d"),
            "type": "morning",
            "service": "Signalist",
            "title": "KOSPI, 외국인 순매수 전환... 반도체 주도주 부상",
            "link": "#" # 나중에 S3 링크 연결
        },
        {
            "date": (today - timedelta(days=1)).strftime("%Y-%m-%d"),
            "type": "night",
            "service": "Secret Note",
            "title": "비트코인 $90k 안착 시도, 알트코인 순환매 장세",
            "link": "#"
        },
        {
            "date": (today - timedelta(days=2)).strftime("%Y-%m-%d"),
            "type": "morning",
            "service": "Signalist",
            "title": "2차전지 섹터 급반등, 밸류업 프로그램 기대감",
            "link": "#"
        }
    ]

# [3] 메인 라우팅
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        email = request.form.get('email')
        name = request.form.get('name')
        selected_services = request.form.getlist('services') 
        agree_terms = request.form.get('agree_terms')

        if not email:
            flash("이메일을 입력해주세요!", "error")
            return redirect(url_for('index'))
        
        if not selected_services:
            flash("최소 하나의 리포트를 선택해주세요.", "error")
            return redirect(url_for('index'))

        if not agree_terms:
            flash("이용 약관 및 개인정보 수집에 동의해주세요.", "error")
            return redirect(url_for('index'))

        sub_signalist = 1 if 'signalist' in selected_services else 0
        sub_moneybag = 1 if 'moneybag' in selected_services else 0

        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                # 중복 확인 및 업데이트 로직 (기존과 동일)
                check_sql = "SELECT id, name FROM subscribers WHERE email = %s"
                cursor.execute(check_sql, (email,))
                existing_user = cursor.fetchone()

                if existing_user:
                    update_sql = """
                        UPDATE subscribers 
                        SET is_signalist = %s, is_moneybag = %s, is_active = 1
                        WHERE email = %s
                    """
                    cursor.execute(update_sql, (sub_signalist, sub_moneybag, email))
                    flash(f"반갑습니다, {existing_user['name']}님! 구독 설정이 업데이트되었습니다. ✅", "success")
                else:
                    token = secrets.token_urlsafe(16)
                    insert_sql = """
                        INSERT INTO subscribers (email, name, unsubscribe_token, is_signalist, is_moneybag) 
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, (email, name, token, sub_signalist, sub_moneybag))
                    flash(f"환영합니다, {name}님! 구독 신청이 완료되었습니다. 🎉", "success")
            
            conn.commit()
            conn.close()
        except Exception as e:
            flash(f"일시적인 오류가 발생했습니다: {e}", "error")
            
        return redirect(url_for('index'))

    # GET 요청 시 아카이브 목록도 같이 보냄
    archives = get_recent_archives()
    return render_template('index.html', archives=archives)

if __name__ == '__main__':
    app.run(debug=True, port=5000)