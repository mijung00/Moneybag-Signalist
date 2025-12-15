from flask import Flask, render_template, request, flash, redirect, url_for
import pymysql
import secrets
import os
import sys
from pathlib import Path

# -----------------------------------------------------------
# [1] 환경 설정 (common.env_loader 사용)
# -----------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))

try:
    from common.env_loader import load_env
    load_env(BASE_DIR)
except ImportError:
    print("⚠️ common 폴더를 찾을 수 없습니다.")

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)  # 메시지 깜빡임(Flash) 기능을 위해 필요

# -----------------------------------------------------------
# [2] DB 연결 함수
# -----------------------------------------------------------
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

# -----------------------------------------------------------
# [3] 페이지 라우팅 (길 안내)
# -----------------------------------------------------------

# 메인 페이지 (구독 신청 화면)
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # 사용자가 폼에 입력한 내용 가져오기
        email = request.form.get('email')
        name = request.form.get('name')

        if not email:
            flash("이메일을 입력해주세요!", "error")
            return redirect(url_for('index'))

        # DB에 저장 시도
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                # 중복 체크
                check_sql = "SELECT id FROM subscribers WHERE email = %s"
                cursor.execute(check_sql, (email,))
                if cursor.fetchone():
                    flash("이미 구독 중인 이메일입니다. 😉", "warning")
                else:
                    # 저장
                    token = secrets.token_urlsafe(16)
                    insert_sql = "INSERT INTO subscribers (email, name, unsubscribe_token) VALUES (%s, %s, %s)"
                    cursor.execute(insert_sql, (email, name, token))
                    conn.commit()
                    flash(f"환영합니다, {name}님! 구독이 완료되었습니다. 🎉", "success")
            conn.close()
        except Exception as e:
            flash(f"에러가 발생했습니다: {e}", "error")
            
        return redirect(url_for('index'))

    return render_template('index.html')

if __name__ == '__main__':
    # 로컬에서 테스트할 때만 실행됨
    app.run(debug=True, port=5000)