import os
import sys
import pymysql
import secrets
from pathlib import Path

# -----------------------------------------------------------
# [1] 사장님 방식대로 환경변수 로더 연결 (dotenv 직접 사용 X)
# -----------------------------------------------------------
# 현재 파일이 있는 위치(루트)를 기준으로 경로 설정
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))

try:
    # 사장님이 만드신 common 폴더의 로더를 불러옵니다
    from common.env_loader import load_env
    load_env(BASE_DIR)
    print("✅ 환경 변수 로드 시스템 가동 (common.env_loader)")
except ImportError:
    # 혹시 경로가 꼬였을 때를 대비한 안전장치
    print("⚠️ 'common' 폴더를 찾을 수 없습니다. 실행 위치를 확인해주세요.")
    sys.exit(1)

# -----------------------------------------------------------
# [2] DB 연결 및 기능 정의
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

def add_subscriber(email, name):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 중복 체크
            sql_check = "SELECT id FROM subscribers WHERE email = %s"
            cursor.execute(sql_check, (email,))
            if cursor.fetchone():
                print(f"✋ 이미 등록된 이메일입니다: {email}")
                return

            # 보안 토큰 생성 및 데이터 삽입
            token = secrets.token_urlsafe(16)
            sql_insert = """
                INSERT INTO subscribers (email, name, unsubscribe_token)
                VALUES (%s, %s, %s)
            """
            cursor.execute(sql_insert, (email, name, token))
            
        conn.commit()
        print(f"🎉 [등록 성공] {name} ({email})")
        print(f"   🔑 보안 키: {token}")
        
    except Exception as e:
        print(f"❌ [에러 발생] {e}")
    finally:
        conn.close()

# -----------------------------------------------------------
# [3] 실행 영역
# -----------------------------------------------------------
if __name__ == "__main__":
    # 여기에 사장님 이메일을 적고 실행해보세요!
    target_email = "admin@fincore.co.kr" 
    target_name = "사장님"
    
    add_subscriber(target_email, target_name)