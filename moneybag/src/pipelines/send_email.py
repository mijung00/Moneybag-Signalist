import os
import markdown
import time
import math
from datetime import datetime
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
# 👇 [수정] Personalization 및 Substitution 모듈 추가
from sendgrid.helpers.mail import Mail, To, Personalization, Substitution
from itsdangerous import URLSafeTimedSerializer
import re
# [추가] SSH 터널링 라이브러리
try:
    from sshtunnel import SSHTunnelForwarder
except ImportError:
    SSHTunnelForwarder = None

# 프로젝트 루트 경로
BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

OUTPUT_DIR = BASE_DIR / "moneybag" / "data" / "out"

class EmailSender:
    def __init__(self):
        self.api_key = os.getenv("SENDGRID_API_KEY")
        sender_name = os.getenv("MONEYBAG_SENDER_NAME", "The Whale Hunter")
        sender_addr = os.getenv("MONEYBAG_SENDER_ADDRESS", "admin@fincore.co.kr")
        self.from_email = f"{sender_name} <{sender_addr}>"
        
        # [추가] 구독 취소 토큰 생성을 위한 Serializer (SECRET_KEY는 application.py와 동일해야 함)
        secret_key = os.getenv('SECRET_KEY', 'a-very-secret-key-that-is-secure')
        self.serializer = URLSafeTimedSerializer(secret_key)
        
        # 실제 구독자 리스트 가져오기
        self.to_emails = self._fetch_subscribers_from_db() 

    def _fetch_subscribers_from_db(self):
        """DB에서 구독자 이메일 리스트를 가져옵니다."""
        try:
            import pymysql
        except ImportError:
            print("⚠️ [EmailSender] pymysql 모듈이 설치되지 않았습니다.")
            return []

        # [추가] SSH 터널링 사용 여부 결정
        use_ssh_tunnel = os.getenv("USE_SSH_TUNNEL", "0") == "1"
        
        db_host = os.getenv("DB_HOST")
        db_port = int(os.getenv("DB_PORT", 3306))
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")

        try:
            # SSH 터널링을 사용하는 경우
            if use_ssh_tunnel and SSHTunnelForwarder:
                ssh_host = os.getenv("SSH_HOST")
                ssh_user = os.getenv("SSH_USER")
                ssh_key_path = os.getenv("SSH_PRIVATE_KEY_PATH")

                if not all([ssh_host, ssh_user, ssh_key_path]):
                    raise ConnectionError("SSH 터널링에 필요한 환경변수(SSH_HOST, SSH_USER, SSH_PRIVATE_KEY_PATH)가 없습니다.")

                print(f"🚇 SSH 터널을 통해 DB에 연결합니다. ({ssh_user}@{ssh_host})")
                
                with SSHTunnelForwarder(
                    (ssh_host, 22),
                    ssh_username=ssh_user,
                    ssh_pkey=os.path.expanduser(ssh_key_path),
                    remote_bind_address=(db_host, db_port)
                ) as tunnel:
                    # 터널을 통해 로컬 포트로 접속
                    conn = pymysql.connect(
                        host='127.0.0.1', port=tunnel.local_bind_port,
                        user=db_user, password=db_password,
                        db=db_name, charset='utf8mb4', cursorclass=pymysql.cursors.SSDictCursor
                    )
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT email FROM subscribers WHERE is_active=1 AND is_moneybag=1")
                        emails = [row['email'] for row in cursor]
                        print(f"✅ [DB Load] 구독자 {len(emails)}명 조회 성공 (SSH 터널 경유)")
                        return emails
            else:
                # 기존 직접 연결 방식
                conn = pymysql.connect(
                    host=db_host, port=db_port,
                    user=db_user, password=db_password,
                    db=db_name, charset='utf8mb4', cursorclass=pymysql.cursors.SSDictCursor
                )
                with conn.cursor() as cursor:
                    cursor.execute("SELECT email FROM subscribers WHERE is_active=1 AND is_moneybag=1")
                    emails = [row['email'] for row in cursor]
                    print(f"✅ [DB Load] 구독자 {len(emails)}명 조회 성공")
                    return emails
        except Exception as e:
            print(f"⚠️ [DB Error] 구독자 조회 실패: {e}")
            # DB 연결 실패 시 테스트 수신자 반환
            test_recipient = os.getenv("TEST_RECIPIENT")
            return [test_recipient] if test_recipient else []

    def _extract_headline_from_html(self, html_content: str) -> str:
        """HTML 콘텐츠에서 제목을 추출합니다."""
        # <title> 태그에서 추출
        title_match = re.search(r'<title>(.*?)</title>', html_content, re.DOTALL | re.IGNORECASE)
        if title_match:
            # "FINCORE | " 접두사 제거
            title = title_match.group(1).strip()
            if "FINCORE | " in title:
                title = title.split("FINCORE | ", 1)[1]
            return title
        
        # <h1> 태그에서 추출
        h1_match = re.search(r'<h1[^>]*>(.*?)</h1>', html_content, re.DOTALL | re.IGNORECASE)
        if h1_match:
            return h1_match.group(1).strip()
        
        return "새로운 리포트"

    def preprocess_markdown(self, text):
        lines = text.split('\n')
        new_lines = []
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith('|'):
                if i > 0 and lines[i-1].strip() != "" and not lines[i-1].strip().startswith('|'):
                    new_lines.append("")
            new_lines.append(line)
            if stripped.startswith('|') and i < len(lines)-1 and not lines[i+1].strip().startswith('|'):
                new_lines.append("")
        return "\n".join(new_lines)

    def convert_md_to_html(self, md_text):
        html_body = self._render_markdown(md_text)
        return self._wrap_body_in_template(html_body)

    def _render_markdown(self, md_text):
        """마크다운 텍스트를 HTML 조각으로 변환합니다."""
        safe_md = self.preprocess_markdown(md_text)
        safe_md = re.sub(r'(?<!\n)\n\s*([-*] )', r'\n\n\1', safe_md)
        safe_md = safe_md.replace("\n**🔥", "\n\n**🔥")
        safe_md = safe_md.replace("\n**1.", "\n\n**1.")
        safe_md = safe_md.replace("\n**2.", "\n\n**2.")
        safe_md = safe_md.replace("\n**3.", "\n\n**3.")

        return markdown.markdown(safe_md, extensions=['tables', 'nl2br'])

    def _wrap_body_in_template(self, body_content):
        """HTML 본문을 받아 전체 이메일 템플릿에 삽입합니다."""
        # [수정] 로컬 테스트를 위해 WEB_BASE_URL 환경변수 사용
        web_base_url = os.getenv("WEB_BASE_URL", "https://www.fincore.co.kr")
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{
                    font-family: 'Apple SD Gothic Neo', 'Malgun Gothic', sans-serif;
                    line-height: 1.7;
                    color: #374151; /* 기본 텍스트 색상 (회색) */
                    background-color: #f4f5f7; /* 전체 배경 */
                    margin: 0;
                    padding: 20px;
                }}
                .container {{
                    max-width: 800px;
                    margin: 0 auto;
                }}
                .card {{
                    background-color: #ffffff;
                    padding: 40px;
                    border-radius: 8px;
                    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
                }}
                h1 {{
                    font-size: 28px;
                    color: #0f172a; /* 진한 파랑-검정 */
                    border-bottom: 2px solid #dbeafe; /* 옅은 파랑 */
                    padding-bottom: 12px;
                    margin-bottom: 30px;
                }}
                h2 {{
                    font-size: 22px;
                    color: #1e3a8a; /* 더 진한 파랑 */
                    margin-top: 40px;
                    border-bottom: 1px solid #e5e7eb;
                    padding-bottom: 8px;
                }}
                h3 {{
                    font-size: 18px;
                    color: #111827;
                    margin-top: 30px;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin: 25px 0;
                    font-size: 14px;
                    border: 1px solid #e5e7eb;
                }}
                th, td {{
                    border: 1px solid #e5e7eb;
                    padding: 12px 15px;
                    text-align: center;
                }}
                th {{
                    background-color: #eff6ff; /* 옅은 파랑 (시그널리스트 스타일) */
                    color: #1e3a8a;
                    font-weight: 600;
                    border-bottom: 2px solid #bfdbfe;
                }}
                tr:nth-child(even) {{ background-color: #f9fafb; }}
                tr:hover {{ background-color: #f3f4f6; }}
                blockquote {{
                    border-left: 4px solid #3b82f6; /* 파란색 */
                    margin: 25px 0;
                    padding: 15px 20px;
                    background-color: #f3f4ff; /* 옅은 파랑 배경 */
                    color: #374151;
                    border-radius: 4px;
                }}
                hr {{ border: 0; height: 1px; background: #e5e7eb; margin: 40px 0; }}
                .footer {{
                    margin-top: 40px;
                    font-size: 12px;
                    color: #6b7280; /* 더 연한 회색 */
                    text-align: center;
                    padding-top: 20px;
                }}
                .footer a {{ color: #4b5563; text-decoration: underline; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="card">
                    {body_content}
                </div>
                <!-- FINCORE_FOOTER_START -->
                <div class="footer">
                    본 메일은 -email- 주소로 발송된 Fincore 뉴스레터입니다.<br>
                    더 이상 수신을 원하지 않으시면 <a href="-unsubscribe_url-">여기</a>를 눌러 구독을 취소해주세요.<br><br>
                    (주)비제이유앤아이 | <a href="{web_base_url}/privacy">개인정보 처리방침</a><br>
                    <p style="margin-top: 10px;">본 메일은 투자 참고용이며, 투자의 책임은 본인에게 있습니다.</p>
                </div>
            </div>
        </body>
        </html>
        """

    def save_html(self, html_content, date_str, mode="morning"):
        try:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            filename = f"Moneybag_Letter_{mode.capitalize()}_{date_str}.html"
            file_path = OUTPUT_DIR / filename
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            print(f"💾 [Save] HTML 저장 완료: {file_path}")
            return file_path
        except Exception as e:
            print(f"⚠️ [Skip] HTML 저장 실패: {e}")
            return None

    def send_html_content(self, html_content: str, subject: str):
        """[NEW] HTML 콘텐츠를 직접 받아서 발송하는 심플 버전"""
        if not self.api_key: 
            print("❌ SendGrid API Key가 없습니다.")
            return

        if not self.to_emails:
            print("❌ 수신자가 없어 메일을 보내지 않습니다.")
            return

        sg = SendGridAPIClient(self.api_key)
        batch_size = 1000
        total_batches = math.ceil(len(self.to_emails) / batch_size)
        total_sent = 0
        total_failed = 0

        print(f"📧 총 {len(self.to_emails)}명에게 '{subject}' 발송 시작... ({total_batches}개 배치)")

        for i in range(total_batches):
            batch_start_time = time.time()
            batch_emails = self.to_emails[i * batch_size : (i + 1) * batch_size]
            message = Mail(from_email=self.from_email, subject=subject, html_content=html_content)
            for email in batch_emails:
                p = Personalization()
                p.add_to(To(email))

                # [추가] 각 이메일별 개인화된 구독 취소 링크 생성
                try:
                    # [수정] 로컬 테스트를 위해 WEB_BASE_URL 환경변수 사용
                    web_base_url = os.getenv("WEB_BASE_URL", "https://www.fincore.co.kr")
                    unsubscribe_token = self.serializer.dumps(email, salt='email-unsubscribe')
                    unsubscribe_url = f"{web_base_url}/unsubscribe/moneybag/{unsubscribe_token}"
                    
                    # [추가] SendGrid Substitution 기능으로 동적 값 주입
                    p.add_substitution(Substitution("-email-", email))
                    p.add_substitution(Substitution("-unsubscribe_url-", unsubscribe_url))
                except Exception as e:
                    print(f"⚠️ 토큰 생성 실패: {email}, {e}")
                message.add_personalization(p)
            try:
                response = sg.send(message)
                if 200 <= response.status_code < 300:
                    total_sent += len(batch_emails)
                    print(f"✅ [Batch {i+1}/{total_batches}] {len(batch_emails)}명 발송 성공 (소요 시간: {time.time() - batch_start_time:.2f}초, Status: {response.status_code})")
                else:
                    total_failed += len(batch_emails)
                    print(f"❌ [Batch {i+1}/{total_batches}] 발송 실패 (Status: {response.status_code})")
                    print(f"   -> SendGrid Body: {response.body}")
            except Exception as e:
                total_failed += len(batch_emails)
                print(f"❌ [Batch {i+1}] API 호출 실패: {e}")
        
        print(f"🏁 발송 완료. 성공: {total_sent}, 실패: {total_failed}")

    def send(self, file_path, mode="morning"):
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        headline = "웨일 헌터 브리핑"
        if lines and lines[0].startswith("# "):
            headline = lines[0].strip().replace("# ", "").replace("🐋 ", "").replace("💰 ", "")
        
        md_text = "".join(lines)
        html_content = self._wrap_body_in_template(self._render_markdown(md_text))
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        self.save_html(html_content, today_str, mode)
        subject = f"[Secret Note] 🐋 {headline}"

        # [리팩토링] 복잡한 발송 로직을 send_html_content 메서드로 위임하여 코드 중복 제거
        self.send_html_content(html_content, subject)

if __name__ == "__main__":
    import sys

    # 1. 인자 파싱 (파일 경로, 테스트 이메일)
    file_to_send_path_str = None
    cli_recipient_email = None
    test_recipient_from_env = os.getenv("TEST_RECIPIENT") # [추가] 환경 변수 읽기

    if len(sys.argv) > 1:
        if "@" in sys.argv[1] and "." in sys.argv[1]:
            cli_recipient_email = sys.argv[1]
        else:
            file_to_send_path_str = sys.argv[1]
            if len(sys.argv) > 2:
                cli_recipient_email = sys.argv[2]
    
    # [추가] CLI 인자 또는 환경 변수에서 테스트 수신자 결정
    final_test_recipient = cli_recipient_email or test_recipient_from_env

    # [단순화] 발송할 파일 결정 (최신 파일 또는 지정된 파일)
    file_to_send = None
    if file_to_send_path_str:
        file_to_send = Path(file_to_send_path_str)
    else:
        out_dir = BASE_DIR / "moneybag" / "data" / "out"
        files = sorted(out_dir.glob("SecretNote_*.md"), key=os.path.getmtime, reverse=True)
        if files:
            file_to_send = files[0]
            print(f"▶️ 최신 파일 자동 선택: {file_to_send.name}")

    if not file_to_send or not file_to_send.exists():
        print(f"❌ 발송할 마크다운 파일을 찾을 수 없습니다. ({file_to_send})")
        sys.exit(1)

    # 발송 실행
    sender = EmailSender()
    is_auto_send = os.getenv("NEWSLETTER_AUTO_SEND", "0") == "1"

    if final_test_recipient:
        print(f"📧 [Single Send Mode] 단건 발송 시작 -> {final_test_recipient}")
        sender.to_emails = [final_test_recipient]
    elif is_auto_send:
        print(f"✅ [Production Mode] DB에 등록된 구독자 {len(sender.to_emails)}명에게 발송합니다.")
    else:
        print("⚠️ [Safe Mode] 실제 발송이 비활성화되었습니다. (NEWSLETTER_AUTO_SEND=1 설정 필요)")
        print(f"-> 테스트 발송을 원하시면 이메일 주소를 인자로 전달하세요.")
        sys.exit(0)

    # 파일명에서 모드(morning/night) 추출
    mode = "morning"
    if "night" in file_to_send.name.lower():
        mode = "night"
        
    sender.send(str(file_to_send), mode=mode)