import os
import markdown
import math
from datetime import datetime
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
# 👇 [수정] Personalization 모듈 추가
from sendgrid.helpers.mail import Mail, To, Personalization
import re

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
        
        # 실제 구독자 리스트 가져오기
        self.to_emails = self._fetch_subscribers_from_db() 

    def _fetch_subscribers_from_db(self):
        """DB에서 구독자 이메일 리스트를 가져옵니다."""
        try:
            import pymysql
        except ImportError:
            print("⚠️ [EmailSender] pymysql 모듈이 설치되지 않았습니다.")
            return []

        try:
            conn = pymysql.connect(
                host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT", 3306)),
                user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
                db=os.getenv("DB_NAME"), charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            with conn.cursor() as cursor:
                cursor.execute("SELECT email FROM subscribers WHERE is_active=1")
                result = cursor.fetchall()
                emails = [row['email'] for row in result]
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
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: 'Apple SD Gothic Neo', 'Malgun Gothic', sans-serif; line-height: 1.6; color: #333; padding: 20px; max-width: 800px; margin: 0 auto; }}
                h1 {{ color: #0056b3; border-bottom: 2px solid #0056b3; padding-bottom: 10px; margin-bottom: 30px; }}
                h2 {{ color: #0056b3; margin-top: 40px; border-bottom: 1px solid #eee; padding-bottom: 5px; font-size: 1.5em; }}
                h3 {{ color: #2c3e50; margin-top: 30px; font-size: 1.2em; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 14px; }}
                th, td {{ border: 1px solid #ddd; padding: 10px; text-align: center; }}
                th {{ background-color: #f8f9fa; color: #555; font-weight: bold; }}
                tr:nth-child(even) {{ background-color: #fdfdfd; }}
                ul {{ margin: 10px 0 20px 20px; padding-left: 0; }}
                li {{ margin-bottom: 8px; list-style-type: disc; }}
                p > strong:first-child {{ color: #d35400; }} 
                blockquote {{ border-left: 4px solid #0056b3; margin: 20px 0; padding: 15px; background-color: #f1f8ff; color: #555; border-radius: 4px; }}
                hr {{ border: 0; height: 1px; background: #eee; margin: 40px 0; }}
                .footer {{ margin-top: 50px; font-size: 12px; color: #888; text-align: center; border-top: 1px solid #eee; padding-top: 20px; }}
            </style>
        </head>
        <body>
            <div class="container">
                {body_content}
                <div class="footer">
                    <p>🐋 <b>웨일 헌터의 시크릿 노트</b> | Moneybag Project</p>
                    <p>본 메일은 투자 참고용이며, 투자의 책임은 본인에게 있습니다.</p>
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

        print(f"📧 총 {len(self.to_emails)}명에게 발송 (API Personalization 적용)")

        for i in range(total_batches):
            batch_emails = self.to_emails[i * batch_size : (i + 1) * batch_size]
            message = Mail(from_email=self.from_email, subject=subject, html_content=html_content)
            for email in batch_emails:
                p = Personalization()
                p.add_to(To(email))
                message.add_personalization(p)
            try:
                sg.send(message)
                print(f"✅ [Batch {i+1}/{total_batches}] {len(batch_emails)}명 발송 성공")
            except Exception as e:
                print(f"❌ [Batch {i+1}] 발송 실패: {e}")

    def send(self, file_path, mode="morning"):
        if not self.api_key: 
            print("❌ SendGrid API Key가 없습니다.")
            return

        if not self.to_emails:
            print("❌ 수신자가 없어 메일을 보내지 않습니다.")
            return

        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        headline = "웨일 헌터 브리핑"
        if lines and lines[0].startswith("# "):
            headline = lines[0].strip().replace("# ", "").replace("🐋 ", "").replace("💰 ", "")
        
        md_text = "".join(lines)
        html_content = self._wrap_body_in_template(self._render_markdown(md_text)) # [수정] 마크다운 렌더링 후 템플릿 적용
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        self.save_html(html_content, today_str, mode)
        subject = f"[Secret Note] 🐋 {headline}"

        # ---------------------------------------------------------
        # [핵심 수정] SendGrid Personalization (Batch Sending)
        # ---------------------------------------------------------
        sg = SendGridAPIClient(self.api_key)
        
        # SendGrid API 한계 고려 (1000명 단위)
        batch_size = 1000
        total_batches = math.ceil(len(self.to_emails) / batch_size)

        print(f"📧 총 {len(self.to_emails)}명에게 발송 (API Personalization 적용)")

        for i in range(total_batches):
            batch_emails = self.to_emails[i * batch_size : (i + 1) * batch_size]
            
            # 1. 메일 기본 틀 생성 (수신자 지정 없이)
            message = Mail(
                from_email=self.from_email,
                subject=subject,
                html_content=html_content
            )

            # 2. Personalization 객체 생성해서 하나씩 추가
            # 이렇게 하면 수신자는 본인 이메일만 'To'에 보임
            for email in batch_emails:
                p = Personalization()
                p.add_to(To(email))
                message.add_personalization(p)

            # 3. 발송
            try:
                sg.send(message)
                print(f"✅ [Batch {i+1}/{total_batches}] {len(batch_emails)}명 발송 성공")
            except Exception as e:
                print(f"❌ [Batch {i+1}] 발송 실패: {e}")

if __name__ == "__main__":
    # [수정] CLI 실행 시 단건 발송 로직 구현
    import sys
    from common.s3_manager import S3Manager
    
    # 1. 인자 파싱
    ref_date = None
    if len(sys.argv) > 1:
        ref_date = sys.argv[1]
    else:
        ref_date = datetime.now().strftime("%Y-%m-%d")

    # 2. 환경변수에서 수신자 확인 (application.py가 설정함)
    recipient = os.getenv("TEST_RECIPIENT")
    
    print(f"📧 [Moneybag Email] 수동 발송 시작: {ref_date} -> {recipient}")

    if not recipient:
        print("❌ 수신자(TEST_RECIPIENT)가 지정되지 않았습니다.")
        sys.exit(0)

    sender = EmailSender()
    sender.to_emails = [recipient]

    # 3. S3에서 HTML 콘텐츠 가져오기
    s3 = S3Manager(bucket_name="fincore-output-storage")
    
    morning_key = f"moneybag/data/out/Moneybag_Letter_Morning_{ref_date}.html"
    night_key = f"moneybag/data/out/Moneybag_Letter_Night_{ref_date}.html"
    
    morning_html_raw = s3.get_text_content(morning_key)
    night_html_raw = s3.get_text_content(night_key)

    parts = []
    if morning_html_raw:
        body_match = re.search(r'<body[^>]*>(.*?)</body>', morning_html_raw, re.DOTALL | re.IGNORECASE)
        parts.append(body_match.group(1) if body_match else morning_html_raw)

    if night_html_raw:
        if morning_html_raw:
            parts.append('<div style="margin: 80px 0; border-top: 2px dashed #e5e7eb;"></div><h2>🌙 Night Report</h2>')
        body_match = re.search(r'<body[^>]*>(.*?)</body>', night_html_raw, re.DOTALL | re.IGNORECASE)
        parts.append(body_match.group(1) if body_match else night_html_raw)
    
    if parts:
        # [수정] HTML 조각들을 합친 후, 이메일 템플릿으로 감싸기 (본문만)
        full_body_html = "".join(parts)
        final_email_html = sender._wrap_body_in_template(full_body_html)
        
        # [수정] 제목 추출 (Morning 또는 Night 리포트에서)
        headline = ""
        if morning_html_raw:
            headline = sender._extract_headline_from_html(morning_html_raw)
        elif night_html_raw:
            headline = sender._extract_headline_from_html(night_html_raw)
        
        subject = f"[The Whale Hunter] {ref_date} | {headline}" if headline != "새로운 리포트" else f"[The Whale Hunter] {ref_date} 리포트가 도착했습니다."
        sender.send_html_content(final_email_html, subject)
    else:
        print(f"⚠️ 해당 날짜({ref_date})의 리포트 파일이 S3에 없습니다.")