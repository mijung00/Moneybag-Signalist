# moneybag/src/pipelines/send_email.py
import os
import markdown
from datetime import datetime
from pathlib import Path
import pandas as pd  # 구독자 파일 읽기용
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, To
import re

# 프로젝트 루트 경로 (moneybag 폴더의 상위 폴더)
BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

# 구독자 파일 설정
SUBSCRIBERS_FILE = BASE_DIR / "subscribers_moneybag.csv"

# 👇 [수정] data 폴더 안으로 정리
OUTPUT_DIR = BASE_DIR / "moneybag" / "data" / "out"

def get_subscribers() -> list[str]:
    """CSV 파일에서 구독자 명단을 읽어옵니다."""
    test_recipient = os.getenv("TEST_RECIPIENT")
    
    # 자동 발송 설정 확인 (없으면 기본 1)
    if os.getenv("NEWSLETTER_AUTO_SEND", "1") != "1":
        print(f"⚠️ 자동 발송 OFF: 테스트 수신자({test_recipient})에게만 발송합니다.")
        return [test_recipient] if test_recipient else []

    if not SUBSCRIBERS_FILE.exists():
        print(f"❌ 구독자 파일이 없습니다: {SUBSCRIBERS_FILE}")
        print(f"   (테스트 수신자 {test_recipient}에게만 발송합니다.)")
        return [test_recipient] if test_recipient else []

    try:
        df = pd.read_csv(SUBSCRIBERS_FILE, encoding='utf-8')
        if 'subscribed' in df.columns and 'email' in df.columns:
            # 구독 중(True)인 사람만 필터링
            subscribers = df[df['subscribed'] == True]['email'].tolist()
            # 이메일 형식이 맞는 것만 추림 (@ 포함)
            clean_list = [e.strip() for e in subscribers if "@" in e and "." in e]
            return clean_list
        else:
            print("❌ CSV 파일 형식이 다릅니다. (email, subscribed 컬럼 필요)")
            return [test_recipient] if test_recipient else []
    except Exception as e:
        print(f"❌ 구독자 파일 읽기 에러: {e}")
        return [test_recipient] if test_recipient else []

class EmailSender:
    def __init__(self):
        self.api_key = os.getenv("SENDGRID_API_KEY")
        
        # [수정] 이메일 이름과 주소를 환경 변수에서 각각 가져와서 조립
        sender_name = os.getenv("MONEYBAG_SENDER_NAME", "The Whale Hunter")
        sender_addr = os.getenv("MONEYBAG_SENDER_ADDRESS", "admin@fincore.co.kr")
        self.from_email = f"{sender_name} <{sender_addr}>"
        
        self.to_emails = get_subscribers()
        
        if not self.to_emails:
            print("⚠️ 발송할 구독자가 없습니다.")

    def preprocess_markdown(self, text):
        """표 깨짐 방지 처리"""
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
        safe_md = self.preprocess_markdown(md_text)
        
        # [핵심 수정 1] 리스트(-) 처리: 일반 텍스트 뒤에 붙으면 강제 개행
        safe_md = re.sub(r'(?<!\n)\n\s*([-*] )', r'\n\n\1', safe_md)

        # [핵심 수정 2] 전략 번호(1, 2, 3) 및 불꽃 아이콘 강제 줄바꿈 (뭉침 방지)
        # 이 부분이 없어서 아까 메일에서 다닥다닥 붙어서 나온 거야.
        safe_md = safe_md.replace("\n**🔥", "\n\n**🔥")
        safe_md = safe_md.replace("\n**1.", "\n\n**1.")
        safe_md = safe_md.replace("\n**2.", "\n\n**2.")
        safe_md = safe_md.replace("\n**3.", "\n\n**3.")

        html_body = markdown.markdown(safe_md, extensions=['tables', 'nl2br'])
        
        # [CSS 스타일] 가독성을 위해 strong 태그(굵은 글씨)에 여백 추가
        styled_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: 'Apple SD Gothic Neo', 'Malgun Gothic', sans-serif; line-height: 1.6; color: #333; padding: 20px; max-width: 800px; margin: 0 auto; }}
                
                h1 {{ color: #0056b3; border-bottom: 2px solid #0056b3; padding-bottom: 10px; margin-bottom: 30px; }}
                h2 {{ color: #0056b3; margin-top: 40px; border-bottom: 1px solid #eee; padding-bottom: 5px; font-size: 1.5em; }}
                h3 {{ color: #2c3e50; margin-top: 30px; font-size: 1.2em; }}
                
                /* 테이블 스타일 */
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 14px; }}
                th, td {{ border: 1px solid #ddd; padding: 10px; text-align: center; }}
                th {{ background-color: #f8f9fa; color: #555; font-weight: bold; }}
                tr:nth-child(even) {{ background-color: #fdfdfd; }}

                /* 리스트 스타일 */
                ul {{ margin: 10px 0 20px 20px; padding-left: 0; }}
                li {{ margin-bottom: 8px; list-style-type: disc; }}
                
                /* [추가] 전략 번호(굵은 글씨)가 문단 처음에 오면 위쪽 여백을 줌 */
                p > strong:first-child {{ color: #d35400; }} 

                /* 인용문 스타일 */
                blockquote {{ border-left: 4px solid #0056b3; margin: 20px 0; padding: 15px; background-color: #f1f8ff; color: #555; border-radius: 4px; }}
                
                /* 구분선 */
                hr {{ border: 0; height: 1px; background: #eee; margin: 40px 0; }}

                .footer {{ margin-top: 50px; font-size: 12px; color: #888; text-align: center; border-top: 1px solid #eee; padding-top: 20px; }}
            </style>
        </head>
        <body>
            <div class="container">
                {html_body}
                <div class="footer">
                    <p>🐋 <b>웨일 헌터의 시크릿 노트</b> | Moneybag Project</p>
                    <p>본 메일은 투자 참고용이며, 투자의 책임은 본인에게 있습니다.</p>
                </div>
            </div>
        </body>
        </html>
        """
        return styled_html

    # 👇 [추가] 이 위치에 save_html 함수를 그대로 붙여넣으세요.
    def save_html(self, html_content, date_str):
        """HTML 파일로 저장"""
        try:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            filename = f"Moneybag_Letter_{date_str}.html"
            file_path = OUTPUT_DIR / filename
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            print(f"💾 [Save] HTML 저장 완료: {file_path}")
            return file_path
        except Exception as e:
            print(f"⚠️ [Skip] HTML 저장 실패: {e}")
            return None



    def send(self, file_path):
        if not self.api_key: 
            print("❌ SendGrid API Key가 없습니다.")
            return

        if not self.to_emails:
            print("❌ 수신자가 없어 메일을 보내지 않습니다.")
            return

        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        headline = "웨일 헌터 브리핑"
        # 헤드라인 추출 시 # 제거
        if lines and lines[0].startswith("# "):
            headline = lines[0].strip().replace("# ", "").replace("🐋 ", "")
        
        md_text = "".join(lines)
        html_content = self.convert_md_to_html(md_text)
        
        
        # 👇 [추가] HTML 내용을 파일로 저장하는 명령
        today_str = datetime.now().strftime("%Y-%m-%d")
        self.save_html(html_content, today_str)
        
        subject = f"[Secret Note] 🐋 {headline}"

        # SendGrid 발송
        message = Mail(
            from_email=self.from_email,
            subject=subject,
            html_content=html_content
        )
        message.to = [To(email) for email in self.to_emails]

        try:
            sg = SendGridAPIClient(self.api_key)
            response = sg.send(message)
            print(f"✅ [Email] '{subject}' 전송 완료! ({len(self.to_emails)}명)")
        except Exception as e:
            print(f"❌ [Email] 전송 실패: {e}")

if __name__ == "__main__":
    pass