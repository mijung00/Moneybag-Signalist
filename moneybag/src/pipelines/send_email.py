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
# 이 파일 위치: project/moneybag/src/pipelines/send_email.py
# parents[0]=pipelines, [1]=src, [2]=moneybag, [3]=project(루트)
BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

# 구독자 파일 설정 (루트 폴더에 있는 subscribers_moneybag.csv)
SUBSCRIBERS_FILE = BASE_DIR / "subscribers_moneybag.csv"

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
        self.from_email = os.getenv("MONEYBAG_FROM_EMAIL") # .env에서 가져옴
        
        # [수정됨] .env 대신 CSV 파일에서 구독자 로드
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
        
        # [수정 1] 리스트(- 또는 *)가 일반 텍스트 바로 뒤에 붙어있으면 
        # 마크다운이 리스트로 인식을 못합니다. 강제로 줄바꿈 2번을 넣어줍니다.
        # 예: "**제목**\n- 내용" -> "**제목**\n\n- 내용"
        safe_md = re.sub(r'(?<!\n)\n\s*([-*] )', r'\n\n\1', safe_md)

        html_body = markdown.markdown(safe_md, extensions=['tables', 'nl2br'])
        
        # [수정 2] CSS 스타일 강화 (ul, li 태그 디자인 추가)
        styled_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: 'Apple SD Gothic Neo', 'Malgun Gothic', sans-serif; line-height: 1.6; color: #333; padding: 20px; }}
                h1 {{ color: #0056b3; border-bottom: 2px solid #0056b3; padding-bottom: 10px; }}
                h2 {{ color: #0056b3; margin-top: 30px; border-bottom: 1px solid #eee; padding-bottom: 5px; }}
                h3 {{ color: #2c3e50; margin-top: 25px; }}
                
                /* 테이블 스타일 */
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 14px; }}
                th, td {{ border: 1px solid #ddd; padding: 10px; text-align: center; }}
                th {{ background-color: #f8f9fa; color: #333; font-weight: bold; }}
                tr:nth-child(even) {{ background-color: #fdfdfd; }}

                /* [추가된 부분] 리스트 스타일 */
                ul {{ margin: 10px 0 20px 20px; padding-left: 0; }}
                li {{ margin-bottom: 5px; list-style-type: disc; }}

                /* 인용문 스타일 */
                blockquote {{ border-left: 4px solid #0056b3; margin: 15px 0; padding: 10px 15px; background-color: #f1f8ff; color: #555; }}

                .footer {{ margin-top: 50px; font-size: 12px; color: #888; text-align: center; border-top: 1px solid #eee; padding-top: 20px; }}
            </style>
        </head>
        <body>
            <div class="container">
                {html_body}
                <div class="footer">
                    <p>🐋 <b>웨일 헌터의 시크릿 노트</b> | Moneybag Project</p>
                </div>
            </div>
        </body>
        </html>
        """
        return styled_html

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
        if lines and lines[0].startswith("# "):
            headline = lines[0].strip().replace("# ", "").replace("🐋 ", "")
        
        md_text = "".join(lines)
        html_content = self.convert_md_to_html(md_text)
        
        subject = f"[Secret Note] 🐋 {headline}"

        # SendGrid 발송 (To 객체 사용)
        message = Mail(
            from_email=self.from_email,
            subject=subject,
            html_content=html_content
        )
        # 여러 명에게 개별 발송 (BCC 효과)
        message.to = [To(email) for email in self.to_emails]

        try:
            sg = SendGridAPIClient(self.api_key)
            response = sg.send(message)
            print(f"✅ [Email] '{subject}' 전송 완료! ({len(self.to_emails)}명)")
        except Exception as e:
            print(f"❌ [Email] 전송 실패: {e}")

if __name__ == "__main__":
    # 테스트용 코드
    pass