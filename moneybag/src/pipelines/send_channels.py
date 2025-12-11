import os
import requests
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

class TelegramSender:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")

    def send_message(self, text):
        """
        텔레그램 메시지 전송 (동기 방식)
        """
        if not self.token or not self.chat_id:
            print("❌ [Telegram] 토큰이나 채팅 ID가 설정되지 않았습니다.")
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        
        # 메시지가 너무 길면 잘라서 보냄 (텔레그램 제한 4096자)
        chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
        
        for chunk in chunks:
            payload = {
                "chat_id": self.chat_id,
                "text": chunk,
                "parse_mode": "Markdown" # 마크다운 문법 지원 (*굵게* 등)
            }
            
            try:
                resp = requests.post(url, json=payload, timeout=5)
                if resp.status_code == 200:
                    print("✅ [Telegram] 전송 성공")
                else:
                    print(f"❌ [Telegram] 전송 실패: {resp.text}")
            except Exception as e:
                print(f"❌ [Telegram] 연결 오류: {e}")

# 테스트용
if __name__ == "__main__":
    sender = TelegramSender()
    sender.send_message("📢 **웨일 헌터**의 텔레그램 연결 테스트입니다.\n성공적으로 도착했습니다! 🚀")