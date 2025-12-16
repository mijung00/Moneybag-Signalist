import os
import requests
from dotenv import load_dotenv

# 환경변수 로드 (안전을 위해 추가)
load_dotenv()

class TelegramSender:
    def __init__(self, token=None, chat_id=None):
        # [1] 인자로 들어온 토큰이 있으면(1순위) 그걸 씀. 
        # [2] 없으면 머니백 환경변수(2순위), [3] 그것도 없으면 공용 토큰(3순위)
        self.token = token or os.getenv("TELEGRAM_BOT_TOKEN_MONEYBAG") or os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
        
        # 디버깅: 토큰이 비어있다면 로그 찍기
        if not self.token:
            print("❌ [TelegramSender] 초기화 실패: 토큰이 없습니다.")
        if not self.chat_id:
            print("❌ [TelegramSender] 초기화 실패: Chat ID가 없습니다.")
        
        # URL 생성
        if self.token:
            self.base_url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        else:
            self.base_url = ""

    def send_message(self, text):
        if not self.token or not self.chat_id:
            print("❌ [Telegram] 전송 불가: 토큰/ID 누락")
            return

        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown"
        }
        try:
            resp = requests.post(self.base_url, json=payload, timeout=5)
            if resp.status_code != 200:
                print(f"❌ [Telegram Error] {resp.text}")
        except Exception as e:
            print(f"❌ [Telegram Exception] {e}")