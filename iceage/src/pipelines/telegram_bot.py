import os
import sys
from pathlib import Path

# 머니백 경로 찾기
BASE_DIR = Path(__file__).resolve().parents[3]
sys.path.append(str(BASE_DIR))

try:
    from moneybag.src.pipelines.send_channels import TelegramSender
except ImportError:
    from src.pipelines.send_channels import TelegramSender

class SignalistTelegramBot:
    def __init__(self, token=None, chat_id=None):
        # [1] 왓치독이 토큰을 줬으면 그걸 쓰고, 안 줬으면 환경변수에서 직접 찾음
        self.token = token or os.getenv("TELEGRAM_BOT_TOKEN_SIGNALIST")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
        
        # [2] ★핵심★ 찾은 토큰을 TelegramSender에게 '직접' 넣어줌
        self.sender = TelegramSender(token=self.token, chat_id=self.chat_id)

    async def send_message(self, message):
        # 비동기 함수지만 내부적으로는 동기 Sender를 호출
        try:
            self.sender.send_message(message)
        except Exception as e:
            print(f"❌ [Signalist Bot Error] {e}")