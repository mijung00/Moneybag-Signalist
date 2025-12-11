# iceage/src/pipelines/telegram_bot.py
import os
import asyncio
from telegram import Bot
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

class SignalistTelegramBot:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
    async def send_message(self, text: str):
        """
        텔레그램 메시지 전송 (비동기)
        """
        if not self.token or not self.chat_id:
            print("❌ [Telegram] 토큰이나 채팅 ID가 설정되지 않았습니다.")
            return

        try:
            bot = Bot(token=self.token)
            # 메시지가 4096자를 넘으면 알아서 나눠 보내주는 로직은 라이브러리에 없으므로 간단 구현
            if len(text) > 4000:
                chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
                for chunk in chunks:
                    await bot.send_message(chat_id=self.chat_id, text=chunk, parse_mode="Markdown")
            else:
                await bot.send_message(chat_id=self.chat_id, text=text, parse_mode="Markdown")
                
            print("✅ [Telegram] 알림 발송 완료")
        except Exception as e:
            print(f"❌ [Telegram] 발송 실패: {e}")

# 테스트 코드
if __name__ == "__main__":
    bot = SignalistTelegramBot()
    asyncio.run(bot.send_message("📢 **Signalist** 법인 계정 봇 연결 테스트! 🚀"))