import os
import requests
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

class SlackNotifier:
    def __init__(self):
        self.webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not self.webhook_url:
            print("⚠️ [Warning] SLACK_WEBHOOK_URL이 설정되지 않았습니다.")

    def send_message(self, message):
        """슬랙으로 메시지 전송"""
        if not self.webhook_url:
            print(f"❌ [Slack] 전송 실패 (URL 없음): {message}")
            return

        payload = {"text": message}
        try:
            response = requests.post(self.webhook_url, json=payload)
            if response.status_code != 200:
                print(f"❌ [Slack] 전송 오류 ({response.status_code}): {response.text}")
        except Exception as e:
            print(f"❌ [Slack] 연결 오류: {e}")

# (하위 호환성을 위해 함수도 남겨둠 - 선택 사항)
def send_slack_message(message):
    notifier = SlackNotifier()
    notifier.send_message(message)

if __name__ == "__main__":
    notifier = SlackNotifier()
    notifier.send_message("🔔 머니백 슬랙 알림 테스트입니다!")