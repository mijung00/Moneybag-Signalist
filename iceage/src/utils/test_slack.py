# iceage/src/utils/test_slack.py
from .slack_notifier import send_slack_message

def main() -> None:
    send_slack_message("[Signalist] 🧪 슬랙 웹훅 테스트 메시지입니다.")

if __name__ == "__main__":
    main()
