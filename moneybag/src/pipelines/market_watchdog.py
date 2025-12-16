import time
import sys
import os
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import socket
import traceback

# [★핵심 1] 경로 강제 설정 (ModuleNotFoundError 방지)
# 현재 파일 위치를 기준으로 프로젝트 루트를 찾아서 sys.path에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
if project_root not in sys.path:
    sys.path.append(project_root)

# 환경 변수 로드
load_dotenv(os.path.join(project_root, ".env"))

# 네트워크가 '영원히 멈춰서' 전체 프로세스가 굳는 걸 막기 위한 기본 타임아웃(초)
SOCKET_TIMEOUT_SEC = int(os.getenv("WATCHDOG_SOCKET_TIMEOUT_SEC", "15"))
socket.setdefaulttimeout(SOCKET_TIMEOUT_SEC)

# 이 파일이 '살아있다'는 표시(매 루프마다 갱신). watchdogs.py(매니저)가 이걸 보고 멈춤/행을 감지해서 재시작함.
HEARTBEAT_PATH = os.getenv("MONEYBAG_HEARTBEAT_PATH", "/tmp/moneybag_market_watchdog.heartbeat")

try:
    from moneybag.src.collectors.crypto_news_rss import CryptoNewsRSS
    from moneybag.src.llm.openai_driver import _chat
    from moneybag.src.pipelines.send_channels import TelegramSender
except ImportError as e:
    print(f"❌ [Import Error] 모듈을 찾을 수 없습니다: {e}")
    # 경로 문제 시 현재 디렉토리도 추가 시도
    sys.path.append(os.getcwd())
    from moneybag.src.collectors.crypto_news_rss import CryptoNewsRSS
    from moneybag.src.llm.openai_driver import _chat
    from moneybag.src.pipelines.send_channels import TelegramSender


class MarketWatchdog:
    def __init__(self):
        self.news_collector = CryptoNewsRSS()

        token = os.getenv("TELEGRAM_BOT_TOKEN_MONEYBAG")
        chat_id = os.getenv("TELEGRAM_CHAT_ID_MONEYBAG")

        # 토큰 상태 로그 출력 (디버깅용)
        if token:
            print(f"🔑 [Moneybag] 토큰 로드 성공: {token[:5]}...")
        else:
            print("❌ [Moneybag] 토큰 없음! 환경변수를 확인하세요.")

        self.telegram = TelegramSender(token=token, chat_id=chat_id)
        self.targets = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]

        self.prev_check_prices = {}
        self.last_alert_prices = {}
        self.cooldown = {}

    def _touch_heartbeat(self):
        try:
            Path(HEARTBEAT_PATH).write_text(datetime.now().isoformat())
        except Exception:
            pass

    def get_binance_price(self, symbol):
        try:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return float(resp.json()["price"])
        except Exception as e:
            print(f"⚠️ [API Error] {symbol}: {e}")
        return None

    def check_market(self):
        self._touch_heartbeat()

        now_str = datetime.now().strftime("%H:%M:%S")
        print(f"\r👀 Moneybag 감시 중... ({now_str})", end="", flush=True)

        for coin in self.targets:
            current_price = self.get_binance_price(coin)
            if current_price is None:
                continue

            if coin not in self.prev_check_prices:
                self.prev_check_prices[coin] = current_price
                self.last_alert_prices[coin] = current_price
                continue

            prev_price = self.prev_check_prices[coin]
            total_change_pct = ((current_price - prev_price) / prev_price) * 100

            # 쿨타임 (30분)
            last_time = self.cooldown.get(coin)
            if last_time and (datetime.now() - last_time).seconds < 1800:
                continue

            trigger_reason = None
            if abs(total_change_pct) >= 1.5:
                trigger_reason = f"{total_change_pct:+.2f}% 변동"

            if trigger_reason:
                print(f"\n⚡ [Moneybag] {coin} 조건 충족: {trigger_reason}")

                data = {
                    "price_change": round(total_change_pct, 4),
                    "current_price": current_price,
                    "reason": trigger_reason,
                }

                self.trigger_emergency_protocol(coin, data)

                self.cooldown[coin] = datetime.now()
                self.last_alert_prices[coin] = current_price

            self.prev_check_prices[coin] = current_price

    def trigger_emergency_protocol(self, coin, data):
        news_items = self.news_collector.collect_all()

        system_prompt = f"""
        너는 'Moneybag 리스크 매니저'다.
        {coin} 급변동 발생. 핵심 뉴스/이슈를 요약하고 대응을 제안해라.
        """
        user_prompt = f"""
        코인: {coin}
        현재가: {data['current_price']}
        변동률: {data['price_change']}%
        사유: {data['reason']}
        뉴스: {news_items}
        """

        try:
            alert_msg = _chat(system_prompt, user_prompt)
            self.telegram.send_message(alert_msg)
            print(f">>> [Moneybag 전송 완료] {coin}")
        except Exception as e:
            print(f"❌ AI/전송 실패: {e}")
            traceback.print_exc()


# [★핵심 2] 시동 버튼
if __name__ == "__main__":
    print("🦅 [System] Moneybag Watchdog 프로세스 시작")
    sys.stdout.flush()

    try:
        dog = MarketWatchdog()
        print("🦅 [System] 감시 루프 진입...")

        while True:
            try:
                dog.check_market()
            except Exception as e:
                print(f"\n❌ [Error] 루프 실행 중 오류: {e}")
                traceback.print_exc()
            time.sleep(10)

    except Exception as fatal_e:
        print(f"💀 [Fatal] 왓치독 치명적 오류: {fatal_e}")
        traceback.print_exc()
