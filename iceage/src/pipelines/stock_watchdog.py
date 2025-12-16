import asyncio
import sys
import os
import requests
from pathlib import Path
from datetime import datetime
import yfinance as yf
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import socket
import traceback

# [★핵심 1] 경로 강제 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
if project_root not in sys.path:
    sys.path.append(project_root)

load_dotenv(os.path.join(project_root, ".env"))

# 네트워크가 '영원히 멈춰서' 전체 프로세스가 굳는 걸 막기 위한 기본 타임아웃(초)
SOCKET_TIMEOUT_SEC = int(os.getenv("WATCHDOG_SOCKET_TIMEOUT_SEC", "15"))
socket.setdefaulttimeout(SOCKET_TIMEOUT_SEC)

# 이 파일이 '살아있다'는 표시(매 루프마다 갱신). watchdogs.py(매니저)가 이걸 보고 멈춤/행을 감지해서 재시작함.
HEARTBEAT_PATH = os.getenv("ICEAGE_HEARTBEAT_PATH", "/tmp/iceage_stock_watchdog.heartbeat")

try:
    from iceage.src.pipelines.telegram_bot import SignalistTelegramBot
    from moneybag.src.llm.openai_driver import _chat
except ImportError:
    # 모듈 경로 대체 시도
    sys.path.append(os.getcwd())
    from src.pipelines.telegram_bot import SignalistTelegramBot
    from moneybag.src.llm.openai_driver import _chat


class StockWatchdog:
    def __init__(self):
        token = os.getenv("TELEGRAM_BOT_TOKEN_SIGNALIST")
        chat_id = os.getenv("TELEGRAM_CHAT_ID_SIGNALIST")

        if token:
            print(f"🔑 [Signalist] 토큰 로드 성공: {token[:5]}...")
        else:
            print("❌ [Signalist] 토큰 없음! 환경변수를 확인하세요.")

        self.bot = SignalistTelegramBot(token=token, chat_id=chat_id)
        self.targets = {"^KS11": "코스피", "^KQ11": "코스닥"}

        # 시총 상위 감시 풀
        self.monitoring_pool = {
            "^KS11": ["005930.KS", "000660.KS", "373220.KS", "207940.KS"],
            "^KQ11": ["247540.KQ", "086520.KQ", "022100.KQ"],
        }

        self.prev_prices = {}
        self.alert_cooldown = {}
        self.alert_baseline = {}

    def _touch_heartbeat(self):
        try:
            Path(HEARTBEAT_PATH).write_text(datetime.now().isoformat())
        except Exception:
            pass

    async def get_current_price(self, ticker):
        try:
            ticker_obj = yf.Ticker(ticker)
            info = getattr(ticker_obj, "fast_info", None) or {}
            price = info.get("last_price")
            if price is None:
                # fast_info가 비어있을 때를 대비
                hist = ticker_obj.history(period="1d")
                if not hist.empty:
                    price = float(hist["Close"].iloc[-1])
            return price
        except Exception as e:
            print(f"⚠️ 가격 조회 실패({ticker}): {e}")
            traceback.print_exc()
            return None

    def get_market_movers(self, index_ticker):
        movers = []
        for stock in self.monitoring_pool.get(index_ticker, []):
            try:
                st = yf.Ticker(stock)
                p = st.fast_info["last_price"]
                prev = st.fast_info["previous_close"]
                pct = ((p - prev) / prev) * 100
                name = stock  # 실제 이름 매핑은 생략
                movers.append(f"{name}({pct:+.2f}%)")
            except Exception:
                continue
        return ", ".join(movers[:3])

    def get_naver_news_headlines(self):
        try:
            url = "https://news.naver.com/main/list.naver?mode=LSD&mid=sec&sid1=101"
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = requests.get(url, headers=headers, timeout=5)
            soup = BeautifulSoup(resp.text, "html.parser")
            titles = soup.select(".type06_headline li dl dt a")

            headlines = []
            for t in titles[:3]:
                headlines.append(t.text.strip())
            return "\n".join(headlines)
        except Exception:
            return "뉴스 수집 실패"

    async def check_market(self):
        self._touch_heartbeat()

        print(
            f"\r👀 Signalist 감시 중... ({datetime.now().strftime('%H:%M:%S')})",
            end="",
            flush=True,
        )

        for ticker_key, name in self.targets.items():
            current_price = await self.get_current_price(ticker_key)
            if current_price is None:
                continue

            # 기준가 설정 (첫 실행 시 혹은 쿨타임 리셋 후)
            if ticker_key not in self.alert_baseline:
                self.alert_baseline[ticker_key] = current_price
                continue

            baseline_price = self.alert_baseline[ticker_key]

            # 변동률 계산 (기준가 대비)
            change_pct = ((current_price - baseline_price) / baseline_price) * 100

            # 스마트 알림 로직
            should_alert = False
            last_time = self.alert_cooldown.get(ticker_key)

            # 1. 기본: 1.5% 이상 변동
            if abs(change_pct) >= 1.5:
                # 쿨타임(1시간) 체크
                if not last_time or (datetime.now() - last_time).seconds >= 3600:
                    should_alert = True
                else:
                    # 2. 스마트 패스: 쿨타임 중이라도 추가 1.0% 더 변동하면 발송 (약식 처리)
                    pass

            if should_alert:
                print(f"\n⚡ [Signalist] {name} 포착! ({change_pct:+.2f}%)")
                await self.send_alert(ticker_key, name, current_price, change_pct)

    async def send_alert(self, ticker_key, name, price, change_pct):
        news_summary = self.get_naver_news_headlines()
        movers_status = self.get_market_movers(ticker_key)

        system_prompt = f"""
        너는 'Signalist 수석 애널리스트'다.
        {name} 급변동 발생. 원인을 분석해라.
        """
        user_prompt = (
            f"지수: {name}, 현재가: {price:,.2f}, 등락률: {change_pct:+.2f}%, "
            f"주도주: {movers_status}, 뉴스: {news_summary}"
        )

        try:
            msg = _chat(system_prompt, user_prompt)
        except Exception:
            msg = f"🚨 **[Signalist] {name}**\n📊 {change_pct:+.2f}%"

        await self.bot.send_message(msg)
        print(f">>> [전송 완료] {name}")

        # ✅ 버그 수정: key를 name이 아니라 ticker_key로 맞춰야 함
        self.alert_cooldown[ticker_key] = datetime.now()
        self.alert_baseline[ticker_key] = price


# [★핵심 2] 시동 버튼 (비동기 루프)
async def main():
    print("🦅 [System] Signalist Watchdog 프로세스 시작")
    sys.stdout.flush()

    dog = StockWatchdog()
    print("🦅 [System] 주식 감시 루프 진입...")

    while True:
        try:
            await dog.check_market()
        except Exception as e:
            print(f"\n❌ [Error] 루프 에러: {e}")
            traceback.print_exc()

        # 10초 대기 (비동기 sleep)
        await asyncio.sleep(10)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 왓치독 종료")
    except Exception as fatal_e:
        print(f"💀 [Fatal] 왓치독 사망: {fatal_e}")
