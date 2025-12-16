import asyncio
import sys
import os
import json
import requests
import socket
import traceback
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import deque
import yfinance as yf
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ----------------------------
# 경로/환경 로드
# ----------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
if project_root not in sys.path:
    sys.path.append(project_root)

load_dotenv(os.path.join(project_root, ".env"))

SOCKET_TIMEOUT_SEC = int(os.getenv("WATCHDOG_SOCKET_TIMEOUT_SEC", "15"))
socket.setdefaulttimeout(SOCKET_TIMEOUT_SEC)

HEARTBEAT_PATH = os.getenv("ICEAGE_HEARTBEAT_PATH", "/tmp/iceage_stock_watchdog.heartbeat")
STATE_DIR = Path(os.getenv("WATCHDOG_STATE_DIR", "/var/app/persistent"))
STATE_PATH = STATE_DIR / "iceage_stock_watchdog_state.json"

try:
    from iceage.src.pipelines.telegram_bot import SignalistTelegramBot
    from moneybag.src.llm.openai_driver import _chat
except ImportError:
    sys.path.append(os.getcwd())
    from src.pipelines.telegram_bot import SignalistTelegramBot
    from moneybag.src.llm.openai_driver import _chat


def now_kst() -> datetime:
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=9)))


def is_weekday_kst(t: datetime) -> bool:
    return t.weekday() < 5  # 0=Mon ... 4=Fri


def hhmm(t: datetime) -> str:
    return t.strftime("%H:%M")


class StockWatchdog:
    def __init__(self):
        token = os.getenv("TELEGRAM_BOT_TOKEN_SIGNALIST")
        chat_id = os.getenv("TELEGRAM_CHAT_ID_SIGNALIST")
        self.bot = SignalistTelegramBot(token=token, chat_id=chat_id)

        self.targets = {"^KS11": "코스피", "^KQ11": "코스닥"}

        # 참고용(주도주 느낌)
        self.monitoring_pool = {
            "^KS11": ["005930.KS", "000660.KS", "373220.KS", "207940.KS"],
            "^KQ11": ["247540.KQ", "086520.KQ", "022100.KQ"],
        }

        # =========================
        # ✅✅ 알림 기준 수정 영역 (여기만 바꾸면 됨)
        # =========================
        self.poll_sec = 10  # 감시 주기(초)

        # “레벨 돌파 알림” 기준: 전일 종가 대비 |변화율| %
        # 예: 1,2,3,5면 |전일대비|가 1%/2%/3%/5%를 처음 넘는 순간마다 알림 가능
        self.levels = [1.0, 2.0, 3.0, 5.0]

        # 기본 쿨타임(같은 레벨에서 반복 울림 방지)
        self.cooldown_sec = 30 * 60  # 30분

        # 단기 급가속(10분 변화율)
        self.th_10m = 0.7  # 10분에 0.7% 이상이면 “급가속” 알림 후보

        # 정기 “생존 신호” 브리핑 시간(죽었는지 확인용)
        self.open_brief_time = "09:05"   # 장 시작 5분 후
        self.close_brief_time = "19:55"  # 장 마감 후
        self.brief_use_llm = True        # 정기 브리핑에도 AI 설명을 붙일지
        # =========================

        # 상태(재시작해도 유지)
        self.price_history = {k: deque(maxlen=6 * 3600 // self.poll_sec) for k in self.targets}
        self.last_alert_time = {}      # ticker -> datetime
        self.last_alert_level = {}     # ticker -> int(level_index)
        self.last_alert_sign = {}      # ticker -> +1 or -1  (부호 전환 감지용)

        self.sent_open_brief_date = None
        self.sent_close_brief_date = None

        self._load_state()

    def _touch_heartbeat(self):
        try:
            Path(HEARTBEAT_PATH).write_text(now_kst().isoformat())
        except Exception:
            pass

    def _ensure_state_dir(self):
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

    def _load_state(self):
        self._ensure_state_dir()
        if not STATE_PATH.exists():
            return
        try:
            raw = json.loads(STATE_PATH.read_text())
            self.last_alert_time = {k: datetime.fromisoformat(v) for k, v in (raw.get("last_alert_time") or {}).items()}
            self.last_alert_level = raw.get("last_alert_level") or {}
            self.last_alert_sign = raw.get("last_alert_sign") or {}
            self.sent_open_brief_date = raw.get("sent_open_brief_date")
            self.sent_close_brief_date = raw.get("sent_close_brief_date")
        except Exception:
            pass

    def _save_state(self):
        try:
            self._ensure_state_dir()
            raw = {
                "last_alert_time": {k: v.isoformat() for k, v in self.last_alert_time.items()},
                "last_alert_level": self.last_alert_level,
                "last_alert_sign": self.last_alert_sign,
                "sent_open_brief_date": self.sent_open_brief_date,
                "sent_close_brief_date": self.sent_close_brief_date,
            }
            STATE_PATH.write_text(json.dumps(raw, ensure_ascii=False))
        except Exception:
            pass

    async def get_current_and_prev_close(self, ticker):
        try:
            t = yf.Ticker(ticker)
            info = getattr(t, "fast_info", None) or {}
            cur = info.get("last_price")
            prev = info.get("previous_close")

            if cur is None or prev is None:
                hist = t.history(period="2d")
                if hist is not None and not hist.empty:
                    cur = float(hist["Close"].iloc[-1])
                    if len(hist) >= 2:
                        prev = float(hist["Close"].iloc[-2])

            if cur is None or prev is None:
                return (None, None)
            return (float(cur), float(prev))
        except Exception as e:
            print(f"⚠️ 가격 조회 실패({ticker}): {e}")
            traceback.print_exc()
            return (None, None)

    def get_market_movers(self, index_ticker):
        movers = []
        for stock in self.monitoring_pool.get(index_ticker, []):
            try:
                st = yf.Ticker(stock)
                info = getattr(st, "fast_info", None) or {}
                p = info.get("last_price")
                prev = info.get("previous_close")
                if p is None or prev is None:
                    continue
                pct = ((p - prev) / prev) * 100
                movers.append(f"{stock}({pct:+.2f}%)")
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

    def _pct_change_since(self, ticker_key, seconds):
        hist = self.price_history[ticker_key]
        if len(hist) < 2:
            return None
        target = now_kst() - timedelta(seconds=seconds)
        base = None
        for t, p in hist:
            if t >= target:
                base = p
                break
        if base is None:
            base = hist[0][1]
        cur = hist[-1][1]
        if base == 0:
            return None
        return ((cur - base) / base) * 100.0

    def _level_index(self, abs_pct: float) -> int:
        idx = 0
        for lv in self.levels:
            if abs_pct >= lv:
                idx += 1
            else:
                break
        return idx  # 0..N

    async def _send_llm_or_plain(self, title: str, context: str):
        if not self.brief_use_llm:
            await self.bot.send_message(f"{title}\n{context}")
            return

        system_prompt = (
            "너는 시장 상황을 '드라이하게' 설명하는 애널리스트다. "
            "과장하지 말고, 불확실성은 불확실하다고 말해라. "
            "매수/매도 지시처럼 보이는 표현은 절대 하지 말고, "
            "'지금 확인할 것' 체크리스트 형태로 정리해라."
        )
        user_prompt = f"{title}\n\n{context}"

        try:
            msg = _chat(system_prompt, user_prompt)
        except Exception:
            msg = f"{title}\n{context}"

        await self.bot.send_message(msg)

    async def _maybe_send_open_close_briefs(self):
        """
        ✅✅ 정기 브리핑(생존 신호)
        - 평일 09:05 / 16:05에 1회씩 보냄
        """
        t = now_kst()
        if not is_weekday_kst(t):
            return

        today = t.date().isoformat()

        # 09:05 오픈 브리핑
        if hhmm(t) >= self.open_brief_time and self.sent_open_brief_date != today:
            lines = [f"🟩 [Signalist] 장초반 브리핑 ({t.strftime('%Y-%m-%d %H:%M')})"]
            for ticker_key, name in self.targets.items():
                cur, prev = await self.get_current_and_prev_close(ticker_key)
                if cur is None or prev is None:
                    continue
                daily = ((cur - prev) / prev) * 100
                ch5 = self._pct_change_since(ticker_key, 5 * 60)
                movers = self.get_market_movers(ticker_key)
                lines.append(f"- {name}: 전일대비 {daily:+.2f}% / 5분 {ch5:+.2f}% (주도주: {movers})" if ch5 is not None
                             else f"- {name}: 전일대비 {daily:+.2f}% (주도주: {movers})")

            news = self.get_naver_news_headlines()
            ctx = "\n".join(lines) + "\n\n[주요 뉴스]\n" + news
            await self._send_llm_or_plain(lines[0], ctx)

            self.sent_open_brief_date = today
            self._save_state()

        # 16:05 마감 브리핑
        if hhmm(t) >= self.close_brief_time and self.sent_close_brief_date != today:
            lines = [f"🟦 [Signalist] 장마감 브리핑 ({t.strftime('%Y-%m-%d %H:%M')})"]
            for ticker_key, name in self.targets.items():
                cur, prev = await self.get_current_and_prev_close(ticker_key)
                if cur is None or prev is None:
                    continue
                daily = ((cur - prev) / prev) * 100
                ch30 = self._pct_change_since(ticker_key, 30 * 60)
                movers = self.get_market_movers(ticker_key)
                lines.append(f"- {name}: 전일대비 {daily:+.2f}% / 30분 {ch30:+.2f}% (주도주: {movers})" if ch30 is not None
                             else f"- {name}: 전일대비 {daily:+.2f}% (주도주: {movers})")

            news = self.get_naver_news_headlines()
            ctx = "\n".join(lines) + "\n\n[주요 뉴스]\n" + news
            await self._send_llm_or_plain(lines[0], ctx)

            self.sent_close_brief_date = today
            self._save_state()

    async def check_market(self):
        self._touch_heartbeat()
        print(f"\r👀 Signalist 감시 중... ({now_kst().strftime('%H:%M:%S')})", end="", flush=True)

        # ✅ 정기 브리핑(죽었는지 확인용)
        await self._maybe_send_open_close_briefs()

        # ✅ 급변 알림
        for ticker_key, name in self.targets.items():
            cur, prev_close = await self.get_current_and_prev_close(ticker_key)
            if cur is None or prev_close is None:
                continue

            self.price_history[ticker_key].append((now_kst(), cur))

            daily_pct = ((cur - prev_close) / prev_close) * 100.0
            abs_daily = abs(daily_pct)
            sign = 1 if daily_pct >= 0 else -1

            cur_level = self._level_index(abs_daily)
            last_level = int(self.last_alert_level.get(ticker_key, 0))
            last_sign = int(self.last_alert_sign.get(ticker_key, sign))

            ch10 = self._pct_change_since(ticker_key, 10 * 60)
            accel = (ch10 is not None and abs(ch10) >= self.th_10m)

            last_t = self.last_alert_time.get(ticker_key)
            in_cooldown = False
            if last_t and (now_kst() - last_t).total_seconds() < self.cooldown_sec:
                in_cooldown = True

            should = False
            reason = ""
            extra = []
            if ch10 is not None:
                extra.append(f"10분 {ch10:+.2f}%")

            # 1) 레벨 “상향 돌파”는 쿨타임이어도 알림(중요)
            if cur_level > last_level and cur_level >= 1:
                should = True
                reason = f"레벨 돌파: |전일대비| ≥ {self.levels[cur_level - 1]:.1f}% (현재 {daily_pct:+.2f}%)"

            # 2) 부호 전환(+ ↔ -)은 레벨이 낮아도 알림 가치가 큼 (옵션처럼 동작)
            if (not should) and (sign != last_sign) and abs_daily >= self.levels[0]:
                should = True
                reason = f"방향 전환: {('상승' if last_sign > 0 else '하락')} → {('상승' if sign > 0 else '하락')} (현재 {daily_pct:+.2f}%)"

            # 3) 단기 급가속(쿨타임 중엔 더 엄격)
            if (not should) and accel:
                if not in_cooldown:
                    should = True
                    reason = f"단기 급가속: 10분 {ch10:+.2f}%"
                else:
                    if abs(ch10) >= (self.th_10m + 0.4):
                        should = True
                        reason = f"🚨 추가 급가속(쿨타임 무시): 10분 {ch10:+.2f}%"

            if should:
                movers_status = self.get_market_movers(ticker_key)
                news_summary = self.get_naver_news_headlines()

                title = f"🚨 [Signalist] {name} 변동 감지"
                ctx = (
                    f"[전일대비] {daily_pct:+.2f}%\n"
                    f"[사유] {reason}\n"
                    f"[참고] {', '.join(extra) if extra else 'N/A'}\n"
                    f"[주도주] {movers_status}\n"
                    f"[뉴스]\n{news_summary}\n"
                )
                await self._send_llm_or_plain(title, ctx)

                self.last_alert_time[ticker_key] = now_kst()
                self.last_alert_level[ticker_key] = cur_level
                self.last_alert_sign[ticker_key] = sign
                self._save_state()


async def main():
    print("🦅 [System] Signalist Watchdog 프로세스 시작")
    sys.stdout.flush()

    dog = StockWatchdog()
    print("🦅 [System] 주식 감시 루프 진입.")

    while True:
        try:
            await dog.check_market()
        except Exception as e:
            print(f"\n❌ [Error] 루프 에러: {e}")
            traceback.print_exc()
        await asyncio.sleep(dog.poll_sec)


if __name__ == "__main__":
    asyncio.run(main())
