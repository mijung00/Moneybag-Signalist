import time
import sys
import os
import json
import requests
import traceback
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import deque
from dotenv import load_dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
if project_root not in sys.path:
    sys.path.append(project_root)

load_dotenv(os.path.join(project_root, ".env"))

SOCKET_TIMEOUT_SEC = int(os.getenv("WATCHDOG_SOCKET_TIMEOUT_SEC", "15"))
socket.setdefaulttimeout(SOCKET_TIMEOUT_SEC)

HEARTBEAT_PATH = os.getenv("MONEYBAG_HEARTBEAT_PATH", "/tmp/moneybag_market_watchdog.heartbeat")
STATE_DIR = Path(os.getenv("WATCHDOG_STATE_DIR", "/var/app/persistent"))
STATE_PATH = STATE_DIR / "moneybag_market_watchdog_state.json"

try:
    from moneybag.src.collectors.crypto_news_rss import CryptoNewsRSS
    from moneybag.src.llm.openai_driver import _chat
    from moneybag.src.pipelines.send_channels import TelegramSender
except ImportError as e:
    print(f"❌ [Import Error] 모듈을 찾을 수 없습니다: {e}")
    sys.path.append(os.getcwd())
    from moneybag.src.collectors.crypto_news_rss import CryptoNewsRSS
    from moneybag.src.llm.openai_driver import _chat
    from moneybag.src.pipelines.send_channels import TelegramSender


def now_kst() -> datetime:
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=9)))


def hhmm(t: datetime) -> str:
    return t.strftime("%H:%M")


class MarketWatchdog:
    def __init__(self):
        token = os.getenv("TELEGRAM_BOT_TOKEN_MONEYBAG")
        chat_id = os.getenv("TELEGRAM_CHAT_ID_MONEYBAG")
        self.telegram = TelegramSender(token=token, chat_id=chat_id)
        self.news_collector = CryptoNewsRSS()

        self.targets = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]

        # =========================
        # ✅✅ 알림 기준 수정 영역 (여기만 바꾸면 됨)
        # =========================
        self.poll_sec = 10

        # “의미 있는 움직임” 기준 (15분 / 1시간)
        self.th_15m = 1.2
        self.th_60m = 2.5

        # 같은 코인 반복 울림 방지(쿨타임)
        self.cooldown_sec = 30 * 60  # 30분

        # 쿨타임 중이라도 ‘추가 급변’이면 알림(예: 알림 후 다시 +1.5% 더)
        self.escalate_extra_pct = 1.5

        # 정기 “생존 신호” 브리핑 시간(죽었는지 확인용) - 코인은 24시간이니 하루 2번 추천
        self.brief_times = ["09:00", "18:35"]
        self.brief_use_llm = False  # 정기 브리핑까지 AI 돌리면 비용/잡음 증가(기본 False)
        # =========================

        self.price_history = {c: deque(maxlen=24 * 3600 // self.poll_sec) for c in self.targets}
        self.last_alert_price = {}
        self.last_alert_time = {}
        self.sent_brief_dates = {}  # "09:00" -> "YYYY-MM-DD"

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
            self.last_alert_price = raw.get("last_alert_price") or {}
            tmap = raw.get("last_alert_time") or {}
            self.last_alert_time = {k: datetime.fromisoformat(v) for k, v in tmap.items()}
            self.sent_brief_dates = raw.get("sent_brief_dates") or {}
        except Exception:
            pass

    def _save_state(self):
        try:
            self._ensure_state_dir()
            raw = {
                "last_alert_price": self.last_alert_price,
                "last_alert_time": {k: v.isoformat() for k, v in self.last_alert_time.items()},
                "sent_brief_dates": self.sent_brief_dates,
            }
            STATE_PATH.write_text(json.dumps(raw, ensure_ascii=False))
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

    def get_binance_24h_change_pct(self, symbol):
        """정기 브리핑용: 24시간 변화율(바이낸스 제공)"""
        try:
            url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                return float(data.get("priceChangePercent"))
        except Exception:
            pass
        return None

    def _append_history(self, coin, price):
        self.price_history[coin].append((now_kst(), price))

    def _pct_change_since(self, coin, seconds):
        hist = self.price_history[coin]
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

    def _should_alert(self, coin, price):
        """
        ✅ 의미 있는 구간 기준 + 쿨타임 예외(추가 급변)
        """
        ch15 = self._pct_change_since(coin, 15 * 60)
        ch60 = self._pct_change_since(coin, 60 * 60)

        last_t = self.last_alert_time.get(coin)
        in_cooldown = False
        if last_t and (now_kst() - last_t).total_seconds() < self.cooldown_sec:
            in_cooldown = True

        # 기본 트리거
        base_trigger = False
        reasons = []
        if ch15 is not None and abs(ch15) >= self.th_15m:
            base_trigger = True
            reasons.append(f"15분 {ch15:+.2f}%")
        if ch60 is not None and abs(ch60) >= self.th_60m:
            base_trigger = True
            reasons.append(f"1시간 {ch60:+.2f}%")

        # 쿨타임 중 추가 급변(알림가 대비)
        if coin not in self.last_alert_price:
            self.last_alert_price[coin] = price
            self._save_state()
            return (False, "", ch15, ch60)

        base = self.last_alert_price[coin]
        ch_from_alert = ((price - base) / base) * 100.0 if base else 0.0

        if in_cooldown and abs(ch_from_alert) >= self.escalate_extra_pct:
            return (True, f"🚨 추가 급변(쿨타임 무시): 알림가 대비 {ch_from_alert:+.2f}%", ch15, ch60)

        if (not in_cooldown) and base_trigger:
            return (True, " / ".join(reasons), ch15, ch60)

        return (False, "", ch15, ch60)

    def _send_llm_or_plain(self, title: str, context: str):
        if not self.brief_use_llm:
            self.telegram.send_message(f"{title}\n{context}")
            return

        system_prompt = (
            "너는 '리스크 매니저'다. 과장하지 말고 드라이하게 정리해라. "
            "매수/매도 지시처럼 보이는 말은 금지. "
            "체크리스트와 관찰 포인트 중심으로 작성해라."
        )
        try:
            msg = _chat(system_prompt, context)
        except Exception:
            msg = f"{title}\n{context}"
        self.telegram.send_message(msg)

    def _maybe_send_briefs(self):
        """
        ✅✅ 정기 브리핑(생존 신호)
        - 매일 09:00 / 21:00 1회씩
        """
        t = now_kst()
        today = t.date().isoformat()
        cur_hhmm = hhmm(t)

        for bt in self.brief_times:
            if cur_hhmm >= bt and self.sent_brief_dates.get(bt) != today:
                lines = [f"🟨 [Moneybag] 정기 브리핑 ({t.strftime('%Y-%m-%d %H:%M')})"]
                for c in self.targets:
                    p = self.get_binance_price(c)
                    if p is None:
                        continue
                    self._append_history(c, p)
                    ch60 = self._pct_change_since(c, 60 * 60)
                    ch24 = self.get_binance_24h_change_pct(c)
                    parts = [f"현재가 {p:,.2f}"]
                    if ch60 is not None:
                        parts.append(f"1시간 {ch60:+.2f}%")
                    if ch24 is not None:
                        parts.append(f"24시간 {ch24:+.2f}%")
                    lines.append(f"- {c}: " + " / ".join(parts))

                self._send_llm_or_plain(lines[0], "\n".join(lines))
                self.sent_brief_dates[bt] = today
                self._save_state()

    def check_market(self):
        self._touch_heartbeat()
        print(f"\r👀 Moneybag 감시 중... ({now_kst().strftime('%H:%M:%S')})", end="", flush=True)

        # ✅ 정기 브리핑(죽었는지 확인용)
        self._maybe_send_briefs()

        # ✅ 급변 알림
        for coin in self.targets:
            price = self.get_binance_price(coin)
            if price is None:
                continue
            self._append_history(coin, price)

            ok, reason, ch15, ch60 = self._should_alert(coin, price)
            if not ok:
                continue

            # 알림 생성(LLM + 뉴스)
            news_items = self.news_collector.collect_all()
            news_text = "특이 뉴스 없음." if not news_items else "\n".join([f"- {item['title']}" for item in news_items[:3]])

            title = f"🚨 [Moneybag] {coin} 변동 감지"
            ctx = (
                f"[현재가] {price}\n"
                f"[사유] {reason}\n"
                f"[참고] 15분={ch15}, 1시간={ch60}\n"
                f"[뉴스]\n{news_text}\n"
            )

            try:
                msg = _chat(
                    "너는 'Moneybag 리스크 매니저'다. 과장 없이 상황을 설명하고 "
                    "확인해야 할 포인트를 체크리스트로 제시해라. 매수/매도 지시는 금지.",
                    ctx,
                )
            except Exception:
                msg = f"{title}\n{ctx}"

            self.telegram.send_message(msg)

            # 상태 업데이트
            self.last_alert_price[coin] = price
            self.last_alert_time[coin] = now_kst()
            self._save_state()


if __name__ == "__main__":
    print("🦅 [System] Moneybag Watchdog 프로세스 시작")
    sys.stdout.flush()

    dog = MarketWatchdog()
    print("🦅 [System] 감시 루프 진입.")

    while True:
        try:
            dog.check_market()
        except Exception as e:
            print(f"\n❌ [Error] 루프 에러: {e}")
            traceback.print_exc()
        time.sleep(dog.poll_sec)
