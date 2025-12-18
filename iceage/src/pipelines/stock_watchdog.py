# iceage/src/pipelines/stock_watchdog.py
import os
import sys
import time
import json
import signal
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import deque
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, List, Dict, Set

import requests
import yfinance as yf
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------
# ✅ 알림 기준 수정 (여기만 건드리면 됨)
# ---------------------------------------------------------------------
SERVICE_NAME = "Signalist"
TZ = ZoneInfo("Asia/Seoul")

TICKERS = {
    "^KS11": "KOSPI",
    "^KQ11": "KOSDAQ",
}

POLL_INTERVAL_SEC = 10

# 변화량 레벨(%)
SIGNALIST_ALERT_LEVELS = [1, 2, 3, 5]

# 10분 급가속 기준
ACCEL_10M_PCT = 1.0

# 기본 쿨타임(분) - 단, “새 레벨 돌파”는 쿨타임 무시
COOLDOWN_MIN = 20

# 정기 “생존 신호” 브리핑 시간(죽었는지 확인용) - KST 기준
OPEN_BRIEF_TIME = "09:05"
CLOSE_BRIEF_TIME = "16:05"
BRIEF_USE_LLM = True
# ---------------------------------------------------------------------


def _repo_root_on_syspath() -> None:
    try:
        from pathlib import Path
        repo_root = Path(__file__).resolve().parents[3]
        repo_root_str = str(repo_root)
        if repo_root_str not in sys.path:
            sys.path.insert(0, repo_root_str)
    except Exception:
        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.insert(0, cwd)


_repo_root_on_syspath()

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------------------------------------------------------------------
# ✅ SecretsManager를 JSON 형태로 저장했을 때도 동작하게(OPENAI_API_KEY 등)
#    예) OPENAI_API_KEY='{"OPENAI_API_KEY":"sk-..."}' → OPENAI_API_KEY='sk-...'
# ---------------------------------------------------------------------
def _normalize_json_env(env_key: str) -> None:
    raw = os.getenv(env_key, "")
    if not raw:
        return
    s = raw.strip()

    # JSON 형태 아니면 그대로 둠
    if not (s.startswith("{") and s.endswith("}")):
        return

    try:
        obj = json.loads(s)
        if not isinstance(obj, dict):
            return

        # 1) env_key와 같은 키가 있으면 그 값을 사용
        v = obj.get(env_key)

        # 2) 없으면 value라는 관용 키를 사용
        if not v:
            v = obj.get("value")

        # 3) 그것도 없으면 dict 안의 "첫번째 문자열 값"을 사용
        if not v:
            for vv in obj.values():
                if isinstance(vv, str) and vv.strip():
                    v = vv.strip()
                    break

        if isinstance(v, str) and v.strip():
            os.environ[env_key] = v.strip()
    except Exception:
        # JSON 파싱 실패면 원문 유지
        pass

# ✅ OpenAI 키를 import 전에 정규화
_normalize_json_env("OPENAI_API_KEY")

try:
    from iceage.src.llm.openai_driver import _chat
except Exception as e:
    print(f"⚠️ [LLM Import] {e}", flush=True)
    _chat = None



def _extract_secret_value(raw: str, env_key: str) -> str:
    if not raw:
        return ""
    s = raw.strip()
    if s.startswith("{") and s.endswith("}"):
        try:
            obj = json.loads(s)
            if isinstance(obj, dict):
                if env_key in obj and isinstance(obj[env_key], str) and obj[env_key].strip():
                    return obj[env_key].strip()
                for v in obj.values():
                    if isinstance(v, str) and v.strip():
                        return v.strip()
        except Exception:
            return s
    return s

# ---------------------------------------------------------------------
# ✅ JSON 시크릿도 정상 처리되도록: OPENAI_API_KEY 정규화 (중요!)
# - Secrets Manager에서 {"OPENAI_API_KEY":"..."} 형태로 들어와도
#   실제 키 문자열만 뽑아서 OPENAI_API_KEY에 다시 넣어준다.
# - 반드시 openai_driver import(_chat) 보다 "먼저" 실행되어야 함
# ---------------------------------------------------------------------
_raw = os.getenv("OPENAI_API_KEY", "")
if _raw:
    os.environ["OPENAI_API_KEY"] = _extract_secret_value(_raw, "OPENAI_API_KEY")

try:
    from iceage.src.llm.openai_driver import _chat
except Exception:
    _chat = None


@dataclass
class TelegramClient:
    token: str
    chat_id: str

    def send(self, text: str) -> None:
        if not self.token or not self.chat_id:
            print("❌ [Telegram] token/chat_id 비어있음", flush=True)
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True}
        try:
            r = requests.post(url, json=payload, timeout=15)
            if r.status_code != 200:
                print(f"❌ [Telegram Error] status={r.status_code} body={r.text[:200]}", flush=True)
        except Exception as e:
            print(f"❌ [Telegram Exception] {e}", flush=True)


class SignalistWatchdog:
    def __init__(self):
        tok_raw = os.getenv("TELEGRAM_BOT_TOKEN_SIGNALIST", "")
        chat_raw = os.getenv("TELEGRAM_CHAT_ID_SIGNALIST", "")
        token = _extract_secret_value(tok_raw, "TELEGRAM_BOT_TOKEN_SIGNALIST")
        chat_id = _extract_secret_value(chat_raw, "TELEGRAM_CHAT_ID_SIGNALIST")
        self.tg = TelegramClient(token=token, chat_id=chat_id)

        self.hist = {t: deque(maxlen=1200) for t in TICKERS}
        self.baseline = {}      # ticker -> (date, price)
        self.sent_levels = {}   # ticker -> (date, set[(sign, level)])
        self.last_alert_time = {t: None for t in TICKERS}

        self._open_brief_date = None
        self._close_brief_date = None
        self._test_alert_sent = False # ✅ 테스트 알림 1회 발송용 플래그

        self._stop = False
        signal.signal(signal.SIGTERM, self._on_stop)
        signal.signal(signal.SIGINT, self._on_stop)

    def _on_stop(self, *_):
        self._stop = True

    def _now(self) -> datetime:
        return datetime.now(TZ)

    def _get_price(self, ticker: str) -> Optional[float]:
        """
        yfinance를 통해 현재가를 조회. fast_info가 빠르지만 실패하거나 장중 업데이트가 안될 수 있어,
        실패 시 history()를 fallback으로 사용해 안정성을 높임.
        """
        try:
            # 1. 빠르지만 가끔 실패하거나 오래된 데이터를 주는 fast_info 먼저 시도
            price = float(yf.Ticker(ticker).fast_info["last_price"])
            return price
        except Exception:
            # 2. fast_info 실패 시, history()로 재시도 (더 안정적)
            logging.warning(f"⚠️ [Price] {ticker} fast_info 조회 실패, history()로 재시도")
            try:
                data = yf.Ticker(ticker).history(period="1d")
                if data is not None and not data.empty:
                    return float(data["Close"].iloc[-1])
            except Exception as e_inner:
                logging.error(f"⚠️ [Price] {ticker} history() 조회도 실패: {e_inner}")
                return None
        return None

    def _pct_over_minutes(self, ticker: str, minutes: int) -> Optional[float]:
        h = self.hist[ticker]
        if len(h) < 2:
            return None
        target_ts = self._now() - timedelta(minutes=minutes)
        old_price = None
        for ts, p in h:
            if ts <= target_ts:
                old_price = p
            else:
                break
        if old_price is None:
            return None
        cur_price = h[-1][1]
        return ((cur_price - old_price) / old_price) * 100.0

    def _ensure_daily_state(self, ticker: str, price: float):
        today = self._now().date()
        if ticker not in self.baseline or self.baseline[ticker][0] != today:
            self.baseline[ticker] = (today, price)
            self.sent_levels[ticker] = (today, set())
            self.last_alert_time[ticker] = None

    def _level_crossed(self, base_price: float, cur_price: float) -> List[Tuple[int, int]]:
        pct = ((cur_price - base_price) / base_price) * 100.0
        sign = 1 if pct >= 0 else -1
        apct = abs(pct)
        crosses = []
        for lv in SIGNALIST_ALERT_LEVELS:
            if apct >= lv:
                crosses.append((sign, lv))
        return crosses

    def _fetch_headlines(self, limit: int = 3) -> str:
        try:
            url = "https://m.stock.naver.com/news/mainnews"
            r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.select("a.NewsList_item__lO7iA")[:limit]
            lines = []
            for it in items:
                title = it.get_text(strip=True)
                href = it.get("href", "")
                if href and href.startswith("/"):
                    href = "https://m.stock.naver.com" + href
                if href:
                    lines.append(f"- {title}\n  {href}")
                else:
                    lines.append(f"- {title}")
            return "\n".join(lines)
        except Exception:
            return ""

    def _llm_comment(self, user_prompt: str) -> str:
        if not BRIEF_USE_LLM or not _chat:
            return ""
        try:
            system = "너는 'Signalist'의 시장 관측 애널리스트다. 투자 조언 금지. 상황 설명만."
            return (_chat(system, user_prompt) or "").strip()
        except Exception:
            return ""

    # ✅ 중요: 브리핑은 “시장 열려있나?”와 무관하게 시간만 맞으면 무조건 실행
    def _send_brief_if_due(self):
        now = self._now()
        hhmm = now.strftime("%H:%M")
        today = now.date()

        if hhmm == OPEN_BRIEF_TIME and self._open_brief_date != today:
            self.tg.send(self._format_brief("장 시작 브리핑"))
            self._open_brief_date = today

        if hhmm == CLOSE_BRIEF_TIME and self._close_brief_date != today:
            self.tg.send(self._format_brief("장 마감 브리핑"))
            self._close_brief_date = today

    def _format_brief(self, tag: str) -> str:
        now = self._now().strftime("%Y-%m-%d %H:%M")
        lines = [f"🟨 {SERVICE_NAME} {tag} ({now} KST)"]
        for t, name in TICKERS.items():
            price = self._get_price(t)
            if price is None:
                continue
            self._ensure_daily_state(t, price)
            base = self.baseline[t][1]
            pct = ((price - base) / base) * 100.0
            lines.append(f"- {name}: {price:,.2f} (기준 대비 {pct:+.2f}%)")

        headlines = self._fetch_headlines(3)
        if headlines:
            lines += ["", "📰 주요 헤드라인", headlines]

        # LLM 프롬프트를 상황에 맞게 생성
        user_prompt = "\n".join(lines)
        if tag == "장 시작 브리핑":
            user_prompt += "\n\n위 내용은 장 시작(09:05) 직후의 상황이다. '개장 전'이라는 표현 대신, '개장 초반' 또는 '장 시작 직후'라는 표현을 사용해서 3~5줄로 요약해줘."
        else:
            user_prompt += "\n\n3~5줄로 요약해줘."

        llm = self._llm_comment(user_prompt)
        if llm:
            lines += ["", "🤖 AI 요약", llm]

        return "\n".join(lines)

    def _format_level_alert(self, name: str, price: float, pct_base: float, sign: int, lv: int,
                           pct10: Optional[float], headlines: str, llm: str) -> str:
        now = self._now().strftime("%Y-%m-%d %H:%M:%S")
        direction = "상승" if sign > 0 else "하락"
        lines = [
            f"🚨 {SERVICE_NAME} 지수 급변 알림 ({now} KST)",
            f"- {name}: {price:,.2f}",
            f"- 기준 대비: {pct_base:+.2f}%",
            f"- 새 레벨 돌파: {direction} {lv}%"
        ]
        if pct10 is not None:
            lines.append(f"- 10분 변화: {pct10:+.2f}%")
        if headlines:
            lines += ["", "📰 주요 헤드라인", headlines]
        if llm:
            lines += ["", "🤖 AI 코멘트", llm]
        return "\n".join(lines)

    def _format_accel_alert(self, name: str, price: float, pct_base: float, pct10: float,
                           headlines: str, llm: str) -> str:
        now = self._now().strftime("%Y-%m-%d %H:%M:%S")
        lines = [
            f"🚨 {SERVICE_NAME} 급가속 알림 ({now} KST)",
            f"- {name}: {price:,.2f}",
            f"- 기준 대비: {pct_base:+.2f}%",
            f"- 사유: 10분 급가속(≥ {ACCEL_10M_PCT:.2f}%) / 실제 10분 변화 {pct10:+.2f}%"
        ]
        if headlines:
            lines += ["", "📰 주요 헤드라인", headlines]
        if llm:
            lines += ["", "🤖 AI 코멘트", llm]
        return "\n".join(lines)

    def run_forever(self):
        print("🦅 [System] Signalist Watchdog 시작", flush=True)
        print("🦅 [System] 주식 감시 루프 진입...", flush=True)

        while not self._stop:
            self._send_brief_if_due()

            now = self._now()
            for ticker, name in TICKERS.items():
                price = self._get_price(ticker)
                if price is None:
                    continue
                
                # --- 테스트 로직: 시작 후 첫 가격 조회 성공 시 1회 알림 ---
                # TODO: 테스트 완료 후 이 블록을 삭제하세요.
                if not self._test_alert_sent:
                    self.tg.send(f"🧪 [Signalist Test] '{name}' 감시 시작. 현재 지수: {price:,.2f}")
                    self._test_alert_sent = True # 모든 티커 중 하나에 대해서만 1회 실행
                # --- 테스트 로직 끝 ---

                self.hist[ticker].append((now, price))
                self._ensure_daily_state(ticker, price)

                base = self.baseline[ticker][1]
                pct_base = ((price - base) / base) * 100.0
                
                pct10 = self._pct_over_minutes(ticker, 10)
                crossed = self._level_crossed(base, price)
                today, sent = self.sent_levels[ticker]

                new_levels = [c for c in crossed if c not in sent]

                last_t = self.last_alert_time.get(ticker)
                cooldown_ok = (last_t is None) or ((now - last_t) >= timedelta(minutes=COOLDOWN_MIN))

                accel_only = (pct10 is not None and abs(pct10) >= ACCEL_10M_PCT and not new_levels)

                if accel_only and not cooldown_ok:
                    continue

                if not new_levels and not accel_only:
                    continue

                headlines = self._fetch_headlines(3)

                llm = ""
                if _chat:
                    try:
                        system = "너는 'Signalist'의 시장 관측 애널리스트다. 투자 조언 금지."
                        user = f"{name} 지수: 기준 대비 {pct_base:+.2f}%, 10분 변화={pct10}. 3~5줄 설명."
                        llm = (_chat(system, user) or "").strip()
                    except Exception:
                        llm = ""

                # 레벨 알림이 있으면: “가장 큰 새 레벨 1개”만 보내고 나머지는 sent 처리
                if new_levels:
                    new_levels_sorted = sorted(new_levels, key=lambda x: x[1], reverse=True)
                    sign, lv = new_levels_sorted[0]
                    self.tg.send(self._format_level_alert(name, price, pct_base, sign, lv, pct10, headlines, llm))
                    for c in new_levels:
                        sent.add(c)
                    self.sent_levels[ticker] = (today, sent)
                    self.last_alert_time[ticker] = now

                # 급가속만으로 알림
                elif accel_only and pct10 is not None:
                    self.tg.send(self._format_accel_alert(name, price, pct_base, pct10, headlines, llm))
                    self.last_alert_time[ticker] = now

            print(f"\r👀 Signalist 감시 중... ({self._now().strftime('%H:%M:%S')})", end="", flush=True)
            time.sleep(POLL_INTERVAL_SEC)


def main():
    SignalistWatchdog().run_forever()


if __name__ == "__main__":
    main()
