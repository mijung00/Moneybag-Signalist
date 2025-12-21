# moneybag/src/pipelines/market_watchdog.py
import os
import sys
import time
import json
import signal
from dataclasses import dataclass
import re
from datetime import datetime, timedelta
from collections import deque
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, List, Dict, Set

import requests

# ---------------------------------------------------------------------
# ✅ 알림 기준 수정 (여기만 건드리면 됨)
# ---------------------------------------------------------------------
SERVICE_NAME = "The Whale Hunter"  # ✅ 서비스명(메시지에 표시될 이름)
TZ = ZoneInfo("Asia/Seoul")

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]

POLL_INTERVAL_SEC = 10

# “의미 있는 움직임” 기준 (15분 / 60분)
TH_15M_PCT = 1.5     # 예: 0.8% 이상이면 알림 고려
TH_60M_PCT = 3.0     # 예: 2.5% 이상이면 알림 고려

# 10분 급가속(추세 가속) 기준
ACCEL_10M_PCT = 2.0  # 예: 10분에 1.2% 이상이면 “급가속” 알림

# 같은 심볼 연속 알림 쿨타임 (기본 30분)
COOLDOWN_MIN = 30

# 쿨타임 중이라도, “마지막 알림 이후 추가 변동”이 이 이상이면 강제로 또 알림
# (예: 2% 급등 알림 후 5분 만에 추가로 +3% 더 가면 다시 알림)
COOLDOWN_BYPASS_PCT = 2.0

# 하루에 2~3번 “생존신호” 브리핑(죽었는지 확인용) - KST 기준
BRIEF_TIMES = ["09:00", "15:00", "21:00"]
BRIEF_USE_LLM = False
BRIEF_ON_START = False
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

def _normalize_env_json(key: str) -> None:
    """
    환경변수 값이 JSON 문자열 형태로 들어온 경우(예: {"OPENAI_API_KEY":"sk-..."}),
    실제 value만 뽑아서 os.environ[key]에 다시 세팅한다.
    """
    raw = os.getenv(key, "")
    if not raw:
        return
    val = _extract_secret_value(raw, key)
    if val and val != raw:
        os.environ[key] = val

# ✅ 중요: LLM 드라이버(_chat) import 전에 API 키를 정규화해야 함
_normalize_env_json("OPENAI_API_KEY")

try:
    from moneybag.src.collectors.crypto_news_rss import CryptoNewsRSS
except Exception:
    CryptoNewsRSS = None

try:
    from moneybag.src.llm.openai_driver import _chat
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


class MarketWatchdog:
    def __init__(self):
        tok_raw = os.getenv("TELEGRAM_BOT_TOKEN_MONEYBAG", "")
        chat_raw = os.getenv("TELEGRAM_CHAT_ID_MONEYBAG", "")
        token = _extract_secret_value(tok_raw, "TELEGRAM_BOT_TOKEN_MONEYBAG")
        chat_id = _extract_secret_value(chat_raw, "TELEGRAM_CHAT_ID_MONEYBAG")
        self.tg = TelegramClient(token=token, chat_id=chat_id)


        self.news = CryptoNewsRSS() if CryptoNewsRSS else None

        self.price_hist = {s: deque(maxlen=1200) for s in SYMBOLS}
        self.last_alert_time = {s: None for s in SYMBOLS}
        self.last_alert_price = {s: None for s in SYMBOLS}

        self._brief_last_date = {t: None for t in BRIEF_TIMES}
        self._startup_brief_sent = False

        self._stop = False
        signal.signal(signal.SIGTERM, self._on_stop)
        signal.signal(signal.SIGINT, self._on_stop)
        
        self.last_global_alert_time = None
        self.last_global_alert_anchor = None  # 기준 가격(대표 심볼 가격)

    def _on_stop(self, *_):
        self._stop = True

    def _now(self) -> datetime:
        return datetime.now(TZ)

    def _binance_price(self, symbol: str) -> Optional[float]:
        url = "https://api.binance.com/api/v3/ticker/price"
        try:
            r = requests.get(url, params={"symbol": symbol}, timeout=10)
            r.raise_for_status()
            return float(r.json()["price"])
        except Exception as e:
            print(f"⚠️ [Price] {symbol} 조회 실패: {e}", flush=True)
            return None

    def _binance_24h(self, symbol: str) -> Tuple[Optional[float], Optional[float]]:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        try:
            r = requests.get(url, params={"symbol": symbol}, timeout=10)
            r.raise_for_status()
            j = r.json()
            return float(j["lastPrice"]), float(j["priceChangePercent"])
        except Exception:
            return None, None

    def _pct_over_minutes(self, symbol: str, minutes: int) -> Optional[float]:
        hist = self.price_hist[symbol]
        if len(hist) < 2:
            return None
        target_ts = self._now() - timedelta(minutes=minutes)
        old_price = None
        for ts, p in hist:
            if ts <= target_ts:
                old_price = p
            else:
                break
        if old_price is None:
            return None
        cur_price = hist[-1][1]
        return ((cur_price - old_price) / old_price) * 100.0

    def _should_brief_now(self) -> List[str]:
        now = self._now()
        hhmm = now.strftime("%H:%M")
        fired = []
        for t in BRIEF_TIMES:
            if hhmm == t and self._brief_last_date.get(t) != now.date():
                fired.append(t)
        return fired

    def _mark_brief_sent(self, t: str) -> None:
        self._brief_last_date[t] = self._now().date()

    def _format_brief(self) -> str:
        now = self._now().strftime("%Y-%m-%d %H:%M")
        lines = [f"🟨 {SERVICE_NAME} 정기 브리핑 ({now} KST)"]
        for sym in SYMBOLS:
            p, chg24 = self._binance_24h(sym)
            if p is None:
                continue
            if chg24 is None:
                lines.append(f"- {sym}: 현재가 {p:,.2f}")
            else:
                lines.append(f"- {sym}: 현재가 {p:,.2f} / 24시간 {chg24:+.2f}%")
        return "\n".join(lines)

    def _maybe_llm(self, user_prompt: str) -> str:
        if not _chat:
            return ""
        try:
            # 시스템 프롬프트는 그대로 유지
            system = "너는 'The Whale Hunter'의 시장 관측 애널리스트다. 투자 조언이 아니라 시장 설명만 드라이하게 제공한다."
            return (_chat(system, user_prompt) or "").strip()
        except Exception as e:
            err_msg = f"❌ [AI 에러] : {e}"
            print(f"⚠️ [LLM] 실패: {e}", flush=True)
            return err_msg

    def _collect_news(self) -> str:
        """[수정] 헤드라인 뿐만 아니라, 뉴스 요약(summary)을 함께 수집하여 AI에게 더 풍부한 재료를 제공합니다."""
        if not self.news:
            return "뉴스 수집기가 없습니다."
        try:
            items = self.news.fetch(limit=5)
            summaries = []
            for item in items[:3]:
                title = item.get("title", "제목 없음")
                snippet = item.get("summary") or item.get("description", "")
                if snippet:
                    snippet = re.sub('<[^<]+?>', '', snippet).strip().replace('\n', ' ')
                    snippet = snippet[:150] + "..." if len(snippet) > 150 else snippet
                    summaries.append(f"- {title}\n  (요약: {snippet})")
                else:
                    summaries.append(f"- {title}")
            return "\n".join(summaries) if summaries else "최신 뉴스가 없습니다."
        except Exception as e:
            print(f"⚠️ [News] 실패: {e}", flush=True)
            return "뉴스 수집에 실패했습니다."

    def _format_alert(self, symbol: str, price: float, pct15: Optional[float], pct60: Optional[float], pct10: Optional[float],
                      reason: str, extra_news: str = "", llm_comment: str = "") -> str:
        now = self._now().strftime("%Y-%m-%d %H:%M:%S")
        lines = [
            f"🚨 {SERVICE_NAME} 급변 알림 ({now} KST)",
            f"- {symbol}: 현재가 {price:,.2f}",
            f"- 사유: {reason}",
        ]
        if pct10 is not None:
            lines.append(f"- 10분 변화: {pct10:+.2f}%")
        if pct15 is not None:
            lines.append(f"- 15분 변화: {pct15:+.2f}%")
        if pct60 is not None:
            lines.append(f"- 60분 변화: {pct60:+.2f}%")
        if extra_news:
            lines += ["", "📰 관련 뉴스", extra_news]
        if llm_comment:
            lines += ["", "🤖 AI 코멘트", llm_comment.strip()]
        return "\n".join(lines)

    def run_forever(self):
        print("🦅 [System] Moneybag(=The Whale Hunter) Watchdog 시작", flush=True)

        # Heartbeat 파일 경로 (watchdogs.py 매니저가 감시함)
        hb_path = os.getenv("MONEYBAG_HEARTBEAT_PATH")

        if BRIEF_ON_START and not self._startup_brief_sent:
            self.tg.send(self._format_brief())
            self._startup_brief_sent = True

        while not self._stop:
            # ✅ Heartbeat 갱신 (나 살아있음)
            if hb_path:
                try:
                    with open(hb_path, 'a'):
                        os.utime(hb_path, None)
                except Exception:
                    pass

            now = self._now()

            # (A) 정기 브리핑
            for t in self._should_brief_now():
                msg = self._format_brief()
                if BRIEF_USE_LLM and _chat:
                    try:
                        # [수정] AI 프롬프트 개선
                        system = "너는 'The Whale Hunter'의 시장 브리핑 작성자다. 투자 조언 금지. 요약만."
                        user = "아래 암호화폐 시장(24h 변동) 및 최신 뉴스 정보를 바탕으로, 현재 시장 상황을 한 문단으로 요약해줘.\n" + msg
                        msg += "\n\n🤖 AI 요약\n" + (_chat(system, user) or "")
                    except Exception:
                        pass
                self.tg.send(msg)
                self._mark_brief_sent(t)

            # (B) 가격 업데이트 + 알림 체크
            for sym in SYMBOLS:
                price = self._binance_price(sym)
                if price is None:
                    continue

                self.price_hist[sym].append((now, price))

                pct10 = self._pct_over_minutes(sym, 10)
                pct15 = self._pct_over_minutes(sym, 15)
                pct60 = self._pct_over_minutes(sym, 60)

                reason = None
                if pct10 is not None and abs(pct10) >= ACCEL_10M_PCT:
                    reason = f"10분 급가속(≥ {ACCEL_10M_PCT:.2f}%)"
                elif pct15 is not None and abs(pct15) >= TH_15M_PCT:
                    reason = f"15분 급변(≥ {TH_15M_PCT:.2f}%)"
                elif pct60 is not None and abs(pct60) >= TH_60M_PCT:
                    reason = f"60분 급변(≥ {TH_60M_PCT:.2f}%)"

                if not reason:
                    continue

                last_t = self.last_alert_time.get(sym)
                last_p = self.last_alert_price.get(sym)
                cooldown_ok = (last_t is None) or ((now - last_t) >= timedelta(minutes=COOLDOWN_MIN))

                bypass_ok = False
                if not cooldown_ok and last_p:
                    extra_move = ((price - last_p) / last_p) * 100.0
                    if abs(extra_move) >= COOLDOWN_BYPASS_PCT:
                        bypass_ok = True

                # ✅ 우루루 방지: 서비스 전체 쿨타임
                g_last_t = self.last_global_alert_time
                g_last_p = self.last_global_alert_anchor
                g_cooldown_ok = (g_last_t is None) or ((now - g_last_t) >= timedelta(minutes=COOLDOWN_MIN))

                g_bypass_ok = False
                if not g_cooldown_ok and g_last_p:
                    # 버그 수정: 글로벌 바이패스는 대표 심볼(BTC) 기준으로 계산
                    btc_price = self._binance_price("BTCUSDT")
                    if btc_price:
                        extra_move_global = ((btc_price - g_last_p) / g_last_p) * 100.0
                        if abs(extra_move_global) >= COOLDOWN_BYPASS_PCT:
                            g_bypass_ok = True

                if not (g_cooldown_ok or g_bypass_ok):
                    continue
                
                if cooldown_ok or bypass_ok:
                    extra_news = self._collect_news()
                    
                    # --- AI 프롬프트 생성 ---
                    _p, p24h = self._binance_24h(sym)
                    prompt_lines = [
                        "아래 정보를 바탕으로 현재 암호화폐 시장 상황을 3~5줄로 간결하게 설명해줘. 너는 'The Whale Hunter'의 시장 관측 애널리스트이며, 투자 조언이 아니라 시장 상황에 대한 건조한 설명만 제공해야 해.",
                        "뉴스 내용과 코인 가격 움직임을 연관지어 설명하면 좋아.",
                        "---",
                        f"- 심볼: {sym}",
                        f"- 현재가: {price:,.4f}",
                        f"- 알림 사유: {reason}"
                    ]
                    if p24h is not None:
                        prompt_lines.append(f"- 24시간 변동: {p24h:+.2f}%")
                    if pct10 is not None:
                        prompt_lines.append(f"- 10분 변동: {pct10:+.2f}%")
                    if pct15 is not None:
                        prompt_lines.append(f"- 15분 변동: {pct15:+.2f}%")
                    if pct60 is not None:
                        prompt_lines.append(f"- 60분 변동: {pct60:+.2f}%")
                    if extra_news:
                        prompt_lines.append(f"- 관련 뉴스:\n{extra_news}")
                    prompt_lines.append("---")
                    
                    llm_comment = self._maybe_llm("\n".join(prompt_lines))
                    # --- AI 프롬프트 생성 끝 ---

                    alert_msg = self._format_alert(sym, price, pct15, pct60, pct10, reason, extra_news, llm_comment)

                    self.tg.send(alert_msg)

                    # (기존) 심볼별 마지막 알림 기록
                    self.last_alert_time[sym] = now
                    self.last_alert_price[sym] = price

                    # ✅ (추가) 서비스 전체 마지막 알림 기록 (우루루 방지)
                    self.last_global_alert_time = now
                    self.last_global_alert_anchor = self._binance_price("BTCUSDT") or price # 기준은 BTC, 실패 시 현재가


            print(f"\r👀 Moneybag 감시 중... ({self._now().strftime('%H:%M:%S')})", end="", flush=True)
            time.sleep(POLL_INTERVAL_SEC)


def main():
    MarketWatchdog().run_forever()


if __name__ == "__main__":
    main()
