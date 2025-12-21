# iceage/src/pipelines/stock_watchdog.py
import os
import sys
import time
import json
import signal
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from collections import deque
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, List, Dict, Set

import requests
import yfinance as yf
import boto3
from botocore.exceptions import ClientError
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
        # [수정] 환경 변수를 먼저 정규화하고 값을 가져옵니다.
        _normalize_json_env("TELEGRAM_BOT_TOKEN_SIGNALIST")
        _normalize_json_env("TELEGRAM_CHAT_ID_SIGNALIST")
        token = os.getenv("TELEGRAM_BOT_TOKEN_SIGNALIST", "")
        chat_id = os.getenv("TELEGRAM_CHAT_ID_SIGNALIST", "")
        self.tg = TelegramClient(token=token, chat_id=chat_id)

        # KIS 클라이언트 제거

        self.hist = {t: deque(maxlen=1200) for t in TICKERS}
        self.baseline = {}      # ticker -> (date, price)
        self.sent_levels = {}   # ticker -> (date, set[(sign, level)])
        self.last_alert_time = {t: None for t in TICKERS}

        self._open_brief_date = None
        self._close_brief_date = None

        self._stop = False
        signal.signal(signal.SIGTERM, self._on_stop)
        signal.signal(signal.SIGINT, self._on_stop)

    def _on_stop(self, *_):
        self._stop = True

    def _now(self) -> datetime:
        return datetime.now(TZ)

    def _get_price(self, ticker: str) -> Optional[float]:
        # 1. 네이버 금융
        naver_symbol = None
        if ticker == "^KS11": naver_symbol = "KOSPI"
        elif ticker == "^KQ11": naver_symbol = "KOSDAQ"
        
        if naver_symbol:
            try:
                url = f"https://m.stock.naver.com/api/index/{naver_symbol}/basic"
                r = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
                if r.status_code == 200:
                    return float(r.json()['closePrice'].replace(',', ''))
            except Exception:
                pass

        # 2. yfinance (백업)
        try:
            # fast_info is faster but can be stale
            return float(yf.Ticker(ticker).fast_info["last_price"])
        except Exception:
            try:
                # history is slower but more reliable
                data = yf.Ticker(ticker).history(period="1d")
                if data is not None and not data.empty:
                    return float(data["Close"].iloc[-1])
                return None
            except Exception:
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
        """[수정] 헤드라인 뿐만 아니라, 기사 본문 일부를 함께 수집하여 AI에게 더 풍부한 재료를 제공합니다."""
        news_summaries = []
        try:
            # 1. 뉴스 목록 페이지 가져오기
            list_url = "https://m.stock.naver.com/news/mainnews"
            r_list = requests.get(list_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            r_list.raise_for_status()
            soup_list = BeautifulSoup(r_list.text, "html.parser")
            items = soup_list.select("a.NewsList_item__lO7iA")[:limit]

            if not items:
                return "주요 뉴스를 찾을 수 없습니다."

            # 2. 각 기사 페이지를 방문하여 본문 일부(snippet) 추출
            for item in items:
                title = item.get_text(strip=True)
                href = item.get("href", "")
                if not href:
                    news_summaries.append(f"- {title}")
                    continue
                
                article_url = href if href.startswith("http") else "https://m.stock.naver.com" + href
                
                try:
                    r_article = requests.get(article_url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
                    r_article.raise_for_status()
                    soup_article = BeautifulSoup(r_article.text, "html.parser")
                    
                    content_div = soup_article.find("div", id="newsct_article")
                    if content_div:
                        # 첫 두 문단을 요약으로 사용하고 길이 제한
                        snippet = " ".join(p.get_text(strip=True) for p in content_div.find_all("p")[:2])
                        snippet = snippet[:150] + "..." if len(snippet) > 150 else snippet
                        news_summaries.append(f"- {title}\n  (요약: {snippet})")
                    else:
                        news_summaries.append(f"- {title}") # 본문 못찾으면 제목만
                except Exception:
                    news_summaries.append(f"- {title}") # 개별 기사 실패 시 제목만

            return "\n".join(news_summaries)
        except Exception as e:
            print(f"⚠️ 뉴스 목록 수집 실패: {e}")
            return "주요 뉴스 수집에 실패했습니다."

    def _llm_comment(self, user_prompt: str) -> str:
        if not _chat:
            return ""
        try:
            # [수정] 시스템 프롬프트를 더 구체적으로 변경
            system = "너는 대한민국 주식 시장을 분석하는 'Signalist'의 수석 애널리스트다. 객관적인 데이터와 뉴스를 바탕으로 시장 상황을 설명하며, 절대로 투자 조언이나 개인적인 예측을 하지 않는다."
            return (_chat(system, user_prompt) or "").strip()
        except Exception as e:
            print(f"⚠️ [LLM Error] {e}")
            return f"AI 코멘트 생성 실패: {e}"

    def _is_market_open_time(self, now: datetime) -> bool:
        """평일 08:30 ~ 16:30 사이인지 확인 (주말 제외)"""
        # 주말(토=5, 일=6)은 휴식
        if now.weekday() >= 5:
            return False
        # 시간 체크
        t = now.time()
        return t >= datetime.strptime("08:30", "%H:%M").time() and \
               t <= datetime.strptime("16:30", "%H:%M").time()

    # ✅ 중요: 브리핑은 “시장 열려있나?”와 무관하게 시간만 맞으면 무조건 실행
    def _send_brief_if_due(self):
        now = self._now()
        
        # [수정] 주말(토,일)에는 브리핑을 보내지 않음
        if now.weekday() >= 5:
            return
            
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

        # [수정] AI 프롬프트를 더 구체적이고 똑똑하게 변경
        prompt_header = "아래 정보를 바탕으로 시장 상황을 3~5줄로 요약해줘. 투자 조언은 절대 금지.\n\n"
        user_prompt = prompt_header + "\n".join(lines)
        
        if tag == "장 시작 브리핑":
            user_prompt += "\n\n'개장 초반' 또는 '장 시작 직후'라는 표현을 사용하고, 간밤의 해외 증시 마감 상황과 연관지어 설명하면 좋아."
        else:
            user_prompt += "\n\n오늘 하루의 시장 흐름(예: 상승 출발 후 하락 마감)을 요약하고, 주요 뉴스가 어떤 영향을 미쳤는지 언급해줘."

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
        
        # Heartbeat 파일 경로 (watchdogs.py 매니저가 감시함)
        hb_path = os.getenv("ICEAGE_HEARTBEAT_PATH")

        while not self._stop:
            # ✅ Heartbeat 갱신 (나 살아있음)
            if hb_path:
                try:
                    with open(hb_path, 'a'):
                        os.utime(hb_path, None)
                except Exception:
                    pass

            self._send_brief_if_due()

            now = self._now()
            
            # [추가] 장 운영 시간(08:30~16:30) 외에는 시세 감시 스킵 (API 호출 절약)
            if not self._is_market_open_time(now):
                # 왓치독은 살아있어야 하므로(Heartbeat) 프로세스는 유지하되, API만 안 부름
                time.sleep(POLL_INTERVAL_SEC)
                continue

            for ticker, name in TICKERS.items():
                price = self._get_price(ticker)

                if price is None:
                    continue

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
                        # [수정] AI 프롬프트를 훨씬 더 구체적으로 개선
                        reason_text = ""
                        if new_levels:
                            sign, lv = sorted(new_levels, key=lambda x: x[1], reverse=True)[0]
                            direction = "상승" if sign > 0 else "하락"
                            reason_text = f"새로운 레벨({direction} {lv}%) 돌파"
                        elif accel_only:
                            reason_text = "10분 급가속"

                        user_prompt = f"""
아래 정보를 바탕으로 한국 증시 상황을 3~5줄로 간결하게 설명해줘. 투자 조언이 아니라 객관적인 상황 설명만 제공해야 해.
뉴스 내용과 지수 움직임을 연관지어 설명하면 좋아.

---
- 지수: {name}
- 현재가: {price:,.2f}
- 기준가 대비: {pct_base:+.2f}%
- 10분 변동: {pct10:+.2f}%
- 알림 사유: {reason_text}
- 주요 뉴스:
{headlines}
---
""".strip()
                        llm = self._llm_comment(user_prompt)
                    except Exception as e:
                        llm = f"AI 코멘트 생성 실패: {e}"

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
