import asyncio
import sys
import os
import requests
from pathlib import Path
from datetime import datetime
import yfinance as yf
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# [1] 환경 변수 로드
BASE_DIR = Path(__file__).resolve().parents[3]
sys.path.append(str(BASE_DIR))
load_dotenv(BASE_DIR / ".env")

try:
    from iceage.src.pipelines.telegram_bot import SignalistTelegramBot
    from moneybag.src.llm.openai_driver import _chat
except ImportError:
    from src.pipelines.telegram_bot import SignalistTelegramBot
    from moneybag.src.llm.openai_driver import _chat

class StockWatchdog:
    def __init__(self):
        token = os.getenv("TELEGRAM_BOT_TOKEN_SIGNALIST")
        chat_id = os.getenv("TELEGRAM_CHAT_ID_SIGNALIST")
        
        if token: print(f"🔑 [Signalist] 토큰 로드 성공: {token[:5]}...")
        else: print("❌ [Signalist] 토큰 없음")

        self.bot = SignalistTelegramBot(token=token, chat_id=chat_id)
        
        self.targets = {"^KS11": "코스피", "^KQ11": "코스닥"}
        
        # 시총 상위 감시 풀
        self.monitoring_pool = {
            "^KS11": [
                ("005930.KS", "삼성전자"), ("000660.KS", "SK하이닉스"), ("373220.KS", "LG엔솔"),
                ("207940.KS", "삼성바이오"), ("005380.KS", "현대차"), ("000270.KS", "기아"),
                ("105560.KS", "KB금융"), ("068270.KS", "셀트리온"), ("005490.KS", "POSCO홀딩스"),
                ("035420.KS", "NAVER")
            ],
            "^KQ11": [
                ("196170.KQ", "알테오젠"), ("247540.KQ", "에코프로비엠"), ("086520.KQ", "에코프로"),
                ("028300.KQ", "HLB"), ("141080.KQ", "리가켐바이오"), ("403870.KQ", "휴젤"),
                ("058470.KQ", "리노공업"), ("035900.KQ", "JYP Ent."), ("263750.KQ", "펄어비스")
            ]
        }
        
        self.alert_cooldown = {} 
        self.last_alert_price = {} # ★ [추가] 마지막으로 알림 보냈을 때 가격 기억

    def is_market_open(self):
        # ★ 실전 배포 시엔 주석 해제해서 장 시간에만 돌게 하세요
        # now = datetime.now()
        # if now.weekday() >= 5: return False 
        # current = now.time()
        # start = datetime.strptime("09:00", "%H:%M").time()
        # end = datetime.strptime("15:30", "%H:%M").time()
        # return start <= current <= end
        return True 

    def get_naver_news_headlines(self):
        try:
            url = "https://finance.naver.com/news/mainnews.naver"
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(url, headers=headers, timeout=3)
            soup = BeautifulSoup(resp.text, 'html.parser')
            news_list = []
            for art in soup.select('.block1 a.tit')[:3]:
                news_list.append(f"- {art.get_text().strip()}")
            return "\n".join(news_list) if news_list else "특이 뉴스 없음"
        except: return "뉴스 수집 실패"

    def get_market_movers(self, ticker_key):
        candidates = []
        target_list = self.monitoring_pool.get(ticker_key, [])
        for code, name in target_list:
            try:
                stock = yf.Ticker(code)
                curr = stock.fast_info.last_price
                prev = stock.fast_info.previous_close
                if curr and prev:
                    pct = ((curr - prev) / prev) * 100
                    candidates.append((name, pct, abs(pct)))
            except: pass
        
        candidates.sort(key=lambda x: x[2], reverse=True)
        return ", ".join([f"{n} {p:+.2f}%" for n, p, _ in candidates[:3]])

    async def check_market(self):
        if not self.is_market_open(): return

        for ticker, name in self.targets.items():
            try:
                stock = yf.Ticker(ticker)
                try:
                    price = stock.fast_info.last_price
                    prev = stock.fast_info.previous_close
                except:
                    hist = stock.history(period="2d")
                    if len(hist) < 2: continue
                    price = hist['Close'].iloc[-1]
                    prev = hist['Close'].iloc[-2]

                if not price or not prev: continue

                change_pct = ((price - prev) / prev) * 100
                
                # [알림 조건 1] 기본 1.5% 이상 변동 시 체크 시작 (실전용)
                if abs(change_pct) >= 1.5:
                    await self.trigger_alert(ticker, name, price, change_pct)
                    
            except Exception:
                pass

    async def trigger_alert(self, ticker_key, name, price, change_pct):
        # --- [스마트 쿨타임 로직] ---
        last_time = self.alert_cooldown.get(name)
        last_price = self.last_alert_price.get(name)
        
        should_send = False
        reason = ""

        # 1. 시간 체크 (1시간 지났나?)
        if not last_time or (datetime.now() - last_time).seconds >= 3600:
            should_send = True
            reason = "정기 알림"
        
        # 2. 급변 체크 (시간 안 지났어도, 추가로 1.0% 이상 움직였나?)
        elif last_price:
            # (현재가 - 직전알림가) / 직전알림가
            gap_pct = ((price - last_price) / last_price) * 100
            if abs(gap_pct) >= 1.0: # ★ 1.0% 이상 추가 변동 시 슈퍼 패스!
                should_send = True
                reason = f"추가 급변 발생 ({gap_pct:+.2f}%)"
        
        # 보낼 필요 없으면 리턴
        if not should_send:
            return

        print(f"\n💡 [AI 분석 중] {name} ({reason})...")

        news_summary = self.get_naver_news_headlines()
        movers_status = self.get_market_movers(ticker_key)

        system_prompt = """
        너는 'Signalist 수석 애널리스트'다.
        지수 변동의 원인을 주도주와 뉴스를 엮어서 분석해.
        
        [보고 양식]
        🚨 **[속보] {지수명} {상태}** ({등락률}%)
        
        📊 **시장 주도주**
        👉 {주도주현황}
        
        🗞️ **주요 뉴스**
        {뉴스내용}
        
        💡 **Signalist Insight**
        (한 줄 분석)
        """
        user_prompt = f"지수: {name}, 현재가: {price:,.2f}, 등락률: {change_pct:+.2f}%, 주도주: {movers_status}, 뉴스: {news_summary}"
        
        try:
            msg = _chat(system_prompt, user_prompt)
        except Exception as e:
            msg = f"🚨 **[Signalist] {name}**\n📊 {change_pct:+.2f}%"
        
        await self.bot.send_message(msg)
        print(f">>> [전송 완료] {name}")
        
        # [중요] 알림 보냈으니 시간과 가격을 갱신
        self.alert_cooldown[name] = datetime.now()
        self.last_alert_price[name] = price # ★ 현재 가격 기억

async def main():
    dog = StockWatchdog()
    await dog.bot.send_message("🦅 Signalist Watchdog (스마트 쿨타임 적용) 가동")
    while True:
        await dog.check_market()
        await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        # 실행 시작 알림
        print("🦅 왓치독 메인 진입 성공")
        asyncio.run(main())
    except Exception as e:
        # 치명적 에러 발생 시 로그 남기고 종료
        print(f"💀 [FATAL ERROR] 왓치독 사망: {e}")
        import traceback
        traceback.print_exc()