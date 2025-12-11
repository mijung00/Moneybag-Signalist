# iceage/src/pipelines/stock_watchdog.py
import time
import asyncio
import yfinance as yf
from datetime import datetime
from iceage.src.pipelines.telegram_bot import SignalistTelegramBot

class StockWatchdog:
    def __init__(self):
        self.bot = SignalistTelegramBot()
        self.targets = {
            "^KS11": "코스피(KOSPI)",
            "^KQ11": "코스닥(KOSDAQ)"
        }
        self.last_prices = {}
        self.alert_cooldown = {} # 알림 도배 방지

    def is_market_open(self):
        """한국 주식 시장 운영 시간 체크 (09:00 ~ 15:30)"""
        now = datetime.now()
        # 주말 제외
        if now.weekday() >= 5: return False 
        # 시간 체크
        current_time = now.time()
        start = datetime.strptime("09:00", "%H:%M").time()
        end = datetime.strptime("15:30", "%H:%M").time()
        return start <= current_time <= end

    async def check_market(self):
        if not self.is_market_open():
            print("\r💤 장 마감 시간입니다. 감시 일시 정지...", end="")
            return

        print(f"\r👀 Signalist 감시 중... ({datetime.now().strftime('%H:%M:%S')})", end="")

        for ticker, name in self.targets.items():
            try:
                # yfinance로 실시간(지연) 데이터 조회
                stock = yf.Ticker(ticker)
                # fast_info가 더 빠르고 가벼움
                current_price = stock.fast_info.last_price
                prev_close = stock.fast_info.previous_close
                
                if current_price is None or prev_close is None:
                    continue

                # 등락률 계산
                change_pct = ((current_price - prev_close) / prev_close) * 100
                
                # [알림 조건] 지수가 1.5% 이상 변동 시 (주가지수는 1.5%면 폭등/폭락임)
                if abs(change_pct) >= 1.5:
                    await self.trigger_alert(name, current_price, change_pct)
                    
            except Exception as e:
                # API 일시적 오류 등은 무시
                pass

    async def trigger_alert(self, name, price, change_pct):
        # 쿨타임 체크 (같은 알림은 2시간에 한 번만)
        last_time = self.alert_cooldown.get(name)
        if last_time and (datetime.now() - last_time).seconds < 7200:
            return

        state = "떡상(급등) 🔥" if change_pct > 0 else "떡락(폭락) 😱"
        
        msg = f"""
🚨 **[긴급] {name} {state}**

📊 **현재가:** {price:,.2f}
📉 **등락률:** {change_pct:+.2f}%

시장이 요동치고 있습니다. 포트폴리오를 점검하세요!
(Signalist Bot)
        """
        
        await self.bot.send_message(msg)
        self.alert_cooldown[name] = datetime.now()

async def main():
    dog = StockWatchdog()
    print("🦅 Signalist Watchdog 가동 시작...")
    
    # 텔레그램 연결 테스트
    await dog.bot.send_message("🦅 **Signalist Watchdog** 가동을 시작합니다.")
    
    while True:
        await dog.check_market()
        await asyncio.sleep(60) # 1분마다 체크

if __name__ == "__main__":
    asyncio.run(main())