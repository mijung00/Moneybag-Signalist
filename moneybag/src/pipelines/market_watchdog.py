import time
import sys
import os
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# [1] 환경 변수 로드
BASE_DIR = Path(__file__).resolve().parents[3]
sys.path.append(str(BASE_DIR))
load_dotenv(BASE_DIR / ".env")

from moneybag.src.collectors.crypto_news_rss import CryptoNewsRSS
from moneybag.src.llm.openai_driver import _chat 
from moneybag.src.pipelines.send_channels import TelegramSender 

class MarketWatchdog:
    def __init__(self):
        self.news_collector = CryptoNewsRSS()
        
        token = os.getenv("TELEGRAM_BOT_TOKEN_MONEYBAG")
        chat_id = os.getenv("TELEGRAM_CHAT_ID_MONEYBAG")

        if token:
             print(f"🔑 [Moneybag] 토큰 로드 완료: {token[:5]}...")
        else:
             print("❌ [Moneybag] 토큰이 없습니다!")

        self.telegram = TelegramSender(token=token, chat_id=chat_id) 
        
        self.targets = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
        
        # 가격 기억용 변수들
        self.prev_check_prices = {}   # 직전 루프(10초 전) 가격
        self.last_alert_prices = {}   # ★ 마지막으로 '알림 보낸' 가격
        self.cooldown = {}            # 시간 쿨타임

    def get_binance_price(self, symbol):
        try:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return float(resp.json()['price'])
        except Exception as e:
            print(f"⚠️ [API Error] {symbol}: {e}")
        return None

    def check_market(self):
        now_str = datetime.now().strftime('%H:%M:%S')
        print(f"\r👀 Moneybag 감시 중... ({now_str})", end="")
        
        for coin in self.targets:
            current_price = self.get_binance_price(coin)
            if current_price is None:
                continue

            # 1. 10초 전 가격과 비교 (순수 변동 확인용)
            prev_price = self.prev_check_prices.get(coin)
            if prev_price is None:
                self.prev_check_prices[coin] = current_price
                continue
            
            # 변동률 계산 (직전 루프 대비)
            # 사실 여기선 '기준가'를 뭘로 하냐가 중요한데, 
            # 급변 감지를 위해선 '직전 알림 가격'과 비교하는 게 더 정확합니다.
            # 하지만 1차 필터링을 위해 루프 간 변동도 봅니다.
            loop_change_pct = ((current_price - prev_price) / prev_price) * 100
            self.prev_check_prices[coin] = current_price # 가격 갱신

            # ---------------------------------------------------------
            # [스마트 알림 로직]
            # ---------------------------------------------------------
            should_send = False
            trigger_reason = ""
            
            # 비교 대상: 마지막으로 알림 보냈던 가격 (없으면 현재가가 기준)
            last_alert_p = self.last_alert_prices.get(coin, current_price)
            last_alert_time = self.cooldown.get(coin)

            # 알림 대비 현재 변동률
            total_change_pct = ((current_price - last_alert_p) / last_alert_p) * 100

            # (상황 1) 쿨타임 끝났음 (1시간 지남)
            if not last_alert_time or (datetime.now() - last_alert_time).seconds >= 3600:
                # 1시간 지났는데, 가격 변동이 1.0% 이상이면 알림
                # (너무 작은 변동은 굳이 알림 안 줘도 됨)
                if abs(total_change_pct) >= 1.0: 
                    should_send = True
                    trigger_reason = "정기 감시"

            # (상황 2) 쿨타임 중임 (1시간 안 지남) -> ★ 스마트 패스
            else:
                # 알림 보낸 가격보다 '추가로' 2.0% 이상 더 움직였나?
                if abs(total_change_pct) >= 2.0:
                    should_send = True
                    trigger_reason = f"🚨 추가 급변 발생 ({total_change_pct:+.2f}%)"

            if should_send:
                print(f"\n⚡ [Moneybag] {coin} 조건 충족: {trigger_reason}")
                
                data = {
                    'price_change': round(total_change_pct, 4),
                    'current_price': current_price,
                    'reason': trigger_reason
                }
                
                self.trigger_emergency_protocol(coin, data)
                
                # [중요] 상태 업데이트 (알림 보냈으니 기준점 재설정)
                self.cooldown[coin] = datetime.now()
                self.last_alert_prices[coin] = current_price

    def trigger_emergency_protocol(self, coin, data):
        news_items = self.news_collector.collect_all()
        if not news_items:
            news_text = "특이 뉴스 없음."
        else:
            news_text = "\n".join([f"- {item['title']}" for item in news_items[:3]])

        system_prompt = """
        너는 'Moneybag 왓치독'이다. 코인 급변동 상황을 보고해.
        [보고 양식]
        🚨 **[Moneybag] {코인명} 급변동**
        
        📊 **상황 브리핑**
        - 등락률: {변동률}% (현재 {현재가})
        - 감지유형: {감지이유}
        
        🗞️ **관련 뉴스**
        {뉴스내용}
        
        🛡️ **AI Insight**
        (이 변동이 일시적인지, 추세적인지 뉴스나 거래 패턴을 보고 1줄로 조언해줘)
        """
        user_prompt = f"대상: {coin}\n데이터: {data}\n뉴스내용:\n{news_text}"
        
        try:
            alert_msg = _chat(system_prompt, user_prompt)
            self.telegram.send_message(alert_msg)
            print(f">>> [Moneybag 전송 완료] {coin}")
        except Exception as e:
            print(f"❌ AI/전송 실패: {e}")

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