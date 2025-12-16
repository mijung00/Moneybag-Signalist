import time
import sys
import os
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# [★핵심 1] 경로 강제 설정 (ModuleNotFoundError 방지)
# 현재 파일 위치를 기준으로 프로젝트 루트를 찾아서 sys.path에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
if project_root not in sys.path:
    sys.path.append(project_root)

# 환경 변수 로드
load_dotenv(os.path.join(project_root, ".env"))

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
             print(f"🔑 [Moneybag] 토큰 로드 완료: {token[:5]}...")
        else:
             print("❌ [Moneybag] 환경변수에서 토큰을 찾을 수 없습니다!")

        self.telegram = TelegramSender(token=token, chat_id=chat_id) 
        self.targets = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
        
        self.prev_check_prices = {}   
        self.last_alert_prices = {}   
        self.cooldown = {}            

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
        # AWS 로그에 남도록 flush=True 추가
        print(f"\r👀 Moneybag 감시 중... ({now_str})", end="", flush=True)
        
        for coin in self.targets:
            current_price = self.get_binance_price(coin)
            if current_price is None:
                continue

            prev_price = self.prev_check_prices.get(coin)
            if prev_price is None:
                self.prev_check_prices[coin] = current_price
                continue
            
            self.prev_check_prices[coin] = current_price

            # 스마트 알림 로직
            should_send = False
            trigger_reason = ""
            
            last_alert_p = self.last_alert_prices.get(coin, current_price)
            last_alert_time = self.cooldown.get(coin)
            total_change_pct = ((current_price - last_alert_p) / last_alert_p) * 100

            if not last_alert_time or (datetime.now() - last_alert_time).seconds >= 3600:
                if abs(total_change_pct) >= 1.0: 
                    should_send = True
                    trigger_reason = "정기 감시"
            else:
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
        """
        user_prompt = f"대상: {coin}\n데이터: {data}\n뉴스내용:\n{news_text}"
        
        try:
            alert_msg = _chat(system_prompt, user_prompt)
            self.telegram.send_message(alert_msg)
            print(f">>> [Moneybag 전송 완료] {coin}")
        except Exception as e:
            print(f"❌ AI/전송 실패: {e}")

# [★핵심 2] 시동 버튼 (이게 없어서 꺼졌던 겁니다)
if __name__ == "__main__":
    print("🦅 [System] Moneybag Watchdog 프로세스 시작")
    sys.stdout.flush() # 로그 강제 출력

    try:
        dog = MarketWatchdog()
        print("🦅 [System] 감시 루프 진입...")
        
        while True:
            try:
                dog.check_market()
            except Exception as e:
                print(f"\n❌ [Error] 루프 실행 중 오류: {e}")
            time.sleep(10)
            
    except Exception as fatal_e:
        print(f"💀 [Fatal] 왓치독 치명적 오류: {fatal_e}")
        import traceback
        traceback.print_exc()