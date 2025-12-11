import time
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# 모듈 임포트
from moneybag.src.analyzers.whale_alert_tracker import WhaleAlertTracker
from moneybag.src.collectors.crypto_news_rss import CryptoNewsRSS
from moneybag.src.llm.openai_driver import _chat 
from moneybag.src.pipelines.send_channels import TelegramSender 
from moneybag.src.utils.slack_notifier import send_slack_message

BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

class MarketWatchdog:
    def __init__(self):
        self.tracker = WhaleAlertTracker()
        self.news_collector = CryptoNewsRSS()
        self.telegram = TelegramSender() # [NEW] 텔레그램 연결
        
        # 감시 대상
        self.targets = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "DOGE/USDT", "XRP/USDT"]
        self.cooldown = {} 

    def check_market(self):
        now_str = datetime.now().strftime('%H:%M:%S')
        print(f"\r👀 웨일 헌터 감시 중... ({now_str})", end="") # 한 줄로 계속 갱신
        
        for coin in self.targets:
            # 쿨타임 체크 (1시간)
            if coin in self.cooldown:
                if (datetime.now() - self.cooldown[coin]).seconds < 3600:
                    continue
                else:
                    del self.cooldown[coin]

            try:
                # 1. 고래/변동성 체크
                result = self.tracker.analyze_volume_anomaly(coin)
                if not result: continue

                # [트리거 조건]
                # A. 고래 시그널 (매집/폭발)
                # B. 가격 변동폭 2% 이상
                is_whale = result['signal'] != "N/A"
                is_volatile = abs(result['price_change']) >= 2.0 # 2% 기준
                
                if is_whale or is_volatile:
                    print(f"\n🚨 [포착] {coin} 움직임 감지! 분석 시작...")
                    self.trigger_emergency_protocol(coin, result)
                    self.cooldown[coin] = datetime.now()

            except Exception as e:
                pass # 감시 중 에러는 조용히 넘어감

    def trigger_emergency_protocol(self, coin, data):
        # 2. 긴급 뉴스 수집
        news_items = self.news_collector.collect_all()
        news_text = ""
        for idx, item in enumerate(news_items[:3], 1):
             news_text += f"- {item['title']}\n"

        # 3. AI 긴급 타전 작성
        system_prompt = """
        너는 전장의 상황병 '웨일 헌터'다. 
        긴급 상황을 **텔레그램 알림용**으로 짧고(5줄 이내) 강렬하게 보고해.
        
        [형식]
        🚨 **[긴급] {코인명} {상태}**
        
        📊 **팩트:** {변동률}% 급등/급락 (거래량 {N}배)
        🗞️ **이유:** (뉴스 중 관련 있는 게 있으면 한 줄 요약, 없으면 "고래의 인위적 개입 의심")
        🛡️ **오더:** (지금 타? 말아? 튀어? 한마디로)
        """
        
        user_prompt = f"""
        대상: {coin}
        데이터: 거래량 {data['vol_spike_ratio']}배, 가격 변동 {data.get('price_change', 0)}%
        상태: {data['signal']}
        뉴스:
        {news_text}
        """
        
        alert_msg = _chat(system_prompt, user_prompt)
        
        # 4. 전송
        print(f">>> [텔레그램 발송]\n{alert_msg}")
        self.telegram.send_message(alert_msg)
        
        # (옵션) 슬랙 전송
        # send_slack_message(f"[Watchdog] {coin}\n{alert_msg}")

if __name__ == "__main__":
    dog = MarketWatchdog()
    print("🦅 웨일 헌터(Watchdog) 가동 시작...")
    while True:
        dog.check_market()
        time.sleep(60) # 1분마다 감시