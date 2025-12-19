import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# 모듈 임포트
from moneybag.src.collectors.cex_price_collector import CexPriceCollector
from moneybag.src.analyzers.funding_rate_anomaly import FundingRateAnalyzer
from moneybag.src.analyzers.whale_alert_tracker import WhaleAlertTracker
from moneybag.src.collectors.crypto_news_rss import CryptoNewsRSS
from moneybag.src.llm.openai_driver import _chat 
from moneybag.src.tools.simple_backtester import SimpleBacktester
from moneybag.src.analyzers.technical_levels import TechnicalLevelsAnalyzer
from moneybag.src.collectors.onchain_collector import OnChainCollector
from common.s3_manager import S3Manager

# --- [추가할 코드 시작] ---
from moneybag.src.processors.market_regime import MarketRegimeAnalyzer 
from moneybag.src.strategies.strategy_selector import BotTraderSelector
from moneybag.src.strategies.final_signal_gen import generate_all_strategies
# --- [추가할 코드 끝] ---


BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

class DailyNewsletter:
    def __init__(self):
        self.price_collector = CexPriceCollector()
        if self.price_collector.binance:
            self.price_collector.binance.apiKey = None
            self.price_collector.binance.secret = None
        self.funding_analyzer = FundingRateAnalyzer()
        self.whale_tracker = WhaleAlertTracker()
        self.news_collector = CryptoNewsRSS()
        self.backtester = SimpleBacktester()
        self.tech_analyzer = TechnicalLevelsAnalyzer()
        self.onchain_collector = OnChainCollector()
        # --- [추가할 코드 시작] ---
        # 새로운 분석가(날씨)와 지휘관(봇) 영입
        self.regime_analyzer = MarketRegimeAnalyzer()
        self.bot_selector = BotTraderSelector()
        # --- [추가할 코드 끝] ---
        
        self.coin_map = {
            "BTC": "BTC", "ETH": "ETH", "SOL": "SOL", "XRP": "XRP", "DOGE": "DOGE",
            "PEPE": "1000PEPE", "SHIB": "1000SHIB"
        }
        self.targets = {
            "Major": ["BTC", "ETH", "SOL"],
            "Meme": ["DOGE", "XRP", "PEPE", "SHIB"]
        }
        self.service_name = "웨일 헌터(Whale Hunter)"

    def create_sentiment_gauge(self, value):
        if value >= 75: icon, color = "🤑", "🟩"
        elif value >= 55: icon, color = "😋", "🟨"
        elif value <= 25: icon, color = "😱", "🟥"
        elif value <= 45: icon, color = "😨", "🟧"
        else: icon, color = "😐", "⬜"
        filled = int(value / 10)
        empty = 10 - filled
        bar = (color * filled) + ("▪️" * empty)
        return f"{icon} **{value}** {bar}"


    def get_market_metrics(self, symbol_list):
        table_str = "| 코인 | 가격(24h) | 김프 | 펀딩비 | 거래량 |\n|---|---|---|---|---|\n"
        for coin in symbol_list:
            future_symbol = self.coin_map.get(coin, coin)
            pair_future = f"{future_symbol}/USDT"
            
            price_data = self.price_collector.fetch_price_data(future_symbol) 
            funding_data = self.funding_analyzer.analyze(pair_future)
            whale_data = self.whale_tracker.analyze_volume_anomaly(pair_future)
            
            if price_data and funding_data:
                price_val = price_data['binance_usdt']
                change_pct = price_data['change_24h']
                chg_icon = "🔺" if change_pct > 0 else "🔹"
                kimp = price_data['kimp_percent']
                kimp_icon = "🔥" if kimp > 3.0 else ("🧊" if kimp < 0 else "")
                try: fr_rate = float(funding_data['funding_rate'].strip('%'))
                except: fr_rate = 0.0
                fr_status = "롱과열" if fr_rate > 0.02 else ("숏우세" if fr_rate < -0.01 else "중립")
                vol_str = "평범"
                if whale_data:
                    ratio = whale_data.get('vol_spike_ratio', 1.0)
                    if ratio > 2.5: vol_str = f"🔥폭발({ratio:.1f}x)"
                    elif ratio > 1.5: vol_str = f"⚡활발({ratio:.1f}x)"
                    elif ratio < 0.6: vol_str = f"💧말라감"
                row = f"| **{coin}** | ${price_val:,.2f}<br>({chg_icon}{change_pct}%) | {kimp}%{kimp_icon} | {fr_rate:.4f}%<br>({fr_status}) | {vol_str} |\n"
                table_str += row
            else:
                table_str += f"| {coin} | ❌수집실패 | - | - | - |\n"
        return table_str + "\n"

    def get_tactical_map(self, symbol_list):
        table_str = "| 코인 | 현재가 | 1차 지지(Buy) | 1차 저항(Sell) | 판세 |\n|---|---|---|---|---|\n"
        for coin in symbol_list:
            future_symbol = self.coin_map.get(coin, coin)
            data = self.tech_analyzer.analyze(f"{future_symbol}/USDT")
            if data:
                trend = data['trend'].replace("우위", "")
                table_str += f"| **{coin}** | ${data['price']:,.0f} | 🟢 **${data['s1']:,.0f}** | 🔴 **${data['r1']:,.0f}** | {trend} |\n"
        return table_str

    def get_market_sentiment_display(self):
        data = self.onchain_collector.get_whale_ammo()
        if not data: return "데이터 수집 실패"
        curr = data['current']['value']
        status = data['current']['status']
        hist = data['history']
        gauge_bar = self.create_sentiment_gauge(curr)
        diff_day = curr - hist['yesterday']
        icon_day = "🔺" if diff_day > 0 else "🔻"
        explanation = "_*산출 기준: 변동성(25%) + 모멘텀(25%) + SNS(15%) + 도미넌스(10%) + 트렌드(10%)_"
        display = f"""
### 🧠 고래 심리 기상도 (Whale Sentiment)
**현재: {status}**
{gauge_bar}
{explanation}

* 📉 **전일 대비:** {hist['yesterday']} → {curr} ({icon_day}{abs(diff_day)})
* 🗓️ **지난주:** {hist['last_week']}
* 🗓️ **지난달:** {hist['last_month']}
"""
        return display

    def collect_news(self):
        news_items = self.news_collector.collect_all()
        summary = ""
        for idx, item in enumerate(news_items[:10], 1):
             pub_date = item.get('published_at', datetime.now().strftime('%H:%M'))
             summary += f"[{idx}] Source: {item['source']} ({pub_date})\nTitle: {item['title']}\nContent: {item.get('summary', '내용없음')}\n\n"
        return summary

    def emergency_check(self):
        btc_data = self.price_collector.fetch_price_data("BTC")
        if btc_data:
            change = btc_data.get('change_24h', 0)
            if abs(change) >= 2.0: return True, change
        return False, 0

# [수정된 generate 함수 전체]
    def generate(self, mode="morning"):
        print(f"🚀 [{mode.upper()}] 웨일 헌터가 데이터를 분석 중입니다...")
        
        # 0. 데이터 준비 (BTC 기준)
        ohlcv = self.price_collector.binance.fetch_ohlcv("BTC/USDT", '1d', limit=1000)
        if not ohlcv:
            print("❌ BTC 데이터 수집 실패")
            return

        df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        
        # 1. [NEW] 레짐 및 서브 국면 분석 (MarketRegimeAnalyzer에게 위임)
        regime_info = self.regime_analyzer.analyze_regime(df)
        
        main_regime = regime_info['main_regime']
        tactical_state = regime_info['tactical_state']
        
        print(f"🧐 현재 시장 국면: {main_regime} | 전술 상황: {tactical_state}")

        sentiment_display = self.get_market_sentiment_display()

        is_emergency, change_rate = self.emergency_check()
        # 기본 상황
        headline_context = "특별한 급등락 없음. 전반적인 시장 분위기와 핵심 이슈를 반영할 것."
        
        if is_emergency:
            type_str = "폭등" if change_rate > 0 else "폭락"
            # 긴급 상황 팩트 전달
            headline_context = f"🚨 [긴급 상황] BTC {change_rate}% {type_str} 발생. 투자자들의 이목을 끌 자극적인 멘트 필요."

        # 2. [NEW] 전략 시뮬레이션 및 봇 선정 (BotTraderSelector에게 위임)
        # (1) 모든 창의적 전략 생성
        all_strategies = generate_all_strategies(df, regime_info)
        
        # (2) 현재 국면에 맞는 사령관(Bot) 소환
        selection_result = self.bot_selector.select_best_strategy(all_strategies, regime_info)
        
        best_strategy = selection_result['selected_strategy']
        commander_name = selection_result['commander']
        commander_desc = selection_result['commander_desc']
        regime_comment = selection_result['regime_comment']
        
        best_strat_name = best_strategy['name']
        
        # 3. 데이터 수집 (기존 유지)
        major_table = self.get_market_metrics(self.targets["Major"])
        meme_table = self.get_market_metrics(self.targets["Meme"])
        tactical_table = self.get_tactical_map(self.targets["Major"])
        news_data = self.collect_news()
        today_date = datetime.now().strftime("%Y.%m.%d")

        # [표 생성] 상위 3개 전략 요약 테이블 만들기
        top_strategies = sorted(all_strategies, key=lambda x: x['score'], reverse=True)[:3]
        strat_table_str = "| 순위 | 전략명 | 유형 | 점수 | 설명 |\n|---|---|---|---|---|\n"
        for i, strat in enumerate(top_strategies, 1):
            strat_table_str += f"| {i} | {strat['name']} | {strat['type']} | {strat['score']} | {strat['desc']} |\n"

        # 4. 프롬프트 (사령관 페르소나 주입 - 여기가 핵심!)
        system_prompt = f"""
        너는 가상자산 시장의 베테랑 트레이더 '{self.service_name}'야.
        
        [⚠️ 오늘의 작전 명령 (Commander's Order)]
        - **지휘관:** {commander_name} ({commander_desc})
        - **전술 판단:** {regime_comment}
        - **메인 전략:** "{best_strat_name}" (이 전략을 중심으로 브리핑해라)
        
        [🎯 헤드라인 작성 미션]  <-- ★ 여기를 추가!
        - 현재 상황: {headline_context}
        - 지시: 위 상황을 바탕으로 클릭을 유도하는 가장 자극적이고 매력적인 한 줄 제목을 창작해라. (명령어 자체를 출력하지 말고, 창작된 제목만 출력할 것)
        
        [🔥🔥 절대 준수 사항]
        1. **뉴스 포맷:** `1. **[제목]**` -> `> 🔍 **팩트:**` -> `> 👁️ **헌터의 뷰:**` 형식을 목숨 걸고 지켜라.
        2. **독백 필수:** 대시보드 아래 독백 란을 비우지 마라.
        3. **사령관 빙의:** 너는 지금 '{commander_name}' 봇이다. 말투와 관점을 그에 맞춰라.
           - Hunter(하이에나): 냉소적, 역추세 강조. "공포에 사라."
           - Surfer(서퍼): 신남, 추세 추종 강조. "물 들어올 때 저어라."
           - Sniper(스나이퍼): 차분함, 변동성 돌파 강조. "기다림이 미덕이다."
           - Farmer(농부): 인내심, 매집 강조. "씨를 뿌릴 때다."
           - Guardian(경비병): 보수적, 현금 비중 강조. "잃지 않는 게 버는 거다."

        [작성 지침]
        1. **역할 분담 (중복 방지):**
           - **독백:** 표의 데이터(김프, 펀딩비, 심리)를 보고 **시장의 감정(분위기)**을 읽어라.
           - **코멘트:** 시뮬레이션 결과표를 보고 **왜 이 전략(Bot)이 소환되었는지** 설명해라.
           - **결론:** 위 두 가지를 종합하여 **구체적인 행동(Action)**을 지시해라.  
        2. **뉴스 레이아웃 (신뢰도 강화):**
           - `1. **[한글 뉴스 제목]**`
           - `> 🔍 **팩트:** (내용)`
           - `> 👁️ **헌터의 뷰:** (해석)`
             `*Original: [영어 원문 제목] | Source: [매체명] ([시간])*`
        3. **전략 설명:** - 1위 전략인 **[{best_strat_name}]**이 왜 지금 시장에 통하는지 논리적으로 설득해라.

        [출력 양식]
        # 🐋 [헤드라인] (여기에 위 미션에 따라 창작한 제목을 출력) <-- ★ 여기를 수정!

        날짜: {today_date} | 시간: {mode.upper()} | 사령관: {commander_name}

        ## 1. 🌍 글로벌 첩보 (Intelligence)
        *오늘 시장의 핵심 재료 5선*
        (뉴스 5개 작성)

        ## 2. 헌터의 대시보드 (Dashboard)
        {sentiment_display}
        
        **[메이저]**
        {major_table}
        **[알트/밈]**
        {meme_table}
        > **🗨️ 헌터의 독백:** (김프와 펀딩비, 심리 지수를 보니 시장 참여자들이 쫄아있는지, 흥분했는지 사령관 관점에서 해석)

        ## 3. ⚔️ 전술 시뮬레이션 (Strategy Lab)
        오늘의 전장 상황: **[{main_regime} - {tactical_state}]**
        {strat_table_str}
        > **💡 헌터의 코멘트:** (오늘 왜 **{commander_name}** 모드로 전환했는지, 그리고 1위 전략이 왜 선택되었는지 설명해라.)

        ## 4. 오늘의 단타 전술 (Scalping Map)
        {tactical_table}

        ## 5. 최종 결론 (The Verdict)
        - **상황 판단:** (현재 시장 국면과 데이터를 종합하여 3줄 이내로 상황을 브리핑해줘.)
        **🔥 오늘의 추천 전략 Top 3**
        (위 '3. 전술 시뮬레이션' 표의 상위 3개 전략을 상세히 설명해.)
        **1. 🥇 {best_strat_name}**
           - "매력 어필 (사령관 말투로)"
           - 가이드: (진입/청산/손절 가이드)       
        **2. 🥈 (2위 전략명)**
           - ...
        **3. 🥉 (3위 전략명)**
           - ...
        
        **종합 코멘트:** (오늘의 사령관 **{commander_name}**으로서 마지막 조언 한마디.)
        """

        user_prompt = f"[뉴스 데이터]\n{news_data}"
        
        print(f"🧠 AI가 시크릿 노트를 작성 중입니다...")
        result_text = _chat(system_prompt, user_prompt)
        
        self.save_to_file(result_text, today_date, mode)
        return result_text


    # [기존 save_to_file 함수를 이걸로 통째로 교체하세요]
    def save_to_file(self, text, date_str, mode):
        # 1. 환경 변수 확인 (기본값: prod)
        env_mode = os.getenv("NEWSLETTER_ENV", "prod").lower()
        
        # 2. dev 환경이면 파일명 뒤에 '-dev' 붙이기
        suffix = "-dev" if env_mode == "dev" else ""
        
        # 예: SecretNote_Morning_2025.12.15-dev.md
        filename = f"SecretNote_{mode.capitalize()}_{date_str}{suffix}.md"
        
        # 3. 저장
        save_path = BASE_DIR / "moneybag" / "data" / "out" / filename
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(text)
            
        print(f"\n✅ [저장 완료] {filename} (환경: {env_mode})")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "morning"
    newsletter = DailyNewsletter()
    print(newsletter.generate(mode))