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

BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

class DailyNewsletter:
    def __init__(self):
        self.price_collector = CexPriceCollector()
        self.funding_analyzer = FundingRateAnalyzer()
        self.whale_tracker = WhaleAlertTracker()
        self.news_collector = CryptoNewsRSS()
        self.backtester = SimpleBacktester()
        self.tech_analyzer = TechnicalLevelsAnalyzer()
        self.onchain_collector = OnChainCollector()
        
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

    def determine_regime(self, symbol="BTC/USDT"):
        try:
            ohlcv = self.price_collector.binance.fetch_ohlcv(symbol, '1d', limit=210)
            if not ohlcv: return "UNKNOWN", 0, 0
            df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            current_price = df.iloc[-1]['c']
            ma200 = df['c'].rolling(window=200).mean().iloc[-1]
            ma50 = df['c'].rolling(window=50).mean().iloc[-1]
            if pd.isna(ma200): return "UNKNOWN", current_price, 0
            
            # 5단계 메인 국면
            if current_price > ma50 and ma50 > ma200: regime = "STRONG_BULL"
            elif ma50 > current_price and current_price > ma200: regime = "WEAK_BULL"
            elif ma200 > ma50 and ma50 > current_price: regime = "STRONG_BEAR"
            elif ma200 > current_price and current_price > ma50: regime = "WEAK_BEAR"
            else: regime = "SIDEWAYS"
            return regime, current_price, ma200
        except: return "UNKNOWN", 0, 0

    # [서브 국면 판독기] - 날씨 판단
    def get_sub_regime(self, symbol="BTC/USDT", main_regime="SIDEWAYS"):
        try:
            ohlcv = self.price_collector.binance.fetch_ohlcv(symbol, '1d', limit=20)
            df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            
            last = df.iloc[-1]
            vol_ratio = last['v'] / df['v'].mean()
            
            # RSI 계산 (간이)
            delta = df['c'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            rsi = (100 - (100 / (1 + rs))).iloc[-1]
            
            sub_regime = "NORMAL"
            
            # 과열/침체
            if rsi > 70: sub_regime = "🔥 OVERHEATED (과열)"
            elif rsi < 30: sub_regime = "🩸 PANIC_SELL (투매)"
            
            # 캔들 패턴
            if "BEAR" in main_regime and last['c'] > last['o'] and vol_ratio > 1.5 and rsi > 50:
                sub_regime = "🔨 DEAD_CAT (데드캣)"
            if "BULL" in main_regime and last['c'] < last['o'] and vol_ratio < 0.8 and rsi < 50:
                sub_regime = "📉 DIP (눌림목)"
            if vol_ratio < 0.5 and abs(last['c'] - last['o']) / last['o'] < 0.01:
                sub_regime = "💤 DRY_OUT (소강)"

            return sub_regime
        except: return "NORMAL"

    def determine_market_condition(self, symbol="BTC/USDT"):
        try:
            ohlcv = self.price_collector.binance.fetch_ohlcv(symbol, '1d', limit=20)
            df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            last = df.iloc[-1]
            prev_vol_ma = df['v'].iloc[:-1].mean()
            if last['v'] > prev_vol_ma * 2.0:
                return "VOLUME_SPIKE_UP" if last['c'] > last['o'] else "VOLUME_SPIKE_DOWN"
            high_20 = df['h'].max()
            low_20 = df['l'].min()
            if last['c'] >= high_20 * 0.98: return "RSI_OVERBOUGHT"
            if last['c'] <= low_20 * 1.02: return "RSI_OVERSOLD"
            return "NORMAL"
        except: return "NORMAL"

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

    # [핵심 수정] generate 메서드 대수술
    def generate(self, mode="morning"):
        print(f"🚀 [{mode.upper()}] 웨일 헌터가 데이터를 분석 중입니다...")
        
        regime, curr_p, ma200 = self.determine_regime("BTC/USDT")
        sub_regime = self.get_sub_regime("BTC/USDT", regime)
        
        print(f"🧐 현재 시장 국면: {regime} ({sub_regime})")

        sentiment_display = self.get_market_sentiment_display()

        is_emergency, change_rate = self.emergency_check()
        headline_instruction = "가장 중요한 이슈를 하나 골라 자극적인 제목을 뽑아라."
        if is_emergency:
            type_str = "폭등" if change_rate > 0 else "폭락"
            headline_instruction = f"⚠️ [긴급] BTC {change_rate}% {type_str}! 원인과 대응책을 제목으로 뽑아라."

        # 1. 백테스트 실행
        simple_regime = "BULL" if "BULL" in regime else "BEAR"
        # 특이 국면일 경우 해당 전략 우선 검토
        condition = "NORMAL"
        if "PANIC" in sub_regime: condition = "RSI_OVERSOLD"
        elif "DEAD_CAT" in sub_regime: condition = "RSI_OVERBOUGHT"
        elif "DIP" in sub_regime: condition = "RSI_OVERSOLD"
        
        # 백테스트 결과 받기
        backtest_report, backtest_comment, best_strat_info = self.backtester.run_multi_strategy_test("BTC/USDT", simple_regime)
        
        # best_strat_info 예시: "📉 투매 줍기 (LONG)"
        best_strat_name = best_strat_info.split("(")[0].strip()
        best_strat_pos = "LONG" if "LONG" in best_strat_info else "SHORT"

        # 2. [중요] 동적 교리(Dynamic Doctrine) 생성
        # 국면(Trend)과 전략(Signal)이 일치하는지, 엇갈리는지 판단
        doctrine = ""
        conflict_mode = False
        
        if "BEAR" in regime:
            if best_strat_pos == "SHORT":
                doctrine = f"현재는 **하락장({regime})**이고, 통계적 승률도 **숏(Short)**을 가리킨다. **추세를 따라가는 매매**가 정석이다. 반등 시 과감하게 매도해라."
            else: # 하락장인데 롱 전략이 나옴
                conflict_mode = True
                doctrine = f"현재는 **하락장({regime})**이지만, 단기적으로 과매도 구간에 진입했다. 통계적으로 **기술적 반등(Long)** 승률이 더 높다. **'짧게 먹고 빠지는 역추세 매매'**로 대응해라."
        else: # BULL
            if best_strat_pos == "LONG":
                doctrine = f"현재는 **상승장({regime})**이고, 전략도 **롱(Long)**이다. 추세가 강력하다. 조정은 매수 기회다."
            else: # 상승장인데 숏 전략이 나옴
                conflict_mode = True
                doctrine = f"현재는 **상승장({regime})**이지만, 단기 과열 신호가 떴다. **리스크 관리(Short Hedge)**가 필요하다. 추세가 꺾이기 전까지는 보수적으로 접근해라."

        # 3. 데이터 수집
        major_table = self.get_market_metrics(self.targets["Major"])
        meme_table = self.get_market_metrics(self.targets["Meme"])
        tactical_table = self.get_tactical_map(self.targets["Major"])
        news_data = self.collect_news()
        today_date = datetime.now().strftime("%Y.%m.%d")

        # 4. 프롬프트 (역할 분담 강화)
        system_prompt = f"""
        너는 가상자산 시장의 베테랑 트레이더 '{self.service_name}'야.
        
        [⚠️ 헌터의 절대 원칙 (Doctrine)]
        {doctrine}
        
        [🔥🔥 절대 준수 사항]
        1. **뉴스 포맷:** `### 1. [제목]` -> `> 🔍 **팩트:**` -> `> 👁️ **헌터의 뷰:**` 형식을 목숨 걸고 지켜라.
        2. **독백 필수:** 대시보드 아래 독백 란을 비우지 마라.
        
        
        [🏷️ [성격태그] 가이드라인 (이 중에서 골라라)]
        - **[안전형]:** 승률 55% 이상이거나, RSI 30 이하에서 줍는 저점 매수 전략
        - **[공격형]:** 승률은 낮아도 한방 수익이 크거나, 저항선을 뚫을 때 들어가는 돌파 매매
        - **[역추세]:** 시장 국면과 반대 포지션 (예: 하락장에서 롱, 상승장에서 숏)
        - **[추세형]:** 시장 국면과 같은 포지션 (예: 하락장에서 숏, 상승장에서 롱)

        [작성 지침]
        1. **역할 분담 (중복 방지):**
           - **독백:** 표의 데이터(김프, 펀딩비, 심리)를 보고 **시장의 감정(분위기)**을 읽어라. (전략 얘기 X)
           - **코멘트:** 백테스트 결과표를 보고 **왜 이 전략이 통계적으로 우수한지** 숫자로 증명해라. (시장 분위기 얘기 X)
           - **결론:** 위 두 가지를 종합하여 **구체적인 행동(Action)**을 지시해라.  
        2. **뉴스 레이아웃 (신뢰도 강화):**
           - 제목은 한글로 매력적으로 의역해라.
           - 절대 뭉뚱그리지 말고, 아래 포맷을 사용하여 **카드뉴스처럼** 보이게 해라.
           - 절대 짧게 요약하지 마라. 독자는 **디테일**을 원한다.
           - **🔍 팩트:** 육하원칙에 의거하여 사건의 전말을 2~3문장으로 상세히 서술해라.
           - **👁️ 헌터의 뷰:** 이 뉴스가 시장에 미칠 파장, 세력의 의도, 매매 힌트를 2문장 이상 깊이 있게 분석해라.
           - 본문(팩트/뷰) 작성 후, **맨 아래에 작은 글씨로 원문 출처를 달아라.**
           - 형식:
             `### 1. [한글 뉴스 제목]`
             `> 🔍 **팩트:** (내용)`
             `> 👁️ **헌터의 뷰:** (해석)`
             `*Original: [영어 원문 제목] | Source: [매체명] ([시간])*` (엔터)
             `(빈 줄)`
        3. **전략 일치 (절대 준수):**
           - 백테스트 결과가 **[{best_strat_name} ({best_strat_pos})]**라면, 너의 결론도 무조건 **{best_strat_pos}**여야 한다.
           - 하락장이라고 무조건 숏이라고 우기지 마라. 데이터가 롱이면 "반등 롱"이라고 해라.
        4. **전략 설명:** 백테스트 결과를 근거로 데이/스윙 전략을 명확히 구분해라.
        5. **독백 작성 (필수):** 대시보드 하단에 있는 '헌터의 독백' 란을 절대 비워두거나 괄호 그대로 두지 마라.
           - 위 심리 지표(공포/탐욕)와 대시보드 데이터(김프, 펀딩비)를 보고 느낀 너의 통찰을 3줄로 작성해라.
        6. **전략:** 백테스트 표를 보고, 승률이 높은 전략을 구체적으로 언급해라.


        [출력 양식]
        (맨 윗줄 제목: # 🐋 [헤드라인] ...)

        날짜: {today_date} | 시간: {mode.upper()} | 국면: {regime} ({sub_regime})

        ## 1. 🌍 글로벌 첩보 (Intelligence)
        *오늘 시장의 핵심 재료 5선*
        (뉴스 5개 작성)

        ## 2. 헌터의 대시보드 (Dashboard)
        {sentiment_display}
        
        **[메이저]**
        {major_table}
        **[알트/밈]**
        {meme_table}
        > **🗨️ 헌터의 독백:** (김프와 펀딩비, 심리 지수를 보니 시장 참여자들이 쫄아있는지, 흥분했는지 해석)

        ## 3. ⚔️ 전술 시뮬레이션 (Strategy Lab)
        {backtest_report}
        > **💡 헌터의 코멘트:** (위 표{backtest_report}에서 승률과 수익률이 가장 좋은 1위 전략에 대해 구체적으로 분석해라.)

        ## 4. 오늘의 단타 전술 (Scalping Map)
        {tactical_table}

        ## 5. 최종 결론 (The Verdict)
        - **상황 판단:** (현재 시장 국면과 데이터를 종합하여 3줄 이내로 상황을 브리핑해줘.)
        **🔥 오늘의 추천 전략 Top 3 (골라 드세요)**
        (위 '3. 전술 시뮬레이션' 표에서 승률 상위 3개 전략을 선정하여 아래 양식으로 작성해. 전략의 성격은 네가 판단해서 [안전형/공격형/역추세] 등의 태그를 달아.)
        **1. [성격태그] 📉 전략명 (Position)**
           - "한 줄 매력 어필 (예: 남들이 공포에 떨 때 줍줍!)"
           - 가이드: (진입/청산/손절 내용 요약)       
        **2. [성격태그] ⚡ 전략명 (Position)**
           - "한 줄 매력 어필"
           - 가이드: ...
        **3. [성격태그] 🌊 전략명 (Position)**
           - "한 줄 매력 어필"
           - 가이드: ...       
        
        **종합 코멘트:** (위 전략들을 수행할 때 주의할 점이나 멘탈 관리 조언 한마디. 특히 1위 전략인 **{best_strat_name}** 위주로 조언해.)
        """

        user_prompt = f"[뉴스 데이터]\n{news_data}"
        
        print(f"🧠 AI가 시크릿 노트를 작성 중입니다...")
        result_text = _chat(system_prompt, user_prompt)
        
        self.save_to_file(result_text, today_date, mode)
        return result_text

    def save_to_file(self, text, date_str, mode):
        filename = f"SecretNote_{mode.capitalize()}_{date_str}.md"
        save_path = BASE_DIR / "moneybag" / "data" / "out" / filename
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"\n✅ [저장 완료] {filename}")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "morning"
    newsletter = DailyNewsletter()
    print(newsletter.generate(mode))