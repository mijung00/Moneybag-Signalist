import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import time
from pathlib import Path
from dotenv import load_dotenv
import json

# 모듈 임포트
from moneybag.src.collectors.cex_price_collector import CexPriceCollector
from moneybag.src.analyzers.funding_rate_anomaly import FundingRateAnalyzer

# ---------------------------------------------------------------------
# ✅ SecretsManager를 JSON 형태로 저장했을 때도 동작하게(OPENAI_API_KEY 등)
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
        pass
_normalize_json_env("OPENAI_API_KEY")
from moneybag.src.analyzers.whale_alert_tracker import WhaleAlertTracker
from moneybag.src.collectors.crypto_news_rss import CryptoNewsRSS

try:
    from moneybag.src.llm.openai_driver import _chat
except Exception as e:
    print(f"⚠️ [LLM Import Error] OpenAI 기능이 비활성화될 수 있습니다: {e}")
    _chat = None

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
                vol_str = "⚠️데이터없음" # [수정] 기본값을 '데이터 없음'으로 변경
                if whale_data:
                    ratio = whale_data.get('vol_spike_ratio', 1.0)
                    if ratio > 2.5: vol_str = f"🔥폭발({ratio:.1f}x)"
                    elif ratio > 1.5: vol_str = f"⚡활발({ratio:.1f}x)"
                    elif ratio < 0.6: vol_str = f"💧말라감"
                    else: vol_str = "평범" # [추가] 데이터가 있을 때만 '평범'으로 표시
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

    def get_market_sentiment_display(self, regime_info: dict):
        # [수정] 외부 API 호출 안정성 강화를 위해 재시도 로직 추가
        data = None
        max_retries = 3
        for attempt in range(max_retries):
            data = self.onchain_collector.get_whale_ammo()
            if data:
                break
            print(f"⚠️ [Retry] 온체인 데이터 수집 재시도... ({attempt + 1}/{max_retries})")
            time.sleep(5) # 5초 후 재시도

        if not data: 
            return "데이터 수집 실패"
        
        raw_score = data['current']['value']
        main_regime = regime_info.get('main_regime', 'Range')

        # [NEW] 국면 보정 로직
        explanation_note = ""
        if main_regime == 'Bear':
            # 하락장에서는 실제 점수 10~50점 사이를 0~100점으로 재조정하여 상대적 위치를 표시
            rescaled_score = ((raw_score - 10) / (50 - 10)) * 100
            display_score = max(0, min(100, int(rescaled_score)))
            explanation_note = "\n\n_*🐻 하락장에서는 심리 점수가 낮게 유지되는 경향이 있어, 최근 추세 내에서의 상대적 위치를 보여주도록 보정되었습니다._"
        elif main_regime == 'Bull':
            # 상승장에서는 실제 점수 50~90점 사이를 0~100점으로 재조정
            rescaled_score = ((raw_score - 50) / (90 - 50)) * 100
            display_score = max(0, min(100, int(rescaled_score)))
            explanation_note = "\n\n_*🐂 상승장에서는 심리 점수가 높게 유지되는 경향이 있어, 최근 추세 내에서의 상대적 위치를 보여주도록 보정되었습니다._"
        else: # 횡보장
            display_score = raw_score
        
        # 보정된 점수를 기준으로 상태(status) 재결정
        if display_score >= 75: status = "극단적 탐욕"
        elif display_score >= 55: status = "탐욕"
        elif display_score <= 25: status = "극단적 공포"
        elif display_score <= 45: status = "공포"
        else: status = "중립"

        hist = data['history']
        gauge_bar = self.create_sentiment_gauge(display_score)
        diff_day = raw_score - hist['yesterday']
        icon_day = "🔺" if diff_day > 0 else "🔻"
        explanation = "_*산출 기준: 변동성(25%) + 모멘텀(25%) + SNS(15%) + 도미넌스(10%) + 트렌드(10%)_"
        
        display = f"""
### 🧠 고래 심리 기상도 (Whale Sentiment)
**현재: {status} (보정치)**
{gauge_bar}
{explanation}{explanation_note}

* 📉 **전일 대비(원본):** {hist['yesterday']} → {raw_score} ({icon_day}{abs(diff_day)})
* 🗓️ **지난주(원본):** {hist['last_week']}
* 🗓️ **지난달(원본):** {hist['last_month']}
"""
        return display

    def collect_news(self):
        news_items = self.news_collector.collect_all()
        summary = ""
        for idx, item in enumerate(news_items[:10], 1):
             # [개선] 여러 뉴스 소스의 다양한 필드명을 처리하기 위한 로직
             # 각 뉴스 아이템(dict)에서 가능한 키들을 순서대로 탐색합니다.

             # 1. 원문 제목 찾기 (시도할 키 목록)
             possible_title_keys = ['original_title', 'title', 'headline']
             original_title = '제목 없음'
             for key in possible_title_keys:
                 if item.get(key):
                     original_title = item[key]
                     break

             # 2. 게시 시각 찾기 (시도할 키 목록)
             possible_date_keys = ['published_at', 'published', 'pubDate', 'updated', 'created_at', 'timestamp']
             pub_date_str = datetime.now().strftime('%Y-%m-%d %H:%M') # 기본값
             for key in possible_date_keys:
                 if item.get(key):
                     # TODO: 날짜 형식이 다양할 수 있으나, 우선 문자열 그대로 전달
                     pub_date_str = str(item[key])
                     break
             
             # 3. 기타 정보 추출
             source = item.get('source', 'N/A')
             content = item.get('summary', '내용 없음')

             # 4. AI에게 전달할 최종 문자열 구성
             summary += f"[[뉴스 #{idx}]]\n원문 제목: {original_title}\n출처: {source}\n게시 시각: {pub_date_str}\n내용: {content}\n\n"
        return summary

    def emergency_check(self):
        btc_data = self.price_collector.fetch_price_data("BTC")
        if btc_data:
            change = btc_data.get('change_24h', 0)
            if abs(change) >= 2.0: return True, change
        return False, 0

    def generate(self, mode="morning"):
        print(f"🚀 [{mode.upper()}] 웨일 헌터가 데이터를 분석 중입니다...")
        
        # 0. 데이터 준비 (BTC 기준)
        ohlcv = self.price_collector.binance.fetch_ohlcv("BTC/USDT", '1d', limit=1000)
        if not ohlcv:
            print("❌ BTC 데이터 수집 실패")
            return

        df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        
        # 1. [NEW] 레짐 및 서브 국면 분석 (MarketRegimeAnalyzer에게 위임)
        # [수정] 더 복합적인 국면 정보를 받도록 변경 (대국면, 전술상황, 확신도 등)
        regime_info = self.regime_analyzer.analyze_regime(df)
        
        main_regime = regime_info['main_regime']
        tactical_state = regime_info['tactical_state']
        
        print(f"🧐 현재 시장 국면: {main_regime} | 전술 상황: {tactical_state}")
 
        sentiment_display = self.get_market_sentiment_display(regime_info)
 
        is_emergency, change_rate = self.emergency_check()
        # 기본 상황
        headline_context = "특별한 급등락 없음. 전반적인 시장 분위기와 핵심 이슈를 반영할 것."
        
        if is_emergency:
            type_str = "폭등" if change_rate > 0 else "폭락"
            # 긴급 상황 팩트 전달
            headline_context = f"🚨 [긴급 상황] BTC {change_rate}% {type_str} 발생. 투자자들의 이목을 끌 자극적인 멘트 필요."
 
        # 2. [수정] 전략 생성 및 '오디션'을 통한 사령관 선정
        # (1) 다양한 계열의 전략들을 모두 생성
        all_strategies = generate_all_strategies(df, regime_info)
        
        # (2) 새로운 점수 시스템으로 최적 전략과 사령관을 '선출'
        selection_result = self.bot_selector.select_best_strategy(all_strategies, regime_info)
        
        best_strategy = selection_result['selected_strategy']
        commander_name = selection_result['commander']
        commander_desc = selection_result['commander_desc']
        regime_comment = selection_result['regime_comment']
        
        best_strat_name = best_strategy['name']
        
        # 3. 리포트용 데이터 수집 (기존과 동일)
        major_table = self.get_market_metrics(self.targets["Major"])
        meme_table = self.get_market_metrics(self.targets["Meme"])
        tactical_table = self.get_tactical_map(self.targets["Major"])
        news_data = self.collect_news()
        today_date = datetime.now().strftime("%Y.%m.%d")

        # [수정] 테이블 생성 로직은 PostProcessor로 이동. 여기서는 플레이스홀더만 남김.

        # 4. [수정] LLM 프롬프트 대폭 수정 (새로운 시스템의 논리를 설명하도록)
        system_prompt = f"""
        너는 가상자산 시장의 베테랑 트레이더 '{self.service_name}'야.
        
        [⚠️ 오늘의 작전 명령 (Commander's Order)]
        - **지휘관:** {commander_name} ({commander_desc})
        - **오늘의 전술 판단:** {regime_comment}
        - **메인 전략:** "{best_strat_name}" (이 전략을 중심으로 브리핑해라)
        
        [🎯 헤드라인 작성 미션]
        - 현재 상황: {headline_context}
        - 지시: 위 상황을 바탕으로 클릭을 유도하는 가장 자극적이고 매력적인 한 줄 제목을 창작해라. (명령어 자체를 출력하지 말고, 창작된 제목만 출력할 것)
        
        [🔥🔥 절대 준수 사항]
        1. **뉴스 포맷:** `### 1. [뉴스 제목]` -> `> 🔍 **팩트:**` -> `> 👁️ **헌터의 뷰:**` -> `*Original...*` 형식을 목숨 걸고 지켜라.
        2. **독백 필수:** 대시보드 아래 독백 란을 비우지 마라. (김프와 펀딩비, 심리 지수를 보고 시장 참여자들이 쫄아있는지, 흥분했는지 사령관 관점에서 해석)
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
           - `### 1. [한글 뉴스 제목]`
           - `> 🔍 **팩트:** (내용)`
           - `> 👁️ **헌터의 뷰:** (해석)`
             `*Original: [영어 원문 제목] | Source: [매체명] ([시간])*`
        3. **전략 설명:** - 1위 전략인 **[{best_strat_name}]**이 왜 지금 시장에 통하는지 논리적으로 설득해라.

        [출력 양식] (아래 구조를 반드시 지켜라)
        # 🐋 [헤드라인] (여기에 위 미션에 따라 창작한 제목을 출력)

        날짜: {today_date} | 시간: {mode.upper()} | 사령관: {commander_name}

        ## 1. 🌍 글로벌 첩보 (Intelligence)
        *오늘 시장의 핵심 재료 5선*
        (뉴스 5개 작성)

        ## 2. 헌터의 대시보드 (Dashboard)
        {sentiment_display}
        > **고래 심리 지수란?** 주요 지갑들의 활동성과 거래소 입출금 물량을 종합하여 시장의 탐욕/공포 상태를 나타내는 Fincore 자체 지표입니다. (0~100, 높을수록 탐욕)
        **[메이저]**
        {major_table}
        **[알트/밈]**
        {meme_table}
        > **🗨️ 헌터의 독백:** (김프와 펀딩비, 심리 지수를 보니 시장 참여자들이 쫄아있는지, 흥분했는지 사령관 관점에서 해석)

        ## 3. ⚔️ 전술 시뮬레이션 (Strategy Lab)
        오늘의 전장 상황: **대국면: {main_regime} | 전술상황: {tactical_state}**
        <!-- STRATEGY_TABLE_PLACEHOLDER -->
        > **💡 헌터의 코멘트:** (오늘 왜 **{commander_name}** 모드로 전환했는지, 그리고 1위 전략이 왜 선택되었는지 설명해라.)

        ## 4. 오늘의 단타 전술 (Scalping Map)
        {tactical_table}

        ## 5. 최종 결론 (The Verdict)
        - **상황 요약:** (현재 시장 국면과 데이터를 종합하여 3줄 이내로 명확하게 상황을 브리핑해라.)
        - **최종 행동 지침:** (오늘 추천된 Top 3 전략: **'{best_strat_name}'** 등을 바탕으로, 투자 성향별(공격적/안정적)로 어떤 스탠스를 취해야 할지 구체적인 행동 가이드를 제시해라. 예를 들어 '공격적인 투자자는 1위 전략을, 안정적인 투자자는 관망 또는 3위 전략을 참고하라'는 식으로 명확하게 지시해라.)
        - **사령관의 마지막 한마디:** (오늘의 사령관 **{commander_name}**으로서, 시장 참여자들에게 전하고 싶은 핵심 메시지나 경고를 담아 강력한 어조로 마무리해라.)
        """

        user_prompt = f"[뉴스 데이터]\n{news_data}"
        
        print(f"🧠 AI가 시크릿 노트를 작성 중입니다...")
        
        if not _chat:
            print("❌ LLM 드라이버 로드 실패로 AI 노트 생성을 건너뜁니다.")
            result_text = "# AI 생성 실패\n\nLLM 드라이버 로딩 중 오류가 발생하여 뉴스레터 본문을 생성하지 못했습니다."
        else:
            result_text = _chat(system_prompt, user_prompt)
        
        saved_path = self.save_to_file(result_text, today_date, mode)
        return saved_path, all_strategies

    def save_to_file(self, text, date_str, mode):
        """[NEW] dev/prod 환경에 따라 파일명을 분리하여 저장합니다."""
        env_mode = os.getenv("NEWSLETTER_ENV", "prod").lower()
        suffix = "-dev" if env_mode == "dev" else ""
        filename = f"SecretNote_{mode.capitalize()}_{date_str}{suffix}.md"
        save_path = BASE_DIR / "moneybag" / "data" / "out" / filename
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(text)
            
        print(f"✅ [저장 완료] {filename} (환경: {env_mode})")
        return save_path

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "morning"
    newsletter = DailyNewsletter()
    print(newsletter.generate(mode))