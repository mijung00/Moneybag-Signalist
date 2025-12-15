import pandas as pd
import numpy as np

class MarketRegimeAnalyzer:
    def __init__(self):
        pass

    def analyze_regime(self, df: pd.DataFrame) -> dict:
        """
        메인 레짐(계절)과 택티컬 레짐(오늘의 날씨)을 분석합니다.
        """
        if df.empty or len(df) < 120:
            return {
                "main_regime": "Unknown",
                "tactical_state": "Neutral",
                "is_high_volatility": False,
                "momentum_score": 0
            }

        # 1. 기본 데이터 추출
        current_price = df['close'].iloc[-1]
        ma_20 = df['close'].rolling(20).mean().iloc[-1]
        ma_120 = df['close'].rolling(120).mean().iloc[-1]
        
        # 2. [Main Regime] 숲을 보는 관점 (기존 로직 유지/이동)
        main_regime = "Bull" if current_price > ma_120 else "Bear"
        
        # 3. [Tactical Regime] 나무와 바람을 보는 관점 (Wild Patch)
        # (1) 변동성 (Shock): 최근 5일 평균 변동폭 대비 오늘 변동폭
        volatility_5d = (df['high'] - df['low']).rolling(5).mean().iloc[-1]
        today_volatility = df['high'].iloc[-1] - df['low'].iloc[-1]
        
        # 0으로 나누기 방지
        if volatility_5d == 0:
            is_high_vol = False
        else:
            is_high_vol = today_volatility > (volatility_5d * 1.5) # 평소보다 1.5배 이상 날뜀

        # (2) 단기 모멘텀 (Speed): 3일 전 대비 등락률
        momentum_3d = (current_price - df['close'].iloc[-4]) / df['close'].iloc[-4]
        
        # (3) 추세 강도 (Trend Power): 20일 이평선 이격도
        trend_power = (current_price - ma_20) / ma_20

        # 4. 택티컬 상태 정의 (매일매일의 기분)
        tactical_state = "Neutral"

        if is_high_vol:
            if momentum_3d < -0.05: # 변동성 크고 5% 이상 급락
                tactical_state = "Panic_Dump" # 패닉장 (하이에나 봇 출동각)
            elif momentum_3d > 0.05:
                tactical_state = "FOMO_Pump" # 광기장 (서퍼 봇 출동각)
            else:
                tactical_state = "High_Vol_Chop" # 위아래 털기 (스나이퍼 봇 출동각)
        else:
            if trend_power > 0.03:
                tactical_state = "Strong_Uptrend" # 강력 상승
            elif trend_power < -0.03:
                tactical_state = "Strong_Downtrend" # 강력 하락
            elif abs(trend_power) < 0.01:
                tactical_state = "Boring_Sideways" # 노잼 횡보 (농부 봇 출동각)
            else:
                tactical_state = "Grinding" # 완만한 진행

        return {
            "main_regime": main_regime,
            "tactical_state": tactical_state,
            "is_high_volatility": is_high_vol,
            "momentum_score": momentum_3d,
            "ma_120": ma_120,
            "ma_20": ma_20
        }