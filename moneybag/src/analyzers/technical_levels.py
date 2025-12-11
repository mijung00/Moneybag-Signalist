import ccxt
import pandas as pd

class TechnicalLevelsAnalyzer:
    def __init__(self):
        self.binance = ccxt.binance()

    def analyze(self, symbol="BTC/USDT"):
        try:
            # 일봉 데이터 (어제, 오늘)
            ohlcv = self.binance.fetch_ohlcv(symbol, '1d', limit=5)
            df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            
            # 어제 캔들 (Yesterday) - 피봇의 기준
            y = df.iloc[-2] 
            
            # 피봇 포인트 계산 (Standard Pivot)
            P = (y['h'] + y['l'] + y['c']) / 3
            R1 = (2 * P) - y['l']
            S1 = (2 * P) - y['h']
            
            # 현재가 위치 파악
            curr = df.iloc[-1]['c']
            
            # 추세 판단
            trend = "중립"
            if curr > P: trend = "▲ 상승 우위"
            if curr < P: trend = "▼ 하락 우위"
            
            return {
                "symbol": symbol,
                "price": curr,
                "pivot": P,
                "r1": R1, # 1차 저항 (익절 라인)
                "s1": S1, # 1차 지지 (진입 라인)
                "trend": trend
            }
        except:
            return None