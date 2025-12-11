import ccxt
import time
import pandas as pd

class FuturesMetricsAnalyzer:
    def __init__(self):
        self.binance = ccxt.binance({'options': {'defaultType': 'future'}})

    def fetch_metrics(self, symbol="BTC/USDT"):
        try:
            # 1. 펀딩비 (Funding Rate)
            funding = self.binance.fetch_funding_rate(symbol)
            fund_rate = float(funding['fundingRate'])
            
            # 2. 미결제약정 (Open Interest)
            # ccxt의 fetch_open_interest 사용
            oi_data = self.binance.fetch_open_interest(symbol)
            oi_amount = float(oi_data['openInterestAmount']) # 코인 개수
            oi_value = float(oi_data['openInterestValue'])   # 달러 가치
            
            # 3. 롱/숏 비율 (Long/Short Ratio)
            # *주의: ccxt 일반 함수로는 어려울 수 있어 바이낸스 공용 API 직접 호출이 나을 수도 있음.
            # 일단 ccxt에서 지원하는지 확인 후, 없으면 requests로 구현.
            # 여기서는 간단히 펀딩비와 OI만으로 추세 판단 로직 구현.

            # [해석 로직]
            analysis = "관망"
            bias = "중립"
            
            # OI가 24시간 거래량 대비 너무 크면 변동성 임박
            
            if fund_rate > 0.01 and oi_value > 1_000_000_000: # 펀딩비 높고 OI 높음
                bias = "🔴 롱 과열 (개미 탑승)"
                analysis = "세력이 롱 물량을 개미에게 넘기고 있을 가능성. 폭락 주의."
            
            elif fund_rate < -0.005 and oi_value > 1_000_000_000: # 펀딩비 낮고 OI 높음
                bias = "🟢 숏 축적 (세력 매집)"
                analysis = "세력이 숏을 모으거나, 개미 털기 중. 숏 스퀴즈 급등 가능성."

            return {
                "symbol": symbol,
                "funding_rate": f"{fund_rate*100:.4f}%",
                "oi_usd": f"${oi_value:,.0f}",
                "bias": bias,
                "comment": analysis
            }

        except Exception as e:
            print(f"[FuturesAnalyzer] Error {symbol}: {e}")
            return None

if __name__ == "__main__":
    analyzer = FuturesMetricsAnalyzer()
    print(analyzer.fetch_metrics("BTC/USDT"))