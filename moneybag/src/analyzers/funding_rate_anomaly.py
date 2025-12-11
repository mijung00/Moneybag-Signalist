import ccxt
import pandas as pd

class FundingRateAnalyzer:
    def __init__(self):
        # 선물 시장 데이터는 options={'defaultType': 'future'} 필수
        self.binance = ccxt.binance({'options': {'defaultType': 'future'}})

    def analyze(self, symbol="BTC/USDT"):
        try:
            # 펀딩비 조회
            funding_info = self.binance.fetch_funding_rate(symbol)
            current_rate = float(funding_info['fundingRate'])
            
            # 연율 환산 (하루 3회 * 365일)
            annual_rate = current_rate * 3 * 365 * 100
            
            # 상태 진단 로직
            signal = "N/A"
            description = ""

            if current_rate > 0.0005:  # 0.05% 이상 (매우 높음)
                signal = "🔥 과열 (Long High)"
                description = "롱 포지션이 너무 많습니다. 롱 스퀴즈(급락) 주의!"
            elif current_rate > 0.0001: # 0.01% (기본)
                signal = "🟢 정상 (Normal)"
                description = "일반적인 상승 추세 혹은 횡보 중입니다."
            elif current_rate < 0:      # 음수 (숏 우세)
                signal = "🧊 숏 우세 (Short High)"
                description = "숏 포지션이 많습니다. 숏 스퀴즈(급등) 가능성!"
            
            return {
                "symbol": symbol,
                "funding_rate": f"{current_rate:.4%}",
                "annual_rate": f"{annual_rate:.2f}%",
                "signal": signal,
                "description": description
            }

        except Exception as e:
            print(f"데이터 조회 실패: {e}")
            return None

# --- 테스트 실행용 ---
if __name__ == "__main__":
    analyzer = FundingRateAnalyzer()
    targets = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT", "PEPE/USDT"]
    
    print(f"📡 바이낸스 선물 펀딩비 분석")
    print("-" * 60)
    
    for t in targets:
        res = analyzer.analyze(t)
        if res:
            print(f"[{res['symbol']}] {res['funding_rate']} (연 {res['annual_rate']})")
            print(f" └ 상태: {res['signal']}")
            print(f" └ 해설: {res['description']}\n")