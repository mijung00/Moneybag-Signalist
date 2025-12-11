import ccxt
import time
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

class KimpCollector:
    def __init__(self):
        # 시세 조회용이라 API Key 없이도 가능 (Rate Limit 주의)
        self.upbit = ccxt.upbit()
        self.binance = ccxt.binance()

    def get_exchange_rate(self):
        """
        업비트의 USDT/KRW 가격을 실시간 환율로 사용 (가장 정확함)
        """
        try:
            ticker = self.upbit.fetch_ticker("USDT/KRW")
            return float(ticker['last'])
        except Exception as e:
            logger.error(f"환율 조회 실패: {e}")
            return 1400.0  # 비상시 고정 환율 (보수적)

    def get_kimp(self, symbol="BTC"):
        """
        특정 코인의 김치프리미엄 계산
        """
        try:
            # 1. 환율 조회
            exchange_rate = self.get_exchange_rate()

            # 2. 업비트 가격 (KRW)
            upbit_ticker = self.upbit.fetch_ticker(f"{symbol}/KRW")
            upbit_price = float(upbit_ticker['last'])

            # 3. 바이낸스 가격 (USDT)
            binance_ticker = self.binance.fetch_ticker(f"{symbol}/USDT")
            binance_price = float(binance_ticker['last'])

            # 4. 김프 계산
            # (업비트가 - (바이낸스가 * 환율)) / (바이낸스가 * 환율) * 100
            global_price_krw = binance_price * exchange_rate
            kimp_price = upbit_price - global_price_krw
            kimp_percent = (kimp_price / global_price_krw) * 100

            return {
                "symbol": symbol,
                "kimp_percent": round(kimp_percent, 2),
                "upbit_price": upbit_price,
                "binance_price": binance_price,
                "exchange_rate": exchange_rate
            }

        except Exception as e:
            logger.error(f"{symbol} 김프 계산 중 오류: {e}")
            return None

# --- 테스트 실행용 ---
if __name__ == "__main__":
    collector = KimpCollector()
    coins = ["BTC", "ETH", "SOL", "XRP", "DOGE"]
    
    print(f"{'COIN':<6} | {'KIMP(%)':<8} | {'UPBIT(KRW)':<12} | {'BINANCE($)':<10}")
    print("-" * 45)
    
    for coin in coins:
        data = collector.get_kimp(coin)
        if data:
            print(f"{data['symbol']:<6} | {data['kimp_percent']:>6.2f}% | {data['upbit_price']:<12,.0f} | {data['binance_price']:<10.2f}")
        time.sleep(0.1)