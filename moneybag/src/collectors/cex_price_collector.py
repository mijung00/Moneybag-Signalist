import ccxt
import yfinance as yf
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

class CexPriceCollector:
    def __init__(self):
        self.binance = ccxt.binance({
            'apiKey': os.getenv('BINANCE_API_KEY'),
            'secret': os.getenv('BINANCE_SECRET_KEY'),
            'enableRateLimit': True,
            'options': {'defaultType': 'future'} 
        })
        self.upbit = ccxt.upbit({
            'enableRateLimit': True,
        })

    def get_usd_krw_rate(self) -> float:
        """
        [수정] 실제 환율(USD/KRW) 조회
        김프 계산 시 USDT 가격이 아닌 '찐 환율'을 써야 정확한 김프가 나옵니다.
        """
        try:
            # 야후 파이낸스에서 원달러 환율 조회
            ticker = yf.Ticker("KRW=X")
            rate = ticker.history(period="1d")['Close'].iloc[-1]
            return float(rate)
        except Exception as e:
            print(f"⚠️ 환율 조회 실패 (기본값 1400 사용): {e}")
            return 1400.0

    def fetch_price_data(self, symbol_code: str = "BTC"):
        """
        :param symbol_code: "BTC", "1000PEPE" (선물 티커 기준)
        """
        # 1. 찐 환율 가져오기
        usd_krw = self.get_usd_krw_rate()

        # 2. 바이낸스 시세 & 변동률 (USDT 마켓)
        bin_symbol = f"{symbol_code}/USDT"
        bin_price = 0.0
        change_24h = 0.0
        
        try:
            bin_ticker = self.binance.fetch_ticker(bin_symbol)
            bin_price = float(bin_ticker['last'])
            # [수정] 24시간 변동률 추가
            change_24h = float(bin_ticker['percentage']) 
        except Exception as e:
            # print(f"❌ 바이낸스 조회 실패 ({symbol_code}): {e}")
            pass

        # 3. 업비트 시세 (KRW 마켓)
        # 1000 단위 제거 (1000PEPE -> PEPE)
        upbit_symbol_code = symbol_code.replace("1000", "")
        up_symbol = f"{upbit_symbol_code}/KRW"
        up_price = 0.0
        
        try:
            up_ticker = self.upbit.fetch_ticker(up_symbol)
            up_price = float(up_ticker['last'])
            
            # [보정] 바이낸스가 1000단위면 업비트 가격도 1000배 뻥튀기해야 비교 가능
            if "1000" in symbol_code:
                up_price *= 1000
                
        except Exception as e:
            # print(f"❌ 업비트 조회 실패 ({up_symbol}): {e}")
            pass

        # 4. 진짜 김프 계산
        # 공식: (업비트가 - (바이낸스가 * 환율)) / (바이낸스가 * 환율) * 100
        kimp_pct = 0.0
        if bin_price > 0 and up_price > 0:
            global_price_krw = bin_price * usd_krw
            kimp_pct = ((up_price - global_price_krw) / global_price_krw) * 100

        return {
            "symbol": symbol_code,
            "binance_usdt": bin_price,
            "change_24h": round(change_24h, 2), # [NEW] 변동률
            "upbit_krw": up_price,
            "exchange_rate": usd_krw,
            "kimp_percent": round(kimp_pct, 2), # [NEW] 찐 김프
            "timestamp": datetime.now().isoformat()
        }