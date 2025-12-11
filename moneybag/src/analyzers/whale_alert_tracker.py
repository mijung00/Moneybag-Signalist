import ccxt
import pandas as pd
import numpy as np

class WhaleAlertTracker:
    def __init__(self):
        # 선물 시장 데이터 사용 (고래들은 선물에서 먼저 움직입니다)
        self.binance = ccxt.binance({'options': {'defaultType': 'future'}})

    def analyze_volume_anomaly(self, symbol="BTC/USDT", timeframe='1h', limit=50):
        """
        [고래 개입 탐지 알고리즘]
        논리: 거래량 폭증(Volume Spike) + 가격 횡보 = 매집(Accumulation)
        """
        try:
            # 캔들 데이터 조회 (Open, High, Low, Close, Volume)
            ohlcv = self.binance.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # 1. 최근 20개 캔들 평균 거래량 계산
            df['vol_ma'] = df['volume'].rolling(window=20).mean()
            
            # 2. 현재 캔들 분석
            last = df.iloc[-1]
            prev_vol_ma = df.iloc[-2]['vol_ma']
            
            # 거래량이 평소보다 몇 배나 터졌는가? (Spike Ratio)
            vol_spike_ratio = last['volume'] / prev_vol_ma if prev_vol_ma > 0 else 0
            
            # 캔들 몸통 크기 (시가 대비 종가 변동폭)
            body_size = abs(last['close'] - last['open']) / last['open'] * 100
            price_change_pct = (last['close'] - last['open']) / last['open'] * 100

            # 3. 시그널 판독
            signal = "N/A"
            comment = "특이사항 없음"

            # 조건: 평소보다 거래량이 2.5배 이상 터졌을 때
            if vol_spike_ratio >= 2.5:
                if body_size < 0.5: 
                    # 케이스 A: 거래량 폭발 + 가격 제자리 (가장 강력한 신호!)
                    signal = "🐳 스텔스 매집 (Whale Accumulation)"
                    comment = f"거래량은 {vol_spike_ratio:.1f}배 터졌는데 가격은 묶여있음. 누군가 물량을 다 받아먹는 중!"
                
                elif price_change_pct > 1.5:
                    # 케이스 B: 거래량 폭발 + 장대 양봉
                    signal = "🚀 상승 발사 (Trend Start)"
                    comment = "강력한 매수세와 함께 추세가 위로 뚫림. 올라타야 할 때."
                
                elif price_change_pct < -1.5:
                    # 케이스 C: 거래량 폭발 + 장대 음봉
                    signal = "🩸 패닉 셀링 (Panic Sell)"
                    comment = "투매가 쏟아지는 중. 바닥인지 지하실인지 확인 필요."

            return {
                "symbol": symbol,
                "vol_spike_ratio": round(vol_spike_ratio, 2),
                "signal": signal,
                "comment": comment
            }

        except Exception as e:
            print(f"Whale Tracker Error ({symbol}): {e}")
            return None

# --- 테스트 실행용 ---
if __name__ == "__main__":
    tracker = WhaleAlertTracker()
    targets = ["BTC/USDT", "ETH/USDT", "DOGE/USDT", "XRP/USDT"]
    
    print("🐳 고래 추적 레이더 가동 중...")
    for t in targets:
        res = tracker.analyze_volume_anomaly(t)
        if res and res['signal'] != "N/A":
            print(f"[{t}] 🚨 {res['signal']} - {res['comment']}")
        else:
            print(f"[{t}] 잠잠함 (거래량 배수: {res['vol_spike_ratio']}배)")