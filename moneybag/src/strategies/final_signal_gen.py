import pandas as pd
import numpy as np

def generate_all_strategies(df, regime_info):
    """
    기존 24개 전략 + 신규 야생성 전략 10개 = 총 34개 전략 생성
    (각 전략마다 'action' 필드로 구체적인 진입/청산 가이드 제공)
    """
    strategies = []
    
    if df is None or df.empty:
        return strategies

    # 데이터 준비
    close = df['close']
    high = df['high']
    low = df['low']
    open_ = df['open']
    vol = df['volume']
    
    # 보조지표 계산
    ma_5 = close.rolling(5).mean()
    ma_20 = close.rolling(20).mean()
    ma_60 = close.rolling(60).mean()
    
    rsi_14 = calculate_rsi(close, 14)
    
    # 볼린저 밴드
    std_20 = close.rolling(20).std()
    bb_upper = ma_20 + (std_20 * 2)
    bb_lower = ma_20 - (std_20 * 2)
    bb_width = (bb_upper - bb_lower) / ma_20

    # MACD
    exp12 = close.ewm(span=12, adjust=False).mean()
    exp26 = close.ewm(span=26, adjust=False).mean()
    macd = exp12 - exp26
    signal = macd.ewm(span=9, adjust=False).mean()

    # Stochastic
    low14 = low.rolling(14).min()
    high14 = high.rolling(14).max()
    k_line = 100 * ((close - low14) / (high14 - low14))
    d_line = k_line.rolling(3).mean()
    
    # CCI
    tp = (high + low + close) / 3
    sma_tp = tp.rolling(20).mean()
    mad = (tp - sma_tp).abs().rolling(20).mean()
    cci = (tp - sma_tp) / (0.015 * mad)

    # ATR
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    atr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1).rolling(14).mean()

    # MFI
    money_flow = tp * vol
    pos_flow = money_flow.where(tp > tp.shift(), 0).rolling(14).sum()
    neg_flow = money_flow.where(tp < tp.shift(), 0).rolling(14).sum()
    mfi_ratio = pos_flow / neg_flow
    mfi = 100 - (100 / (1 + mfi_ratio))

    # Williams %R
    wr = (high14 - close) / (high14 - low14) * -100

    current_price = close.iloc[-1]
    prev_close = close.iloc[-2]
    
    # -------------------------------------------------------------------------
    # [Group 1] 기존 BULL 전략 (12종) - Surfer/Sniper용
    # -------------------------------------------------------------------------
    
    # 1. 추세 돌파 (Day)
    vol_ratio = vol.iloc[-1] / vol.rolling(20).mean().iloc[-1]
    change = (close.iloc[-1] - prev_close) / prev_close * 100
    if vol_ratio > 2.0 and change > 3.0:
        strategies.append({
            "name": "Rocket Breakout", 
            "type": "Momentum", 
            "score": 85, 
            "desc": "거래량 2배 실린 급등. 단기 모멘텀이 매우 강함.",
            "action": "진입: 전일 고점 돌파<br>익절: +3~5%<br>손절: -2%"
        })

    # 2. MACD 골든크로스 (Swing)
    if macd.iloc[-1] > signal.iloc[-1] and macd.iloc[-2] <= signal.iloc[-2]:
        strategies.append({
            "name": "MACD Golden Cross", 
            "type": "Trend", 
            "score": 80, 
            "desc": "MACD가 시그널을 상향 돌파. 추세 상승 전환.",
            "action": "진입: 골든크로스 종가<br>익절: MACD 꺾일 때<br>손절: 전저점 이탈"
        })

    # 3. 이평선 정배열 (Swing)
    if ma_5.iloc[-1] > ma_20.iloc[-1] and ma_20.iloc[-1] > ma_60.iloc[-1]:
        strategies.append({
            "name": "Perfect Order (정배열)", 
            "type": "Trend", 
            "score": 75, 
            "desc": "5일>20일>60일 정배열 완성. 안정적인 상승세.",
            "action": "진입: 5일선 지지 시<br>익절: 5일선 이탈<br>손절: 20일선 이탈"
        })

    # 4. RSI 눌림목 (Swing)
    if rsi_14.iloc[-1] < 45 and ma_20.iloc[-1] < current_price: 
        strategies.append({
            "name": "RSI Dip Buy", 
            "type": "Trend", 
            "score": 78, 
            "desc": "상승장 속 일시적 조정. 매수 기회.",
            "action": "진입: RSI 45 이하<br>익절: RSI 70<br>손절: RSI 30 이탈"
        })

    # 5. 스토캐스틱 골든 (Day)
    if k_line.iloc[-1] < 20 and k_line.iloc[-1] > d_line.iloc[-1]:
        strategies.append({
            "name": "Stochastic Golden", 
            "type": "Reversal", 
            "score": 70, 
            "desc": "침체권에서 골든크로스. 단기 반등 신호.",
            "action": "진입: K선 20 상향 돌파<br>익절: K선 80<br>손절: 전저점"
        })

    # 6. 윌리엄스 %R 과매도 탈출 (Day)
    if wr.iloc[-2] < -80 and wr.iloc[-1] > -80:
        strategies.append({
            "name": "Williams %R Breakout", 
            "type": "Momentum", 
            "score": 72, 
            "desc": "과매도 구간 탈출. 매수세 유입 시작.",
            "action": "진입: -80 상향 돌파<br>익절: -20 도달<br>손절: -80 재이탈"
        })

    # 7. 밴드 상단 돌파 (Day)
    if current_price > bb_upper.iloc[-1]:
        strategies.append({
            "name": "Bollinger Breakout", 
            "type": "Momentum", 
            "score": 82, 
            "desc": "볼린저밴드 상단 돌파. 강력한 상승 파동.",
            "action": "진입: 밴드 상단 돌파<br>익절: 밴드 내 복귀<br>손절: 중심선 이탈"
        })

    # 8. ATR 변동성 돌파 (Day)
    if current_price > prev_close + (2 * atr.iloc[-1]):
        strategies.append({
            "name": "ATR Explosion", 
            "type": "Momentum", 
            "score": 88, 
            "desc": "변동성(ATR) 2배 이상의 강력한 상승.",
            "action": "진입: 불타기(시장가)<br>익절: +5% 이상<br>손절: -1ATR"
        })

    # 9. CCI 우물 탈출 (Day)
    if cci.iloc[-1] > -100 and cci.iloc[-2] <= -100:
        strategies.append({
            "name": "CCI Well Escape", 
            "type": "Reversal", 
            "score": 74, 
            "desc": "CCI 침체권 탈출. 반등 초입.",
            "action": "진입: -100 상향 돌파<br>익절: 0선 터치<br>손절: -100 하회"
        })

    # 10. MFI 머니플로우 (Swing)
    if mfi.iloc[-1] < 20:
        strategies.append({
            "name": "MFI Oversold", 
            "type": "Reversal", 
            "score": 76, 
            "desc": "자금 흐름 지표(MFI) 과매도. 저점 매수 기회.",
            "action": "진입: MFI 20 이하<br>익절: MFI 80<br>손절: 전저점"
        })

    # 11. 적삼병 (Swing)
    if (close.iloc[-1] > open_.iloc[-1]) and (close.iloc[-2] > open_.iloc[-2]) and (close.iloc[-3] > open_.iloc[-3]):
        if close.iloc[-1] > close.iloc[-2] > close.iloc[-3]:
            strategies.append({
                "name": "Three White Soldiers", 
                "type": "Trend", 
                "score": 85, 
                "desc": "3일 연속 양봉. 매수세가 시장 장악.",
                "action": "진입: 3일차 종가<br>익절: 5일선 이탈<br>손절: 1일차 시가"
            })

    # 12. 인사이드바 돌파 (Day)
    if high.iloc[-2] > high.iloc[-1] and low.iloc[-2] < low.iloc[-1]: 
        if current_price > high.iloc[-2]:
             strategies.append({
                "name": "Inside Bar Breakout", 
                "type": "Momentum", 
                "score": 80, 
                "desc": "수렴(잉태형) 후 상방 돌파.",
                "action": "진입: 전일 고점 돌파<br>익절: +3%<br>손절: 전일 저점"
             })

    # -------------------------------------------------------------------------
    # [Group 2] 기존 BEAR 전략 (12종) - Hunter/Guardian용
    # -------------------------------------------------------------------------
    
    # 13. 투매 줍기 (역추세)
    if rsi_14.iloc[-1] < 30:
        strategies.append({
            "name": "RSI Panic Buy", 
            "type": "Reversal", 
            "score": 90, 
            "desc": "RSI 30 미만 과매도. 기술적 반등 확률 높음.",
            "action": "진입: RSI 30 터치<br>익절: +3%<br>손절: -5%"
        })

    # 14. CCI 급락 반등
    if cci.iloc[-1] < -150:
         strategies.append({
            "name": "CCI Crash Buy", 
            "type": "Reversal", 
            "score": 85, 
            "desc": "CCI -150 이하 극심한 공포. 과대 낙폭.",
            "action": "진입: -150 하회 분할매수<br>익절: -100 회복<br>손절: 전저점 이탈"
         })

    # 15. 추세 하락 (Short)
    if vol_ratio > 2.0 and change < -3.0:
        strategies.append({
            "name": "Volume Crash (Short)", 
            "type": "Momentum", 
            "score": 85, 
            "desc": "거래량 실린 급락. 추가 하락 가능성 높음.",
            "action": "진입: 반등 시 숏<br>익절: 전저점<br>손절: 당일 고점 돌파"
        })

    # 16. MACD 데드크로스 (Short)
    if macd.iloc[-1] < signal.iloc[-1] and macd.iloc[-2] >= signal.iloc[-2]:
        strategies.append({
            "name": "MACD Dead Cross (Short)", 
            "type": "Trend", 
            "score": 80, 
            "desc": "MACD 하향 이탈. 하락 추세 시작.",
            "action": "진입: 데드크로스 확정<br>익절: MACD 반등<br>손절: 전고점"
        })

    # 17. 이평선 역배열 (Short)
    if ma_5.iloc[-1] < ma_20.iloc[-1] and ma_20.iloc[-1] < ma_60.iloc[-1]:
        strategies.append({
            "name": "Death Cross Order (Short)", 
            "type": "Trend", 
            "score": 75, 
            "desc": "완벽한 역배열 하락세.",
            "action": "진입: 5일선 저항 확인<br>익절: 5일선 돌파<br>손절: 20일선 돌파"
        })

    # 18. 과열 숏 (Short)
    if rsi_14.iloc[-1] > 60 and main_regime_is_bear(ma_20.iloc[-1], ma_60.iloc[-1]): 
        strategies.append({
            "name": "Bear Market Rally (Short)", 
            "type": "Reversal", 
            "score": 78, 
            "desc": "하락장 속 기술적 반등 과열. 다시 하락할 타이밍.",
            "action": "진입: 저항선 근처 숏<br>익절: RSI 40<br>손절: 전고점"
        })
    
    # 19. 스토캐스틱 고점 (Short)
    if k_line.iloc[-1] > 80 and k_line.iloc[-1] < d_line.iloc[-1]:
         strategies.append({
            "name": "Stochastic Overbought (Short)", 
            "type": "Reversal", 
            "score": 70, 
            "desc": "과매수권에서 데드크로스.",
            "action": "진입: 80 하향 이탈<br>익절: 20 도달<br>손절: 80 상향 돌파"
         })

    # 20. 밴드 상단 저항 (Short)
    if high.iloc[-1] >= bb_upper.iloc[-1] and close.iloc[-1] < open_.iloc[-1]: 
        strategies.append({
            "name": "Bollinger Rejection (Short)", 
            "type": "Reversal", 
            "score": 75, 
            "desc": "밴드 상단 터치 후 저항.",
            "action": "진입: 음봉 마감 확인<br>익절: 중심선(20일선)<br>손절: 상단 돌파"
        })

    # 21. ATR 하락 돌파 (Short)
    if current_price < prev_close - (2 * atr.iloc[-1]):
        strategies.append({
            "name": "ATR Crash (Short)", 
            "type": "Momentum", 
            "score": 88, 
            "desc": "변동성 동반한 폭락.",
            "action": "진입: 추격 숏<br>익절: +5%<br>손절: +1ATR"
        })

    # 22. MFI 자금 이탈 (Short)
    if mfi.iloc[-1] > 80:
         strategies.append({
            "name": "MFI Overbought (Short)", 
            "type": "Reversal", 
            "score": 76, 
            "desc": "자금 유입 과다. 조정 임박.",
            "action": "진입: 80 하향 이탈<br>익절: MFI 20<br>손절: 전고점"
         })

    # 23. 흑삼병 (Short)
    if (close.iloc[-1] < open_.iloc[-1]) and (close.iloc[-2] < open_.iloc[-2]) and (close.iloc[-3] < open_.iloc[-3]):
         strategies.append({
            "name": "Three Black Crows (Short)", 
            "type": "Trend", 
            "score": 85, 
            "desc": "3일 연속 음봉. 매도세 장악.",
            "action": "진입: 3일차 종가<br>익절: 5일선 회복<br>손절: 1일차 시가"
         })

    # 24. 인사이드바 하락 (Short)
    if high.iloc[-2] > high.iloc[-1] and low.iloc[-2] < low.iloc[-1]:
        if current_price < low.iloc[-2]:
             strategies.append({
                "name": "Inside Bar Breakdown (Short)", 
                "type": "Momentum", 
                "score": 80, 
                "desc": "수렴 후 하방 이탈.",
                "action": "진입: 전일 저점 이탈<br>익절: +3%<br>손절: 전일 고점"
             })

    # -------------------------------------------------------------------------
    # [Group 3] 신규 야생성 전략 (New 10) - 상황별 특수부대
    # -------------------------------------------------------------------------
    
    # 25. The Wick Hunter (꼬리 낚시)
    body = abs(close - open_)
    lower_wick = df[['open', 'close']].min(axis=1) - low
    if lower_wick.iloc[-1] > (body.iloc[-1] * 2.5) and lower_wick.iloc[-1] > 0:
        strategies.append({
            "name": "The Wick Hunter", 
            "type": "Reversal", 
            "score": 88, 
            "desc": "긴 아래꼬리 발생. 저가 매수세 유입.",
            "action": "진입: 꼬리 중간값 이하<br>익절: 몸통 상단<br>손절: 꼬리 최저점"
        })

    # 26. RSI Divergence Sniper
    if rsi_14.iloc[-1] < 40 and close.iloc[-1] < close.iloc[-5] and rsi_14.iloc[-1] > rsi_14.iloc[-5]:
        strategies.append({
            "name": "RSI Bullish Divergence", 
            "type": "Reversal", 
            "score": 92, 
            "desc": "가격 하락에도 매수 강도 증가. 반전 임박.",
            "action": "진입: 양봉 발생 시<br>익절: RSI 50<br>손절: 전저점"
        })

    # 27. Deep Panic Buy (중복이지만 점수 강화)
    if rsi_14.iloc[-1] < 20:
        strategies.append({
            "name": "Deep Panic Buy (Extreme)", 
            "type": "Reversal", 
            "score": 98, 
            "desc": "RSI 20 미만. 극도로 드문 기회.",
            "action": "진입: 분할 매수 시작<br>익절: RSI 40<br>손절: -10%"
        })

    # 28. Turtle Soup (함정 매매)
    low_20_val = low.rolling(20).min().shift(1).iloc[-1]
    if low.iloc[-1] < low_20_val and close.iloc[-1] > low_20_val:
        strategies.append({
            "name": "Turtle Soup Buy", 
            "type": "Reversal", 
            "score": 89, 
            "desc": "신저가 갱신 실패(휩소). 개미 털기 확인.",
            "action": "진입: 다시 말아올릴 때<br>익절: 이전 고점<br>손절: 신저가"
        })

    # 29. Volatility Breakout (래리 윌리엄스)
    prev_range = high.iloc[-2] - low.iloc[-2]
    breakout_level = open_.iloc[-1] + (prev_range * 0.5)
    if current_price > breakout_level:
        strategies.append({
            "name": "Volatility Breakout (VBO)", 
            "type": "Momentum", 
            "score": 83, 
            "desc": "전일 변동폭의 0.5배 이상 상승 돌파.",
            "action": "진입: 돌파 가격(지정가)<br>익절: 익일 시가<br>손절: 진입가 -2%"
        })

    # 30. Quiet Squeeze (폭발 대기)
    min_width_100 = bb_width.rolling(100).min().iloc[-1]
    if bb_width.iloc[-1] <= min_width_100 * 1.1:
        strategies.append({
            "name": "Quiet Squeeze", 
            "type": "Momentum", 
            "score": 77, 
            "desc": "변동성 극소. 곧 큰 방향성(폭발) 대기.",
            "action": "진입: 박스권 돌파 시<br>익절: 추세 추종<br>손절: 박스권 반대 이탈"
        })

    # 31. Support Level Buy (박스권 하단)
    dist_from_ma20 = abs(current_price - ma_20.iloc[-1]) / ma_20.iloc[-1]
    if dist_from_ma20 < 0.02 and close.iloc[-1] > open_.iloc[-1]:
        strategies.append({
            "name": "Support Level Buy", 
            "type": "Season", 
            "score": 70, 
            "desc": "20일선 지지 테스트 성공. 손익비 좋음.",
            "action": "진입: 20일선 근처<br>익절: 전고점<br>손절: 20일선 이탈"
        })

    # 32. Altcoin Rotation (레짐 연동)
    if regime_info.get('tactical_state') == "Boring_Sideways" and regime_info.get('main_regime') == "Bull":
        strategies.append({
            "name": "Altcoin Rotation Play", 
            "type": "Season", 
            "score": 65, 
            "desc": "비트 횡보 중. 알트 순환매 기대.",
            "action": "진입: 시총 낮은 알트<br>익절: +10% 펌핑 시<br>손절: -5%"
        })

    # 전략이 하나도 없으면 기본 전략
    if not strategies:
        strategies.append({
            "name": "Wait & See", 
            "type": "Neutral", 
            "score": 50, 
            "desc": "뚜렷한 신호 없음. 관망 권장.",
            "action": "진입: 없음<br>익절: -<br>손절: -"
        })

    return strategies

def calculate_rsi(series, period):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def main_regime_is_bear(ma20, ma60):
    return ma20 < ma60