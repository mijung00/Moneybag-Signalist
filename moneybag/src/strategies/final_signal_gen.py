import pandas as pd
import numpy as np
from moneybag.src.tools.simple_backtester import SimpleBacktester

def generate_all_strategies(df, regime_info):
    """
    기존의 강력한 백테스팅 로직을 유지하면서,
    34개 신규/기존 전략을 모두 통합하여 검증된 결과를 반환합니다.
    (Fix: 스칼라 변수를 Series로 변경하여 백테스트 오류 해결)
    """
    strategies = []
    
    # 데이터가 없으면 빈 리스트 반환
    if df is None or df.empty:
        return strategies

    # -------------------------------------------------------------------------
    # 1. 보조지표 계산 (전체 히스토리 Series)
    # -------------------------------------------------------------------------
    close = df['close']
    high = df['high']
    low = df['low']
    open_ = df['open']
    vol = df['volume']

    # 이평선
    ma_5 = close.rolling(5).mean()
    ma_20 = close.rolling(20).mean()
    ma_60 = close.rolling(60).mean()
    
    # RSI
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi_14 = 100 - (100 / (1 + rs))
    
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

    # Stochastic & Williams %R
    low14 = low.rolling(14).min()
    high14 = high.rolling(14).max()
    k_line = 100 * ((close - low14) / (high14 - low14))
    d_line = k_line.rolling(3).mean()
    wr = (high14 - close) / (high14 - low14) * -100

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

    # [수정 포인트] 아래 변수들을 스칼라가 아닌 Series로 정의해야 백테스트가 가능함
    # -----------------------------------------------------------------------
    # 기존 오류 코드: vol_ratio = vol.iloc[-1] / ... (X)
    # 수정된 코드: vol_ratio = vol / ... (O)
    vol_ratio = vol / vol.rolling(20).mean()
    
    # 등락률 Series
    change_pct = close.pct_change() * 100
    
    # 전일 종가 Series (shift 1)
    prev_close_series = close.shift(1)

    # -------------------------------------------------------------------------
    # 2. 실전 백테스팅 엔진 (검증 로직)
    # -------------------------------------------------------------------------
    def run_backtest(condition_series, hold_days=3):
        """
        과거 365일 데이터에서 해당 조건이 발생했을 때의 성과를 검증
        """
        lookback = 730
        # 조건 시리즈가 Series인지 확인 (안전장치)
        if not isinstance(condition_series, (pd.Series, np.ndarray)):
            return 0, 0.0, 0

        if len(condition_series) > lookback:
            subset = condition_series.iloc[-lookback:]
        else:
            subset = condition_series
        
        # True인 지점(날짜 인덱스) 찾기
        try:
            entry_indices = subset[subset].index
        except:
            return 0, 0.0, 0
        
        wins = 0
        total_trades = 0
        total_return = 0.0
        
        for idx in entry_indices:
            # 미래 데이터가 없으면 패스 (오늘 발생한 신호 포함)
            # idx는 정수형 위치 인덱스가 아니라 라벨 인덱스일 수 있음. 
            # 안전하게 정수 위치로 변환하여 계산
            try:
                # pandas Index.get_loc 등을 써야 하지만, 여기선 iloc 슬라이싱으로 처리된 subset이므로
                # 원본 df에서의 정수 위치를 찾는 게 정확함.
                # 편의상 index가 datetime이면 로직이 복잡해지므로, 
                # 위에서 subset을 만들지 않고 전체 df 기준으로 loop 도는 게 안전함.
                pass 
            except: continue

        # [Re-implementation for Robustness]
        # 위 방식 대신 단순 for loop가 더 안전함 (벡터화는 condition 생성에서 이미 됨)
        # ------------------------------------------------
        
        # 최근 180일 ~ 어제까지 루프 (오늘은 미래 수익률을 모르므로 제외)
        start_idx = max(0, len(df) - lookback)
        end_idx = len(df) - hold_days 
        
        wins = 0
        total_trades = 0
        total_return = 0.0

        for i in range(start_idx, end_idx):
            if condition_series.iloc[i]: # 조건 만족 시
                entry_price = close.iloc[i]
                exit_price = close.iloc[i + hold_days]
                
                ret = (exit_price - entry_price) / entry_price * 100
                
                if ret > 0: wins += 1
                total_return += ret
                total_trades += 1

        if total_trades == 0:
            return 0, 0.0, 0 
            
        win_rate = (wins / total_trades) * 100
        avg_ret = total_return / total_trades
        return win_rate, avg_ret, total_trades

    # -------------------------------------------------------------------------
    # 3. 전략 정의 (34개 풀세트) - 모든 변수는 Series여야 함
    # -------------------------------------------------------------------------
    
    definitions = [
        # [Group 1] Momentum / Breakout
        (
            (vol_ratio > 2.0) & (change_pct > 3.0),
            "Rocket Breakout", "Momentum", 1,
            "거래량 2배 실린 급등. 단기 모멘텀 강세.", 
            "진입: 고점 돌파\n익절: +3~5%\n손절: -2%"
        ),
        (
            (close > open_ + (high.shift(1) - low.shift(1)) * 0.5),
            "Volatility Breakout", "Momentum", 1,
            "전일 변동폭의 0.5배 이상 상승 돌파.", 
            "진입: 돌파가(지정가)\n익절: 시가 청산\n손절: -2%"
        ),
        (
            (bb_width <= bb_width.rolling(100).min() * 1.1),
            "Quiet Squeeze", "Momentum", 3,
            "변동성 극소(스퀴즈). 곧 폭발 임박.", 
            "진입: 박스권 돌파시\n익절: 추세 추종\n손절: 박스권 이탈"
        ),
        (
            (close > prev_close_series + (2 * atr)),
            "ATR Explosion", "Momentum", 1,
            "변동성(ATR) 2배 이상의 강력한 상승.", 
            "진입: 불타기\n익절: +5% 이상\n손절: -1ATR"
        ),
        (
            (close > bb_upper),
            "Bollinger Breakout", "Momentum", 1,
            "볼린저밴드 상단 돌파. 강력한 파동.", 
            "진입: 상단 돌파\n익절: 밴드 복귀시\n손절: 중심선 이탈"
        ),
        (
            (high.shift(2) > high.shift(1)) & (low.shift(2) < low.shift(1)) & (close > high.shift(2)),
            "Inside Bar Breakout", "Momentum", 1,
            "수렴(잉태형) 후 상방 돌파.", 
            "진입: 전일 고점 돌파\n익절: +3%\n손절: 전일 저점"
        ),
        (
            (wr.shift(1) < -80) & (wr > -80),
            "Williams %R Breakout", "Momentum", 1,
            "과매도 구간 탈출. 매수세 유입.", 
            "진입: -80 상향 돌파\n익절: -20 도달\n손절: -80 재이탈"
        ),

        # [Group 2] Trend Following
        (
            (macd > signal) & (macd.shift(1) <= signal.shift(1)),
            "MACD Golden Cross", "Trend", 3,
            "MACD 시그널 상향 돌파. 추세 전환.", 
            "진입: 종가\n익절: MACD 꺾임\n손절: 전저점"
        ),
        (
            (ma_5 > ma_20) & (ma_20 > ma_60),
            "Perfect Order", "Trend", 5,
            "5>20>60 정배열. 안정적 상승세.", 
            "진입: 5일선 지지\n익절: 5일선 이탈\n손절: 20일선 이탈"
        ),
        (
            (rsi_14 < 45) & (ma_20 < close),
            "RSI Dip Buy", "Trend", 3,
            "상승장 속 일시적 조정(눌림목).", 
            "진입: RSI 45 이하\n익절: RSI 70\n손절: RSI 30 이탈"
        ),
        (
            (close > open_) & (close.shift(1) > open_.shift(1)) & (close.shift(2) > open_.shift(2)),
            "Three White Soldiers", "Trend", 3,
            "3일 연속 양봉. 매수세 장악.", 
            "진입: 3일차 종가\n익절: 5일선 이탈\n손절: 1일차 시가"
        ),

        # [Group 3] Reversal (역추세/저점매수)
        (
            (rsi_14 < 30),
            "RSI Panic Buy", "Reversal", 3,
            "RSI 30 미만 과매도. 기술적 반등.", 
            "진입: 분할 매수\n익절: RSI 40\n손절: -5%"
        ),
        (
            (rsi_14 < 20),
            "Deep Panic Buy", "Reversal", 3,
            "RSI 20 미만 극심한 공포. 과대 낙폭.", 
            "진입: 적극 매수\n익절: RSI 40\n손절: -10%"
        ),
        (
            ((df[['open', 'close']].min(axis=1) - low) > abs(close - open_) * 2.5) & (close > open_),
            "The Wick Hunter", "Reversal", 2,
            "긴 아래꼬리 발생. 저가 매수세 유입.", 
            "진입: 꼬리 중간값\n익절: 몸통 상단\n손절: 최저점"
        ),
        (
            (cci < -150),
            "CCI Crash Buy", "Reversal", 2,
            "CCI -150 이하 투매. 과매도.", 
            "진입: -150 하회\n익절: -100 회복\n손절: 전저점"
        ),
        (
            (low < low.rolling(20).min().shift(1)) & (close > low.rolling(20).min().shift(1)),
            "Turtle Soup Buy", "Reversal", 2,
            "신저가 갱신 실패(휩소). 반전 신호.", 
            "진입: 말아올릴 때\n익절: 전고점\n손절: 신저가"
        ),
        (
            (mfi < 20),
            "MFI Oversold", "Reversal", 3,
            "자금 흐름 지표(MFI) 과매도.", 
            "진입: MFI 20 이하\n익절: MFI 80\n손절: 전저점"
        ),
        (
            (k_line < 20) & (k_line > d_line),
            "Stochastic Golden", "Reversal", 2,
            "침체권에서 골든크로스. 반등 신호.", 
            "진입: K선 20 상향\n익절: K선 80\n손절: 전저점"
        ),
        (
            (rsi_14 < 40) & (close < close.shift(5)) & (rsi_14 > rsi_14.shift(5)),
            "RSI Bullish Divergence", "Reversal", 3,
            "가격 하락에도 매수 강도 증가(다이버전스).", 
            "진입: 양봉 발생시\n익절: RSI 50\n손절: 전저점"
        ),
        (
            (cci > -100) & (cci.shift(1) <= -100),
            "CCI Well Escape", "Reversal", 2,
            "CCI 침체권 탈출. 반등 초입.", 
            "진입: -100 상향\n익절: 0선 터치\n손절: -100 하회"
        ),
        
        # [Group 4] Short / Bear (하락장용)
        (
            (rsi_14 > 70),
            "RSI Overbought (Short)", "Reversal", 2,
            "RSI 70 이상 과열. 조정 임박.", 
            "진입: 70 하향 이탈\n익절: 50\n손절: 전고점"
        ),
        (
            (ma_5 < ma_20) & (ma_20 < ma_60),
            "Death Cross (Short)", "Trend", 5,
            "역배열 하락 추세.", 
            "진입: 5일선 저항\n익절: 5일선 돌파\n손절: 20일선 돌파"
        ),
        (
            (high >= bb_upper) & (close < open_),
            "Bollinger Rejection (Short)", "Reversal", 2,
            "밴드 상단 터치 후 저항(음봉).", 
            "진입: 음봉 마감\n익절: 중심선\n손절: 상단 돌파"
        ),
        (
            (macd < signal) & (macd.shift(1) >= signal.shift(1)),
            "MACD Dead Cross (Short)", "Trend", 3,
            "MACD 하향 이탈. 하락 시작.", 
            "진입: 데드크로스\n익절: 반등시\n손절: 전고점"
        ),
        (
            (mfi > 80),
            "MFI Overbought (Short)", "Reversal", 3,
            "자금 유입 과다. 조정 가능성.", 
            "진입: 80 하향 이탈\n익절: MFI 20\n손절: 전고점"
        ),
        (
            (close < prev_close_series - (2 * atr)),
            "ATR Crash (Short)", "Momentum", 1,
            "변동성 동반한 폭락.", 
            "진입: 추격 숏\n익절: +5%\n손절: +1ATR"
        ),
        (
            (k_line > 80) & (k_line < d_line),
            "Stochastic Overbought (Short)", "Reversal", 2,
            "과매수권 데드크로스.", 
            "진입: 80 하향\n익절: 20 도달\n손절: 80 상향"
        ),
        (
            (vol_ratio > 2.0) & (change_pct < -3.0),
            "Volume Crash (Short)", "Momentum", 1,
            "거래량 실린 급락.", 
            "진입: 반등시 숏\n익절: 전저점\n손절: 당일 고점"
        ),
        (
            (close < open_) & (close.shift(1) < open_.shift(1)) & (close.shift(2) < open_.shift(2)),
            "Three Black Crows (Short)", "Trend", 3,
            "3일 연속 음봉. 매도세 장악.", 
            "진입: 3일차 종가\n익절: 5일선 회복\n손절: 1일차 시가"
        ),
        (
            (high.shift(2) > high.shift(1)) & (low.shift(2) < low.shift(1)) & (close < low.shift(2)),
            "Inside Bar Breakdown (Short)", "Momentum", 1,
            "수렴 후 하방 이탈.", 
            "진입: 전일 저점 이탈\n익절: +3%\n손절: 전일 고점"
        ),
        (
            (rsi_14 > 60) & (ma_20 < ma_60),
            "Bear Market Rally (Short)", "Reversal", 2,
            "하락장 속 과도한 반등(과열).", 
            "진입: 저항선 근처\n익절: RSI 40\n손절: 전고점"
        ),
        (
            (cci < -150), 
            "CCI Crash (Short Cover)", "Reversal", 1,
            "과매도권 도달. 숏 포지션 청산.", 
            "진입: 청산(매수)\n익절: -\n손절: -"
        )
    ]

    # -------------------------------------------------------------------------
    # 4. 전략 검증 및 결과 생성
    # -------------------------------------------------------------------------
    for cond_series, name, type_, hold, desc, action in definitions:
        # (1) 오늘 신호 여부 (여기서 iloc[-1]을 호출해도 안전함, cond_series가 Series이므로)
        try:
            is_triggered = cond_series.iloc[-1]
        except:
            is_triggered = False
        
        # (2) 과거 데이터 백테스트
        win_rate, avg_ret, count = run_backtest(cond_series, hold_days=hold)
        
        # (3) 점수 산정
        base_score = 50
        if count < 3:
            score = 40 
        else:
            score = base_score + (win_rate - 50) + (avg_ret * 3)
            
        score = min(99, max(1, int(score)))

        # 오늘 신호가 떴다면 결과에 추가
        if is_triggered:
            validated_desc = f"{desc} (검증: 승률 {win_rate:.0f}%, 평균 {avg_ret:+.1f}%)"
            
            strategies.append({
                "name": name,
                "type": type_,
                "score": score,
                "desc": validated_desc,
                "action": action,
                "win": f"{win_rate:.0f}%",
                "ret": f"{avg_ret:+.1f}%",
                "count": count
            })

    if not strategies:
        strategies.append({
            "name": "Wait & See", 
            "type": "Neutral", 
            "score": 50, 
            "desc": "뚜렷한 신호 없음. 관망 권장.",
            "action": "진입: -\n익절: -\n손절: -",
            "win": "-", "ret": "-", "count": 0
        })

    return strategies

def calculate_rsi(series, period):
    pass