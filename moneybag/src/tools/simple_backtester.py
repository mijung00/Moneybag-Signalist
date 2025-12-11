import ccxt
import pandas as pd
import numpy as np

class SimpleBacktester:
    def __init__(self):
        self.binance = ccxt.binance()

    # ... (fetch_data, calculate_indicators 메서드는 기존과 100% 동일, 생략 없음) ...
    def fetch_data(self, symbol="BTC/USDT", days=365):
        ohlcv = self.binance.fetch_ohlcv(symbol, '1d', limit=days + 60)
        df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        df['date'] = pd.to_datetime(df['ts'], unit='ms')
        return df

    def calculate_indicators(self, df):
        # (기존 코드 그대로 유지)
        df['change'] = df['c'].pct_change() * 100
        df['vol_ma'] = df['v'].rolling(20).mean()
        df['vol_ratio'] = df['v'] / df['vol_ma']
        df['ma5'] = df['c'].rolling(5).mean()
        df['ma20'] = df['c'].rolling(20).mean()
        df['ma60'] = df['c'].rolling(60).mean()
        df['ma120'] = df['c'].rolling(120).mean()
        delta = df['c'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        std = df['c'].rolling(20).std()
        df['bb_upper'] = df['ma20'] + (2 * std)
        df['bb_lower'] = df['ma20'] - (2 * std)
        exp12 = df['c'].ewm(span=12, adjust=False).mean()
        exp26 = df['c'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp12 - exp26
        df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        low14 = df['l'].rolling(14).min()
        high14 = df['h'].rolling(14).max()
        df['k'] = 100 * ((df['c'] - low14) / (high14 - low14))
        df['d'] = df['k'].rolling(3).mean()
        tp = (df['h'] + df['l'] + df['c']) / 3
        sma_tp = tp.rolling(20).mean()
        mad = (tp - sma_tp).abs().rolling(20).mean()
        df['cci'] = (tp - sma_tp) / (0.015 * mad)
        tr1 = df['h'] - df['l']
        tr2 = (df['h'] - df['c'].shift()).abs()
        tr3 = (df['l'] - df['c'].shift()).abs()
        df['atr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1).rolling(14).mean()
        money_flow = tp * df['v']
        pos_flow = money_flow.where(tp > tp.shift(), 0).rolling(14).sum()
        neg_flow = money_flow.where(tp < tp.shift(), 0).rolling(14).sum()
        mfi_ratio = pos_flow / neg_flow
        df['mfi'] = 100 - (100 / (1 + mfi_ratio))
        df['wr'] = (high14 - df['c']) / (high14 - low14) * -100
        return df

    def run_multi_strategy_test(self, symbol="BTC/USDT", regime="BULL"):
        try:
            df = self.fetch_data(symbol)
            df = self.calculate_indicators(df)
            
            strategies = []
            
            # ... (전략 정의 부분은 기존과 100% 동일, 생략 없음) ...
            # (위에 작성해주신 24개 전략 리스트 그대로 사용)
            if "BULL" in regime:
                strategies.extend([
                    {"name": "🚀 추세 돌파 (Day)", "type": "DAY", "pos": "LONG", "hold": 1, "desc": "거래량 2배+급등", "action": "진입: 전일 고점 돌파<br>익절: +3~5%<br>손절: -2%", "cond": lambda r: r['vol_ratio']>2.0 and r['change']>3.0},
                    {"name": "🌊 MACD 골든크로스", "type": "SWING", "pos": "LONG", "hold": 3, "desc": "MACD 상향 돌파", "action": "진입: 골든크로스 종가<br>익절: MACD 꺾일 때<br>손절: 전저점", "cond": lambda r: r['macd'] > r['signal'] and df.iloc[r.name-1]['macd'] <= df.iloc[r.name-1]['signal']},
                    {"name": "📈 이평선 정배열", "type": "SWING", "pos": "LONG", "hold": 5, "desc": "5>20>60 정배열", "action": "진입: 5일선 위<br>익절: 5일선 이탈<br>손절: 20일선 이탈", "cond": lambda r: r['ma5'] > r['ma20'] and r['ma20'] > r['ma60']},
                    {"name": "🩸 RSI 눌림목", "type": "SWING", "pos": "LONG", "hold": 5, "desc": "RSI<45 조정", "action": "진입: RSI 45 이하<br>익절: RSI 70<br>손절: RSI 30 이탈", "cond": lambda r: r['rsi']<45},
                    {"name": "🌊 스토캐스틱 골든", "type": "DAY", "pos": "LONG", "hold": 2, "desc": "K<20 상향", "action": "진입: K선 20 돌파<br>익절: K선 80<br>손절: 전저점", "cond": lambda r: r['k']<20 and r['k'] > r['d']},
                    {"name": "📉 윌리엄스 %R", "type": "DAY", "pos": "LONG", "hold": 1, "desc": "과매도 탈출", "action": "진입: -80 상향 돌파<br>익절: -20 도달<br>손절: -80 하회", "cond": lambda r: r['wr'] < -80},
                    {"name": "⚡ 밴드 상단 돌파", "type": "DAY", "pos": "LONG", "hold": 1, "desc": "밴드 상단 돌파", "action": "진입: 밴드 상단 돌파<br>익절: 밴드 복귀<br>손절: 중심선 이탈", "cond": lambda r: r['c']>r['bb_upper']},
                    {"name": "💥 ATR 변동성 돌파", "type": "DAY", "pos": "LONG", "hold": 1, "desc": "2ATR 상승", "action": "진입: 불타기<br>익절: +5%<br>손절: -1ATR", "cond": lambda r: r['c'] > df.iloc[r.name-1]['c'] + (2 * r['atr'])},
                    {"name": "🚀 CCI 우물 탈출", "type": "DAY", "pos": "LONG", "hold": 2, "desc": "CCI -100 돌파", "action": "진입: -100 상향 돌파<br>익절: 0선<br>손절: -100 하회", "cond": lambda r: r['cci'] > -100 and df.iloc[r.name-1]['cci'] <= -100},
                    {"name": "💰 MFI 머니플로우", "type": "SWING", "pos": "LONG", "hold": 4, "desc": "MFI<20 과매도", "action": "진입: MFI 20 이하<br>익절: MFI 80<br>손절: 전저점", "cond": lambda r: r['mfi'] < 20},
                    {"name": "🕯️ 적삼병 (3 Soldiers)", "type": "SWING", "pos": "LONG", "hold": 3, "desc": "3일 연속 양봉", "action": "진입: 3일차 종가<br>익절: 5일선 이탈<br>손절: 1일차 시가", "cond": lambda r: r['c'] > df.iloc[r.name-1]['c'] and df.iloc[r.name-1]['c'] > df.iloc[r.name-2]['c']},
                    {"name": "🧘 인사이드바 돌파", "type": "DAY", "pos": "LONG", "hold": 1, "desc": "수렴 후 돌파", "action": "진입: 전일 고점 돌파<br>익절: +3%<br>손절: 전일 저점", "cond": lambda r: df.iloc[r.name-1]['h'] > r['h'] and df.iloc[r.name-1]['l'] < r['l'] and r['c'] > df.iloc[r.name-1]['h']}
                ])
            else: # BEAR
                strategies.extend([
                    {"name": "📉 투매 줍기 (역추세)", "type": "DAY", "pos": "LONG", "hold": 1, "desc": "RSI<30 반등", "action": "진입: RSI 30 터치<br>익절: +2~3%<br>손절: -5%", "cond": lambda r: r['rsi']<30},
                    {"name": "🌊 CCI 급락 반등", "type": "DAY", "pos": "LONG", "hold": 1, "desc": "CCI -150 공포", "action": "진입: -150 하회<br>익절: -100 회복<br>손절: 전저점", "cond": lambda r: r['cci'] < -150},
                    {"name": "🔨 추세 하락 (Short)", "type": "SWING", "pos": "SHORT", "hold": 3, "desc": "거래량 실린 하락", "action": "진입: 반등 시 숏<br>익절: 전저점<br>손절: 고점 돌파", "cond": lambda r: r['vol_ratio']>2.0 and r['change']<-3.0},
                    {"name": "📉 MACD 데드크로스", "type": "SWING", "pos": "SHORT", "hold": 3, "desc": "MACD 하향 이탈", "action": "진입: 데드크로스<br>익절: MACD 반등<br>손절: 전고점", "cond": lambda r: r['macd'] < r['signal'] and df.iloc[r.name-1]['macd'] >= df.iloc[r.name-1]['signal']},
                    {"name": "📉 이평선 역배열", "type": "SWING", "pos": "SHORT", "hold": 5, "desc": "역배열 완성", "action": "진입: 5일선 저항<br>익절: 5일선 돌파<br>손절: 20일선 돌파", "cond": lambda r: r['ma5'] < r['ma20'] and r['ma20'] < r['ma60']},
                    {"name": "🚫 과열 숏 (Day)", "type": "DAY", "pos": "SHORT", "hold": 1, "desc": "RSI>60 반등", "action": "진입: 저항선 근처<br>익절: RSI 40<br>손절: 전고점", "cond": lambda r: r['rsi'] > 60},
                    {"name": "🔥 스토캐스틱 고점", "type": "DAY", "pos": "SHORT", "hold": 2, "desc": "K>80 하향", "action": "진입: 80 하향 이탈<br>익절: 20 도달<br>손절: 80 상향", "cond": lambda r: r['k']>80 and r['k'] < r['d']},
                    {"name": "⚡ 밴드 상단 저항", "type": "DAY", "pos": "SHORT", "hold": 1, "desc": "상단 터치 후 음봉", "action": "진입: 음봉 마감 시<br>익절: 중심선<br>손절: 상단 돌파", "cond": lambda r: r['h'] >= r['bb_upper'] and r['c'] < r['o']},
                    {"name": "💥 ATR 하락 돌파", "type": "DAY", "pos": "SHORT", "hold": 1, "desc": "2ATR 하락", "action": "진입: 추격 숏<br>익절: +5%<br>손절: +1ATR", "cond": lambda r: r['c'] < df.iloc[r.name-1]['c'] - (2 * r['atr'])},
                    {"name": "💰 MFI 자금 이탈", "type": "SWING", "pos": "SHORT", "hold": 3, "desc": "MFI>80 하락", "action": "진입: 80 하향 이탈<br>익절: MFI 20<br>손절: 전고점", "cond": lambda r: r['mfi'] > 80},
                    {"name": "🕯️ 흑삼병 (3 Crows)", "type": "SWING", "pos": "SHORT", "hold": 3, "desc": "3일 연속 음봉", "action": "진입: 3일차 종가<br>익절: 5일선 회복<br>손절: 1일차 시가", "cond": lambda r: r['c'] < df.iloc[r.name-1]['c'] and df.iloc[r.name-1]['c'] < df.iloc[r.name-2]['c']},
                    {"name": "🧘 인사이드바 하락", "type": "DAY", "pos": "SHORT", "hold": 1, "desc": "수렴 후 하락", "action": "진입: 전일 저점 이탈<br>익절: +3%<br>손절: 전일 고점", "cond": lambda r: df.iloc[r.name-1]['h'] > r['h'] and df.iloc[r.name-1]['l'] < r['l'] and r['c'] < df.iloc[r.name-1]['l']}
                ])

            # --- 시뮬레이션 실행 ---
            results_list = []
            for strat in strategies:
                signals = []
                hold_day = strat['hold']
                for i in range(60, len(df) - hold_day):
                    row = df.iloc[i]
                    if strat["cond"](row):
                        entry = row['c']
                        exit = df.iloc[i+hold_day]['c']
                        roi = (exit - entry) / entry * 100
                        if strat["pos"] == "SHORT": roi *= -1
                        signals.append(roi)
                
                if signals:
                    win_rate = len([x for x in signals if x > 0]) / len(signals) * 100
                    avg_ret = sum(signals) / len(signals)
                    count = len(signals)
                    
                    # [필터링] 
                    if win_rate < 40.0: continue # 승률 너무 낮으면 탈락
                    if count < 3: continue # 표본 너무 적으면 탈락
                    
                    # 점수 계산
                    score = win_rate + (avg_ret * 5)
                    
                    results_list.append({
                        "name": strat['name'], "type": strat['type'], "pos": strat['pos'],
                        "win": win_rate, "ret": avg_ret, "score": score, "action": strat['action'], "count": count
                    })

            # [핵심 수정] Top Picks를 Day와 Swing으로 분리하여 선발 (쿼터제)
            results_list.sort(key=lambda x: x['score'], reverse=True)
            
            # Day Top 3
            day_picks = [r for r in results_list if r['type'] == 'DAY'][:3]
            
            # Swing Top 3
            swing_picks = [r for r in results_list if r['type'] == 'SWING'][:3]
            
            # 전체 Top 1 (Best Strategy) 선정
            if results_list:
                best = results_list[0]
                best_strat_name = best['name']
                best_strat_pos = best['pos']
                best_text = f"{best_strat_name} ({best_strat_pos})"
            else:
                best_text = "관망 (Wait)"
                best_strat_name = "관망"
                best_strat_pos = "NEUTRAL"

            # 테이블 생성 함수
            def make_table(picks):
                if not picks:
                    return "| - | - | - | - | - | 조건에 맞는 전략 없음 |\n"
                
                rows = "| 전략명 | 포지션 | 승률 | 평균수익 | 횟수 | 실전 액션 가이드 |\n|---|---|---|---|---|---|\n"
                for res in picks:
                    win_icon = "🔴" if res['win'] >= 60 else ("🟡" if res['win'] >= 50 else "🔹")
                    ret_icon = "🔴" if res['ret'] > 0 else "🔹"
                    pos_str = "🟢 롱(매수)" if res['pos'] == "LONG" else "🔴 숏(매도)"
                    action_fmt = res['action']
                    
                    rows += f"| {res['name']} | {pos_str} | {win_icon}{res['win']:.0f}% | {ret_icon}{res['ret']:+.1f}% | {res['count']}회 | {action_fmt} |\n"
                return rows

            day_table = make_table(day_picks)
            swing_table = make_table(swing_picks)

            final_report = f"""
            **[⚡ 데이트레이딩 (1일 보유) Top Picks]**
            {day_table}
            
            **[🌊 스윙 전략 (3~5일 보유) Top Picks]**
            {swing_table}
            """
            
            analysis_text = f"총 24개 전략 중 **[{best_strat_name}]** 전략이 가장 우수한 성과(승률 {best.get('win',0):.0f}%)를 보였다."
            
            return final_report, analysis_text, best_text

        except Exception as e:
            return f"Error: {e}", "", "NEUTRAL"