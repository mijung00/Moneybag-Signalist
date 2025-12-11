import ccxt
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

class StrategyLab:
    def __init__(self):
        self.binance = ccxt.binance({
            'apiKey': os.getenv('BINANCE_API_KEY'),
            'secret': os.getenv('BINANCE_SECRET_KEY'),
            'enableRateLimit': True,
            'options': {'defaultType': 'future'} 
        })
        self.target_coins = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT", "DOGE/USDT"]

    def fetch_data_period(self, symbol, start_str, end_str):
        """
        [기간 지정 데이터 수집]
        start_str ~ end_str 사이의 데이터를 모두 긁어옵니다.
        """
        print(f"📥 [{symbol}] 데이터 수집 중 ({start_str} ~ {end_str})", end="")
        
        # 날짜 -> 타임스탬프 변환 (UTC 기준)
        start_dt = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        
        since = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)
        
        all_ohlcv = []
        
        try:
            while True:
                ohlcv = self.binance.fetch_ohlcv(symbol, '1d', since=since, limit=1000)
                if not ohlcv: break
                
                # 범위 체크
                first_ts = ohlcv[0][0]
                last_ts = ohlcv[-1][0]
                
                if first_ts > end_ts: break # 시작부터 범위 밖이면 종료
                
                all_ohlcv.extend(ohlcv)
                
                since = last_ts + 1
                if last_ts >= end_ts: break # 종료일 도달
                
                print(".", end="", flush=True)
                time.sleep(self.binance.rateLimit / 1000)

            print(f" 완료! ({len(all_ohlcv)}봉)")
            
            if not all_ohlcv:
                return pd.DataFrame()

            df = pd.DataFrame(all_ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
            
            # [수정] utc=True 옵션 추가하여 타임존 에러 해결
            df['date'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
            
            # 정확한 기간 필터링
            mask = (df['date'] >= start_dt) & (df['date'] <= end_dt)
            df = df.loc[mask].copy()
            
            return df.reset_index(drop=True)
            
        except Exception as e:
            print(f"\n❌ 수집 실패: {e}")
            return pd.DataFrame()

    def run_simulation(self):
        # [수정] 검증 기간을 오늘까지로 확장
        START_DATE = "2021-01-01"
        today_str = datetime.now().strftime("%Y-%m-%d")
        END_DATE = today_str 
        
        print(f"\n🧪 [웨일 헌터 전략 연구소] 기간 검증 ({START_DATE} ~ {END_DATE}) 가동...\n")
        
        report_card = []
        holding_periods = [3, 5] # 3일, 5일 보유만 검증

        for symbol in self.target_coins:
            df = self.fetch_data_period(symbol, START_DATE, END_DATE)
            if df.empty: continue

            # --- 지표 계산 ---
            # 1. 국면 판단용 (MA200)
            df['ma200'] = df['close'].rolling(window=200).mean()
            
            # 2. 전략용 지표
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
            
            df['vol_ma'] = df['volume'].rolling(20).mean()
            df['vol_ratio'] = df['volume'] / df['vol_ma']
            df['change'] = df['close'].pct_change() * 100

            # --- [전략 정의] ---
            strategies = [
                # 🟢 롱 전략 (상승장)
                ("🩸패닉바잉(RSI<30)", "LONG", lambda r: r['rsi'] < 30),
                ("🚀추세돌파(Vol>2x)", "LONG", lambda r: r['vol_ratio'] > 2.0 and r['change'] > 3.0),
                
                # 🔴 숏 전략 (하락장)
                ("🔥과열숏(RSI>70)", "SHORT", lambda r: r['rsi'] > 70),
                ("📉폭락숏(Vol>2x)", "SHORT", lambda r: r['vol_ratio'] > 2.0 and r['change'] < -3.0)
            ]

            for strat_name, position, condition in strategies:
                # 국면별 테스트
                # 1. 상승장 (Bull): 현재가가 200일선 위
                self._test_strategy(symbol, df, strat_name, position, condition, 5, "🐮상승장", 
                                    lambda r: r['close'] > r['ma200'], report_card)
                # 2. 하락장 (Bear): 현재가가 200일선 아래
                self._test_strategy(symbol, df, strat_name, position, condition, 5, "🐻하락장", 
                                    lambda r: r['close'] < r['ma200'], report_card)

        # --- 결과 출력 ---
        self.print_report(report_card, "🐮상승장")
        self.print_report(report_card, "🐻하락장")

    def _test_strategy(self, coin, df, strat_name, position, condition, days, regime_name, regime_filter, report_card):
        signals = []
        bm_returns = []

        # MA200 계산을 위해 앞 200개 제외, 보유기간 5일 제외
        for i in range(200, len(df) - days):
            row = df.iloc[i]
            
            # 국면 필터 (Regime Filter)
            if not regime_filter(row): continue 

            # 벤치마크 (단순 보유)
            entry = row['close']
            exit = df.iloc[i+days]['close']
            
            # 숏 벤치마크는 역방향 수익률 (공매도 가정)
            bm_ret = (exit - entry) / entry * 100
            if position == "SHORT": bm_ret *= -1
            bm_returns.append(bm_ret)

            # 전략 진입
            if condition(row):
                if position == "LONG":
                    ret = (exit - entry) / entry * 100
                else:
                    ret = (entry - exit) / entry * 100
                signals.append(ret)
        
        if signals:
            win_rate = len([x for x in signals if x > 0]) / len(signals) * 100
            avg_ret = sum(signals) / len(signals)
            bm_ret = sum(bm_returns) / len(bm_returns) if bm_returns else 0.0
            
            report_card.append({
                'coin': coin.split('/')[0],
                'regime': regime_name,
                'strategy': strat_name,
                'position': position,
                'win_rate': win_rate,
                'avg_ret': avg_ret,
                'bm_ret': bm_ret,
                'count': len(signals)
            })

    def print_report(self, report_card, target_regime):
        print("\n" + "="*90)
        print(f"📊 [기간 검증: 21년~24년 {target_regime}] (보유: 5일)")
        print("="*90)
        print(f"{'코인':<6} | {'전략명':<18} | {'포지션':<6} | {'승률':<8} | {'수익(BM)':<12} | {'횟수'}")
        print("-" * 90)
        
        filtered = [r for r in report_card if r['regime'] == target_regime]
        # 수익률 높은 순 정렬
        filtered.sort(key=lambda x: x['avg_ret'], reverse=True)
        
        for row in filtered:
            # 벤치마크보다 평균수익이 높으면 붉은색 강조
            ret_mark = "🔴" if row['avg_ret'] > row['bm_ret'] else "  "
            pos_icon = "🟢" if row['position'] == "LONG" else "🔴"
            
            print(f"{row['coin']:<6} | {row['strategy']:<18} | {pos_icon}{row['position']:<5} | "
                  f"{row['win_rate']:>5.1f}% | {ret_mark}{row['avg_ret']:>5.2f}%({row['bm_ret']:>5.2f}) | {row['count']}회")
        print("-" * 90)

if __name__ == "__main__":
    lab = StrategyLab()
    lab.run_simulation()