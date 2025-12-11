# iceage/src/pipelines/final_strategy_selector.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd
import numpy as np
import glob
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, date

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from iceage.src.utils.trading_days import (
    TradingCalendar, 
    CalendarConfig, 
    compute_reference_date
)

DATA_DIR = PROJECT_ROOT / "iceage" / "data"
PROCESSED_DIR = DATA_DIR / "processed"

def _normalize_code(x):
    try: return str(int(float(x))).zfill(6)
    except: return str(x).strip().zfill(6)

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

class StrategySelector:
    def __init__(self, ref_date: str):
        self.ref_date = ref_date
        self.target_date = datetime.strptime(ref_date, "%Y-%m-%d").date()
        
    def load_historical_data(self, lookback_days: int = 90) -> pd.DataFrame:
        # 데이터가 있는 날짜까지만 로딩
        all_files = sorted(glob.glob(str(PROCESSED_DIR / "volume_anomaly_v2_*.csv")))
        valid_files = [f for f in all_files if os.path.basename(f).replace("volume_anomaly_v2_", "").replace(".csv", "") <= self.ref_date]
        target_files = valid_files[-lookback_days:]
        
        if not target_files: return pd.DataFrame()
            
        dfs = []
        for f in target_files:
            try:
                df = pd.read_csv(f)
                d_str = os.path.basename(f).replace("volume_anomaly_v2_", "").replace(".csv", "")
                df['date'] = pd.to_datetime(d_str)
                
                # 컬럼 보정
                if 'tv_z' not in df.columns:
                    if 'vol_sigma' in df.columns: df['tv_z'] = df['vol_sigma']
                    else: df['tv_z'] = 0.0
                
                if 'code' in df.columns: df['code'] = df['code'].apply(_normalize_code)
                
                if 'change_rate' in df.columns:
                    df['chg'] = pd.to_numeric(df['change_rate'], errors='coerce')
                else: df['chg'] = 0.0
                
                req_cols = ['date', 'code', 'name', 'close', 'open', 'high', 'low', 'chg', 'tv_z', 'size_bucket']
                avail = [c for c in req_cols if c in df.columns]
                dfs.append(df[avail])
            except: continue
            
        if not dfs: return pd.DataFrame()
        full_df = pd.concat(dfs)
        full_df = full_df.sort_values(['code', 'date']).reset_index(drop=True)
        return full_df

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        # 1. 기본 지표
        df['is_spike'] = (df['tv_z'] >= 2.0).astype(int)
        grouped = df.groupby('code')
        df['spike_count_60d'] = grouped['is_spike'].transform(lambda x: x.rolling(60, min_periods=30).sum())
        df['ma60'] = grouped['close'].transform(lambda x: x.rolling(60, min_periods=40).mean())
        df['price_60d_ago'] = grouped['close'].transform(lambda x: x.shift(60))
        
        df['body'] = df['close'] - df['open']
        df['upper_shadow'] = df['high'] - df[['close', 'open']].max(axis=1)
        df['shadow_ratio'] = df['upper_shadow'] / df['close'] * 100

        # 2. [New] Silent Titan 지표 (Volatility, RSI)
        df['daily_ret'] = grouped['close'].pct_change()
        df['volatility_20'] = grouped['daily_ret'].transform(lambda x: x.rolling(20).std() * 100)
        df['rsi_14'] = grouped['close'].transform(lambda x: calculate_rsi(x, 14))
        
        return df

    def select_targets(self) -> dict:
        full_df = self.load_historical_data(lookback_days=100)
        if full_df.empty: return {}
        
        full_df = self.calculate_indicators(full_df)
        today_df = full_df[full_df['date'].dt.date == self.target_date].copy()
        if today_df.empty: return {}

        # 상한가/하한가 근접(±25% 이상) 종목 원천 배제
        if 'chg' in today_df.columns:
            mask_limit = abs(today_df['chg']) < 25.0
            today_df = today_df[mask_limit].copy()

        selected = {
            "panic_buying": [], 
            "fallen_angel": [], 
            "kings_shadow": [],
            "overheat_short": []
        }
        
        # 1. Panic Buying (Small, Buy)
        mask_pb = (today_df['size_bucket'] == 'small') & (today_df['tv_z'] >= 2.5) & (today_df['chg'] <= -3.0)
        selected["panic_buying"] = today_df[mask_pb].to_dict('records')
        
        # 2. Fallen Angel (Mid, Buy)
        mask_fa = (today_df['size_bucket'] == 'mid') & \
                  (today_df['spike_count_60d'] >= 3) & \
                  (today_df['close'] < today_df['price_60d_ago']) & \
                  ((today_df['chg'] <= -2.0) & (today_df['chg'] >= -10.0)) & \
                  (today_df['tv_z'] >= 1.0)
        selected["fallen_angel"] = today_df[mask_fa].to_dict('records')
        
        # 3. King's Shadow (Large, Buy) -> [Updated] Silent Titan Strategy
        # "침묵의 거인": 변동성 낮고(2.5%↓), 추세 좋고(RSI 60↑), 거래량 안 튀는(1.5σ↓) 종목
        mask_ks = (today_df['size_bucket'] == 'large') & \
                  (today_df['volatility_20'] <= 2.5) & \
                  (today_df['rsi_14'] >= 60) & \
                  (today_df['tv_z'] >= 0.0) & (today_df['tv_z'] <= 1.5) & \
                  (today_df['shadow_ratio'] < 2.0)
                  
        selected["kings_shadow"] = today_df[mask_ks].to_dict('records')

        # 4. Overheat Short (All, Sell)
        mask_oh = (today_df['tv_z'] >= 8.0) & (today_df['chg'] >= 15.0)
        selected["overheat_short"] = today_df[mask_oh].to_dict('records')
        
        # [Safety Check] 패닉바잉이 올랐는데 잡히는 경우 제거
        real_panic = []
        for p in selected["panic_buying"]:
            if float(p.get('chg', 0)) > -2.0:
                print(f"[WARN] 패닉바잉 조건 불일치 제외: {p.get('name')} (chg: {p.get('chg')}%)")
                continue
            real_panic.append(p)
        selected["panic_buying"] = real_panic

        return selected