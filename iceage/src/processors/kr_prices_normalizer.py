# iceage/src/processors/kr_prices_normalizer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

from iceage.src.data_schemas import KR_PRICE_COLUMNS, validate_kr_price_columns


# 네이버 시세_퀀트 포맷 기준 컬럼 매핑
# 왼쪽: 우리 표준 이름 / 오른쪽: raw csv 컬럼명
COLUMN_MAPPING: Dict[str, str] = {
    "name": "종목명",
    "close": "현재가",
    "change_pct": "등락률",
    "volume": "거래량",
    "turnover": "거래대금",  # 필요시 참고용 (표준 스키마엔 안 써도 됨)
}


def _raw_path(ref_date: date) -> Path:
    # collector가 저장한 경로와 동일해야 함
    return Path("iceage") / "data" / "raw" / f"kr_prices_{ref_date.isoformat()}.csv"


def _processed_path(ref_date: date) -> Path:
    return Path("iceage") / "data" / "processed" / f"kr_prices_{ref_date.isoformat()}.csv"


def _to_number(s: str) -> float:
    """
    '12,345', '+3.21%', '-1.50%' 같은 문자열을 숫자로 변환.
    """
    if pd.isna(s):
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).strip()
    s = s.replace(",", "")
    s = s.replace("%", "")
    # '▲', '▼' 같은 기호가 섞여 있을 수 있으니 숫자,부호,점만 남기자
    filtered = []
    for ch in s:
        if ch.isdigit() or ch in "+-.":
            filtered.append(ch)
    if not filtered:
        return 0.0
    try:
        return float("".join(filtered))
    except ValueError:
        return 0.0


def normalize_kr_prices(ref_date: date) -> Path:
    """
    네이버 raw CSV -> 표준 컬럼 CSV 로 변환.
    - 종목명/현재가/등락률/거래량만 있어도 동작
    - change_pct/prev_close/avg_20d_volume 채워 넣기
    """
    raw_path = _raw_path(ref_date)
    if not raw_path.exists():
        raise FileNotFoundError(f"raw kr prices not found: {raw_path}")

    df_raw = pd.read_csv(raw_path)

    # 1) 컬럼 이름 매핑
    rename_map = {}
    for std_col, raw_col in COLUMN_MAPPING.items():
        if raw_col in df_raw.columns:
            rename_map[raw_col] = std_col

    df = df_raw.rename(columns=rename_map)

    # 2) 최소 필수 컬럼 체크 (name/close/volume)
    if not validate_kr_price_columns(list(df.columns)):
        raise ValueError(
            f"normalized kr prices missing required columns. got: {df.columns.tolist()}"
        )

    # 3) 숫자형 컬럼 파싱
    if "close" in df.columns:
        df["close"] = df["close"].apply(_to_number)
    if "volume" in df.columns:
        df["volume"] = df["volume"].apply(_to_number)
    if "change_pct" in df.columns:
        df["change_pct"] = df["change_pct"].apply(_to_number)

    # 🔹 거래대금(turnover) 숫자화
    if "turnover" in df.columns:
        df["turnover"] = df["turnover"].apply(_to_number)
    else:
        # raw에 거래대금이 따로 없으면 close * volume 으로 근사
        if "close" in df.columns and "volume" in df.columns:
            df["turnover"] = df["close"] * df["volume"]
        else:
            df["turnover"] = 0.0

    # 🔹 거래대금(turnover) 숫자화
    if "turnover" in df.columns:
        df["turnover"] = df["turnover"].apply(_to_number)
    else:
        if "close" in df.columns and "volume" in df.columns:
            df["turnover"] = df["close"] * df["volume"]
        else:
            df["turnover"] = 0.0

    # 🔹 거래대금 상위 1,000개만 유니버스로 사용
    if df["turnover"].gt(0).any():
        df = df.sort_values("turnover", ascending=False)
        df = df.head(1000).copy()


    # 4) date 채우기
    df["date"] = ref_date.isoformat()

    # 5) prev_close / avg_20d_volume 생성
    if "change_pct" in df.columns:
        # close = prev_close * (1 + pct/100)  → prev_close = close / (1+pct/100)
        df["prev_close"] = df["close"] / (1.0 + df["change_pct"] / 100.0)
    else:
        df["change_pct"] = 0.0
        df["prev_close"] = df["close"]

        # avg_20d_volume 은 지금은 데이터 없으니 임시로 volume 사용
    df["avg_20d_volume"] = df["volume"]

    # 🔹 여기서 과거 데이터 기반 vol_sigma 계산해서 붙이기
    df["vol_sigma"] = compute_volume_sigma(ref_date, df)

    # 6) 표준 컬럼 순서 정렬 (있는 것만 사용)
    cols = [c for c in KR_PRICE_COLUMNS if c in df.columns]

    # KR_PRICE_COLUMNS에 vol_sigma가 없더라도, 있으면 맨 뒤에 추가
    extra_cols = [c for c in ["vol_sigma"] if c in df.columns and c not in cols]
    df = df[cols + extra_cols]

    processed_path = _processed_path(ref_date)
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(processed_path, index=False, encoding="utf-8-sig")

    return processed_path



def ensure_normalized(ref_date: date) -> Path:
    """
    processed 파일이 없으면 normalize 실행 후 경로 반환.
    있으면 그대로 경로만 반환.
    """
    processed_path = _processed_path(ref_date)
    if processed_path.exists():
        return processed_path
    return normalize_kr_prices(ref_date)

def _find_col(df: pd.DataFrame, candidates) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def compute_volume_sigma(
    ref_date: date,
    df_today: pd.DataFrame,
    lookback_days: int = 60,
) -> pd.Series:
    """
    ref_date 기준 오늘 시세(df_today)를 입력받아,
    최근 lookback_days일 동안의 거래량 히스토리로부터
    종목별 거래량 z-score(vol_sigma)를 계산한다.

    z = (log(volume_t) - mean(log(volume_{t-N..t-1}))) / std(...)
    """

    # 오늘 데이터에서 코드/거래량 컬럼 찾기
    code_today = _find_col(df_today, ["code", "종목코드", "ticker"])
    vol_today = _find_col(df_today, ["volume", "거래량", "VOL", "vol"])

    if code_today is None or vol_today is None:
        return pd.Series(0.0, index=df_today.index)

    base_dir = Path("iceage") / "data" / "raw"
    history_frames: list[pd.DataFrame] = []

    # 1) 과거 raw 파일들에서 공통 키 "code" / "volume" 형태로 정리
    for i in range(1, lookback_days + 1):
        d = ref_date - timedelta(days=i)
        path = base_dir / f"kr_prices_{d.isoformat()}.csv"
        if not path.exists():
            continue

        try:
            tmp = pd.read_csv(path)
        except Exception:
            continue

        raw_code = _find_col(tmp, ["code", "종목코드", "ticker"])
        raw_vol = _find_col(tmp, ["volume", "거래량", "VOL", "vol"])

        if raw_code is None or raw_vol is None:
            continue

        tmp = tmp[[raw_code, raw_vol]].copy()
        tmp.rename(columns={raw_code: "code", raw_vol: "volume"}, inplace=True)

        tmp["code"] = tmp["code"].astype(str).str.zfill(6)
        tmp["volume"] = (
            tmp["volume"]
            .astype(str)
            .str.replace(",", "", regex=False)
        )
        tmp["volume"] = pd.to_numeric(tmp["volume"], errors="coerce")
        tmp = tmp.dropna(subset=["volume"])
        if tmp.empty:
            continue

        tmp["log_vol"] = np.log1p(tmp["volume"].astype(float))
        history_frames.append(tmp[["code", "log_vol"]])

    if not history_frames:
        # 과거 데이터가 하나도 없으면 0으로
        return pd.Series(0.0, index=df_today.index)

    history = pd.concat(history_frames, ignore_index=True)

    stats = (
        history.groupby("code")["log_vol"]
        .agg(["mean", "std"])
        .rename(columns={"mean": "mu", "std": "sigma"})
        .reset_index()
    )
    stats["sigma"] = stats["sigma"].replace(0, np.nan)

    # 2) 오늘 데이터도 동일한 키 "code"로 맞춰주기
    today = df_today.copy()
    today["code"] = today[code_today].astype(str).str.zfill(6)

    vol_clean = (
        today[vol_today]
        .astype(str)
        .str.replace(",", "", regex=False)
    )
    vol_clean = pd.to_numeric(vol_clean, errors="coerce")
    log_v_today = np.log1p(vol_clean)

    merged = today[["code"]].copy()
    merged["log_v_today"] = log_v_today
    merged = merged.merge(stats, on="code", how="left")

    z = (merged["log_v_today"] - merged["mu"]) / merged["sigma"]
    z = z.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    z.index = df_today.index

    return z
