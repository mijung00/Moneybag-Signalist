# iceage/src/data_sources/kr_price_history.py

from __future__ import annotations

from pathlib import Path
from datetime import date, datetime, timedelta
from typing import List, Optional

import pandas as pd


# 프로젝트 루트 기준으로 data 디렉터리 경로 설정
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_REF_DIR = PROJECT_ROOT / "data" / "reference"


def _date_to_str(d: date) -> str:
    """YYYY-MM-DD 문자열로 변환."""
    return d.strftime("%Y-%m-%d")


def _safe_read_csv(path: Path) -> Optional[pd.DataFrame]:
    """파일이 없으면 None, 있으면 DataFrame."""
    if not path.exists():
        return None
    return pd.read_csv(path)

def _normalize_naver_price_df(df: pd.DataFrame, ref_date: date) -> pd.DataFrame:
    """
    네이버 시가총액 페이지에서 가져온 원본 포맷(한글 컬럼)을
    괴리율 엔진이 기대하는 표준 스키마로 맞춰준다.

    입력 df 예시 컬럼:
      - 종목명, 현재가, 전일비, 등락률, 거래량, 거래대금, market, code

    출력 df는 최소한:
      - trade_date, code, name, close, volume, trading_value, change_rate, change
    """
    df = df.copy()

    # code 6자리 정규화
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(6)

    # 종목명 → name
    if "종목명" in df.columns and "name" not in df.columns:
        df["name"] = df["종목명"].astype(str)

    # 현재가 → close
    if "현재가" in df.columns:
        s = df["현재가"].astype(str)
        s = s.str.replace(",", "", regex=False).str.replace(" ", "", regex=False)
        df["close"] = pd.to_numeric(s, errors="coerce")

    # 거래량 → volume
    if "거래량" in df.columns:
        s = df["거래량"].astype(str)
        s = s.str.replace(",", "", regex=False).str.replace(" ", "", regex=False)
        df["volume"] = pd.to_numeric(s, errors="coerce")

    # 거래대금 → trading_value (있으면 옮겨두고, 나중에 close*volume 로도 보정)
    if "거래대금" in df.columns and "trading_value" not in df.columns:
        s = df["거래대금"].astype(str)
        s = s.str.replace(",", "", regex=False).str.replace(" ", "", regex=False)
        df["trading_value"] = pd.to_numeric(s, errors="coerce")

    # 등락률 → change_rate (% 단위)
    if "등락률" in df.columns and "change_rate" not in df.columns:
        s = df["등락률"].astype(str)
        # 1) 콤마/공백/퍼센트 제거
        s = s.str.replace(",", "", regex=False)
        s = s.str.replace("%", "", regex=False)
        s = s.str.replace(" ", "", regex=False)
        # 2) 숫자/부호/점(.)만 남기고 다 제거 (예: "상승2.31" 같은 경우 방지)
        s = s.str.replace(r"[^0-9\-\+\.]", "", regex=True)
        df["change_rate"] = pd.to_numeric(s, errors="coerce")

    # 전일비 → change (절대가격 변화, 선택적)
    if "전일비" in df.columns and "change" not in df.columns:
        s = df["전일비"].astype(str)
        s = s.str.replace(",", "", regex=False).str.replace(" ", "", regex=False)
        s = s.str.replace(r"[^0-9\-\+\.]", "", regex=True)
        df["change"] = pd.to_numeric(s, errors="coerce")

    # 네이버 데이터에는 trade_date 컬럼이 없으니 ref_date로 채워줌
    df["trade_date"] = ref_date

    return df



def load_daily_prices(ref_date: date) -> pd.DataFrame:
    """
    KRX 기반 일별 시세를 표준 스키마로 읽어온다.
    기대하는 컬럼(최소):
      trade_date, code, name, market, market_name, sector_name,
      close, change, change_rate, open, high, low,
      volume, trading_value, market_cap, listed_shares
    """
    date_str = _date_to_str(ref_date)
    path = DATA_RAW_DIR / f"kr_prices_{date_str}.csv"

    df = _safe_read_csv(path)
    if df is None:
        raise FileNotFoundError(f"일별 시세 파일을 찾을 수 없습니다: {path}")

    # ⚠️ 네이버 폴백 포맷인지 감지 (한글 컬럼들)
    #    예: 종목명, 현재가, 전일비, 등락률, 거래량, 거래대금, market, code
    if "현재가" in df.columns and "close" not in df.columns:
        print("[INFO] 네이버 시세 포맷 감지: 컬럼을 표준 스키마로 변환합니다.")
        df = _normalize_naver_price_df(df, ref_date)

    # trade_date를 datetime.date로 정리
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    else:
        # 혹시 모를 경우 파일명 기준으로 채워 넣기
        df["trade_date"] = ref_date

    # code는 6자리 문자열로 보장
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(6)

    # 1차 숫자 컬럼 정리 (있는 것만)
    numeric_cols = [
        "close",
        "change",
        "change_rate",
        "open",
        "high",
        "low",
        "volume",
        "trading_value",
        "market_cap",
        "listed_shares",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- (신규) 거래대금(trading_value), 시가총액(market_cap) 보정 로직 ---

    # 1) trading_value 없거나 전부 NaN이면 close * volume 으로 계산
    if "trading_value" not in df.columns or df["trading_value"].isna().all():
        if {"close", "volume"} <= set(df.columns):
            close_num = pd.to_numeric(df["close"], errors="coerce")
            vol_num = pd.to_numeric(df["volume"], errors="coerce")
            df["trading_value"] = close_num * vol_num
            print(
                "[INFO] trading_value 컬럼이 없어 close*volume 으로 계산해서 채웠습니다."
            )
        else:
            print(
                "[WARN] trading_value, close, volume 중 일부가 없어 "
                "거래대금을 계산하지 못했습니다."
            )

    # 2) market_cap 없거나 전부 0/NaN 이면 listed_shares로 계산
    need_mcap = (
        "market_cap" not in df.columns
        or df["market_cap"].isna().all()
        or (pd.to_numeric(df["market_cap"], errors="coerce") <= 0).all()
    )

    if need_mcap:
        try:
            # 같은 기준일의 리스팅 파일에서 상장주식수 가져오기
            listing = load_listing(ref_date)
        except FileNotFoundError:
            print(
                "[WARN] 리스팅 파일이 없어 market_cap을 계산하지 못했습니다. "
                "(kr_listing_YYYY-MM-DD.csv 확인 필요)"
            )
        else:
            listing = listing[["code", "listed_shares"]].copy()
            listing["code"] = listing["code"].astype(str).str.zfill(6)

            df = df.merge(listing, on="code", how="left", suffixes=("", "_list"))

            if "listed_shares" in df.columns:
                close_num = pd.to_numeric(df["close"], errors="coerce")
                shares = pd.to_numeric(df["listed_shares"], errors="coerce")
                df["market_cap"] = close_num * shares
                print(
                    "[INFO] market_cap 컬럼이 없어 close*listed_shares 로 계산해서 채웠습니다."
                )
            else:
                print(
                    "[WARN] 리스팅에 listed_shares 가 없어 market_cap을 계산하지 못했습니다."
                )

    return df




def load_listing(ref_date: date) -> pd.DataFrame:
    """
    KRX 종목 기본정보(리스팅)를 읽어온다.
    기대하는 컬럼(최소):
      code, name, abbr, name_eng, isin, market,
      market_type, security_group, sector_name,
      stock_kind, par_value, listed_shares, list_date
    """
    date_str = _date_to_str(ref_date)
    path = DATA_REF_DIR / f"kr_listing_{date_str}.csv"

    df = _safe_read_csv(path)

    # 오늘 기준 리스팅 파일이 없으면, 최근 며칠 안에서 가장 최신 파일로 폴백
    if df is None:
        for back in range(1, 5):  # 최대 4일 전까지 탐색
            cand_date = ref_date - timedelta(days=back)
            cand_str = _date_to_str(cand_date)
            cand_path = DATA_REF_DIR / f"kr_listing_{cand_str}.csv"
            cand_df = _safe_read_csv(cand_path)
            if cand_df is not None:
                print(
                    f"[WARN] 리스팅 파일이 {date_str} 기준으로는 없어서 "
                    f"{cand_str} 기준 리스팅으로 폴백합니다."
                )
                df = cand_df
                break

    if df is None:
        raise FileNotFoundError(f"리스팅 파일을 찾을 수 없습니다: {path}")

    # list_date를 datetime.date로
    if "list_date" in df.columns:
        df["list_date"] = pd.to_datetime(df["list_date"]).dt.date

    # 숫자 컬럼 정리
    numeric_cols = ["par_value", "listed_shares"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # code는 6자리 문자열로 보장
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(6)

    return df



def load_price_history(ref_date: date, window_days: int = 60) -> pd.DataFrame:
    """
    ref_date 기준으로 과거 N일(캘린더 기준) 동안 존재하는
    kr_prices_YYYY-MM-DD.csv 파일을 모두 읽어서 합친다.

    - 실제 '영업일 60개'가 아니라, 캘린더 기준으로 대략 window_days 만큼 뒤로 가며
      존재하는 파일을 모두 합치는 방식.
    - volume_anomaly_v2 등에서 이걸 기반으로 코드를 그룹핑해서 통계 계산.

    반환 컬럼은 load_daily_prices와 동일하며,
    trade_date, code 기준으로 정렬되어 있다.
    """
    dfs: List[pd.DataFrame] = []
    seen_dates: List[date] = []

    # 캘린더 기준으로 window_days + 여유분만큼 뒤로 가며 수집
    # (예: 60일 기준이면 주말/휴일 감안해서 한 80일 정도 루프)
    max_back = window_days + 20
    for i in range(max_back):
        day = ref_date - timedelta(days=i)
        try:
            df_day = load_daily_prices(day)
        except FileNotFoundError:
            continue

        dfs.append(df_day)
        seen_dates.append(day)

        # 실제 수집된 distinct trade_date가 window_days 개수에 도달하면 종료
        if len(set(seen_dates)) >= window_days:
            break

    if not dfs:
        raise FileNotFoundError(
            f"{window_days}일 기준으로 사용할 시세 파일이 하나도 없습니다. "
            f"(ref_date={ref_date})"
        )

    hist = pd.concat(dfs, ignore_index=True)

    # 정렬
    hist = hist.sort_values(["code", "trade_date"]).reset_index(drop=True)

    return hist
