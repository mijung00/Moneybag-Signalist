# iceage/src/collectors/krx_daily_price_collector.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
import requests
# [수정] 모든 환경 설정은 common.config가 책임집니다.
import common.config

# 기존 PROJECT_ROOT는 'iceage' 디렉터리 기준으로 그대로 둬도 됨
PROJECT_ROOT = Path(__file__).resolve().parents[2]   # ...\iceage
REPO_ROOT = PROJECT_ROOT.parent                      # ...\project  ✅

DATA_DIR = PROJECT_ROOT / "data" / "raw"



KOSPI_URL = os.getenv(
    "KRX_STK_BYDD_TRD_URL",
    "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd",
)
KOSDAQ_URL = os.getenv(
    "KRX_KSQ_BYDD_TRD_URL",
    "https://data-dbg.krx.co.kr/svc/apis/sto/ksq_bydd_trd",
)


def _ensure_auth():
    """ConfigLoader를 통해 KRX 인증 키를 안전하게 가져와서 반환합니다."""
    key = os.getenv("KRX_AUTH_KEY")
    # 값이 없거나, ARN 형태일 경우 Secrets Manager에서 가져옵니다.
    if not key or key.startswith("arn:aws:secretsmanager"):
        key = config.ensure_secret("KRX_AUTH_KEY")
    if not key:
        raise RuntimeError("KRX_AUTH_KEY가 설정되어 있지 않습니다.")
    return key


def _parse_number(val: str | float | int | None) -> float | None:
    """
    KRX 응답은 '-', ',' 가 섞인 문자열인 경우가 많아서
    숫자로 변환해 주는 헬퍼.
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)

    s = str(val).strip()
    if s in ("", "-", "NaN", "nan", "null"):
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _fetch_market(
    date_str: str,
    market: Literal["KOSPI", "KOSDAQ"],
) -> pd.DataFrame:
    """
    특정 기준일 / 특정 시장(KOSPI / KOSDAQ)의 일별매매정보를 가져와서 DataFrame 으로 반환.
    """
    auth_key = _ensure_auth()

    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"날짜 형식이 잘못되었습니다: {date_str} (YYYY-MM-DD)")

    bas_dd = d.strftime("%Y%m%d")

    if market == "KOSPI":
        url = KOSPI_URL
    else:
        url = KOSDAQ_URL

    headers = {
        "AUTH_KEY": auth_key,
    }
    params = {
        "basDd": bas_dd,
    }

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = data.get("OutBlock_1", [])
    if not rows:
        print(f"[WARN] {date_str} {market} 기준 데이터가 없습니다.")
        return pd.DataFrame()

    raw_df = pd.DataFrame(rows)

    # 공통 컬럼들 스펙 기준으로 rename 
    rename_map = {
        "BAS_DD": "trade_date",
        "ISU_CD": "isu_cd",        # 표준코드 (12자리)
        "ISU_NM": "name",
        "MKT_NM": "market_name",
        "SECT_TP_NM": "sector_name",
        "TDD_CLSPRC": "close",
        "CMPPREVDD_PRC": "change",
        "FLUC_RT": "change_rate",
        "TDD_OPNPRC": "open",
        "TDD_HGPRC": "high",
        "TDD_LWPRC": "low",
        "ACC_TRDVOL": "volume",
        "ACC_TRDVAL": "trading_value",
        "MKTCAP": "market_cap",
        "LIST_SHRS": "listed_shares",
    }
    df = raw_df.rename(columns=rename_map)

    df["source_market"] = market  # 'KOSPI' / 'KOSDAQ'

    # 숫자 컬럼 변환
    num_cols = [
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
    for col in num_cols:
        if col in df.columns:
            df[col] = df[col].apply(_parse_number)

    # trade_date → datetime.date 로 변환
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").dt.date
    else:
        df["trade_date"] = d

    # 종목 코드 통일 (단축코드는 나중에 기본정보에서 합칠 수 있음)
    if "isu_cd" in df.columns:
        df["isu_cd"] = df["isu_cd"].astype(str)

    return df


def fetch_daily_prices(date_str: str) -> pd.DataFrame:
    """
    유가증권 + 코스닥 전체 종목 일별매매정보를 합쳐서 반환.
    """
    kospi_df = _fetch_market(date_str, "KOSPI")
    kosdaq_df = _fetch_market(date_str, "KOSDAQ")

    frames = [x for x in [kospi_df, kosdaq_df] if not x.empty]
    if not frames:
        print(f"[WARN] {date_str} 기준으로 어떤 시장에서도 데이터가 없습니다.")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # -------------------------------
    # ① 표준 컬럼(code / market) 추가
    # -------------------------------
    if "isu_cd" in df.columns:
        df["isu_cd"] = df["isu_cd"].astype(str)
        df["code"] = df["isu_cd"]
    else:
        df["code"] = None

    if "source_market" in df.columns:
        df["market"] = df["source_market"]
    else:
        df["market"] = None

    # -------------------------------
    # ② 컬럼 순서 표준화
    # -------------------------------
    cols_order = [
        "trade_date",
        "code",
        "name",
        "market",
        "market_name",
        "sector_name",
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
        # 원본 컬럼도 뒤에 남겨두면 디버깅/조인할 때 편함
        "isu_cd",
        "source_market",
    ]
    existing = [c for c in cols_order if c in df.columns]
    df = df[existing].copy()

    return df



def save_daily_prices(date_str: str) -> Path | None:
    """
    kr_prices_YYYY-MM-DD.csv 로 저장.
    (기존 Naver 기반 kr_stock_price_collector 를 대체하는 용도로 설계)
    """
    df = fetch_daily_prices(date_str)
    if df.empty:
        print(f"[WARN] {date_str} 기준 KRX 일별 시세가 비어 있습니다.")
        return None

    # trade_date 가 ref_date 와 다르면 아직 KRX 데이터가 안 올라온 것일 수 있다.
    if "trade_date" in df.columns:
        try:
            # trade_date 컬럼은 YYYYMMDD 형식일 가능성이 높으니 문자열로 맞추기
            trade_dates = pd.to_datetime(df["trade_date"].astype(str)).dt.date
            latest = trade_dates.max()
            if latest.strftime("%Y-%m-%d") != date_str:
                print(
                    f"[WARN] KRX 일별 시세 trade_date={latest} 이고, "
                    f"요청 기준일={date_str} 과 달라서 아직 데이터가 안 올라온 것 같습니다."
                )
                return None
        except Exception as e:
            print(f"[WARN] trade_date 확인 중 예외 발생: {e}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / f"kr_prices_{date_str}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] KRX 일별 시세 저장 완료: {out_path}")
    return out_path



def main():
    """
    사용 예:
      (.venv) PS C:\\project> python -m iceage.src.collectors.krx_daily_price_collector 2025-11-14
    """
    if len(sys.argv) < 2:
        print("사용법: python -m iceage.src.collectors.krx_daily_price_collector YYYY-MM-DD")
        sys.exit(1)

    ref_date = sys.argv[1]
    out_path = save_daily_prices(ref_date)
    if out_path is None:
        # daily_runner 에서 이 에러를 감지해서 네이버로 폴백할 수 있게,
        # 저장 실패면 exit code 1로 종료.
        print("[ERROR] KRX 일별 시세 저장 실패 (빈 데이터 혹은 trade_date 불일치).")
        sys.exit(1)


if __name__ == "__main__":
    main()
