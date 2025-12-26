# iceage/src/collectors/krx_listing_collector.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
import numpy as np
import requests

# [수정] 이제 모든 환경 변수와 시크릿은 config 객체를 통해 안전하게 접근합니다.
from common.config import config

# --- 경로 설정 ---
# 이 파일의 위치(iceage/src/collectors)를 기준으로 프로젝트 루트('iceage')를 찾습니다.
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except IndexError:
    # 스크립트가 다른 위치에서 실행될 경우를 대비한 예외 처리
    PROJECT_ROOT = Path.cwd()

# 상장 목록은 data/reference 폴더에 저장합니다.
DATA_DIR = PROJECT_ROOT / "data" / "reference"
# --- 경로 설정 끝 ---


def _get_krx_key():
    return os.getenv("KRX_AUTH_KEY") or os.getenv("KRX_AUTH_KEY".replace("AUTH_", ""))  # 필요시만

def _ensure_auth():
    key = _get_krx_key()
    if not key: # KRX_AUTH_KEY가 아직 로드되지 않았다면 config를 통해 로드 시도
        key = config.ensure_secret("KRX_AUTH_KEY")
    if not key: # 그래도 없으면 에러 발생
        raise RuntimeError("KRX_AUTH_KEY가 설정되어 있지 않습니다.")
    return key

KOSPI_BASE_URL = os.getenv(
    "KRX_STK_ISU_BASE_URL",
    "https://data-dbg.krx.co.kr/svc/apis/sto/stk_isu_base_info",
)
KOSDAQ_BASE_URL = os.getenv(
    "KRX_KSQ_ISU_BASE_URL",
    "https://data-dbg.krx.co.kr/svc/apis/sto/ksq_isu_base_info",
)

def _fetch_base_info(
    date_str: str,
    market: Literal["KOSPI", "KOSDAQ"],
) -> pd.DataFrame:
    """
    유가증권 / 코스닥 종목기본정보를 가져와서 DataFrame 으로 반환.
    """
    auth_key = _ensure_auth()

    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"날짜 형식이 잘못되었습니다: {date_str} (YYYY-MM-DD)")

    bas_dd = d.strftime("%Y%m%d")

    if market == "KOSPI":
        url = KOSPI_BASE_URL
    else:
        url = KOSDAQ_BASE_URL

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
        print(f"[WARN] {date_str} {market} 기준 기본정보 데이터가 없습니다.")
        return pd.DataFrame()

    raw_df = pd.DataFrame(rows)

    # 스펙 기반 컬럼 매핑 
    rename_map = {
        "ISU_CD": "isu_cd",            # 표준코드
        "ISU_SRT_CD": "short_code",    # 단축코드
        "ISU_NM": "name",              # 한글 종목명
        "ISU_ABBRV": "abbr",           # 한글 약명
        "ISU_ENG_NM": "name_eng",
        "LIST_DD": "list_date",
        "MKT_TP_NM": "market_type",
        "SECUGRP_NM": "security_group",
        "SECT_TP_NM": "sector_name",
        "KIND_STKCERT_TP_NM": "stock_kind",
        "PARVAL": "par_value",
        "LIST_SHRS": "listed_shares",
    }
    df = raw_df.rename(columns=rename_map)

    df["source_market"] = market

    # 날짜/숫자 변환
    if "list_date" in df.columns:
        # LIST_DD 는 YYYYMMDD 형식 
        df["list_date"] = pd.to_datetime(df["list_date"], format="%Y%m%d", errors="coerce").dt.date

    for col in ["par_value", "listed_shares"]:
        if col in df.columns:
            s = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.strip()
                .replace(
                    {
                        "": None,
                        "-": None,
                        "무액면": None,   # ← 여기서 '무액면'을 결측치로 처리
                    }
                )
            )
            df[col] = pd.to_numeric(s, errors="coerce")


    if "isu_cd" in df.columns:
        df["isu_cd"] = df["isu_cd"].astype(str)
    if "short_code" in df.columns:
        df["short_code"] = df["short_code"].astype(str)

    return df


def fetch_listing(date_str: str) -> pd.DataFrame:
    """
    유가증권 + 코스닥 종목기본정보 전체 합치기.
    """
    kospi_df = _fetch_base_info(date_str, "KOSPI")
    kosdaq_df = _fetch_base_info(date_str, "KOSDAQ")

    frames = [x for x in [kospi_df, kosdaq_df] if not x.empty]
    if not frames:
        print(f"[WARN] {date_str} 기준으로 어떤 시장에서도 기본정보가 없습니다.")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # -------------------------------
    # ① 표준 컬럼 추가 (code / isin / market)
    # -------------------------------
    # ISIN
    if "isu_cd" in df.columns:
        df["isu_cd"] = df["isu_cd"].astype(str)
        df["isin"] = df["isu_cd"]
    else:
        df["isin"] = None

    # 6자리 종목코드
    if "short_code" in df.columns:
        df["short_code"] = df["short_code"].astype(str)
        df["code"] = df["short_code"].str.zfill(6)
    else:
        df["code"] = None

    # 시장 코드 (KOSPI / KOSDAQ)
    if "source_market" in df.columns:
        df["market"] = df["source_market"]
    else:
        df["market"] = None

    # -------------------------------
    # ② 컬럼 순서 표준화
    # -------------------------------
    cols_order = [
        "code",            # 6자리 코드
        "name",
        "abbr",
        "name_eng",
        "isin",
        "market",          # KOSPI / KOSDAQ
        "market_type",
        "security_group",
        "sector_name",
        "stock_kind",
        "par_value",
        "listed_shares",
        "list_date",
        # 원본 컬럼들도 뒤에 남겨두면 디버깅할 때 편함
        "isu_cd",
        "short_code",
        "source_market",
    ]
    existing = [c for c in cols_order if c in df.columns]
    df = df[existing].copy()

    return df



def save_listing(date_str: str) -> Path | None:
    """
    kr_listing_YYYY-MM-DD.csv 로 저장.
    (기존 kr_listing_collector 대체 예정)
    """
    df = fetch_listing(date_str)
    if df.empty:
        return None

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / f"kr_listing_{date_str}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] KRX 종목기본정보 저장 완료: {out_path}")
    return out_path


def main():
    """
    사용 예:
      (.venv) PS C:\\project> python -m iceage.src.collectors.krx_listing_collector 2025-11-14
    """
    if len(sys.argv) < 2:
        print("사용법: python -m iceage.src.collectors.krx_listing_collector YYYY-MM-DD")
        sys.exit(1)

    ref_date = sys.argv[1]
    save_listing(ref_date)


if __name__ == "__main__":
    main()
