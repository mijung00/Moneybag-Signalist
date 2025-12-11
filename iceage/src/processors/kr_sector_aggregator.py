# iceage/src/processors/kr_sector_aggregator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path
from typing import List, Dict

import numpy as np
import pandas as pd
import sys

# 경로 안전장치
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

@dataclass
class SectorThemeAggregate:
    sector: str
    avg_return: float       # 섹터 평균 등락률
    breadth: float          # 상승 종목 비율 (0~1)
    turnover_sum: float     # 거래대금 합계
    score: float            # 랭킹용 스코어
    top_stocks: List[str]   # 대표 종목 이름들

def _naver_theme_map(ref_date: date) -> Dict[str, str]:
    base_dir = PROJECT_ROOT / "iceage"
    theme_path = base_dir / "data" / "raw" / f"naver_themes_{ref_date.isoformat()}.csv"
    if not theme_path.exists(): return {}

    df = pd.read_csv(theme_path, dtype={"code": str})
    if "code" not in df.columns or "naver_label" not in df.columns: return {}

    df["code"] = df["code"].astype(str).str.zfill(6)
    df["naver_label"] = df["naver_label"].astype(str).str.strip()
    df = df.dropna(subset=["naver_label"])

    listing_path = _listing_path(ref_date)
    if not listing_path.exists():
        listing_path = _listing_path(ref_date - pd.Timedelta(days=1))

    if listing_path.exists():
        df_listing = pd.read_csv(listing_path, dtype={"code": str})
        df_listing["code"] = df_listing["code"].astype(str).str.zfill(6)
        listed_codes = set(df_listing["code"])
        df = df[df["code"].isin(listed_codes)]

    if df.empty: return {}

    labels_path = base_dir / "configs" / "theme_labels.csv"
    if labels_path.exists():
        df_labels = pd.read_csv(labels_path, dtype=str)
        for col in ["naver_label", "canonical_theme", "role", "status"]:
            if col not in df_labels.columns: df_labels[col] = ""
        
        df_labels["naver_label"] = df_labels["naver_label"].astype(str).str.strip()
        df = df.merge(df_labels, on="naver_label", how="left", suffixes=("", "_meta"))
        
        df["canonical_theme"] = df["canonical_theme"].where(
            df["canonical_theme"].notna() & (df["canonical_theme"].astype(str) != ""),
            df["naver_label"],
        )
        df["role"] = df["role"].fillna("concept")
        df["status"] = df["status"].fillna("active")
        
        df = df[df["status"] == "active"]
        df = df[df["role"] != "background"]

    if df.empty: return {}

    # 4개 이상인 테마만 매핑 정보에 포함
    df_counts = df.groupby("canonical_theme")["code"].nunique().reset_index(name="stock_count")
    df = df.merge(df_counts, on="canonical_theme", how="left")
    df = df[df["stock_count"] >= 4]

    if df.empty: return {}

    df_unique = df.sort_values(["code", "canonical_theme"]).drop_duplicates(subset=["code"], keep="first")
    return dict(zip(df_unique["code"], df_unique["canonical_theme"]))

def _load_sector_overrides() -> Dict[str, str]:
    path = PROJECT_ROOT / "iceage" / "configs" / "sector_overrides.csv"
    if not path.exists(): return {}
    df = pd.read_csv(path, dtype={"code": str})
    if "code" not in df.columns or "sector" not in df.columns: return {}
    df["code"] = df["code"].astype(str).str.zfill(6)
    return dict(zip(df["code"], df["sector"]))

def _listing_path(ref_date: date) -> Path:
    return PROJECT_ROOT / "iceage" / "data" / "reference" / f"kr_listing_{ref_date.isoformat()}.csv"

def _industry_to_sector_map() -> Dict[str, str]:
    path = PROJECT_ROOT / "iceage" / "configs" / "industry_to_sector.csv"
    if not path.exists(): return {}
    df = pd.read_csv(path)
    if "industry" not in df.columns or "sector" not in df.columns: return {}
    return dict(zip(df["industry"], df["sector"]))

def aggregate_sector_themes(ref_date: date, top_n: int = 5) -> Path:
    # RAW 데이터를 직접 로드
    raw_price_path = PROJECT_ROOT / "iceage" / "data" / "raw" / f"kr_prices_{ref_date.isoformat()}.csv"
    
    if not raw_price_path.exists():
        print(f"[WARN] Raw price file not found: {raw_price_path}")
        return _save_empty_result(ref_date)

    df_price = pd.read_csv(raw_price_path)

    # [핵심 1] 숫자 변환 헬퍼 (콤마, 퍼센트 제거)
    def _clean(x):
        if isinstance(x, (int, float)): return float(x)
        try: return float(str(x).replace("%", "").replace(",", ""))
        except: return 0.0

    # [핵심 2] 한글 컬럼명("등락률") 인식 추가
    candidates = ["FLUC_RT", "fluc_rate", "change_rate", "Change", "chg_pct", "change_pct", "등락률"]
    found_col = next((c for c in candidates if c in df_price.columns), None)

    if found_col:
        df_price["change_pct"] = df_price[found_col].apply(_clean)
    else:
        # 없으면 강제 계산 (시가, 종가 기반)
        c_col = next((c for c in ["close", "TDD_CLSPRC", "현재가", "종가"] if c in df_price.columns), None)
        o_col = next((c for c in ["open", "TDD_OPNPRC", "시가"] if c in df_price.columns), None)
        
        if c_col and o_col:
             df_price["close_val"] = df_price[c_col].apply(_clean)
             df_price["open_val"] = df_price[o_col].apply(_clean)
             df_price["open_val"] = df_price["open_val"].replace(0, np.nan) # 0으로 나누기 방지
             df_price["change_pct"] = (df_price["close_val"] - df_price["open_val"]) / df_price["open_val"] * 100
             df_price["change_pct"] = df_price["change_pct"].fillna(0.0)
        else:
            df_price["change_pct"] = 0.0

    # 코드 정규화 (한글 "종목코드" 등 인식)
    code_col = next((c for c in ["code", "Code", "ISU_SRT_CD", "ticker", "종목코드"] if c in df_price.columns), "code")
    if code_col not in df_price.columns:
        return _save_empty_result(ref_date)

    df_price["code"] = df_price[code_col].astype(str).str.zfill(6)
    
    # 이름 ("종목명" 인식)
    name_col = next((c for c in ["ISU_ABBRV", "name", "stock_name", "종목명"] if c in df_price.columns), "name")
    if name_col in df_price.columns:
        df_price["name"] = df_price[name_col].astype(str).str.strip()
    else:
        df_price["name"] = ""

    # 상장 리스트 병합
    listing_path = _listing_path(ref_date)
    if not listing_path.exists():
        listing_path = _listing_path(ref_date - pd.Timedelta(days=1))
    
    if listing_path.exists():
        df_listing = pd.read_csv(listing_path, dtype={"code": str})
        df_listing["code"] = df_listing["code"].astype(str).str.zfill(6)
        
        if "industry" not in df_listing.columns:
            if "sector_name" in df_listing.columns:
                df_listing["industry"] = df_listing["sector_name"].fillna("기타")
            else:
                df_listing["industry"] = "기타"

        df = df_price.merge(df_listing[["code", "industry"]], on="code", how="left")
        df = df.dropna(subset=["industry"])
    else:
        # 리스트 파일 없으면 그냥 가격 데이터만 사용 (최소한의 방어)
        df = df_price
        df["industry"] = "기타"

    if df.empty: return _save_empty_result(ref_date)

    # [핵심 3] 거래대금 계산 & 심폐소생술
    # 한글 "거래대금", "거래량", "현재가" 인식 추가
    val_col = next((c for c in ["TRDVAL", "trading_value", "amount", "turnover", "거래대금"] if c in df.columns), None)
    if val_col:
        df["turnover"] = df[val_col].apply(_clean)
    else:
        df["turnover"] = 0.0
    
    # 만약 거래대금 합계가 0이면(데이터 누락 시), [현재가 * 거래량]으로 강제 복구
    if df["turnover"].sum() == 0:
        vol_col = next((c for c in ["volume", "ACC_TRDVOL", "거래량"] if c in df.columns), None)
        close_col = next((c for c in ["close", "TDD_CLSPRC", "현재가", "종가"] if c in df.columns), None)
        
        if vol_col and close_col:
            df["turnover"] = df[close_col].apply(_clean) * df[vol_col].apply(_clean)

    # 매핑 및 집계
    mapping = _industry_to_sector_map()
    if mapping:
        df["sector"] = df["industry"].map(mapping).fillna(df["industry"])
    else:
        df["sector"] = df["industry"]

    overrides = _load_sector_overrides()
    if overrides:
        df["sector"] = df.apply(lambda row: overrides.get(row["code"], row["sector"]), axis=1)

    theme_map = _naver_theme_map(ref_date)
    if theme_map:
        df["sector"] = df["code"].map(theme_map).fillna(df["sector"])

    # 필터링
    labels_path = PROJECT_ROOT / "iceage" / "configs" / "theme_labels.csv"
    if labels_path.exists():
        df_labels = pd.read_csv(labels_path, dtype=str)
        if "canonical_theme" in df_labels.columns:
            active = df_labels[df_labels.get("status", "active") == "active"]
            active = active[active.get("role", "concept") != "background"]
            allowed_themes = set(active["canonical_theme"].dropna())
            if allowed_themes:
                df = df[df["sector"].isin(allowed_themes)]

    if df.empty: return _save_empty_result(ref_date)

    grouped = df.groupby("sector")
    records: List[SectorThemeAggregate] = []

    for sector, g in grouped:
        # -------------------------------------------------------
        # [핵심 4] 종목 수 5개 미만 테마는 집계에서 제외 (왜곡 방지)
        # -------------------------------------------------------
        if len(g) < 5:
            continue
            
        avg_return = float(g["change_pct"].mean())
        breadth = float((g["change_pct"] > 0).mean())
        turnover_sum = float(g["turnover"].sum())

        score = avg_return * 1.0 + breadth * 20.0 + float(np.log10(turnover_sum + 1.0)) * 0.5
        top = g.sort_values("change_pct", ascending=False).head(3)["name"].tolist()

        records.append(SectorThemeAggregate(sector, avg_return, breadth, turnover_sum, score, top))

    records = sorted(records, key=lambda x: x.score, reverse=True)[:top_n]

    out_dir = PROJECT_ROOT / "iceage" / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"kr_sector_themes_{ref_date.isoformat()}.json"

    with out_path.open("w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in records], f, ensure_ascii=False, indent=2)

    print(f"✅ 섹터 테마 집계 완료 (RAW 데이터 사용): {out_path}")
    return out_path

def _save_empty_result(ref_date: date) -> Path:
    out_dir = PROJECT_ROOT / "iceage" / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"kr_sector_themes_{ref_date.isoformat()}.json"
    out_path.write_text("[]", encoding="utf-8")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        ref = date.fromisoformat(sys.argv[1])
    else:
        ref = date.today()
    aggregate_sector_themes(ref)