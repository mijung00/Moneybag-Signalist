# iceage/src/processors/build_sector_overrides_from_naver.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Dict, List

import pandas as pd


def _naver_theme_path(ref_date: date) -> Path:
    return Path("iceage") / "data" / "raw" / f"naver_themes_{ref_date.isoformat()}.csv"


def _listing_path(ref_date: date) -> Path:
    return Path("iceage") / "data" / "reference" / f"kr_listing_{ref_date.isoformat()}.csv"


def _naver_label_to_sector_path() -> Path:
    return Path("iceage") / "configs" / "naver_label_to_sector.csv"


def build_sector_overrides_from_naver(ref_date: date) -> Path:
    theme_path = _naver_theme_path(ref_date)
    if not theme_path.exists():
        raise FileNotFoundError(theme_path)

    listing_path = _listing_path(ref_date)
    if not listing_path.exists():
        raise FileNotFoundError(listing_path)

    label_map_path = _naver_label_to_sector_path()
    if not label_map_path.exists():
        raise FileNotFoundError(label_map_path)

    df_theme = pd.read_csv(theme_path, dtype={"code": str})
    df_theme["code"] = df_theme["code"].astype(str).str.zfill(6)

    df_listing = pd.read_csv(listing_path, dtype={"code": str})
    df_listing["code"] = df_listing["code"].astype(str).str.zfill(6)
    df_listing["name"] = df_listing["name"].astype(str).str.strip()

    df_map = pd.read_csv(label_map_path)
    if "naver_label" not in df_map.columns or "sector" not in df_map.columns:
        raise RuntimeError("naver_label_to_sector.csv 는 'naver_label','sector' 컬럼이 필요합니다.")

    label_to_sector: Dict[str, str] = dict(zip(df_map["naver_label"], df_map["sector"]))

    # code별로 naver_label 모으기
    labels_by_code: Dict[str, List[str]] = defaultdict(list)
    for _, row in df_theme.iterrows():
        code = row["code"]
        label = str(row.get("naver_label", "")).strip()
        if not label:
            continue
        labels_by_code[code].append(label)

    # code별로 sector 선택
    records = []
    for code, labels in labels_by_code.items():
        # naver_label -> sector 변환
        sectors: List[str] = []
        for lb in labels:
            if lb in label_to_sector:
                sectors.append(label_to_sector[lb])

        if not sectors:
            continue  # 매핑되는 섹터가 없으면 override 생성 안 함

        # 가장 많이 등장한 섹터를 선택 (동률이면 첫 번째)
        counts = Counter(sectors)
        sector, _ = counts.most_common(1)[0]

        # 종목명 붙이기
        name_row = df_listing[df_listing["code"] == code]
        if not name_row.empty:
            name = name_row.iloc[0]["name"]
        else:
            # listing에 없으면 theme 데이터의 name 사용
            name = df_theme[df_theme["code"] == code].iloc[0]["name"]

        records.append({"code": code, "name": name, "sector": sector})

    if not records:
        raise RuntimeError("네이버 테마 기반으로 생성할 섹터 override 가 없습니다.")

    df_out = pd.DataFrame(records).drop_duplicates(subset=["code"])
    out_path = Path("iceage") / "configs" / "sector_overrides.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"✅ 네이버 테마 기반 sector_overrides 생성 완료: {out_path}")
    return out_path


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 2:
        ref = date.fromisoformat(sys.argv[1])
    else:
        ref = date.today()

    build_sector_overrides_from_naver(ref)
