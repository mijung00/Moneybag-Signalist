# iceage/src/utils/build_industry_to_sector_map.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd


def main(ref_date: str | None = None) -> None:
    if ref_date is None:
        ref = date.today().isoformat()
    else:
        ref = ref_date

    listing_path = Path("iceage") / "data" / "reference" / f"kr_listing_{ref}.csv"
    if not listing_path.exists():
        raise FileNotFoundError(listing_path)

    df = pd.read_csv(listing_path)

    if "industry" not in df.columns:
        raise RuntimeError("listing 파일에 'industry' 컬럼이 없습니다.")

    industries = (
        df["industry"]
        .astype(str)
        .str.strip()
        .dropna()
        .drop_duplicates()
        .sort_values()
    )

    out_dir = Path("iceage") / "configs"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "industry_to_sector.csv"

    # 기본값은 industry와 sector를 동일하게 두고, 나중에 엑셀에서 sector만 수정
    out_df = pd.DataFrame({"industry": industries, "sector": industries})
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"✅ 업종→섹터 매핑 뼈대 저장 완료: {out_path}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 2:
        main(sys.argv[1])
    else:
        main()
