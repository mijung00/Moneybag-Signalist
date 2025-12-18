# iceage/src/tools/test_market_snapshot_manually.py
# -*- coding: utf-8 -*-
import logging
from datetime import date

from iceage.src.data_sources.market_snapshot import (
    get_market_overview,
    format_for_markdown,
)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Run for today
    ref_date = date.today()
    print(f"📅 Fetching market snapshot for {ref_date}...")
    
    snapshot = get_market_overview(ref_date)
    
    if not any(snapshot.values()):
        print("❌ Failed to fetch any market data.")
    else:
        print("✅ Successfully fetched market data.")
        print("\n" + "="*30)
        print("🏛️ Market Overview (Markdown Format)")
        print("="*30)
        markdown_summary = format_for_markdown(snapshot)
        print(markdown_summary)
        print("\n" + "="*30)
        print("RAW JSON data")
        print("="*30)
        import json
        print(json.dumps(snapshot, indent=2, ensure_ascii=False))
