# iceage/src/processors/kr_news_cleaner.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

def _parse_published_at(raw: str) -> str:
    """
    SerpAPI에서 넘어오는 날짜 문자열을 최대한 ISO 형식으로 맞춰보되,
    실패하면 그냥 원본 문자열을 그대로 돌려준다.
    """
    if not raw:
        return ""
    raw = str(raw).strip()
    # 이미 ISO 형식이면 그대로
    try:
        return datetime.fromisoformat(raw).isoformat()
    except Exception:
        # "10/23/2025, 07:00 AM, +0000 UTC" 같은 건 그냥 원본 보존
        return raw



def _raw_path(ref_date: date) -> Path:
    return Path("iceage") / "data" / "raw" / f"kr_news_{ref_date.isoformat()}.jsonl"


def _clean_one(a: Dict) -> Optional[Dict]:
    title = (a.get("title") or "").strip()
    if not title:
        return None

    snippet = (a.get("snippet") or "").strip()
    source = (a.get("source") or "").strip()
    link = (a.get("link") or "").strip()

    # raw에는 date / published_at 둘 중 하나가 있을 수 있으니 둘 다 봐준다
    published_at_raw = a.get("published_at") or a.get("date") or ""
    iso_dt = _parse_published_at(published_at_raw)

    # 🔥 여기서 kind/code/name 도 같이 살려준다
    kind = a.get("kind") or ""
    code = a.get("code") or ""
    name = a.get("name") or ""

    return {
        "title": title,
        "snippet": snippet,
        "source": source,
        "link": link,
        "published_at": iso_dt,
        "kind": kind,
        "code": code,
        "name": name,
    }



def clean_kr_news(ref_date: date) -> Path:
    """
    국내 뉴스 raw(jsonl)를 읽어서
    - _clean_one()으로 필드 정리
    - (title, source) 기준 중복 제거
    - cleaned jsonl 로 저장

    raw 파일이 없거나 비어 있어도 예외를 던지지 않고,
    경고 로그만 남기고 빈 cleaned 파일을 생성해서 반환한다.
    """
    raw_path = _raw_path(ref_date)
    if not raw_path.exists():
        # ✅ 파일이 없어도 파이프라인이 죽지 않도록 방어
        print(f"[WARN] 국내 뉴스 raw 파일이 없어 클렌징을 스킵합니다: {raw_path}")
        out_dir = Path("iceage") / "data" / "processed"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"kr_news_cleaned_{ref_date.isoformat()}.jsonl"
        # 기존 파일이 있으면 유지, 없으면 빈 파일 생성
        if not out_path.exists():
            out_path.write_text("", encoding="utf-8")
        print(f"✅ 국내 뉴스 cleaned (빈 파일) 생성/유지: {out_path}")
        return out_path

    cleaned: List[Dict] = []
    seen = set()

    with raw_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                a = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[WARN] 국내 뉴스 raw JSON 파싱 실패: {e} / line={line[:80]!r}")
                continue

            c = _clean_one(a)
            if not c:
                continue

            key = (c.get("title"), c.get("source"))
            if key in seen:
                continue
            seen.add(key)
            cleaned.append(c)

    out_dir = Path("iceage") / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"kr_news_cleaned_{ref_date.isoformat()}.jsonl"

    with out_path.open("w", encoding="utf-8") as f:
        for c in cleaned:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")

    print(f"✅ 국내 뉴스 cleaned 저장 완료: {out_path}")
    return out_path



if __name__ == "__main__":
    import sys
    from datetime import date as _date

    if len(sys.argv) >= 2:
        ref = _date.fromisoformat(sys.argv[1])
    else:
        ref = _date.today()

    clean_kr_news(ref)
