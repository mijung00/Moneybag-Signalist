import json
import os
from datetime import date, datetime, timezone
from typing import Dict, List

import requests
from dotenv import load_dotenv
from pathlib import Path

SERPAPI_ENDPOINT = "https://serpapi.com/search"

# --- 프로젝트 루트(.env) 로드 ---
BASE_DIR = Path(__file__).resolve().parents[3]  # C:\project
load_dotenv(BASE_DIR / ".env")
# --------------------------------


def _get_api_key() -> str:
    """
    .env 에 있는 SERPAPI_KEY (또는 예전 이름 SERPAPI_API_KEY)를 읽어서 반환.
    없으면 RuntimeError.
    """
    key = os.getenv("SERPAPI_KEY") or os.getenv("SERPAPI_API_KEY")
    if not key:
        raise RuntimeError("SERPAPI_KEY가 설정되어 있지 않습니다. (.env 확인)")
    return key



def fetch_kr_news_raw(
    ref_date: date,
    num_results: int = 50,
    *,
    timeout: int = 20,
    max_retries: int = 3,
) -> List[Dict]:
    """
    SerpAPI를 이용해 국내 증시 관련 구글 뉴스 수집.

    - 시간 초과/일시적인 네트워크 에러가 나면 최대 max_retries 번까지 재시도
    - 그래도 실패하면 예외를 던지는 대신 경고만 출력하고 빈 리스트([])를 반환한다.
    """
    api_key = _get_api_key()

    params = {
        "engine": "google_news",
        "q": "코스피 OR 코스닥 OR 증시 OR 주식 시장",
        "hl": "ko",
        "gl": "kr",
        "api_key": api_key,
        "num": num_results,
    }

    last_error: Exception | None = None
    data: Dict | None = None

    for attempt in range(1, max_retries + 1):
        try:
            # 🔧 여기! SERPAPI_ENDPOINT 로 수정
            res = requests.get(SERPAPI_ENDPOINT, params=params, timeout=timeout)
            res.raise_for_status()
            data = res.json()
            break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            print(f"[WARN] 국내 뉴스 SerpAPI 요청 {attempt}/{max_retries}회 실패 (네트워크/타임아웃): {e}")
        except requests.RequestException as e:
            last_error = e
            print(f"[WARN] 국내 뉴스 SerpAPI 요청 {attempt}/{max_retries}회 실패 (HTTP 에러): {e}")
            break
        except Exception as e:
            last_error = e
            print(f"[WARN] 국내 뉴스 SerpAPI 요청 중 알 수 없는 예외 발생: {e}")
            break

    if last_error is not None and not isinstance(data, dict):
        print("[WARN] 국내 뉴스 SerpAPI 요청이 반복 실패하여 빈 결과를 반환합니다.")
        return []

    if not isinstance(data, dict):
        print("[WARN] 국내 뉴스 SerpAPI 응답 형식이 예상과 달라 빈 결과를 반환합니다.")
        return []

    articles: List[Dict] = []
    for item in data.get("news_results", []):
        source = item.get("source")
        if isinstance(source, dict):
            source_name = source.get("name")
        else:
            source_name = source

        articles.append(
            {
                "title": item.get("title", ""),
                "snippet": item.get("snippet", ""),
                "source": source_name,
                "link": item.get("link", ""),
                "date": item.get("date", ""),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return articles



def save_kr_news_raw(ref_date: date) -> str:
    """
    fetch_kr_news_raw 결과를 iceage/data/raw/kr_news_YYYY-MM-DD.jsonl 로 저장.
    """
    articles = fetch_kr_news_raw(ref_date)

    out_dir = "iceage/data/raw"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"kr_news_{ref_date.isoformat()}.jsonl")

    with open(out_path, "w", encoding="utf-8") as f:
        for art in articles:
            f.write(json.dumps(art, ensure_ascii=False) + "\n")

    print(f"✅ 국내 뉴스 raw 저장 완료: {out_path}")
    return out_path


def main() -> None:
    import sys

    if len(sys.argv) >= 2:
        ref = date.fromisoformat(sys.argv[1])
    else:
        ref = date.today()

    save_kr_news_raw(ref)


if __name__ == "__main__":
    main()
