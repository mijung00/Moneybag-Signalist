import os
import sys
import json
from pathlib import Path
from typing import Tuple

# ---------------------------------------------------------------------
# ✅ SecretsManager를 JSON 형태로 저장했을 때도 동작하게(OPENAI_API_KEY 등)
# ---------------------------------------------------------------------
def _normalize_json_env(env_key: str) -> None:
    raw = os.getenv(env_key, "")
    if not raw:
        return
    s = raw.strip()

    # JSON 형태 아니면 그대로 둠
    if not (s.startswith("{") and s.endswith("}")):
        return

    try:
        obj = json.loads(s)
        if not isinstance(obj, dict):
            return

        # 1) env_key와 같은 키가 있으면 그 값을 사용
        v = obj.get(env_key)

        # 2) 없으면 value라는 관용 키를 사용
        if not v:
            v = obj.get("value")

        # 3) 그것도 없으면 dict 안의 "첫번째 문자열 값"을 사용
        if not v:
            for vv in obj.values():
                if isinstance(vv, str) and vv.strip():
                    v = vv.strip()
                    break

        if isinstance(v, str) and v.strip():
            os.environ[env_key] = v.strip()
    except Exception:
        pass
_normalize_json_env("OPENAI_API_KEY")
from iceage.src.llm.openai_driver import generate_social_snippets_from_markdown

BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "out"


def get_env_suffix() -> str:
    env = os.getenv("NEWSLETTER_ENV", "dev").lower()
    return "" if env == "prod" else "-dev"


def load_newsletter_markdown(ref_date: str) -> str:
    suffix = get_env_suffix()
    md_name = f"Signalist_Daily_{ref_date}{suffix}.md"
    md_path = OUT_DIR / md_name

    if not md_path.exists():
        raise FileNotFoundError(f"뉴스레터 마크다운 파일을 찾을 수 없습니다: {md_path}")

    return md_path.read_text(encoding="utf-8")


def save_social_outputs(ref_date: str, snippets: dict) -> Path:
    """
    인스타용 결과물만 파일로 저장한다. (영상 대본 제거됨)
    - 인스타 캡션(+해시태그)
    """
    suffix = get_env_suffix()
    social_dir = OUT_DIR / "social"
    social_dir.mkdir(parents=True, exist_ok=True)

    # 1) Instagram 캡션 + 해시태그
    ig_path = social_dir / f"Signalist_Instagram_{ref_date}{suffix}.txt"
    ig_text = snippets["instagram_caption"].strip()

    hashtags = snippets.get("instagram_hashtags", "").strip()
    if hashtags:
        ig_text += "\n\n" + hashtags

    ig_path.write_text(ig_text, encoding="utf-8")

    # [삭제됨] YouTube 쇼츠용 스크립트 저장 로직 제거
    # [삭제됨] YouTube 데일리 스크립트 저장 로직 제거

    return ig_path


def main(ref_date: str | None = None) -> None:
    if ref_date is None:
        if len(sys.argv) >= 2:
            ref_date = sys.argv[1]
        else:
            raise SystemExit("사용법: python -m iceage.src.pipelines.social_contents YYYY-MM-DD")

    print(f"\n📱 SNS 콘텐츠 생성 시작 (ref_date={ref_date}, env={os.getenv('NEWSLETTER_ENV', 'dev')})")

    newsletter_md = load_newsletter_markdown(ref_date)

    # 이제 유튜브 대본 없이 인스타 캡션만 받아옴
    snippets = generate_social_snippets_from_markdown(newsletter_md)

    ig_path = save_social_outputs(ref_date, snippets)

    print(f"✅ 인스타그램 캡션 저장: {ig_path}")
    print("✅ SNS 콘텐츠 생성 완료 (영상 스크립트 제외)\n")


if __name__ == "__main__":
    ref = sys.argv[1] if len(sys.argv) >= 2 else None
    main(ref)