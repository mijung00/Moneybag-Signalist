import os
import sys
from pathlib import Path
from typing import Tuple

from iceage.src.llm.openai_driver import generate_social_snippets_from_markdown

BASE_DIR = Path(__file__).resolve().parents[2]  # iceage 폴더 기준
OUT_DIR = BASE_DIR / "out"


def get_env_suffix() -> str:
    """
    NEWSLETTER_ENV 값에 따라 파일명 suffix 결정
    - prod  -> ""  (운영용)
    - 그 외 -> "_dev"
    """
    env = os.getenv("NEWSLETTER_ENV", "dev").lower()
    return "" if env == "prod" else "_dev"


def load_newsletter_markdown(ref_date: str) -> str:
    suffix = get_env_suffix()
    md_name = f"Signalist_Daily_{ref_date}{suffix}.md"
    md_path = OUT_DIR / md_name

    if not md_path.exists():
        raise FileNotFoundError(f"뉴스레터 마크다운 파일을 찾을 수 없습니다: {md_path}")

    return md_path.read_text(encoding="utf-8")


def save_social_outputs(ref_date: str, snippets: dict) -> Tuple[Path, Path]:
    """
    인스타/유튜브용 결과물을 파일로 저장한다.
    - 인스타 캡션(+해시태그)
    - 유튜브 쇼츠 스크립트
    - (옵션) 3~4분 분량 유튜브 데일리 스크립트
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

    # 2) YouTube 쇼츠용 스크립트
    yt_shorts_path = social_dir / f"Signalist_YouTubeShorts_{ref_date}{suffix}.md"
    lines = [
        f"# {snippets['youtube_title'].strip()}",
        "",
        "## Thumbnail",
        snippets["youtube_thumbnail_text"].strip(),
        "",
        "## Script",
        snippets["youtube_script"].strip(),
        "",
    ]
    yt_shorts_path.write_text("\n".join(lines), encoding="utf-8")

    # 3) 3~4분 분량 유튜브 데일리 스크립트 (옵션)
    yt_long_script = snippets.get("youtube_long_script", "").strip()
    if yt_long_script:
        yt_long_path = social_dir / f"Signalist_YouTubeDaily_{ref_date}{suffix}.md"
        yt_long_path.write_text(yt_long_script, encoding="utf-8")
        print(f"✅ 유튜브 데일리 스크립트 저장: {yt_long_path}")

    return ig_path, yt_shorts_path



def main(ref_date: str | None = None) -> None:
    # CLI 인자로 ref_date 받기 (없으면 argv[1] 시도)
    if ref_date is None:
        if len(sys.argv) >= 2:
            ref_date = sys.argv[1]
        else:
            raise SystemExit("사용법: python -m iceage.src.pipelines.social_contents YYYY-MM-DD")

    print(f"\n📱 SNS 콘텐츠 생성 시작 (ref_date={ref_date}, env={os.getenv('NEWSLETTER_ENV', 'dev')})")

    newsletter_md = load_newsletter_markdown(ref_date)

    snippets = generate_social_snippets_from_markdown(newsletter_md)

    ig_path, yt_path = save_social_outputs(ref_date, snippets)

    print(f"✅ 인스타그램 캡션 저장: {ig_path}")
    print(f"✅ 유튜브 쇼츠 스크립트 저장: {yt_path}")
    print("✅ SNS 콘텐츠 생성 완료\n")


if __name__ == "__main__":
    ref = sys.argv[1] if len(sys.argv) >= 2 else None
    main(ref)
