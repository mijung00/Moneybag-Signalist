# iceage/src/pipelines/render_newsletter_html.py
# -*- coding: utf-8 -*-
"""
Signalist_Daily_YYYY-MM-DD.md -> HTML 이메일 템플릿 렌더러

사용법:
    python -m iceage.src.pipelines.render_newsletter_html 2025-11-07
    # 인자를 안 주면 오늘 날짜 기준으로 시도
"""

import os
import sys
import datetime as dt
from pathlib import Path
from dotenv import load_dotenv

import markdown


PROJECT_ROOT = Path(__file__).resolve().parents[2]  # C:\project\iceage
OUT_DIR = PROJECT_ROOT / "out"  # 🔧 여기서 iceage 한 번만

# 실제 .env 는 C:\project\.env 에 있으므로 parent 기준으로 로드
load_dotenv(PROJECT_ROOT.parent / ".env")

def _get_newsletter_env_suffix() -> str:
    env = os.getenv("NEWSLETTER_ENV", "prod").strip().lower()
    if env in ("", "prod"):
        return ""
    # [수정] 파일명이 2025-12-12-dev.md 형식이므로 언더바(_)가 아니라 하이픈(-)이어야 함
    return f"-{env}"


def render_markdown_to_html(ref_date: str) -> Path:
    suffix = _get_newsletter_env_suffix()

    # [수정] 변수명에 하이픈(-) 사용 불가 -> 언더바(_)로 변경
    md_path = OUT_DIR / f"Signalist_Daily_{ref_date}{suffix}.md"
    
    if not md_path.exists():
        raise FileNotFoundError(f"Markdown 파일을 찾을 수 없습니다: {md_path}")

    md_text = md_path.read_text(encoding="utf-8")

    # [추가] 마크다운 첫 줄에서 제목 추출
    first_line = md_text.split('\n', 1)[0]
    headline = f"Signalist Daily — {ref_date}" # 기본값
    if first_line.startswith("# "):
        # '# ' 제거하고 공백 정리
        headline = first_line.replace("# ", "").strip()

    # 표 / 리스트 / 코드블럭 등을 잘 렌더링하기 위해 확장 사용
    body_html = markdown.markdown(
        md_text,
        extensions=[
            "tables",
            "fenced_code",
            "sane_lists",
        ],
    )


    # 이메일용 기본 HTML 템플릿 (inline CSS 위주)
    html_template = f"""<!doctype html>
<html lang="ko">
  <head>
    <meta charset="utf-8" />
    <title>{headline}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      /* 전체 레이아웃 */
      body {{
        margin: 0;
        padding: 0;
        background-color: #f4f5f7;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI",
                     system-ui, sans-serif;
      }}
      .container {{
        max-width: 760px;
        margin: 0 auto;
        padding: 24px 16px 40px;
      }}
      .card {{
        background-color: #ffffff;
        border-radius: 12px;
        padding: 32px 28px;
        box-shadow: 0 4px 18px rgba(15, 23, 42, 0.06);
      }}

      /* 타이포그래피 */
      h1 {{
        font-size: 28px;
        margin: 0 0 4px;
        line-height: 1.3;
      }}
      h2 {{
        font-size: 20px;
        margin: 32px 0 8px;
        border-bottom: 1px solid #dbeafe;   /* 기존 #e5e7eb → 옅은 파랑 */
        padding-bottom: 4px;
        color: #0f172a;                      /* 제목 색 살짝 진하게 */
      }}
      h3 {{
        font-size: 16px;
        margin: 20px 0 6px;
        color: #111827;
      }}
      h4 {{
        font-size: 14px;
        margin: 16px 0 4px;
      }}
      p {{
        font-size: 14px;
        line-height: 1.7;
        color: #374151;
        margin: 8px 0;
      }}
      strong {{
        color: #111827;
      }}
      em {{
        color: #4b5563;
      }}
      ul, ol {{
        padding-left: 20px;
        margin: 6px 0 10px;
      }}
      li {{
        font-size: 14px;
        line-height: 1.6;
        color: #374151;
        margin: 2px 0;
      }}

      /* 테이블 (Signalist Today / History 등) */
      table {{
        width: 100%;
        border-collapse: collapse;
        margin: 12px 0 18px;
        font-size: 13px;
      }}
      th, td {{
        border: 1px solid #e5e7eb;
        padding: 6px 8px;
        text-align: left;
        vertical-align: middle;
      }}
      th {{
        background-color: #eff6ff;           /* 옅은 파랑 */
        font-weight: 600;
        white-space: nowrap;
        border-bottom: 2px solid #d1d5db;
      }}
      tr:nth-child(even) td {{
        background-color: #fafafa;
      }}
      tr:hover td {{
        background-color: #f1f5f9;
      }}

      /* 인용문 / 구분선 */
      blockquote {{
        margin: 12px 0;
        padding: 8px 12px;
        border-left: 3px solid #3b82f6;
        background-color: #f3f4ff;
        color: #374151;
        font-style: italic;
      }}
      hr {{
        border: none;
        border-top: 1px solid #e5e7eb;
        margin: 20px 0;
      }}

      /* 링크 */
      a {{
        color: #2563eb;
        text-decoration: none;
      }}
      a:hover {{
        text-decoration: underline;
      }}

      /* 섹션 간 여백 */
      .card > *:first-child {{
        margin-top: 0;
      }}
      .card > *:last-child {{
        margin-bottom: 0;
      }}

      /* 푸터 */
      .footer {{
        text-align: center;
        font-size: 11px;
        color: #9ca3af;
        margin-top: 16px;
      }}
      .footer a {{
        color: #6b7280;
      }}
    </style>
  </head>
  <body>
    <div class="container">
      <div class="card">
        {body_html}
      </div>
      <div style="text-align: center; font-size: 12px; color: #888888; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eeeeee;">
        본 메일은 -email- 주소로 발송된 Fincore 뉴스레터입니다.<br>
        더 이상 수신을 원하지 않으시면 <a href="-unsubscribe_url-" style="color: #555555; text-decoration: underline;">여기</a>를 눌러 구독을 취소해주세요.<br><br>
        (주)비제이유앤아이 | <a href="https://www.fincore.trade/privacy" style="color: #555555;">개인정보 처리방침</a>
      </div>
    </div>
  </body>
</html>


"""

    html_path = OUT_DIR / f"Signalist_Daily_{ref_date}{suffix}.html"
    html_path.write_text(html_template, encoding="utf-8")
    return html_path


def main() -> None:
    if len(sys.argv) > 1:
        ref_date = sys.argv[1]
    else:
        ref_date = dt.date.today().isoformat()

    html_path = render_markdown_to_html(ref_date)
    print(f"✅ HTML 뉴스레터 저장 완료: {html_path}")


if __name__ == "__main__":
    main()