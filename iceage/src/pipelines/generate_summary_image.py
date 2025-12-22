# iceage/src/pipelines/generate_summary_image.py
import os
import sys
from pathlib import Path
import markdown
from html2image import Html2Image
# --- 경로 설정 ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
except IndexError:
    PROJECT_ROOT = Path.cwd()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# --- 의존성 임포트 ---
try:
    from iceage.src.llm.openai_driver import _chat
except ImportError:
    print("⚠️ [LLM Import Error] OpenAI 기능이 비활성화될 수 있습니다.")
    _chat = None

try:
    from common.s3_manager import S3Manager
except ImportError:
    print("⚠️ [S3 Import Error] S3 업로드 기능이 비활성화될 수 있습니다.")
    S3Manager = None

class SummaryImageGenerator:
    def __init__(self, ref_date: str):
        self.ref_date = ref_date
        self.service_name = "The Signalist"
        self.md_path = PROJECT_ROOT / "iceage" / "out" / f"Signalist_Daily_{self.ref_date}.md"
        self.output_dir = PROJECT_ROOT / "iceage" / "out" / "summary_images"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.s3_manager = S3Manager(bucket_name="fincore-output-storage") if S3Manager else None

    def _summarize_with_llm(self, md_content: str) -> str:
        """LLM을 사용하여 온라인 커뮤니티 스타일의 짧은 요약본을 생성합니다."""
        if not _chat:
            return "### AI 요약 실패\nLLM 드라이버를 로드할 수 없습니다."

        system_prompt = """
        당신은 유머 감각을 갖춘 주식 시장 분석가입니다. 아래의 데일리 리포트 내용을 온라인 커뮤니티(디시인사이드 주식 갤러리 등)에 올릴 짧고 흥미로운 '요약본'으로 만들어주세요.

        [요구사항]
        1. **분량:** 전체 텍스트가 20~30줄을 넘지 않도록 매우 간결하게 작성하세요.
        2. **핵심 내용:** 오늘의 시장 분위기(온도), 가장 흥미로운 '레이더 포착 종목' 1~2개, 그리고 핵심 뉴스 1개를 중심으로 요약하세요.
        3. **스타일:** 딱딱한 보고서가 아닌, 커뮤니티 유저들이 좋아할 만한 말투를 사용하세요. (예: ~했음, ~함, ㅋㅋ, ㄷㄷ 등)
        4. **형식:** Markdown 형식을 사용하고, 이모지(📈, 📉, 🔥, 🚀)를 적절히 활용하여 가독성을 높이세요.
        5. **금지:** 외부 링크, URL, 구독 유도 문구는 절대 포함하지 마세요.
        """
        user_prompt = f"아래는 오늘자 '{self.service_name}' 리포트 전문입니다. 요구사항에 맞춰 요약본을 만들어주세요.\n\n---\n\n{md_content}"

        try:
            summary = _chat(system_prompt, user_prompt)
            return summary if summary else "AI가 요약에 실패했습니다. 원본 내용을 확인해주세요."
        except Exception as e:
            print(f"⚠️ AI 요약 중 오류 발생: {e}")
            return f"### AI 요약 중 오류 발생\n{e}"

    def _wrap_in_html(self, summary_md: str) -> str:
        """요약된 마크다운을 이미지 렌더링용 HTML로 변환합니다."""
        body_html = markdown.markdown(summary_md, extensions=['tables', 'fenced_code'])
        
        html_template = f"""
        <!DOCTYPE html>
        <html lang="ko">
        <head>
            <meta charset="UTF-8">
            <style>
                body {{
                    font-family: 'Malgun Gothic', 'Pretendard', sans-serif;
                    background-color: #ffffff;
                    padding: 40px;
                    width: 720px; /* 최종 이미지 가로 800px */
                    box-sizing: border-box;
                }}
                h1, h2, h3 {{ color: #111827; margin-bottom: 10px; }}
                h1 {{ font-size: 36px; }}
                h2 {{ font-size: 28px; border-bottom: 1px solid #eee; padding-bottom: 5px; }}
                h3 {{ font-size: 22px; }}
                p, li {{ font-size: 18px; line-height: 1.7; color: #374151; }}
                strong {{ color: #000; }}
            </style>
        </head>
        <body>
            {body_html}
        </body>
        </html>
        """
        return html_template

    def run(self):
        """메인 실행 흐름"""
        print(f"🚀 '{self.service_name}' 요약 콘텐츠 생성을 시작합니다. (기준일: {self.ref_date})")

        if not self.md_path.exists():
            print(f"❌ 원본 뉴스레터 파일을 찾을 수 없습니다: {self.md_path}")
            return

        md_content = self.md_path.read_text(encoding='utf-8')
        summary_md = self._summarize_with_llm(md_content)
        summary_html = self._wrap_in_html(summary_md)

        # --- Plan B: Save MD and HTML files instead of generating an image ---
        print("📝 요약본을 MD 및 HTML 파일로 저장 중입니다...")

        # MD 파일 저장
        md_filename = f"Signalist_Summary_{self.ref_date}.md"
        md_filepath = self.output_dir / md_filename
        md_filepath.write_text(summary_md, encoding='utf-8')
        print(f"✅ 로컬에 MD 파일 저장 완료: {md_filepath}")

        # HTML 파일 저장
        html_filename = f"Signalist_Summary_{self.ref_date}.html"
        html_filepath = self.output_dir / html_filename
        html_filepath.write_text(summary_html, encoding='utf-8')
        print(f"✅ 로컬에 HTML 파일 저장 완료: {html_filepath}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python -m iceage.src.pipelines.generate_summary_image YYYY-MM-DD")
        sys.exit(1)
    
    target_date = sys.argv[1]
    SummaryImageGenerator(ref_date=target_date).run()