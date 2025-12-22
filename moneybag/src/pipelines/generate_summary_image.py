# moneybag/src/pipelines/generate_summary_image.py
import os
import sys
import re
from pathlib import Path
import markdown
import requests
# --- 경로 설정 ---
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
except IndexError:
    PROJECT_ROOT = Path.cwd()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# --- 의존성 임포트 ---
try:
    from moneybag.src.llm.openai_driver import _chat
except ImportError:
    print("⚠️ [LLM Import Error] OpenAI 기능이 비활성화될 수 있습니다.")
    _chat = None

try:
    from common.s3_manager import S3Manager
except ImportError:
    print("⚠️ [S3 Import Error] S3 업로드 기능이 비활성화될 수 있습니다.")
    S3Manager = None

class SummaryImageGenerator:
    def __init__(self, mode: str):
        self.mode = mode.lower()
        self.service_name = "The Whale Hunter"
        self.md_path = self._find_latest_md()
        if self.md_path:
            # 파일명에서 날짜 추출 (SecretNote_Morning_2025.12.21.md)
            date_str_match = re.search(r'(\d{4}\.\d{2}\.\d{2})', self.md_path.name)
            self.ref_date = date_str_match.group(1).replace('.', '-') if date_str_match else "latest"
        else:
            self.ref_date = "unknown"
        self.apiflash_key = os.getenv("APIFLASH_ACCESS_KEY")

        self.output_dir = PROJECT_ROOT / "moneybag" / "data" / "out" / "summary_images"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.s3_manager = S3Manager(bucket_name="fincore-output-storage") if S3Manager else None

    def _find_latest_md(self) -> Path | None:
        """지정된 모드의 가장 최신 뉴스레터 MD 파일을 찾습니다."""
        md_dir = PROJECT_ROOT / "moneybag" / "data" / "out"
        pattern = f"SecretNote_{self.mode.capitalize()}_*.md"
        files = list(md_dir.glob(pattern))
        if not files:
            return None
        return max(files, key=os.path.getctime)

    def _summarize_with_llm(self, md_content: str) -> str:
        """LLM을 사용하여 암호화폐 커뮤니티 스타일의 짧은 요약본을 생성합니다."""
        if not _chat:
            return "### AI 요약 실패\nLLM 드라이버를 로드할 수 없습니다."

        system_prompt = """
        당신은 암호화폐 시장의 '고래 사냥꾼'입니다. 아래의 시크릿 노트 내용을 온라인 커뮤니티(코인판, 디시인사이드 등)에 올릴 짧고 흥미로운 '요약본'으로 만들어주세요.

        [요구사항]
        1. **분량:** 전체 텍스트가 20~30줄을 넘지 않도록 매우 간결하게 작성하세요.
        2. **핵심 내용:** 오늘의 사령관(Commander)과 그의 한마디, 고래 심리 지수, 가장 중요한 추천 전략 1개, 그리고 핵심 글로벌 첩보 1개를 중심으로 요약하세요.
        3. **스타일:** 딱딱한 보고서가 아닌, 커뮤니티 유저들이 좋아할 만한 말투를 사용하세요. (예: ~했음, ~함, ㅋㅋ, ㄷㄷ, 형들 등)
        4. **형식:** Markdown 형식을 사용하고, 이모지(🐋, 🚀, 🥶, 🔥)를 적절히 활용하여 가독성을 높이세요.
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
        """요약된 마크다운을 다크모드 이미지 렌더링용 HTML로 변환합니다."""
        body_html = markdown.markdown(summary_md, extensions=['tables', 'fenced_code'])
        
        html_template = f"""
        <!DOCTYPE html>
        <html lang="ko">
        <head>
            <meta charset="UTF-8">
            <style>
                body {{
                    font-family: 'Malgun Gothic', 'Pretendard', sans-serif;
                    background-color: #191c24; /* 다크 배경 */
                    color: #ebeeef; /* 밝은 텍스트 */
                    padding: 40px;
                    width: 720px; /* 최종 이미지 가로 800px */
                    box-sizing: border-box;
                }}
                h1, h2, h3 {{ color: #ffffff; margin-bottom: 10px; }}
                h1 {{ font-size: 36px; }}
                h2 {{ font-size: 28px; border-bottom: 1px solid #3a3f51; padding-bottom: 5px; }}
                h3 {{ font-size: 22px; color: #b46eff; }}
                p, li {{ font-size: 18px; line-height: 1.7; color: #c5c8d3; }}
                strong {{ color: #ffffff; }}
                blockquote {{
                    border-left: 3px solid #b46eff;
                    background-color: #262a35;
                    padding: 15px;
                    margin: 20px 0;
                    border-radius: 4px;
                }}
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
        print(f"🚀 '{self.service_name}' 요약 이미지 생성을 시작합니다. (모드: {self.mode})")

        if not self.md_path or not self.md_path.exists():
            print(f"❌ 원본 뉴스레터 파일을 찾을 수 없습니다. (모드: {self.mode})")
            return
        
        if not self.apiflash_key:
            print("❌ APIFLASH_ACCESS_KEY 환경변수가 설정되지 않았습니다. 이미지 생성을 건너뜁니다.")
            return

        # [디버깅] 실제로 사용하려는 키가 무엇인지 안전하게 로깅
        key_to_log = f"{self.apiflash_key[:4]}...{self.apiflash_key[-4:]}" if self.apiflash_key and len(self.apiflash_key) > 8 else "Invalid or short key"
        print(f"🔑 Using ApiFlash Key: {key_to_log}")

        md_content = self.md_path.read_text(encoding='utf-8')
        summary_md = self._summarize_with_llm(md_content)
        summary_html = self._wrap_in_html(summary_md)

        print("📸 ApiFlash API를 사용하여 요약본을 이미지로 변환 중입니다...")
        
        api_url = "https://api.apiflash.com/v1/urltoimage"
        
        # [수정] access_key는 URL 파라미터로, html 본문은 JSON payload로 분리
        params = {
            "access_key": self.apiflash_key
        }
        
        json_payload = {
            "html": summary_html,
            "format": "png",
            "fresh": True, # 캐시 방지
            "width": 800, # HTML에 패딩이 있으므로 720(컨텐츠)+80(패딩)=800
        }
        
        # API 호출 시 params와 json을 함께 사용
        response = requests.post(api_url, params=params, json=json_payload)

        if response.status_code == 200:
            output_filename = f"WhaleHunter_Summary_{self.ref_date}_{self.mode}.png"
            local_image_path = self.output_dir / output_filename
            with open(local_image_path, "wb") as f:
                f.write(response.content)
            print(f"✅ 로컬에 이미지 저장 완료: {local_image_path}")

            if self.s3_manager:
                s3_key = f"moneybag/out/summary_images/{output_filename}"
                print(f"☁️ S3에 업로드 중... (Key: {s3_key})")
                if self.s3_manager.upload_file(local_file_path=str(local_image_path), s3_key=s3_key):
                    print("✅ S3 업로드 완료!")
                else:
                    print("❌ S3 업로드 실패.")
        else:
            try:
                error_message = response.json().get("message", response.text)
            except requests.exceptions.JSONDecodeError:
                error_message = response.text
            print(f"❌ ApiFlash 오류 발생 (Status: {response.status_code}): {error_message}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python -m moneybag.src.pipelines.generate_summary_image [morning|night]")
        sys.exit(1)
    
    target_mode = sys.argv[1]
    SummaryImageGenerator(mode=target_mode).run()