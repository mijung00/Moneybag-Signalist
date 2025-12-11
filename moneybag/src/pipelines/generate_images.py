import os
from html2image import Html2Image
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from moneybag.src.pipelines.send_email import EmailSender

BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

class ImageGenerator:
    def __init__(self):
        # 크롬 경로 자동 탐색을 위해 별도 설정 없이 시도
        self.hti = Html2Image(output_path=str(BASE_DIR / "moneybag/data/out/images"))
        self.email_sender = EmailSender()

    def generate_images(self, md_file_path):
        if not os.path.exists(md_file_path): return

        with open(md_file_path, "r", encoding="utf-8") as f:
            md_text = f.read()

        full_html = self.email_sender.convert_md_to_html(md_text)
        
        # Summary 생성 로직 (헤드라인 + 대시보드 + 결론)
        lines = md_text.split('\n')
        summary_lines = []
        capture = False
        
        for line in lines:
            if line.startswith("# "): # 제목
                summary_lines.append(line)
            elif "## 1. 헌터의 대시보드" in line: # 대시보드 시작
                capture = True
                summary_lines.append(line)
            elif "## 2. " in line: # 대시보드 끝
                capture = False
            elif "## 5. " in line: # 결론 시작
                capture = True
                summary_lines.append(line)
            elif capture:
                summary_lines.append(line)
        
        summary_html = self.email_sender.convert_md_to_html("\n".join(summary_lines))

        filename = os.path.basename(md_file_path).replace(".md", "")
        print(f"📸 이미지 생성 중... ({filename})")
        
        # [수정] 높이를 5000으로 대폭 늘려 잘림 방지
        self.hti.screenshot(html_str=full_html, save_as=f"{filename}_full.png", size=(750, 5000))
        self.hti.screenshot(html_str=summary_html, save_as=f"{filename}_summary.png", size=(750, 3000))
        
        print(f"✅ 이미지 생성 완료: {filename}_*.png")

if __name__ == "__main__":
    out_dir = BASE_DIR / "moneybag" / "data" / "out"
    files = sorted(out_dir.glob("SecretNote_*.md"), key=os.path.getmtime, reverse=True)
    if files:
        gen = ImageGenerator()
        gen.generate_images(files[0])