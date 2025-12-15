import os
import re
import random
import textwrap
from datetime import datetime
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

# --------------------------------------------------------------------------
# [설정] 경로 및 디렉토리 세팅 (사용자님 환경에 맞춤)
# --------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[3]  # 프로젝트 루트 (Moneybag-Signalist 폴더)
ASSET_DIR = BASE_DIR / "moneybag" / "assets"    # 폰트, 이미지 템플릿 위치
DATA_DIR = BASE_DIR / "moneybag" / "data" / "out" # MD 파일 및 결과물 위치
OUTPUT_DIR = DATA_DIR / "cardnews"              # 카드뉴스 저장될 곳

class CardNewsFactory:
    def __init__(self):
        # 1. 폰트 경로 설정
        self.path_bold = str(ASSET_DIR / "Bold.ttf")
        self.path_medium = str(ASSET_DIR / "Medium.ttf")
        
        # 2. 색상 팔레트
        self.color_bg_text = "#333333"
        self.color_accent = "#6200EE" # 보라색 (시그니처)
        self.color_purple = "#6200EE"
        self.color_green = "#00C853"
        self.color_red = "#D50000"
        self.color_white = "#FFFFFF"
        self.color_gray = "#666666"

        # 3. 폰트 로드 (없으면 기본 폰트 사용 방지용 예외처리)
        try:
            self.font_title = ImageFont.truetype(self.path_bold, 70)
            self.font_header = ImageFont.truetype(self.path_bold, 50)
            self.font_body = ImageFont.truetype(self.path_medium, 32)
            self.font_small = ImageFont.truetype(self.path_medium, 26)
            self.font_accent = ImageFont.truetype(self.path_bold, 40)
            self.font_mini = ImageFont.truetype(self.path_medium, 22)
        except Exception as e:
            print(f"⚠️ 폰트 로드 실패 ({e}). 시스템 기본 폰트를 사용합니다.")
            self.font_title = ImageFont.load_default()
            self.font_header = ImageFont.load_default()
            self.font_body = ImageFont.load_default()
            # ... (나머지도 기본으로)

        # 4. 템플릿 이미지 경로 (있으면 쓰고 없으면 흰 배경)
        self.selected_cover_bg = str(ASSET_DIR / "cover_01.png")
        self.selected_body_bg = str(ASSET_DIR / "body_01.png")

    # --------------------------------------------------------------------------
    # [핵심 1] MD 파일 파싱 (사령관 정보 + 액션 가이드 완벽 통합)
    # --------------------------------------------------------------------------
    def parse_markdown(self, file_path):
        print(f"📂 파일 파싱 시작: {file_path.name}")
        with open(file_path, 'r', encoding='utf-8') as f: lines = f.readlines()
        
        fname = os.path.basename(file_path)
        parts = fname.replace("SecretNote_", "").replace(".md", "").split("_")
        
        # 기본 데이터 구조
        data = {
            "mode": parts[0].upper() if parts else "MORNING", 
            "date": parts[1] if len(parts) > 1 else datetime.now().strftime("%Y.%m.%d"),
            "headline": "웨일 헌터의 시크릿 노트",
            "sentiment": "N/A", "monologue": "",
            "metrics": {"btc_price": "-", "kimp": "-", "funding": "-"}, 
            "strategies": [], "news": [],
            "commander": "Unknown Bot" # [NEW] 사령관 정보 기본값
        }

        current_section = None
        reading_mode = None 

        for line in lines:
            line = line.strip()
            
            # [NEW] 사령관 정보 파싱 (파일 상단 '사령관:' 찾기)
            if "사령관:" in line:
                # 예: 날짜: 2025.12.12 | 시간: MORNING | 사령관: Hunter (하이에나)
                segments = line.split("|")
                for seg in segments:
                    if "사령관" in seg:
                        data['commander'] = seg.split(":")[1].strip()

            # 1. 섹션 감지
            if "헌터의 대시보드" in line: current_section = "DASHBOARD"; continue
            elif "전술 시뮬레이션" in line or "Top Picks" in line: current_section = "STRATEGY"; continue
            elif "글로벌 첩보" in line: current_section = "NEWS"; reading_mode = None; continue
            elif "최종 결론" in line: current_section = "VERDICT"; continue

            # 2. 헤드라인 & 센티먼트
            if line.startswith("# ") and "웨일 헌터" not in line:
                data['headline'] = self.clean_text(line.replace("# ", "").replace("🐋 ", ""))
            elif "고래 심리" in line and "Fear" in line:
                # 예: **현재: Fear** -> Fear 추출
                data['sentiment'] = self.clean_text(line.split(":", 1)[1])

            # 3. [DASHBOARD]
            if current_section == "DASHBOARD":
                if "BTC" in line and "|" in line and "가격" not in line:
                    parts = [self.clean_text(p) for p in line.split("|") if p.strip()]
                    if len(parts) >= 4:
                        data['metrics']['btc_price'] = parts[1].split("<")[0].split("(")[0].strip()
                        data['metrics']['kimp'] = parts[2].split("%")[0] + "%"
                        data['metrics']['funding'] = parts[3].split("<")[0].split("(")[0].strip()
                elif "헌터의 독백:" in line:
                    data['monologue'] = self.clean_text(line.split("헌터의 독백:")[-1])
                elif line.startswith(">") and "헌터의 독백" not in line:
                    data['monologue'] += " " + self.clean_text(line.replace(">", ""))

            # 4. [STRATEGY] (보내주신 최신 로직 적용)
            elif current_section == "STRATEGY":
                if line.startswith("|") and "전략명" not in line and "---" not in line:
                    parts = [self.clean_text(p) for p in line.split("|") if p.strip()]
                    
                    if len(parts) >= 5:
                        # [중요] 액션 가이드는 항상 마지막 컬럼
                        action_text = parts[-1].replace("<br>", "\n") 
                        
                        data['strategies'].append({
                            "name": parts[0], 
                            "pos": parts[1], 
                            "win": parts[2],
                            "ret": parts[3], 
                            "action": action_text
                        })

            # 5. [NEWS] (보내주신 최신 로직 적용)
            elif current_section == "NEWS":
                if line.startswith("## "): current_section = None; continue
                
                if re.match(r'^(###|\d+\.|\*\*|\-)', line) and "팩트" not in line and "뷰" not in line:
                    title = re.sub(r'^(###|\d+\.|\-)\s*', '', self.clean_text(line)).strip()
                    title = title.replace("[", "").replace("]", "")
                    if len(title) > 5:
                        data['news'].append({"title": title, "fact": "", "view": ""})
                        reading_mode = None
                
                elif "팩트:" in line:
                    reading_mode = "FACT"
                    content = line.split("팩트:", 1)[1].strip()
                    if data['news']: data['news'][-1]['fact'] = content
                
                elif "뷰:" in line or "시선:" in line:
                    reading_mode = "VIEW"
                    content = line.split(":", 1)[1].strip()
                    if data['news']: data['news'][-1]['view'] = content
                
                elif reading_mode and line and not line.startswith("-") and not line.startswith("*"):
                    if data['news']:
                        if reading_mode == "FACT": data['news'][-1]['fact'] += " " + line
                        elif reading_mode == "VIEW": data['news'][-1]['view'] += " " + line

        return data

    def clean_text(self, text):
        return text.replace("**", "").replace("__", "").strip()

    # --------------------------------------------------------------------------
    # [핵심 2] 표지 생성 (사령관 배지 추가)
    # --------------------------------------------------------------------------
    def create_cover(self, data, save_path):
        bg_path = self.selected_cover_bg
        try: img = Image.open(bg_path).convert("RGBA")
        except: img = Image.new('RGB', (1080, 1080), (20, 20, 30))
        draw = ImageDraw.Draw(img)
        
        # 1. 사령관 배지 (제목 위에 표시)
        commander_text = f"🤖 오늘의 지휘관: {data.get('commander', 'System')}"
        # 배지 배경 계산
        bbox = draw.textbbox((0, 0), commander_text, font=self.font_header)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]
        badge_x, badge_y = 100, 250
        padding = 15
        
        # 반투명 검정 배경
        draw.rectangle(
            [badge_x - padding, badge_y - padding, badge_x + text_w + padding, badge_y + text_h + padding],
            fill=(0, 0, 0, 180), outline=self.color_green, width=3
        )
        draw.text((badge_x, badge_y), commander_text, font=self.font_header, fill=self.color_green)

        # 2. 헤드라인
        headline = data['headline']
        lines = textwrap.wrap(headline, width=14)
        y_text = 400 # 배지 아래
        
        for line in lines[:3]:
            draw.text((100, y_text), line, font=self.font_title, fill="white")
            y_text += 100
            
        # 3. 날짜
        date_str = f"{data['date']} | {data['mode']}"
        draw.text((100, 150), date_str, font=self.font_small, fill="#AAAAAA")

        img.save(save_path)
        print("✅ [Card 1] 표지 생성 완료")

    # --------------------------------------------------------------------------
    # [핵심 3] 전략 카드 생성 (사령관 모드 표시 + 최신 줄바꿈 로직)
    # --------------------------------------------------------------------------
    def create_strategy_card(self, strat, idx, save_path, commander_name):
        bg_path = self.selected_body_bg
        try: img = Image.open(bg_path).convert("RGBA")
        except: img = Image.new('RGB', (1080, 1080), (255, 255, 255))
        draw = ImageDraw.Draw(img)
        
        # 1. 타이틀
        draw.text((80, 80), f"⚔️ 추천 전략 #{idx}", font=self.font_header, fill=self.color_purple)
        
        # [NEW] 사령관 모드 표시 (Strategy 텍스트 대신)
        sub_title = f"Commander Mode: [{commander_name.split('(')[0]}] Active 🟢"
        draw.text((80, 150), sub_title, font=self.font_accent, fill=self.color_green)
        
        # 2. 전략명
        draw.text((80, 250), strat['name'], font=self.font_header, fill=self.color_accent)
        
        # 3. 포지션 (롱/숏)
        pos_color = self.color_green if "롱" in strat['pos'] or "LONG" in strat['pos'] else self.color_red
        draw.rectangle([(80, 350), (400, 420)], fill=pos_color)
        draw.text((110, 365), strat['pos'], font=self.font_accent, fill=self.color_white)
        
        # 4. 통계
        stats = f"승률: {strat['win']}  |  수익: {strat['ret']}"
        draw.text((80, 480), stats, font=self.font_accent, fill=self.color_gray)
        
        # 5. 액션 가이드 박스
        draw.rectangle([(60, 600), (1020, 950)], outline="#DDDDDD", width=4, fill="#F9F9F9")
        
        # [NEW] 보내주신 텍스트 줄바꿈 로직 적용
        raw_actions = strat['action'].split("\n")
        wrapped_lines = []
        for line in raw_actions:
            # 한 줄에 32자 정도가 적당 (폰트 크기에 따라 조절)
            wrapped = textwrap.wrap(line, width=32) 
            wrapped_lines.extend(wrapped)

        y = 650
        for i, line in enumerate(wrapped_lines):
            if y > 920: break 
            
            # 첫 번째 줄에는 체크 표시, 나머지는 공백
            prefix = "✔ " if i == 0 or (len(raw_actions) > i and line == raw_actions[i] if i < len(raw_actions) else False) else "  "
            # 단순화를 위해 그냥 텍스트만 찍음 (이미 raw_actions가 잘 나뉘어 있다면)
            
            draw.text((100, y), line, font=self.font_body, fill=self.color_bg_text)
            y += 50 # 줄간격

        img.save(save_path)
        print(f"✅ [Card 3-{idx}] 전략 카드 생성 완료")

    # --------------------------------------------------------------------------
    # 나머지 카드 생성 메서드 (대시보드, 뉴스) - 기존 유지 or 기본 틀
    # --------------------------------------------------------------------------
    def create_dashboard_card(self, data, save_path):
        # (기존 로직이 있다면 그대로 사용하시면 됩니다. 여기선 간략화)
        bg_path = self.selected_body_bg
        try: img = Image.open(bg_path).convert("RGBA")
        except: img = Image.new('RGB', (1080, 1080), (255, 255, 255))
        draw = ImageDraw.Draw(img)
        
        draw.text((80, 100), "📊 헌터의 대시보드", font=self.font_header, fill=self.color_purple)
        metrics = data['metrics']
        draw.text((100, 300), f"BTC: {metrics['btc_price']}", font=self.font_body, fill="black")
        draw.text((100, 400), f"김프: {metrics['kimp']}", font=self.font_body, fill="black")
        
        # 독백
        if data['monologue']:
            lines = textwrap.wrap(data['monologue'], width=30)
            y = 600
            for line in lines[:5]:
                draw.text((100, y), line, font=self.font_body, fill=self.color_gray)
                y += 50
        img.save(save_path)

    def create_news_card(self, news_item, idx, save_path):
        bg_path = self.selected_body_bg
        try: img = Image.open(bg_path).convert("RGBA")
        except: img = Image.new('RGB', (1080, 1080), (255, 255, 255))
        draw = ImageDraw.Draw(img)
        
        draw.text((80, 80), f"🌍 글로벌 첩보 #{idx}", font=self.font_header, fill=self.color_purple)
        
        # 제목
        lines = textwrap.wrap(news_item['title'], width=20)
        y = 200
        for line in lines:
            draw.text((80, y), line, font=self.font_header, fill="black")
            y += 70
            
        # 팩트 & 뷰
        y += 50
        draw.text((80, y), "[FACT]", font=self.font_accent, fill=self.color_accent)
        y += 50
        for line in textwrap.wrap(news_item['fact'], width=35)[:4]:
            draw.text((80, y), line, font=self.font_body, fill="#555555")
            y += 45
            
        y += 50
        draw.text((80, y), "[VIEW]", font=self.font_accent, fill=self.color_green)
        y += 50
        for line in textwrap.wrap(news_item['view'], width=35)[:4]:
            draw.text((80, y), line, font=self.font_body, fill="#555555")
            y += 45
            
        img.save(save_path)

    def get_latest_note(self):
        # 최신 MD 파일 찾기
        files = list(DATA_DIR.glob("SecretNote_*.md"))
        if not files: return None
        return max(files, key=os.path.getctime)

    def run(self):
        print("🏭 [콘텐츠 공장] 카드뉴스 생산 가동...")
        md_file = self.get_latest_note()
        if not md_file: 
            print("❌ 시크릿 노트 파일이 없습니다.")
            return
        
        data = self.parse_markdown(md_file)
        commander = data.get('commander', 'Unknown')
        
        print(f"📄 파싱 완료: {data['headline']} (사령관: {commander})")
        
        save_dir = OUTPUT_DIR / datetime.now().strftime("%Y-%m-%d")
        save_dir.mkdir(parents=True, exist_ok=True)
        
        # 1. 표지
        self.create_cover(data, save_dir / "01_cover.png")
        
        # 2. 대시보드
        self.create_dashboard_card(data, save_dir / "02_dashboard.png")
        
        # 3. 전략 카드 (Top 3)
        for i, strat in enumerate(data['strategies'][:3], 1):
            self.create_strategy_card(strat, i, save_dir / f"03_strategy_{i}.png", commander)
            
        # 4. 뉴스 카드 (Top 3)
        for i, news in enumerate(data['news'][:3], 1):
            self.create_news_card(news, i, save_dir / f"04_news_{i}.png")
            
        print(f"✨ 모든 카드뉴스 생성 완료: {save_dir}")

if __name__ == "__main__":
    factory = CardNewsFactory()
    factory.run()