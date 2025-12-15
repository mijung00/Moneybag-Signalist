import os
import re
import random
import textwrap
from datetime import datetime
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

# [설정] 경로 및 디렉토리 세팅
BASE_DIR = Path(__file__).resolve().parents[3]
ASSET_DIR = BASE_DIR / "moneybag" / "assets"
DATA_DIR = BASE_DIR / "moneybag" / "data" / "out"
OUTPUT_DIR = DATA_DIR / "cardnews"

class CardNewsFactory:
    def __init__(self):
        # 1. 폰트 경로 설정
        self.path_bold = str(ASSET_DIR / "Bold.ttf")
        self.path_medium = str(ASSET_DIR / "Medium.ttf")
        
        # 2. 색상 팔레트
        self.color_bg_text = "#333333"
        self.color_accent = "#6200EE"
        self.color_purple = "#6200EE"
        self.color_green = "#00C853"
        self.color_red = "#D50000"
        self.color_white = "#FFFFFF"
        self.color_gray = "#666666"
        self.color_primary = "#FF5F00"

        # 3. 폰트 로드
        try:
            self.font_title = ImageFont.truetype(self.path_bold, 70)
            self.font_header = ImageFont.truetype(self.path_bold, 50)
            self.font_body = ImageFont.truetype(self.path_medium, 32)
            self.font_small = ImageFont.truetype(self.path_medium, 26)
            self.font_accent = ImageFont.truetype(self.path_bold, 40)
            self.font_mini = ImageFont.truetype(self.path_medium, 22)
        except:
            print(f"⚠️ 폰트 로드 실패. 기본 폰트를 사용합니다.")
            self.font_title = ImageFont.load_default()
            self.font_header = ImageFont.load_default()
            self.font_body = ImageFont.load_default()
            self.font_small = ImageFont.load_default()
            self.font_accent = ImageFont.load_default()
            self.font_mini = ImageFont.load_default()

        # 4. 템플릿 이미지 경로
        self.selected_cover_bg = str(ASSET_DIR / "cover_01.png")
        self.selected_body_bg = str(ASSET_DIR / "body_01.png")

    # --------------------------------------------------------------------------
    # [핵심 1] MD 파일 파싱 (오류 수정됨)
    # --------------------------------------------------------------------------
    def parse_markdown(self, file_path):
        print(f"📂 파일 파싱 시작: {file_path.name}")
        with open(file_path, 'r', encoding='utf-8') as f: lines = f.readlines()
        
        fname = os.path.basename(file_path)
        parts = fname.replace("SecretNote_", "").replace(".md", "").split("_")
        
        # 모드 추출 (MORNING / NIGHT)
        mode = parts[0].upper() if parts else "MORNING"
        # 👇 가져온 날짜에서 점(.)을 하이픈(-)으로 강제 교체!
        raw_date = parts[1] if len(parts) > 1 else datetime.now().strftime("%Y-%m-%d")
        date = raw_date.replace(".", "-")
        
        data = {
            "mode": mode,
            "date": date,
            "headline": "웨일 헌터의 시크릿 노트",
            "metrics": {"btc_price": "-", "kimp": "-", "funding": "-"}, 
            "strategies": [], "news": [],
            "commander": "Unknown Bot",
            "monologue": ""
        }

        current_section = None
        reading_mode = None 

        for line in lines:
            line = line.strip()
            
            if "사령관:" in line:
                segments = line.split("|")
                for seg in segments:
                    if "사령관" in seg:
                        try: data['commander'] = seg.split(":")[1].strip()
                        except: pass

            # 1. 섹션 감지
            if "헌터의 대시보드" in line: current_section = "DASHBOARD"; continue
            elif "전술 시뮬레이션" in line or "Top Picks" in line: current_section = "STRATEGY"; continue
            elif "글로벌 첩보" in line: current_section = "NEWS"; reading_mode = None; continue
            elif "최종 결론" in line: current_section = "VERDICT"; continue

            # 2. 헤드라인
            if line.startswith("# ") and "웨일 헌터" not in line:
                data['headline'] = self.clean_text(line.replace("# ", "").replace("🐋 ", ""))

            # 3. [DASHBOARD]
            if current_section == "DASHBOARD":
                if "BTC" in line and "|" in line and "가격" not in line:
                    parts = [self.clean_text(p) for p in line.split("|") if p.strip()]
                    if len(parts) >= 4:
                        data['metrics']['btc_price'] = parts[1].split("<")[0].split("(")[0].strip()
                        data['metrics']['kimp'] = parts[2].split("%")[0] + "%"
                elif "헌터의 독백:" in line:
                    data['monologue'] = self.clean_text(line.split("헌터의 독백:")[-1])
                elif line.startswith(">") and "헌터의 독백" not in line:
                    data['monologue'] += " " + self.clean_text(line.replace(">", ""))

            # 4. [STRATEGY] 파싱 오류 수정 (인덱스 보정)
            elif current_section == "STRATEGY":
                # 표: | 순위 | 전략명 | 유형 | 점수 | 설명 |
                if line.startswith("|") and "전략명" not in line and "---" not in line:
                    cols = [self.clean_text(p) for p in line.split("|") if p.strip()]
                    
                    if len(cols) >= 5:
                        # cols[0]: 순위, cols[1]: 전략명, cols[2]: 유형(포지션), cols[3]: 점수, cols[4]: 설명
                        action_text = cols[-1].replace("<br>", "\n") 
                        data['strategies'].append({
                            "name": cols[1], # [수정] 전략명 위치
                            "pos": cols[2],  # [수정] 포지션 위치
                            "win": cols[3],  # 점수/승률
                            "ret": "",       # 수익률 (필요시 추가 파싱)
                            "action": action_text
                        })

            # 5. [NEWS]
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
    # 카드 생성 로직 (기존 배경 활용 + 텍스트만 얹기)
    # --------------------------------------------------------------------------
    def create_cover(self, data, save_path):
        bg_path = self.selected_cover_bg
        try: img = Image.open(bg_path).convert("RGBA")
        except: 
            img = Image.new('RGB', (1080, 1080), (255, 255, 255))
            draw = ImageDraw.Draw(img)
            draw.rectangle([(0,0), (1080,1080)], outline=self.color_primary, width=30)
        
        draw = ImageDraw.Draw(img)
        
        # 1. 사령관 배지
        commander_text = f"🤖 오늘의 지휘관: {data.get('commander', 'System')}"
        draw.text((100, 250), commander_text, font=self.font_header, fill=self.color_green)

        # 2. 헤드라인
        headline = data['headline']
        lines = textwrap.wrap(headline, width=14)
        y_text = 400
        for line in lines[:3]:
            draw.text((100, y_text), line, font=self.font_title, fill=self.color_bg_text) # 배경이 흰색이면 검은 글씨
            y_text += 100
            
        # 3. 날짜
        date_str = f"{data['date']} | {data['mode']}"
        draw.text((100, 150), date_str, font=self.font_small, fill=self.color_gray)

        img.save(save_path)

    def create_strategy_card(self, strat, idx, save_path, commander_name):
        bg_path = self.selected_body_bg
        try: img = Image.open(bg_path).convert("RGBA")
        except: 
            img = Image.new('RGB', (1080, 1080), (255, 255, 255))
            draw = ImageDraw.Draw(img)
            draw.rectangle([(0,0), (1080,1080)], outline=self.color_primary, width=30)

        draw = ImageDraw.Draw(img)
        
        # 타이틀
        draw.text((80, 80), f"⚔️ 추천 전략 #{idx}", font=self.font_header, fill=self.color_purple)
        sub_title = f"Commander Mode: [{commander_name.split('(')[0]}] Active 🟢"
        draw.text((80, 150), sub_title, font=self.font_accent, fill=self.color_green)
        
        # 전략명
        draw.text((80, 250), strat['name'], font=self.font_header, fill=self.color_accent)
        
        # 포지션
        pos_color = self.color_green if "롱" in strat['pos'] or "LONG" in strat['pos'] else self.color_red
        draw.rectangle([(80, 350), (400, 420)], fill=pos_color)
        draw.text((110, 365), strat['pos'], font=self.font_accent, fill=self.color_white)
        
        # 통계
        stats = f"점수: {strat['win']}" # 수익률 필드가 비어있을 수 있어 점수로 대체
        draw.text((80, 480), stats, font=self.font_accent, fill=self.color_gray)
        
        # 가이드 박스
        draw.rectangle([(60, 600), (1020, 950)], outline="#DDDDDD", width=4, fill="#F9F9F9")
        raw_actions = strat['action'].split("\n")
        y = 650
        for line in raw_actions:
            if y > 920: break 
            wrapped = textwrap.wrap(line, width=32)
            for w_line in wrapped:
                draw.text((100, y), w_line, font=self.font_body, fill=self.color_bg_text)
                y += 50

        img.save(save_path)

    def create_dashboard_card(self, data, save_path):
        bg_path = self.selected_body_bg
        try: img = Image.open(bg_path).convert("RGBA")
        except: img = Image.new('RGB', (1080, 1080), (255, 255, 255))
        draw = ImageDraw.Draw(img)
        
        draw.text((80, 100), "📊 헌터의 대시보드", font=self.font_header, fill=self.color_purple)
        metrics = data['metrics']
        draw.text((100, 300), f"BTC: {metrics['btc_price']}", font=self.font_body, fill="black")
        draw.text((100, 400), f"김프: {metrics['kimp']}", font=self.font_body, fill="black")
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
        lines = textwrap.wrap(news_item['title'], width=20)
        y = 200
        for line in lines:
            draw.text((80, y), line, font=self.font_header, fill="black")
            y += 70
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
        print(f"📄 파싱 완료: {data['headline']} (모드: {data['mode']})")
        
        # [핵심 수정] 저장 경로 분리 (날짜/모드)
        # 예: .../2025-12-15/morning/
        save_dir = OUTPUT_DIR / data['date'] / data['mode'].lower()
        save_dir.mkdir(parents=True, exist_ok=True)
        
        self.create_cover(data, save_dir / "01_cover.png")
        self.create_dashboard_card(data, save_dir / "02_dashboard.png")
        for i, strat in enumerate(data['strategies'][:3], 1):
            self.create_strategy_card(strat, i, save_dir / f"03_strategy_{i}.png", commander)
        for i, news in enumerate(data['news'][:3], 1):
            self.create_news_card(news, i, save_dir / f"04_news_{i}.png")
            
        print(f"✨ 모든 카드뉴스 생성 완료: {save_dir}")

if __name__ == "__main__":
    factory = CardNewsFactory()
    factory.run()