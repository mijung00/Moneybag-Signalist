import os
import re
import random
import textwrap
from datetime import datetime
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

BASE_DIR = Path(__file__).resolve().parents[3]
ASSET_DIR = BASE_DIR / "moneybag" / "assets"
DATA_DIR = BASE_DIR / "moneybag" / "data" / "out"
OUTPUT_DIR = DATA_DIR / "cardnews"

class CardNewsFactory:
    def __init__(self):
        self.path_bold = str(ASSET_DIR / "Bold.ttf")
        self.path_medium = str(ASSET_DIR / "Medium.ttf")
        
        try:
            self.font_title = ImageFont.truetype(self.path_bold, 70)
            self.font_header = ImageFont.truetype(self.path_bold, 50)
            self.font_body = ImageFont.truetype(self.path_medium, 32) # [수정] 34 -> 32px 축소
            self.font_small = ImageFont.truetype(self.path_medium, 26) # [수정] 28 -> 26px 축소
            self.font_accent = ImageFont.truetype(self.path_bold, 40)
            self.font_mini = ImageFont.truetype(self.path_medium, 22)
        except:
            print("⚠️ 폰트 로드 실패. 기본 폰트 사용")
            self.font_title = ImageFont.load_default()
            self.font_header = ImageFont.load_default()
            self.font_body = ImageFont.load_default()
            self.font_small = ImageFont.load_default()
            self.font_accent = ImageFont.load_default()
            self.font_mini = ImageFont.load_default()

        self.color_bg_text = "#1A1A2E" 
        self.color_accent = "#060318"
        self.color_title = "#1A1A2E" 
        self.color_gray = "#555555" 
        self.color_red = "#E74C3C"
        self.color_green = "#27AE60"
        self.color_white = "#FFFFFF"
        self.color_purple = "#240522"

        self.selected_cover_bg, self.selected_body_bg = self.select_theme()

    def select_theme(self):
        covers = list(ASSET_DIR.glob("cover_*.png"))
        if not covers: return None, None
        selected_cover = random.choice(covers)
        theme_num = selected_cover.name.split("_")[1]
        selected_body = ASSET_DIR / f"body_{theme_num}"
        if not selected_body.exists():
            bodies = list(ASSET_DIR.glob("body_*.png"))
            selected_body = random.choice(bodies) if bodies else None
        return selected_cover, selected_body

    def get_latest_note(self):
        files = sorted(DATA_DIR.glob("SecretNote_*.md"), key=os.path.getmtime, reverse=True)
        return files[0] if files else None

    def clean_text(self, text):
        """[강력한 클리너] 마크다운 볼드체, 백틱, 불필요한 공백 제거"""
        text = text.replace("**", "").replace("`", "").replace("##", "")
        # [NEW] 괄호 안에 있는 변동률 등 지저분한 것 정리 (필요시)
        return text.strip()

    def parse_markdown(self, file_path):
        print(f"📂 파일 파싱 시작: {file_path.name}")
        with open(file_path, 'r', encoding='utf-8') as f: lines = f.readlines()
        
        fname = os.path.basename(file_path)
        parts = fname.replace("SecretNote_", "").replace(".md", "").split("_")
        
        data = {
            "mode": parts[0].upper(), "date": parts[1],
            "headline": "웨일 헌터의 시크릿 노트",
            "sentiment": "N/A", "monologue": "",
            "metrics": {"btc_price": "-", "kimp": "-", "funding": "-"}, 
            "strategies": [], "news": []
        }

        current_section = None
        # [NEW] 뉴스 내용(팩트/뷰)을 여러 줄 읽기 위한 변수들
        reading_mode = None 

        for line in lines:
            line = line.strip()
            
            # 1. 섹션 감지
            if "헌터의 대시보드" in line: current_section = "DASHBOARD"; continue
            elif "전술 시뮬레이션" in line or "Top Picks" in line: current_section = "STRATEGY"; continue
            elif "글로벌 첩보" in line: current_section = "NEWS"; reading_mode = None; continue
            elif "최종 결론" in line: current_section = "VERDICT"; continue

            # 2. 헤드라인 & 센티먼트
            if line.startswith("# ") and "웨일 헌터" not in line:
                data['headline'] = self.clean_text(line.replace("# ", "").replace("🐋 ", ""))
            elif "고래 심리" in line and "Fear" in line:
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

            # 4. [STRATEGY] (여기가 문제였음! 🚨)
            elif current_section == "STRATEGY":
                if line.startswith("|") and "전략명" not in line and "---" not in line:
                    parts = [self.clean_text(p) for p in line.split("|") if p.strip()]
                    
                    # [수정 포인트] 칸 개수가 5개든 6개든, '액션가이드'는 무조건 맨 마지막에 있음!
                    # parts[4] 대신 parts[-1]을 사용하면 됨
                    if len(parts) >= 5:
                        action_text = parts[-1].replace("<br>", "\n") # parts[-1] = 리스트의 맨 마지막 요소
                        
                        data['strategies'].append({
                            "name": parts[0], 
                            "pos": parts[1], 
                            "win": parts[2],
                            "ret": parts[3], 
                            "action": action_text # 이제 31회가 아니라 진짜 가이드가 들어갑니다
                        })

            # 5. [NEWS] (줄바꿈 내용 읽기 강화 버전)
            elif current_section == "NEWS":
                if line.startswith("## "): current_section = None; continue
                
                # 뉴스 제목
                if re.match(r'^(###|\d+\.|\*\*|\-)', line) and "팩트" not in line and "뷰" not in line:
                    title = re.sub(r'^(###|\d+\.|\-)\s*', '', self.clean_text(line)).strip()
                    title = title.replace("[", "").replace("]", "")
                    if len(title) > 5:
                        data['news'].append({"title": title, "fact": "", "view": ""})
                        reading_mode = None
                
                # 팩트
                elif "팩트:" in line:
                    reading_mode = "FACT"
                    content = line.split("팩트:", 1)[1].strip()
                    if data['news']: data['news'][-1]['fact'] = content
                
                # 뷰
                elif "뷰:" in line or "시선:" in line:
                    reading_mode = "VIEW"
                    content = line.split(":", 1)[1].strip()
                    if data['news']: data['news'][-1]['view'] = content
                
                # [중요] 내용 이어 붙이기 (여러 줄일 경우)
                elif reading_mode and line and not line.startswith("-") and not line.startswith("*"):
                    if data['news']:
                        if reading_mode == "FACT":
                            data['news'][-1]['fact'] += " " + line
                        elif reading_mode == "VIEW":
                            data['news'][-1]['view'] += " " + line

        return data

    def draw_text_centered(self, draw, text, font, y, color, width=1080):
        text_w = font.getlength(text)
        x = (width - text_w) / 2
        draw.text((x, y), text, font=font, fill=color)
        return y + font.size + 15

    # 1. 표지
    def create_cover(self, data, save_path):
        bg_path = self.selected_cover_bg
        try: img = Image.open(bg_path).convert("RGBA") if bg_path else Image.new('RGB', (1080, 1080), (240, 240, 240))
        except: img = Image.new('RGB', (1080, 1080), (240, 240, 240))
        draw = ImageDraw.Draw(img)
        
        self.draw_text_centered(draw, f"{data['date']} | {data['mode']} LOG", self.font_small, 150, self.color_gray)
        
        lines = textwrap.wrap(data['headline'], width=18)
        y = 400
        for line in lines:
            y = self.draw_text_centered(draw, line, self.font_title, y, self.color_title)
            
        if data['sentiment'] != "N/A":
            y = 800
            try: sent_val = int(''.join(filter(str.isdigit, data['sentiment'].split("/")[0])))
            except: sent_val = 50
            sent_color = self.color_red if sent_val <= 40 else (self.color_green if sent_val >= 60 else self.color_accent)
            self.draw_text_centered(draw, "🧠 Market Sentiment", self.font_small, y, self.color_gray)
            self.draw_text_centered(draw, data['sentiment'], self.font_accent, y+50, sent_color)
        img.save(save_path)

    # 2. 대시보드 (겹침 해결 및 디자인 개선)
    def create_dashboard_card(self, data, save_path):
        bg_path = self.selected_body_bg
        try: img = Image.open(bg_path).convert("RGBA") if bg_path else Image.new('RGB', (1080, 1080), (255, 255, 255))
        except: img = Image.new('RGB', (1080, 1080), (255, 255, 255))
        draw = ImageDraw.Draw(img)
        
        draw.text((80, 100), "🔍 헌터의 상황판 (Market View)", font=self.font_header, fill=self.color_accent)
        
        # 메트릭 박스 (높이 살짝 줄임)
        draw.rectangle([(80, 200), (1000, 350)], outline="#DDDDDD", width=3, fill="#F9F9F9")
        
        btc = data['metrics'].get('btc_price', '-')
        kimp = data['metrics'].get('kimp', '-')
        fund = data['metrics'].get('funding', '-')
        
        # 데이터 배치 (Y좌표 조정)
        draw.text((120, 230), "BTC 가격", font=self.font_small, fill=self.color_gray)
        draw.text((120, 280), btc, font=self.font_accent, fill=self.color_bg_text)
        
        draw.text((450, 230), "김프(Kimp)", font=self.font_small, fill=self.color_gray)
        kimp_color = self.color_red if "🔥" in kimp else self.color_bg_text
        draw.text((450, 280), kimp, font=self.font_accent, fill=kimp_color)
        
        draw.text((750, 230), "펀딩비", font=self.font_small, fill=self.color_gray)
        fund_color = self.color_bg_text
        try: 
            if float(fund.strip('%')) > 0.01: fund_color = self.color_red
        except: pass
        draw.text((750, 280), fund, font=self.font_accent, fill=fund_color)

        # 독백 (겹침 방지를 위해 Y좌표 내림)
        y = 420 
        draw.text((80, y), "💬 헌터's Comment", font=self.font_accent, fill=self.color_accent)
        y += 80
        
        monologue = data.get('monologue', '').strip()
        if not monologue: monologue = "특이사항 없음."
        
        # [수정] 독백 글자수 제한 (너무 길면 자름)
        monologue = textwrap.shorten(monologue, width=150, placeholder="...")
        
        lines = textwrap.wrap(monologue, width=28) # 폰트 줄어서 너비 늘림
        for line in lines:
            draw.text((100, y), line, font=self.font_body, fill=self.color_bg_text)
            y += 55
            
        draw.text((100, 950), "* 주요 지표: 김치프리미엄, 펀딩비, 고래활성도 종합 분석", font=self.font_mini, fill="#888888")
        img.save(save_path)

    # 3. 전략 카드 (업그레이드 버전)
    def create_strategy_card(self, strat, idx, save_path):
        bg_path = self.selected_body_bg
        try: img = Image.open(bg_path).convert("RGBA") if bg_path else Image.new('RGB', (1080, 1080), (255, 255, 255))
        except: img = Image.new('RGB', (1080, 1080), (255, 255, 255))
        draw = ImageDraw.Draw(img)
        
        draw.text((80, 100), f"⚔️ 추천 전략 #{idx}", font=self.font_header, fill=self.color_purple)
        draw.text((80, 250), "Strategy", font=self.font_small, fill="#AAAAAA")
        draw.text((80, 300), strat['name'], font=self.font_header, fill=self.color_accent)
        
        pos_color = self.color_green if "롱" in strat['pos'] else self.color_red
        draw.rectangle([(80, 400), (400, 470)], fill=pos_color)
        draw.text((110, 415), strat['pos'], font=self.font_accent, fill=self.color_white)
        
        stats = f"승률: {strat['win']}  |  수익: {strat['ret']}"
        draw.text((80, 520), stats, font=self.font_accent, fill=self.color_gray)
        
        draw.rectangle([(60, 650), (1020, 950)], outline="#DDDDDD", width=4, fill="#F9F9F9")
        
        # [수정] 텍스트가 길면 자동으로 줄바꿈 (Wrap) 처리
        # 기존: actions = strat['action'].split("\n") 
        # 변경: 원본 텍스트를 줄바꿈 기준으로 나누고, 각 줄이 너무 길면 다시 나눔
        raw_actions = strat['action'].split("\n")
        wrapped_lines = []
        for line in raw_actions:
            # 한 줄에 35자 정도가 적당 (폰트 크기에 따라 조절)
            wrapped = textwrap.wrap(line, width=35) 
            wrapped_lines.extend(wrapped)

        y = 700
        for i, line in enumerate(wrapped_lines):
            # 너무 많이 써서 칸을 넘어가면 중단 (디자인 깨짐 방지)
            if y > 920: 
                break 
            
            # 첫 번째 줄에는 체크 표시, 나머지는 들여쓰기
            prefix = "✔ " if i == 0 or (len(wrapped_lines) > 0 and line == wrapped_lines[0]) else "  " 
            # 위 로직은 단순화해서, 그냥 모든 줄에 체크를 붙이거나, 
            # 아니면 원본 줄바꿈 단위로 체크를 붙이는 게 나을 수 있음.
            # 여기서는 깔끔하게 '모든 줄' 말고 '의미 단위'로 체크가 붙게 수정이 필요하지만
            # 일단 안전하게 모든 줄에 그냥 텍스트만 출력하고, 원본 데이터에 이미 글머리기호가 있다면 그대로 둠
            
            draw.text((100, y), line, font=self.font_body, fill=self.color_bg_text)
            y += 50 # 줄간격 60 -> 50으로 살짝 좁힘 (내용 많이 넣기 위해)
            
        img.save(save_path)

    # 4. 뉴스 카드 (글자수 제한 대신 줄바꿈 적용 + 폰트 크기 조절)
    def create_news_card(self, news, idx, save_path):
        bg_path = self.selected_body_bg
        try: img = Image.open(bg_path).convert("RGBA") if bg_path else Image.new('RGB', (1080, 1080), (255, 255, 255))
        except: img = Image.new('RGB', (1080, 1080), (255, 255, 255))
        draw = ImageDraw.Draw(img)
        
        draw.text((80, 100), f"🌍 핵심 첩보 #{idx}", font=self.font_header, fill=self.color_accent)
        
        y = 220
        # 제목 (최대 3줄로 줄바꿈)
        title_lines = textwrap.wrap(news['title'], width=24)
        for i, line in enumerate(title_lines):
            if i >= 3: break # 3줄 넘어가면 자름
            draw.text((80, y), line, font=self.font_accent, fill=self.color_bg_text)
            y += 60
        
        # 팩트 박스
        y_fact_start = 420 # 위치 고정
        if news['fact']:
            # 박스 그리기
            draw.rectangle([(80, y_fact_start), (1000, y_fact_start + 250)], fill="#F0F0F0")
            draw.text((120, y_fact_start + 30), "🔍 FACT CHECK", font=self.font_mini, fill=self.color_accent)
            
            # 내용 줄바꿈 (width는 폰트 크기에 따라 조절 필요. 약 36~38자)
            fact_lines = textwrap.wrap(news['fact'], width=36) 
            y_text = y_fact_start + 80
            for i, line in enumerate(fact_lines):
                if y_text > y_fact_start + 220: break # 박스 넘어가면 자름
                draw.text((120, y_text), line, font=self.font_small, fill=self.color_bg_text)
                y_text += 40
            
        # 뷰 박스
        y_view_start = 700 # 위치 고정
        if news['view']:
            # 박스 그리기
            draw.rectangle([(80, y_view_start), (1000, y_view_start + 250)], fill="#E8F4FD")
            draw.text((120, y_view_start + 30), "👁️ HUNTER's VIEW", font=self.font_mini, fill=self.color_accent)
            
            # 내용 줄바꿈
            view_lines = textwrap.wrap(news['view'], width=36)
            y_text = y_view_start + 80
            for i, line in enumerate(view_lines):
                if y_text > y_view_start + 220: break # 박스 넘어가면 자름
                draw.text((120, y_text), line, font=self.font_small, fill=self.color_bg_text)
                y_text += 40

        img.save(save_path)
        print(f"✅ [Card 4-{idx}] 뉴스 카드 생성 완료")

    def run(self):
        print("🏭 [콘텐츠 공장] 카드뉴스 생산 가동...")
        md_file = self.get_latest_note()
        if not md_file: 
            print("❌ 시크릿 노트 파일이 없습니다.")
            return
        
        data = self.parse_markdown(md_file)
        print(f"📄 파싱 완료: {data['headline']}")
        print(f"   -> 전황: {data['metrics']}")
        print(f"   -> 전략: {len(data['strategies'])}개, 뉴스: {len(data['news'])}개")
        
        save_dir = OUTPUT_DIR / datetime.now().strftime("%Y-%m-%d")
        save_dir.mkdir(parents=True, exist_ok=True)
        
        self.create_cover(data, save_dir / "01_cover.png")
        self.create_dashboard_card(data, save_dir / "02_dashboard.png")
        
        for i, strat in enumerate(data['strategies'][:2]):
            self.create_strategy_card(strat, i+1, save_dir / f"03_strategy_{i+1}.png")
        for i, news in enumerate(data['news'][:3]):
            self.create_news_card(news, i+1, save_dir / f"04_news_{i+1}.png")
            
        print(f"✨ 카드뉴스 생산 완료: {save_dir}")

if __name__ == "__main__":
    factory = CardNewsFactory()
    factory.run()