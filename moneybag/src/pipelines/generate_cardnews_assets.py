import os
import re
import textwrap
from datetime import datetime
from pathlib import Path
import requests
from PIL import Image, ImageDraw, ImageFont

# [NEW] 고래 이동 추적을 위한 트래커 임포트
try:
    from moneybag.src.analyzers.whale_alert_tracker import WhaleAlertTracker
except ImportError:
    WhaleAlertTracker = None

# [설정] 경로 및 디렉토리 세팅
BASE_DIR = Path(__file__).resolve().parents[3]
ASSET_DIR = BASE_DIR / "moneybag" / "assets"
DATA_DIR = BASE_DIR / "moneybag" / "data" / "out"
OUTPUT_DIR = DATA_DIR / "cardnews"

# --- [NEW] 다크모드 색상 팔레트 ---
C_BG_DARK = (25, 28, 36)
C_BG_CARD = (38, 42, 53)
C_TEXT_LIGHT = (235, 235, 245)
C_TEXT_DIM = (150, 150, 160)
C_ACCENT_PURPLE = (180, 110, 255)
C_ACCENT_GREEN = (0, 200, 83)
C_ACCENT_RED = (213, 0, 0)
C_ACCENT_BLUE = (41, 98, 255)

class CardNewsFactory:
    def __init__(self):
        # 1. 폰트 경로 설정
        self.path_bold = str(ASSET_DIR / "Bold.ttf")
        self.path_medium = str(ASSET_DIR / "Medium.ttf")
        
        # 3. 폰트 로드
        try:
            self.font_title = ImageFont.truetype(self.path_bold, 70)
            self.font_header = ImageFont.truetype(self.path_bold, 50)
            self.font_body = ImageFont.truetype(self.path_medium, 36)
            self.font_small = ImageFont.truetype(self.path_medium, 28)
            self.font_accent = ImageFont.truetype(self.path_bold, 40)
            self.font_mini = ImageFont.truetype(self.path_medium, 22)
        except:
            print(f"⚠️ 폰트 로드 실패. 기본 폰트를 사용합니다.")
            # Fallback to default fonts if custom ones fail
            self.font_title, self.font_header, self.font_body, self.font_small, self.font_accent, self.font_mini = [ImageFont.load_default()]*6

        # 4. 템플릿 이미지 경로 (다크모드용)
        self.selected_cover_bg = str(ASSET_DIR / "cover_dark_01.png")
        self.selected_body_bg = str(ASSET_DIR / "body_dark_01.png")

    def _create_base_image(self, bg_path_str):
        """템플릿을 로드하거나, 없을 경우 기본 다크 배경을 생성합니다."""
        try:
            return Image.open(bg_path_str).convert("RGBA")
        except FileNotFoundError:
            img = Image.new('RGB', (1080, 1080), C_BG_DARK)
            return img

    def _draw_text_centered(self, draw, text, font, center_x, y, color):
        bbox = draw.textbbox((0, 0), text, font=font)
        text_w = bbox[2] - bbox[0]
        draw.text((center_x - text_w / 2, y), text, font=font, fill=color)

    def _draw_gauge(self, draw, box, percentage, color):
        """게이지 바를 그립니다. (0-100)"""
        x, y, w, h = box
        draw.rounded_rectangle([x, y, x + w, y + h], fill=C_BG_CARD, radius=h//2)
        fill_w = w * (percentage / 100)
        draw.rounded_rectangle([x, y, x + fill_w, y + h], fill=color, radius=h//2)

    # --------------------------------------------------------------------------
    # [핵심 1] MD 파일 파싱 (대대적 개선)
    # --------------------------------------------------------------------------
    def parse_markdown(self, file_path):
        print(f"📂 파일 파싱 시작: {file_path.name}")
        with open(file_path, 'r', encoding='utf-8') as f: lines = f.readlines()
        md_text = "\n".join(lines)

        fname = os.path.basename(file_path)
        parts = fname.replace("SecretNote_", "").replace(".md", "").split("_")
        mode = parts[0].upper() if parts else "MORNING"
        raw_date = parts[1] if len(parts) > 1 else datetime.now().strftime("%Y-%m-%d")
        date = raw_date.replace(".", "-")

        data = {
            "mode": mode,
            "date": date,
            "headline": "웨일 헌터의 시크릿 노트",
            "dashboard_metrics": [],
            "scalping_map": [],
            "strategies": [],
            "news": [],
            "commander": "Unknown Bot",
            "monologue": "",
            "sentiment": 50,
        }

        # 정규식을 사용하여 각 섹션별 내용 추출
        def get_section(name):
            match = re.search(rf"## \d+\. .*?{name}.*?\n(.*?)(?=\n## \d+\. |\Z)", md_text, re.S)
            return match.group(1).strip() if match else ""

        # 헤드라인, 사령관
        headline_match = re.search(r"# 🐋 \[(.*?)\]", md_text)
        if headline_match: data['headline'] = headline_match.group(1)
        commander_match = re.search(r"사령관: (\w+)", md_text)
        if commander_match: data['commander'] = commander_match.group(1)

        # 대시보드 & 독백
        dashboard_content = get_section("대시보드")
        if dashboard_content:
            monologue_match = re.search(r"헌터의 독백:\s*(.*)", dashboard_content, re.S)
            if monologue_match: data['monologue'] = self.clean_text(monologue_match.group(1))
            
            sentiment_match = re.search(r"\*\*(\d+)\*\*", dashboard_content)
            if sentiment_match: data['sentiment'] = int(sentiment_match.group(1))

            table_matches = re.findall(r"\| \*\*(\w+)\*\* \|(.*?)\|(.*?)\|(.*?)\|(.*)\|\n", dashboard_content)
            for match in table_matches:
                data['dashboard_metrics'].append({
                    "coin": match[0], "price": self.clean_text(match[1]),
                    "kimp": self.clean_text(match[2]), "funding": self.clean_text(match[3]),
                    "volume": self.clean_text(match[4])
                })

        # 스캘핑 맵
        scalping_content = get_section("단타 전술")
        if scalping_content:
            table_matches = re.findall(r"\| \*\*(.*?)\*\* \|(.*?)\|(.*?)\|(.*?)\|(.*)\|\n", scalping_content)
            for match in table_matches:
                data['scalping_map'].append({
                    "coin": match[0], "price": self.clean_text(match[1]),
                    "support": self.clean_text(match[2]), "resistance": self.clean_text(match[3]),
                    "trend": self.clean_text(match[4])
                })

        # 추천 전략
        verdict_content = get_section("최종 결론")
        if verdict_content:
            strategy_matches = re.findall(r"\*\*(\d)\. \S+ (.*?)\*\*\n\s*-\s*\"(.*?)\"\n\s*-\s*가이드:\s*\((.*?)\)", verdict_content, re.S)
            for match in strategy_matches:
                data['strategies'].append({
                    "name": match[1], "appeal": match[2], "guide": match[3]
                })

        # 뉴스
        news_content = get_section("글로벌 첩보")
        if news_content:
            news_blocks = re.split(r'\n### \d+\. ', news_content)
            for block in news_blocks:
                if not block.strip(): continue
                title_match = re.search(r"\[(.*?)\]", block)
                fact_match = re.search(r"🔍 \*\*팩트:\*\* (.*?)\n", block, re.S)
                view_match = re.search(r"👁️ \*\*헌터의 뷰:\*\* (.*?)\n", block, re.S)
                if title_match:
                    data['news'].append({
                        "title": title_match.group(1).strip(),
                        "fact": fact_match.group(1).strip() if fact_match else "",
                        "view": view_match.group(1).strip() if view_match else ""
                    })

        return data

    def clean_text(self, text):
        return text.replace("**", "").replace("__", "").replace(">", "").replace("🔍", "").replace("👁️", "").strip()

    # --------------------------------------------------------------------------
    # [업그레이드] 카드 생성 로직 (다크모드 및 신규 카드 추가)
    # --------------------------------------------------------------------------
    def create_cover(self, data, save_path):
        img = self._create_base_image(self.selected_cover_bg)
        draw = ImageDraw.Draw(img)

        draw.text((80, 100), "SECRET NOTE", font=self.font_header, fill=C_TEXT_DIM)
        draw.text((80, 150), "THE WHALE HUNTER", font=self.font_title, fill=C_TEXT_LIGHT)

        headline = data['headline']
        lines = textwrap.wrap(headline, width=15)
        y_text = 300
        for line in lines[:3]:
            draw.text((80, y_text), line, font=self.font_title, fill=C_TEXT_LIGHT)
            y_text += 100

        date_str = f"{data['date']} | {data['mode']}"
        self._draw_text_centered(draw, date_str, self.font_small, 540, 950, C_TEXT_DIM)

        img.save(save_path)

    def create_commander_briefing_card(self, data, save_path):
        img = self._create_base_image(self.selected_body_bg)
        draw = ImageDraw.Draw(img)
        commander = data.get('commander', 'System')
        monologue = data.get('monologue', '시장을 관망합니다.')

        draw.text((80, 100), f"COMMANDER'S BRIEFING", font=self.font_header, fill=C_TEXT_DIM)
        draw.text((80, 160), f"“{commander}”", font=self.font_title, fill=C_ACCENT_PURPLE)
        
        y_text = 350
        wrapped_text = textwrap.wrap(f"{monologue}", width=28)
        for line in wrapped_text:
            self._draw_text_centered(draw, line, self.font_body, 540, y_text, C_TEXT_LIGHT)
            y_text += 60

        img.save(save_path)

    def create_whale_dashboard_card(self, data, save_path):
        img = self._create_base_image(self.selected_body_bg)
        draw = ImageDraw.Draw(img)
        draw.text((80, 100), "WHALE DASHBOARD", font=self.font_header, fill=C_TEXT_DIM)

        # 1. 고래 심리 지수
        sentiment = data.get('sentiment', 50)
        s_color = C_ACCENT_RED if sentiment < 45 else (C_ACCENT_GREEN if sentiment > 55 else C_TEXT_DIM)
        s_text = "공포" if sentiment < 45 else ("탐욕" if sentiment > 55 else "중립")
        draw.text((100, 200), "고래 심리 지수", font=self.font_accent, fill=C_TEXT_LIGHT)
        self._draw_gauge(draw, (100, 260, 880, 40), sentiment, s_color)
        self._draw_text_centered(draw, f"{s_text} ({sentiment})", self.font_body, 540, 320, s_color)

        # 2. 주요 지표 (김프, 펀딩비)
        y = 450
        for metric in data.get('dashboard_metrics', []):
            if metric['coin'] not in ['BTC', 'ETH']: continue
            
            draw.text((100, y), metric['coin'], font=self.font_accent, fill=C_TEXT_LIGHT)
            
            # 김프
            kimp_val = float(re.findall(r"[-+]?\d*\.\d+|\d+", metric['kimp'])[0])
            kimp_icon = "🔥" if kimp_val > 2.5 else ("🧊" if kimp_val < 0 else "")
            draw.text((400, y), f"김프: {kimp_val:.2f}% {kimp_icon}", font=self.font_body, fill=C_TEXT_DIM)

            # 펀딩비
            try:
                fund_val = float(re.findall(r"[-+]?\d*\.\d+|\d+", metric['funding'])[0])
                fund_text = "롱 우세" if fund_val > 0.01 else ("숏 우세" if fund_val < -0.01 else "중립")
                fund_color = C_ACCENT_GREEN if fund_val > 0 else C_ACCENT_RED
                draw.text((700, y), f"펀딩비: {fund_text}", font=self.font_body, fill=fund_color)
            except:
                draw.text((700, y), f"펀딩비: -", font=self.font_body, fill=C_TEXT_DIM)
            
            y += 100
        img.save(save_path)

    def create_whale_tracker_card(self, save_path):
        """[수정] Whale Alert API를 직접 호출하여 실제 고래 거래 내역을 시각화합니다."""
        img = self._create_base_image(self.selected_body_bg)
        draw = ImageDraw.Draw(img)
        draw.text((80, 100), "WHALE TRACKER", font=self.font_header, fill=C_TEXT_DIM)

        api_key = os.getenv("WHALE_ALERT_API_KEY")
        if not api_key:
            self._draw_text_centered(draw, "Whale Alert API 키 없음", self.font_body, 540, 500, C_TEXT_DIM)
            img.save(save_path)
            return

        try:
            response = requests.get(
                "https://api.whale-alert.io/v1/transactions",
                params={'api_key': api_key, 'limit': 5, 'min_value': 500000}, # 50만달러 이상 거래만
                timeout=15
            )
            response.raise_for_status()
            data = response.json()
            txs = data.get('transactions', [])
        except Exception as e:
            print(f"⚠️ Whale Alert API 호출 실패: {e}")
            self._draw_text_centered(draw, "고래 추적 데이터 로딩 실패", self.font_body, 540, 500, C_TEXT_DIM)
            img.save(save_path)
            return

        if not txs:
            self._draw_text_centered(draw, "최근 고래 움직임 없음", self.font_body, 540, 500, C_TEXT_DIM)
            img.save(save_path)
            return

        y = 250
        for tx in txs:
            amount_usd = tx.get('amount_usd', 0)
            amount_usd_str = f"${amount_usd:,.0f}"
            
            from_owner = tx['from'].get('owner', 'Unknown').capitalize()
            to_owner = tx['to'].get('owner', 'Unknown').capitalize()

            direction, icon, color = "이체", "↔️", C_TEXT_DIM
            if tx['to']['owner_type'] == 'exchange':
                direction, icon, color = "입금", "➡️", C_ACCENT_GREEN
            elif tx['from']['owner_type'] == 'exchange':
                direction, icon, color = "출금", "⬅️", C_ACCENT_RED
            
            line1 = f"{icon} {amount_usd_str} 규모"
            line2 = f"{from_owner} → {to_owner} ({direction})"
            
            draw.text((100, y), line1, font=self.font_accent, fill=color)
            draw.text((100, y + 55), line2, font=self.font_small, fill=C_TEXT_DIM)
            y += 150

        img.save(save_path)

    def create_scalping_map_card(self, data, save_path):
        img = self._create_base_image(self.selected_body_bg)
        draw = ImageDraw.Draw(img)
        draw.text((80, 100), "SCALPING MAP", font=self.font_header, fill=C_TEXT_DIM)

        y = 250
        for item in data.get('scalping_map', [])[:3]:
            try:
                price = float(item['price'].replace('$', '').replace(',', ''))
                support = float(item['support'].replace('$', '').replace(',', ''))
                resistance = float(item['resistance'].replace('$', '').replace(',', ''))
            except ValueError:
                continue

            draw.text((100, y), item['coin'], font=self.font_accent, fill=C_TEXT_LIGHT)

            chart_box_y = y + 50
            chart_height = 80
            total_range = resistance - support if resistance > support else 1
            
            # 저항선
            draw.line([(100, chart_box_y), (980, chart_box_y)], fill=C_ACCENT_RED, width=3)
            draw.text((100, chart_box_y - 30), f"저항 ${resistance:,.0f}", font=self.font_mini, fill=C_ACCENT_RED)
            # 지지선
            draw.line([(100, chart_box_y + chart_height), (980, chart_box_y + chart_height)], fill=C_ACCENT_GREEN, width=3)
            draw.text((100, chart_box_y + chart_height + 5), f"지지 ${support:,.0f}", font=self.font_mini, fill=C_ACCENT_GREEN)
            
            # 현재가 위치
            price_pos_y = (chart_box_y + chart_height) - ((price - support) / total_range) * chart_height
            price_pos_y = max(chart_box_y, min(price_pos_y, chart_box_y + chart_height))
            draw.line([(100, price_pos_y), (980, price_pos_y)], fill=C_TEXT_DIM, width=2, dash=[5, 5])
            draw.text((900, price_pos_y - 15), f"현재 ${price:,.0f}", font=self.font_mini, fill=C_TEXT_LIGHT)

            y += 200
        img.save(save_path)

    def create_strategy_card(self, strat, idx, news_list, save_path):
        img = self._create_base_image(self.selected_body_bg)
        draw = ImageDraw.Draw(img)

        draw.text((80, 100), f"RECOMMENDED STRATEGY #{idx}", font=self.font_header, fill=C_TEXT_DIM)
        draw.text((80, 180), strat['name'], font=self.font_title, fill=C_ACCENT_PURPLE)

        y = 300
        wrapped_appeal = textwrap.wrap(f"“{strat['appeal']}”", width=30)
        for line in wrapped_appeal:
            draw.text((100, y), line, font=self.font_body, fill=C_TEXT_LIGHT)
            y += 50

        y += 50
        draw.rectangle((80, y, 1000, y + 250), fill=C_BG_CARD)
        guide_y = y + 30
        draw.text((110, guide_y), "가이드:", font=self.font_accent, fill=C_TEXT_DIM)
        guide_y += 60
        for line in textwrap.wrap(strat['guide'], width=35):
            draw.text((110, guide_y), line, font=self.font_body, fill=C_TEXT_LIGHT)
            guide_y += 50

        # [NEW] 관련 첩보 연결
        if news_list:
            y = 800
            draw.text((80, y), "KEY INTELLIGENCE", font=self.font_small, fill=C_TEXT_DIM)
            y += 40
            for news in news_list[:2]:
                draw.text((80, y), f"• {textwrap.shorten(news['title'], width=50, placeholder='...')}", font=self.font_mini, fill=C_TEXT_DIM)
                y += 35

        img.save(save_path)

    def create_news_card(self, news_item, idx, save_path):
        img = self._create_base_image(self.selected_body_bg)
        draw = ImageDraw.Draw(img)
        draw.text((80, 100), f"GLOBAL INTELLIGENCE #{idx}", font=self.font_header, fill=C_TEXT_DIM)
        
        y = 200
        wrapped_title = textwrap.wrap(news_item['title'], width=25)
        for line in wrapped_title:
            draw.text((80, y), line, font=self.font_title, fill=C_TEXT_LIGHT)
            y += 80

        y += 30
        draw.text((80, y), "🔍 FACT", font=self.font_accent, fill=C_ACCENT_BLUE)
        y += 60
        for line in textwrap.wrap(news_item['fact'], width=35)[:4]:
            draw.text((100, y), line, font=self.font_body, fill=C_TEXT_DIM)
            y += 50
        
        y += 30
        draw.text((80, y), "👁️ VIEW", font=self.font_accent, fill=C_ACCENT_GREEN)
        y += 60
        for line in textwrap.wrap(news_item['view'], width=35)[:4]:
            draw.text((100, y), line, font=self.font_body, fill=C_TEXT_DIM)
            y += 50

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
        print(f"📄 파싱 완료: {data['headline']} (모드: {data['mode']})")
        
        save_dir = OUTPUT_DIR / data['date'] / data['mode'].lower()
        save_dir.mkdir(parents=True, exist_ok=True)
        
        # [NEW] 카드 생성 순서 변경 및 신규 카드 추가
        self.create_cover(data, save_dir / "01_cover.png")
        self.create_commander_briefing_card(data, save_dir / "02_commander_briefing.png")
        self.create_whale_dashboard_card(data, save_dir / "03_whale_dashboard.png")
        self.create_whale_tracker_card(save_dir / "04_whale_tracker.png")
        self.create_scalping_map_card(data, save_dir / "05_scalping_map.png")
        
        for i, strat in enumerate(data['strategies'][:2], 1): # 최대 2개 전략
            self.create_strategy_card(strat, i, data['news'], save_dir / f"06_strategy_{i}.png")
        
        for i, news in enumerate(data['news'][:2], 1): # 최대 2개 뉴스
            self.create_news_card(news, i, save_dir / f"07_news_{i}.png")
            
        print(f"✨ 모든 카드뉴스 생성 완료: {save_dir}")

if __name__ == "__main__":
    CardNewsFactory().run()
