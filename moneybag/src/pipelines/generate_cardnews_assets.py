import os
import re
import sys
import textwrap
from datetime import datetime
from pathlib import Path
import requests
from PIL import Image, ImageDraw, ImageFont

# [NEW] 고래 이동 추적을 위한 트래커 임포트
try: # [수정] Moralis 트래커를 사용하도록 변경
    from moneybag.src.analyzers.moralis_tracker import MoralisTracker
except ImportError:
    MoralisTracker = None

# [설정] 경로 및 디렉토리 세팅
BASE_DIR = Path(__file__).resolve().parents[3]
ASSET_DIR = BASE_DIR / "moneybag" / "assets"
DATA_DIR = BASE_DIR / "moneybag" / "data" / "out"
# [수정] .env 파일 로드 로직 추가
sys.path.append(str(BASE_DIR))
from common.env_loader import load_env
load_env(BASE_DIR)

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
    def parse_markdown(self, file_path: Path) -> dict:
        """
        [수정] daily_newsletter.py의 출력 형식에 맞춰 파서를 전면 수정합니다.
        """
        print(f"📂 파일 파싱 시작: {file_path.name}")
        with open(file_path, 'r', encoding='utf-8') as f:
            md_text = f.read()

        # [Fallback] 파일명에서 날짜와 모드 먼저 추출
        fname_match = re.search(r"SecretNote_(\w+)_(\d{4}\.\d{2}\.\d{2})", file_path.name)
        mode_from_fname, date_from_fname = ("unknown", "nodate")
        if fname_match:
            mode_from_fname = fname_match.group(1).upper()
            date_from_fname = fname_match.group(2).replace('.', '-')

        data = {
            "headline": "",
            "commander_name": "",
            "commander_quote": "",
            "dashboard_items": [],
            "scalping_map_items": [],
            "strategies": [],
            "news": [],
            "date": date_from_fname, # Fallback 값으로 초기화
            "mode": mode_from_fname, # Fallback 값으로 초기화
        }

        # 1. 헤드라인, 날짜, 모드, 사령관 이름 추출 (본문 우선)
        # [수정] LLM의 유연한 출력에 대응하기 위해 공백 처리를 강화한 정규식
        header_match = re.search(r'# 🐋 \[(.*?)\]\s*날짜:\s*(.*?)\s*\|\s*시간:\s*(.*?)\s*\|\s*사령관:\s*(.*?)\s*\n', md_text, re.S)
        if header_match:
            data['headline'] = header_match.group(1).strip()
            data['date'] = header_match.group(2).strip().replace('.', '-')
            data['mode'] = header_match.group(3).strip()
            data['commander_name'] = header_match.group(4).strip()
        else:
            # 헤더 파싱 실패 시, 헤드라인이라도 찾아본다.
            headline_match_fallback = re.search(r'# 🐋 (.*)', md_text)
            if headline_match_fallback:
                data['headline'] = headline_match_fallback.group(1).strip().replace('[','').replace(']','')

        # 섹션 분리
        sections = re.split(r'\n## \d+\. ', md_text)
        
        for section in sections[1:]:
            # 사령관 브리핑 (독백) & 대시보드
            if "헌터의 대시보드" in section:
                quote_match = re.search(r'> \*\*🗨️ 헌터의 독백:\*\* (.*?)(?=\n\n\*\*\[메이저\]\*\*|\n\n\*\*\[알트/밈\]\*\*|\Z)', section, re.S)
                if quote_match:
                    data['commander_quote'] = quote_match.group(1).strip()

                sentiment_match = re.search(r'현재: (.*?)\n.*?\*\*(\d+)\*\*', section, re.S)
                if sentiment_match:
                    data['dashboard_items'].append({"key": "고래 심리 지수", "value": f"{sentiment_match.group(1).strip()} ({sentiment_match.group(2).strip()})"})

                # [수정] 대시보드 테이블 파싱 로직 강화
                dashboard_table_match = re.search(r'\|\s*코인\s*\|.*?\|\n\|---.*?---\|\n(.*?)(?=\n\n|\Z)', section, re.S)
                if dashboard_table_match:
                    table_content = dashboard_table_match.group(1)
                    rows = re.findall(r'\| \*\*(.*?)\*\* \|(.*?)\|(.*?)\|(.*?)\|(.*)\|\n', table_content)
                    for coin, price, kimp, funding, _ in rows:
                        data['dashboard_items'].append({"key": coin.strip(), "value": price.strip().split('<br>')[0]})
                        data['dashboard_items'].append({"key": f"{coin.strip()} 김프", "value": kimp.strip()})
                        data['dashboard_items'].append({"key": f"{coin.strip()} 펀딩비", "value": funding.strip()})


            # 스캘핑 맵 (단타 전술)
            elif "단타 전술" in section:
                rows = re.findall(r'\| \*\*(.*?)\*\* \|.*?\|(.*?)\|(.*?)\|.*\n', section)
                for coin, support, resistance in rows:
                    data['scalping_map_items'].append({
                        "coin": coin.strip(),
                        "resistance": resistance.strip().replace('🔴', '').replace('**', '').strip(),
                        "support": support.strip().replace('🟢', '').replace('**', '').strip()
                    })

            # 전략 (최종 결론)
            elif "최종 결론" in section:
                # [수정] 가이드 내용이 여러 줄이거나 괄호가 없는 경우도 파싱하도록 정규식 수정
                strat_blocks = re.findall(r'\*\*(?:\d\. 🥇|🥈|🥉)\s*(.*?)\*\*\s*\n\s*-\s*"(.*?)"\s*\n\s*-\s*가이드:\s*(.*?)(?=\n\s*\*\*|\Z)', section, re.S)
                for name, appeal, guide_text in strat_blocks:
                    data['strategies'].append({
                        "name": name.strip(),
                        "appeal": appeal.strip(),
                        "guide": guide_text.strip()
                    })

            # 뉴스 (글로벌 첩보)
            elif "글로벌 첩보" in section:
                news_items = re.split(r'\n### \d+\. ', section)[1:]
                for item in news_items:
                    # [수정] 뉴스 제목, 팩트, 헌터의 뷰 모두 추출
                    title_match = re.search(r'\[(.*?)\]', item) # 뉴스 제목
                    fact_match = re.search(r'> 🔍 \*\*팩트:\*\* (.*?)\n', item, re.S) # 뉴스 요약 (팩트)
                    view_match = re.search(r'> 👁️ \*\*헌터의 뷰:\*\* (.*?)(?=\n\*Original:|\Z)', item, re.S) # 헌터의 뷰
                    if title_match and fact_match and view_match:
                        data['news'].append({
                            "title": title_match.group(1).strip(),
                            "summary": fact_match.group(1).strip(), # 팩트가 뉴스 요약
                            "hunter_view": view_match.group(1).strip() # 헌터의 뷰
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
        """[개선] 좌측 정렬 및 상단 여백 추가"""
        img = self._create_base_image(self.selected_body_bg)
        draw = ImageDraw.Draw(img)
        commander = data.get('commander_name', 'System')
        quote = data.get('commander_quote', '시장을 관망합니다.')

        y_start = 200 # 시작 위치를 아래로 내림
        draw.text((80, y_start), f"COMMANDER'S BRIEFING", font=self.font_header, fill=C_TEXT_DIM)
        draw.text((80, y_start + 70), f"“{commander}”", font=self.font_title, fill=C_ACCENT_PURPLE)
        
        y_text = y_start + 220
        wrapped_text = textwrap.wrap(f"{quote}", width=25)
        for line in wrapped_text:
            draw.text((100, y_text), line, font=self.font_body, fill=C_TEXT_LIGHT)
            y_text += 60

        img.save(save_path)

    def create_whale_dashboard_card(self, data, save_path):
        """[개선] 리스트 형식 파싱 및 각주 추가"""
        img = self._create_base_image(self.selected_body_bg)
        draw = ImageDraw.Draw(img)
        draw.text((80, 100), "WHALE DASHBOARD", font=self.font_header, fill=C_TEXT_DIM)

        y = 220
        # [수정] 고래 심리 지수 게이지 렌더링
        sentiment_value = 0
        for item in data.get('dashboard_items', []):
            key = item.get('key', '')
            value = item.get('value', '')
            if "고래 심리 지수" in key:
                sentiment_match = re.search(r'\((\d+)\)', value)
                if sentiment_match:
                    sentiment_value = int(sentiment_match.group(1))
                    sentiment_status = value.split('(')[0].strip()
                    
                    draw.text((100, y), "고래 심리 지수", font=self.font_accent, fill=C_TEXT_LIGHT)
                    self._draw_gauge(draw, (100, y + 50, 880, 40), sentiment_value, C_ACCENT_PURPLE)
                    self._draw_text_centered(draw, f"{sentiment_status} ({sentiment_value})", self.font_body, 540, y + 100, C_ACCENT_PURPLE)
                    y += 180 # 게이지 공간 확보
                continue
            # [수정] 아래 2열 배치 로직으로 통합되었으므로 이 부분의 개별 항목 그리기는 제거합니다.

        # [수정] 대시보드 항목이 너무 많을 경우를 대비하여 2열로 배치
        items_to_display = [item for item in data.get('dashboard_items', []) if "고래 심리 지수" not in item['key']]
        if items_to_display:
            col1_x = 100
            col2_x = 550
            current_y = y
            for i, item in enumerate(items_to_display):
                x_pos = col1_x if i % 2 == 0 else col2_x
                key = item.get('key', '')
                value = item.get('value', '')
                color = C_TEXT_LIGHT
                if "김프" in key: color = C_ACCENT_GREEN
                elif "펀딩비" in key: color = C_ACCENT_RED
                draw.text((x_pos, current_y), f"• {key}: {value}", font=self.font_body, fill=color)
                if i % 2 == 1: current_y += 70
            if len(items_to_display) % 2 == 0: current_y += 70

        # [수정] 고래 심리 지수 각주를 LLM 프롬프트에서 직접 생성하도록 변경했으므로, 여기서는 제거
        img.save(save_path)

    def create_whale_tracker_card(self, save_path):
        """[수정] Moralis API를 사용하여 실제 고래 거래 내역을 시각화합니다."""
        img = self._create_base_image(self.selected_body_bg)
        draw = ImageDraw.Draw(img)
        draw.text((80, 100), "WHALE TRACKER", font=self.font_header, fill=C_TEXT_DIM)
        draw.text((80, 160), "최근 12시간 $1M 이상 대규모 거래", font=self.font_small, fill=C_TEXT_DIM)
        if not MoralisTracker:
            self._draw_text_centered(draw, "Moralis 트래커 로드 실패", self.font_body, 540, 500, C_TEXT_DIM)
            img.save(save_path)
            return

        tracker = MoralisTracker()
        try:
            data = tracker.get_large_transactions(limit=5)
            txs = data.get('transactions', [])
        except Exception as e:
            print(f"⚠️ Moralis API 호출 실패: {e}")
            self._draw_text_centered(draw, "고래 추적 데이터 로딩 실패", self.font_body, 540, 500, C_TEXT_DIM)
            img.save(save_path)
            return

        if not txs:
            self._draw_text_centered(draw, "최근 대규모 움직임 없음", self.font_body, 540, 500, C_TEXT_DIM)
            img.save(save_path)
            return

        y = 250
        for tx in txs[:5]: # 최대 5개만 표시
            amount_usd = tx.get('amount_usd', 0)
            symbol = tx.get('symbol', '')
            amount_usd_str = f"{symbol} ${amount_usd:,.0f}"
            
            from_owner = tx['from'].get('owner', 'Unknown Wallet')
            to_owner = tx['to'].get('owner', 'Unknown Wallet')

            direction, icon, color = "이체", "↔️", C_TEXT_DIM
            if tx['to'].get('owner_type') == 'exchange' and 'Exchange' in to_owner:
                direction, icon, color = "입금", "➡️", C_ACCENT_GREEN
            elif tx['from'].get('owner_type') == 'exchange' and 'Exchange' in from_owner:
                direction, icon, color = "출금", "⬅️", C_ACCENT_RED
            
            line1 = f"{icon} {amount_usd_str} 규모"
            line2 = f"{from_owner} → {to_owner} ({direction})"
            
            draw.text((100, y), line1, font=self.font_accent, fill=color)
            draw.text((100, y + 55), line2, font=self.font_small, fill=C_TEXT_DIM)
            y += 150

        img.save(save_path)

    def create_scalping_map_card(self, data, save_path):
        """[개선] 새로운 MD 형식에 맞춰 파싱 및 렌더링"""
        img = self._create_base_image(self.selected_body_bg)
        draw = ImageDraw.Draw(img)
        draw.text((80, 100), "SCALPING MAP", font=self.font_header, fill=C_TEXT_DIM)

        y = 250
        items = data.get('scalping_map_items', [])
        if not items:
            self._draw_text_centered(draw, "데이터 없음", self.font_body, 540, 500, C_TEXT_DIM)
            img.save(save_path)
            return

        for item in items[:3]:
            coin = item.get('coin', '???')
            resistance = item.get('resistance', '0')
            support = item.get('support', '0')

            draw.text((100, y), coin, font=self.font_accent, fill=C_TEXT_LIGHT)
            draw.text((400, y), f"저항: {resistance}", font=self.font_body, fill=C_ACCENT_RED)
            draw.text((700, y), f"지지: {support}", font=self.font_body, fill=C_ACCENT_GREEN)
            y += 100

        img.save(save_path)

    def create_strategy_card(self, strat, idx, save_path):
        """[개선] 새로운 MD 형식에 맞춰 파싱 및 렌더링"""
        img = self._create_base_image(self.selected_body_bg)
        draw = ImageDraw.Draw(img)

        draw.text((80, 100), f"AI STRATEGY #{idx}", font=self.font_header, fill=C_TEXT_DIM)
        
        name = strat.get('name', '전략 이름 없음')
        appeal = strat.get('appeal', '매력 어필 없음')
        guide = strat.get('guide', '가이드 없음')

        draw.text((80, 180), name, font=self.font_title, fill=C_ACCENT_PURPLE)

        y = 300
        for line in textwrap.wrap(f"\"{appeal}\"\n\n가이드: {guide}", width=35):
            draw.text((100, y), line, font=self.font_body, fill=C_TEXT_DIM)
            y += 50

        img.save(save_path)

    def create_news_card(self, news_item, idx, save_path):
        """[개선] 텍스트 잘림 방지를 위해 width 조정"""
        img = self._create_base_image(self.selected_body_bg)
        draw = ImageDraw.Draw(img)
        draw.text((80, 100), f"GLOBAL INTELLIGENCE #{idx}", font=self.font_header, fill=C_TEXT_DIM)
        
        y = 200
        # 뉴스 제목
        # [수정] 제목이 카드 밖으로 나가는 것을 방지하기 위해 width를 23으로 조정
        wrapped_title = textwrap.wrap(news_item['title'], width=23)
        for line in wrapped_title:
            draw.text((80, y), line, font=self.font_title, fill=C_TEXT_LIGHT)
            y += 80

        y += 30
        # 뉴스 요약 (팩트)
        draw.text((80, y), "🔍 FACT", font=self.font_accent, fill=C_ACCENT_BLUE)
        y += 60
        for line in textwrap.wrap(news_item['summary'], width=35):
            draw.text((100, y), line, font=self.font_body, fill=C_TEXT_DIM)
            y += 50

        y += 30
        # 헌터의 뷰
        draw.text((80, y), "👁️ HUNTER'S VIEW", font=self.font_accent, fill=C_ACCENT_GREEN)
        y += 60
        for line in textwrap.wrap(news_item['hunter_view'], width=35):
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
            self.create_strategy_card(strat, i, save_dir / f"06_strategy_{i}.png")
        
        for i, news in enumerate(data['news'][:2], 1): # 최대 2개 뉴스
            self.create_news_card(news, i, save_dir / f"07_news_{i}.png")
            
        print(f"✨ 모든 카드뉴스 생성 완료: {save_dir}")

if __name__ == "__main__":
    CardNewsFactory().run()
