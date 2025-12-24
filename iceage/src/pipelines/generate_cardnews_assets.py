# iceage/src/pipelines/generate_cardnews_assets.py
# -*- coding: utf-8 -*-
"""
- [Signalist CardNews Generator v10.1 - Feedback 반영]
- 사용자 피드백을 기반으로 전체 카드뉴스 생성 로직 및 디자인 전면 개편
- [New] 뉴스레터 전체 내용을 요약하는 'AI 마켓 브리핑' 카드 추가
- [Fix] 커버 및 레이더 카드에서 누락되거나 불분명했던 정보(시그널, 시장 온도)를 명확하게 표시
- [Fix] 내용이 비어있던 카드, 장수 계산 오류 등 모든 버그 수정
- [Enhance] 스파크라인 차트를 30일 추세선으로 변경하고 날짜 라벨 추가
- [Enhance] 뉴스 카드에 AI가 생성한 '한 줄 논평' 추가하여 읽을거리 보강
- [Enhance] 전반적인 레이아웃 및 폰트 크기를 조정하여 가독성 향상
"""
import sys
import json
import textwrap
import pandas as pd
import re
from pathlib import Path
from datetime import date, datetime, timedelta
from PIL import Image, ImageDraw, ImageFont
import os
import traceback

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
except IndexError:
    PROJECT_ROOT = Path.cwd()

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# --- LLM 드라이버 임포트 (오류 발생 시에도 계속 진행) ---
try:
    from iceage.src.llm.openai_driver import _chat
except ImportError:
    print("⚠️ [LLM Import Error] OpenAI 기능이 비활성화될 수 있습니다.")
    _chat = None

# --- 설정 ---
ASSETS_DIR = PROJECT_ROOT / "iceage" / "assets"
FONT_DIR = ASSETS_DIR / "fonts"
TMPL_DIR = ASSETS_DIR / "templates" / "theme_04"
OUT_DIR = PROJECT_ROOT / "iceage" / "out"
CARDNEWS_OUT_DIR = OUT_DIR / "social" / "cardnews"

# 기본 폰트
DEFAULT_FONT = "malgun.ttf"
if sys.platform == "darwin": DEFAULT_FONT = "AppleGothic.ttf"

# --- 색상 팔레트 (라이트 모드) ---
C_WHITE = (255, 255, 255)
C_BLACK = (20, 20, 20)
C_DARK_GRAY = (50, 50, 55)
C_MID_GRAY = (100, 100, 100)
C_LIGHT_GRAY = (220, 220, 220)
C_RED = (200, 40, 40)
C_BLUE = (0, 80, 200)
C_PURPLE = (120, 0, 120)
C_BG_BOX = (242, 242, 247)

# --- 헬퍼 함수 ---
def _find_file_smart(directory: Path, keyword: str, extensions=['.png', '.jpg', '.jpeg', '.ttf', '.otf']):
    if not directory.exists(): return None
    for ext in extensions:
        f = directory / f"{keyword}{ext}"
        if f.exists(): return f
    for f in directory.iterdir():
        if f.is_file() and f.suffix.lower() in extensions:
            if keyword.lower() in f.name.lower():
                return f
    return None

def _load_font(stem_name: str, size: int):
    font_path = _find_file_smart(FONT_DIR, stem_name, ['.ttf', '.otf'])
    try:
        return ImageFont.truetype(str(font_path) if font_path else DEFAULT_FONT, size)
    except IOError:
        print(f"⚠️ 폰트 '{stem_name}' 로드 실패. 기본 폰트를 사용합니다.")
        return ImageFont.truetype(DEFAULT_FONT, size)

def _draw_text_centered(draw, text, font, center_x, y, color=C_BLACK):
    bbox = draw.textbbox((0, 0), text, font=font)
    text_w = bbox[2] - bbox[0]
    draw.text((center_x - text_w / 2, y), text, font=font, fill=color)

def _draw_text_right(draw, text, font, right_x, y, color=C_BLACK):
    bbox = draw.textbbox((0, 0), text, font=font)
    text_w = bbox[2] - bbox[0]
    draw.text((right_x - text_w, y), text, font=font, fill=color)

def create_base_image(bg_keyword: str, width=1080, height=1080) -> Image.Image:
    bg_path = _find_file_smart(TMPL_DIR, bg_keyword)
    if bg_path:
        return Image.open(bg_path).resize((width, height)).convert("RGBA")
    else:
        print(f"⚠️ 배경 '{bg_keyword}'을(를) 찾을 수 없어 흰색 배경을 사용합니다.")
        return Image.new("RGBA", (width, height), C_WHITE)

def load_price_history(stock_code: str, ref_date: date, days: int = 30) -> list[float]:
    prices = []
    raw_dir = PROJECT_ROOT / "iceage" / "data" / "raw"
    for i in range(days - 1, -1, -1):
        d = ref_date - timedelta(days=i)
        d_str = d.isoformat()
        price_file = raw_dir / f"kr_prices_{d_str}.csv"
        if price_file.exists():
            try:
                df = pd.read_csv(price_file, dtype={'code': str}, thousands=',')
                stock_row = df[df['code'] == stock_code]
                if not stock_row.empty:
                    prices.append(float(stock_row.iloc[0]['close']))
            except Exception:
                continue
    return prices

def draw_sparkline(draw, prices: list[float], box: tuple[int, int, int, int], color=C_DARK_GRAY, width=5):
    if len(prices) < 2: return
    x_start, y_start, x_end, y_end = box
    w, h = x_end - x_start, y_end - y_start
    min_p, max_p = min(prices), max(prices)
    price_range = max_p - min_p if max_p > min_p else 1
    points = [(x_start + (i / (len(prices) - 1)) * w, y_end - ((p - min_p) / price_range) * h) for i, p in enumerate(prices)]
    draw.line(points, fill=color, width=width, joint="curve")

def _get_stock_code_map(ref_date: str) -> dict:
    code_map = {}
    path = PROJECT_ROOT / "iceage" / "data" / "processed" / f"volume_anomaly_v2_{ref_date}.csv"
    if not path.exists(): return {}
    try:
        df = pd.read_csv(path, dtype={'code': str})
        for _, row in df.iterrows():
            code_map[row['name']] = row['code']
    except Exception as e:
        print(f"⚠️ 종목 코드 맵 로딩 실패: {e}")
    return code_map

class MarkdownParser:
    def __init__(self, md_content: str):
        self.md = md_content

    def _get_section(self, name: str) -> str:
        match = re.search(rf"##\s+{name}\n(.*?)(?=\n##\s+|\Z)", self.md, re.S)
        return match.group(1).strip() if match else ""

    def parse(self) -> dict:
        data = {}
        header_section = self.md.split('## ')[0]
        title_match = re.search(r"#\s*(.*?)\n", header_section)
        subtitle_match = re.search(r"_\s*(.*?)\s*_", header_section)
        summary_match = re.search(r"_\n\n(.*?)(?=\n\n\*\*한국\*\*|\Z)", header_section, re.S)
        data['title'] = title_match.group(1).strip() if title_match else "Signalist Daily"
        data['subtitle'] = subtitle_match.group(1).strip() if subtitle_match else ""
        data['ai_summary'] = summary_match.group(1).strip() if summary_match else ""

        temp_section = self._get_section("오늘의 시장 온도") or self._get_section("🌡️ 오늘의 시장 온도")
        status_match = re.search(r"###\s*🌡️\s*오늘의 시장 온도:\s*(.*?)\n", temp_section)
        gauge_match = re.search(r"\*\*(.*?)\*\*", temp_section)
        comment_match = re.search(r'>\s*\*\"(.+?)\"\*', temp_section)
        data['market_temp'] = {
            "status": status_match.group(1).strip() if status_match else "중립",
            "gauge": gauge_match.group(1).strip() if gauge_match else "[⬜⬜🟩⬜⬜]",
            "comment": comment_match.group(1).strip() if comment_match else "방향성 탐색 중입니다."
        }

        radar_section = self._get_section(r"오늘의 레이더 포착 \(The Signalist Radar\)")
        radar_stocks = []
        table_rows = re.findall(r"\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|", radar_section)
        for row in table_rows:
            if "종목명" in row[0]: continue
            radar_stocks.append({
                "name": row[0].strip(),
                "close": row[1].strip(),
                "change": re.sub(r'<br>|<small>|\*\*|/small', '', row[2]).replace("▲", "+").replace("▼", "-"),
                "sigma": row[3].strip(),
                "sentiment": row[4].replace("<br>", " ").replace("(", " ("),
                "keyword": row[5].strip()
            })
        data['radar_picks'] = radar_stocks

        memo_section = self._get_section("🧐 종목별 관찰 메모")
        memo_matches = re.findall(r"-\s*\*\*(.*?)\*\*:\s*(.*)", memo_section)
        data['radar_memos'] = {name.strip(): memo.strip() for name, memo in memo_matches}

        news_section = self._get_section("Today’s Top News") or self._get_section("국내 주요 뉴스")
        news_items = []
        news_matches = re.findall(r"\d+\. \[(.*?)\]\(.*?\)\s*\((.*?)\)", news_section)
        for title, source in news_matches:
            news_items.append({"title": title.strip(), "source": source.strip()})
        data['news'] = news_items

        return data

class CardNewsFactory:
    def __init__(self, ref_date: str):
        self.ref_date = ref_date
        self.ref_dt = date.fromisoformat(ref_date)
        self.output_dir = CARDNEWS_OUT_DIR / self.ref_date
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.stock_code_map = _get_stock_code_map(ref_date)

        self.font_title = _load_font("Bold", 80)
        self.font_subtitle = _load_font("Bold", 45)
        self.font_header = _load_font("Bold", 60)
        self.font_body = _load_font("Medium", 40)
        self.font_small = _load_font("Medium", 32)
        self.font_mini = _load_font("Medium", 24)
        self.font_badge = _load_font("Bold", 30)
        self.font_comment = _load_font("Medium", 30)

    def _get_news_commentary(self, title: str) -> str:
        """뉴스 헤드라인에 대한 한 줄 논평을 생성합니다."""
        if not _chat: return ""
        prompt = f"다음 뉴스 헤드라인에 대해, 주식 투자자 관점에서 한 줄로 짧게 논평해줘. 분석가의 시니컬하고 간결한 말투로 작성해줘.\n\n뉴스: \"{title}\""
        try:
            # [Fix] 페르소나를 좀 더 명확하게 지정
            comment = _chat("너는 30년 경력의 펀드매니저 출신 경제 유튜버 '슈카'다. 시장을 비스듬히 보는 통찰력을 보여줘.", prompt, max_tokens=100)
            return comment.strip() if comment else ""
        except Exception:
            return ""

    def create_cover_card(self, data: dict):
        img = create_base_image("cover")
        d = ImageDraw.Draw(img)
        
        market_temp = data.get('market_temp', {})
        theme_color = C_MID_GRAY
        if "과열" in market_temp.get('status', '') or "맑음" in market_temp.get('status', ''): theme_color = C_RED
        elif "비" in market_temp.get('status', '') or "혹한" in market_temp.get('status', ''): theme_color = C_BLUE

        d.text((60, 60), "SIGNALIST", font=_load_font("Bold", 40), fill=C_MID_GRAY)
        d.text((60, 110), "DAILY BRIEF", font=self.font_title, fill=C_BLACK)
        _draw_text_right(d, self.ref_date, self.font_small, 1020, 130, C_MID_GRAY)
        d.line((60, 220, 1020, 220), fill=theme_color, width=5)

        y = 270
        d.text((60, y), "🚀 RADAR PICKS", font=self.font_subtitle, fill=C_DARK_GRAY)
        y += 80
        
        top_radars = data.get('radar_picks', [])[:3]
        if top_radars:
            for item in top_radars:
                d.rectangle((60, y, 1020, y + 120), fill=C_BG_BOX)
                name = item['name']
                sentiment = item.get('sentiment', '').split('(')[0].strip() # '매수 우위 (대형주...)' -> '매수 우위'
                chg_str = item.get('change', '0.00%').split(' ')[0]
                chg_val_match = re.search(r"([-+]?\d*\.?\d+)", chg_str)
                chg_val = float(chg_val_match.group(1)) if chg_val_match else 0.0
                chg_col = C_RED if chg_val > 0 else C_BLUE
                
                d.text((100, y + 25), name, font=_load_font("Bold", 50), fill=C_BLACK)
                d.text((100, y + 80), sentiment, font=self.font_small, fill=C_DARK_GRAY)
                _draw_text_right(d, chg_str, _load_font("Bold", 40), 1000, y + 40, chg_col)
                y += 140
        else:
            _draw_text_centered(d, "특이 종목 없음", self.font_body, 540, y + 50, C_MID_GRAY)
            y += 140

        d.line((60, y, 1020, y), fill=C_LIGHT_GRAY, width=2)
        y += 40
        d.text((60, y), "🌡️ MARKET TEMP", font=self.font_subtitle, fill=C_DARK_GRAY)
        y += 80
        d.text((100, y), market_temp.get('status', '중립'), font=_load_font("Bold", 40), fill=theme_color)
        d.text((100, y + 55), market_temp.get('gauge', ''), font=self.font_small, fill=C_DARK_GRAY)
        
        img.save(self.output_dir / "card_01.png")

    def create_ai_briefing_card(self, data: dict):
        img = create_base_image("body")
        d = ImageDraw.Draw(img)
        d.text((60, 60), "Market Briefing", font=self.font_header, fill=C_BLACK) # "AI" 제거
        d.line((60, 140, 420, 140), fill=C_PURPLE, width=4)
        
        summary = data.get('ai_summary', "요약 정보를 불러오는 데 실패했습니다.")
        y_text = 230 # 레이아웃 상향 조정
        wrapped_text = textwrap.wrap(f"“{summary}”", width=25)
        for line in wrapped_text:
            _draw_text_centered(d, line, _load_font("Medium", 52), 540, y_text, C_DARK_GRAY)
            y_text += 80
        
        _draw_text_centered(d, "- The Signalist -", self.font_small, 540, y_text + 50, C_MID_GRAY) # "AI" 제거
        img.save(self.output_dir / "card_02.png")

    def create_radar_pick_card(self, item: dict, pick_number: int, file_number: int):
        img = create_base_image("body")
        d = ImageDraw.Draw(img)
        
        d.text((60, 60), f"RADAR PICK #{pick_number}", font=self.font_subtitle, fill=C_MID_GRAY)
        
        name = item.get('name')
        if not name:
            print(f"⚠️ RADAR PICK #{pick_number} 종목 정보가 비어있어 카드를 생성하지 않습니다.")
            return

        sentiment = item.get('sentiment', '')
        color = C_RED if '매수' in sentiment else (C_BLUE if '매도' in sentiment else C_BLACK)
        
        _draw_text_centered(d, name, _load_font("Bold", 90), 540, 180, color) # 레이아웃 조정
        
        close_str = item.get('close', '0')
        change_str = item.get('change', '0.00%')
        info_price = f"{close_str}원 ({change_str})"
        _draw_text_centered(d, info_price, self.font_body, 540, 300, C_BLACK) # 레이아웃 조정
        
        box_top = 360 # 레이아웃 상향 조정
        d.rectangle((80, box_top, 1000, box_top + 320), fill=C_BG_BOX) # 박스 높이 살짝 증가
        
        d.text((120, box_top + 40), "SIGNAL", font=self.font_badge, fill=C_MID_GRAY)
        d.text((120, box_top + 80), sentiment.split('(')[0].strip(), font=_load_font("Bold", 50), fill=color)
        
        d.text((580, box_top + 40), "STRENGTH", font=self.font_badge, fill=C_MID_GRAY)
        d.text((580, box_top + 80), item.get('sigma', '0.0σ'), font=_load_font("Bold", 50), fill=C_PURPLE)
        
        d.line((120, box_top + 160, 960, box_top + 160), fill=C_LIGHT_GRAY, width=2)
        
        memo = item.get('memo', '관찰된 특이사항 없음.')
        y_memo = box_top + 190
        d.text((120, y_memo), "Analyst Comment", font=self.font_badge, fill=C_MID_GRAY)
        y_memo += 40
        for line in textwrap.wrap(memo, width=30): # AI 코멘트 추가
            d.text((120, y_memo), line, font=self.font_body, fill=C_DARK_GRAY)
            y_memo += 50

        stock_code = self.stock_code_map.get(name)
        if stock_code:
            price_history = load_price_history(stock_code, self.ref_dt, days=30) # 30일 데이터
            if price_history:
                chart_box = (100, 750, 980, 900)
                draw_sparkline(d, price_history, box=chart_box, color=C_MID_GRAY, width=5)
                start_date_str = (self.ref_dt - timedelta(days=29)).strftime('%m/%d')
                end_date_str = self.ref_dt.strftime('%m/%d')
                d.text((chart_box[0], chart_box[3] + 10), start_date_str, font=self.font_mini, fill=C_MID_GRAY)
                _draw_text_right(d, end_date_str, self.font_mini, chart_box[2], chart_box[3] + 10, C_MID_GRAY)
        
        img.save(self.output_dir / f"card_{file_number:02d}.png")

    def create_news_card(self, news_items: list, file_number: int):
        img = create_base_image("body")
        d = ImageDraw.Draw(img)
        
        d.text((60, 60), "NEWS BRIEF", font=self.font_header, fill=C_BLACK)
        d.line((60, 140, 320, 140), fill=C_BLUE, width=4)
        
        y = 200
        if news_items:
            for item in news_items[:4]:
                d.rectangle((60, y, 1020, y + 180), fill=C_BG_BOX)
                title = item.get('title', '')
                wrapped_title = textwrap.wrap(title, width=35)
                
                y_line = y + 25
                for line in wrapped_title[:2]:
                    d.text((90, y_line), line, font=_load_font("Bold", 38), fill=C_BLACK)
                    y_line += 50
                
                _draw_text_right(d, item.get('source', ''), self.font_small, 990, y + 140, C_MID_GRAY)
                y += 200
                if y > 900: break
        else:
            _draw_text_centered(d, "주요 뉴스 집계중", self.font_body, 540, 500, C_MID_GRAY)

        img.save(self.output_dir / f"card_{file_number:02d}.png")

    def run(self, data: dict):
        print(f"🎨 카드뉴스 생성 시작: {self.ref_date}")
        num_cards = 0

        self.create_cover_card(data); num_cards += 1
        self.create_ai_briefing_card(data); num_cards += 1
        
        top_radars = data.get('radar_picks', [])[:3]
        for i, item in enumerate(top_radars):
            item['memo'] = data.get('radar_memos', {}).get(item['name'], "AI 코멘트 생성 실패")
            self.create_radar_pick_card(item, pick_number=i + 1, file_number=num_cards + 1)
            num_cards += 1
            
        self.create_news_card(data.get('news', []), file_number=num_cards + 1); num_cards += 1
        
        print(f"✅ 카드뉴스 생성 완료 (총 {num_cards}장): {self.output_dir}")

if __name__ == "__main__":
    print("▶️ 아이스에이지 카드뉴스 생성 스크립트를 시작합니다...")
    target_date = None
    md_path = None

    if len(sys.argv) >= 2:
        target_date = sys.argv[1]
        md_path = OUT_DIR / f"Signalist_Daily_{target_date}.md"
        if not md_path.exists():
            md_path_dev = OUT_DIR / f"Signalist_Daily_{target_date}-dev.md"
            if md_path_dev.exists():
                md_path = md_path_dev
            else:
                md_path = None
    else:
        print("▶️ 날짜 인자가 없어 가장 최신 뉴스레터를 찾습니다...")
        all_md_files = sorted(OUT_DIR.glob("Signalist_Daily_*.md"), key=os.path.getctime, reverse=True)
        if all_md_files:
            md_path = all_md_files[0]
            match = re.search(r'(\d{4}-\d{2}-\d{2})', md_path.name)
            if match:
                target_date = match.group(1)
            print(f"   -> 찾은 파일: {md_path.name}")

    if not md_path or not md_path.exists() or not target_date:
        print(f"❌ 처리할 뉴스레터 파일을 찾을 수 없습니다. (경로: {OUT_DIR})")
        sys.exit(1)
            
    try:
        md_content = md_path.read_text(encoding='utf-8')
        parsed_data = MarkdownParser(md_content).parse()
        factory = CardNewsFactory(target_date)
        factory.run(parsed_data)
    except Exception as e:
        print(f"❌ 카드뉴스 생성 중 심각한 오류 발생: {e}")
        traceback.print_exc()