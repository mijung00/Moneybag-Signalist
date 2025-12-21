# iceage/src/pipelines/generate_cardnews_assets.py
# -*- coding: utf-8 -*-
"""
[Signalist CardNews Generator v8.0 - MD-First Refactoring]
- [Refactor] 뉴스레터 MD 파일을 '단일 진실 공급원(SSoT)'으로 사용하도록 전체 구조 변경
- [Fix] 데이터 재수집 로직 제거, MD 파서(Parser)를 통해 모든 정보 추출
- [Fix] 뉴스 카드 생성 시 발생하던 ValueError 해결
"""
import sys
import json
import textwrap
import pandas as pd
import re
from pathlib import Path
from datetime import date, datetime, timedelta
from PIL import Image, ImageDraw, ImageFont

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
except:
    PROJECT_ROOT = Path.cwd()

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# --- 설정 ---
ASSETS_DIR = PROJECT_ROOT / "iceage" / "assets"
FONT_DIR = ASSETS_DIR / "fonts"
TMPL_DIR = ASSETS_DIR / "templates" / "theme_04"
OUT_DIR = PROJECT_ROOT / "iceage" / "out"
CARDNEWS_OUT_DIR = OUT_DIR / "social" / "cardnews"

# 기본 폰트
DEFAULT_FONT = "malgun.ttf"
if sys.platform == "darwin": DEFAULT_FONT = "AppleGothic.ttf"

# --- [색상 팔레트] ---
C_BLACK = (20, 20, 20)
C_WHITE = (255, 255, 255)
C_DARK_GRAY = (50, 50, 55)
C_MID_GRAY = (100, 100, 100)
C_RED = (200, 40, 40)
C_BLUE = (0, 80, 200)
C_PURPLE = (120, 0, 120)
C_BG_BOX = (242, 242, 247)

class MarkdownParser:
    """뉴스레터 마크다운 파일을 파싱하여 구조화된 데이터로 변환하는 클래스"""
    def __init__(self, md_content: str):
        self.md = md_content

    def _get_section(self, name: str) -> str:
        match = re.search(rf"## {name}\n(.*?)(?=\n## |\Z)", self.md, re.S)
        return match.group(1).strip() if match else ""

    def parse(self) -> dict:
        data = {}
        # 1. 헤더 및 AI 요약
        header_section = self.md.split('## ')[1]
        title_match = re.search(r"# (.*?)\n", header_section)
        subtitle_match = re.search(r"_\s*(.*?)\s*_\n", header_section)
        summary_match = re.search(r"_\n\n(.*?)\n\n\*\*한국\*\*", header_section, re.S)
        data['title'] = title_match.group(1).strip() if title_match else "Signalist Daily"
        data['subtitle'] = subtitle_match.group(1).strip() if subtitle_match else ""
        data['ai_summary'] = summary_match.group(1).strip() if summary_match else ""

        # 2. 시장 온도
        temp_section = self._get_section("오늘의 시장 온도")
        status_match = re.search(r"### 🌡️ 오늘의 시장 온도: (.*?)\n", temp_section)
        gauge_match = re.search(r"\*\*(.*?)\*\*", temp_section)
        comment_match = re.search(r'> \*"(.+?)"', temp_section)
        data['market_temp'] = {
            "status": status_match.group(1).strip() if status_match else "중립",
            "gauge": gauge_match.group(1).strip() if gauge_match else "[⬜⬜🟩⬜⬜]",
            "comment": comment_match.group(1).strip() if comment_match else "방향성 탐색 중입니다."
        }

        # 3. 레이더 포착 종목
        radar_section = self._get_section("오늘의 레이더 포착")
        radar_stocks = []
        table_rows = re.findall(r"\| (.*?) \| (.*?) \| (.*?) \| (.*?) \| (.*?) \| (.*?) \|", radar_section)
        for row in table_rows:
            if "종목명" in row[0]: continue
            radar_stocks.append({
                "name": row[0].strip(),
                "close": row[1].strip(),
                "change": row[2].replace("<br>", " ").replace("**", "").replace("▲", "+").replace("▼", "-"),
                "sigma": row[3].strip(),
                "sentiment": row[4].replace("<br>", " "),
                "keyword": row[5].strip()
            })
        data['radar_picks'] = radar_stocks

        # 4. 주요 뉴스
        news_section = self._get_section("Today’s Top News")
        news_items = []
        # 정규식으로 뉴스 항목 추출: "1. [제목](링크) (출처)"
        news_matches = re.findall(r"\d+\. \[(.*?)\]\(.*?\)\s*\((.*?)\)", news_section)
        for title, source in news_matches:
            news_items.append({"title": title.strip(), "source": source.strip()})
        data['news'] = news_items

        # 5. 주도 테마
        theme_section = self._get_section("Today’s Market Themes")
        top_theme_match = re.search(r"### (.*?)\n", theme_section)
        if top_theme_match:
            stocks_match = re.search(r"- 대표 종목: (.*)", theme_section)
            data['top_theme'] = {
                "name": top_theme_match.group(1).strip(),
                "stocks": [s.strip() for s in stocks_match.group(1).split(',')] if stocks_match else []
            }
        else:
            data['top_theme'] = None
            
        return data

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
    except:
        return ImageFont.truetype(DEFAULT_FONT, size)

def load_price_history(stock_code: str, ref_date: date, days: int = 7) -> list[float]:
    """과거 N일간의 종가 데이터를 로드하여 스파크라인용으로 반환합니다."""
    prices = []
    raw_dir = PROJECT_ROOT / "iceage" / "data" / "raw"
    for i in range(days - 1, -1, -1): # From oldest to newest
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

def _get_stock_code_map(ref_date: str) -> dict:
    """종목명 -> 종목코드를 매핑하는 맵을 생성합니다."""
    code_map = {}
    # 가장 확실한 소스인 volume_anomaly 파일 사용
    path = PROJECT_ROOT / "iceage" / "data" / "processed" / f"volume_anomaly_v2_{ref_date}.csv"
    if not path.exists(): return {}
    try:
        df = pd.read_csv(path, dtype={'code': str})
        for _, row in df.iterrows():
            code_map[row['name']] = row['code']
    except Exception as e:
        print(f"⚠️ 종목 코드 맵 로딩 실패: {e}")
    return code_map

def draw_sparkline(draw, prices: list[float], box: tuple[int, int, int, int], color=C_DARK_GRAY, width=4):
    """주어진 박스 안에 스파크라인을 그립니다."""
    if len(prices) < 2: return
    x_start, y_start, x_end, y_end = box
    w, h = x_end - x_start, y_end - y_start
    min_p, max_p = min(prices), max(prices)
    price_range = max_p - min_p if max_p > min_p else 1
    points = [(x_start + (i / (len(prices) - 1)) * w, y_end - ((p - min_p) / price_range) * h) for i, p in enumerate(prices)]
    draw.line(points, fill=color, width=width, joint="curve")

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
        print(f"[WARN] 배경 '{bg_keyword}' 못 찾음. 기본 생성.")
        return Image.new("RGBA", (width, height), (245, 245, 245, 255))

def generate_cardnews(ref_date: str, data: dict):
    print(f"🎨 카드뉴스 생성 시작: {ref_date}")
    
    ref_dt = date.fromisoformat(ref_date)
    
    # --- 데이터 추출 ---
    top_radars = data.get('radar_picks', [])[:3]
    news_list = data.get('news', [])
    market_temp = data.get('market_temp', {})
    ai_summary = data.get('ai_summary', '')
    top_theme = data.get('top_theme')
    stock_code_map = _get_stock_code_map(ref_date)

    # --- 테마 색상 결정 ---
    theme_color = C_MID_GRAY # [수정] 기본 색상으로 먼저 초기화합니다.
    if "과열" in market_temp.get('status', '') or "맑음" in market_temp.get('status', ''):
        theme_color = C_RED
    elif "비" in market_temp.get('status', '') or "혹한" in market_temp.get('status', ''):
        theme_color = C_BLUE

    # --- 2. 이미지 생성 ---
    todays_out = CARDNEWS_OUT_DIR / ref_date
    todays_out.mkdir(parents=True, exist_ok=True)
    
    card_idx = 1
    
    # -----------------------------------------------------
    # [Card 1] Cover (레이더 집중형)
    # -----------------------------------------------------
    img = create_base_image("cover")
    d = ImageDraw.Draw(img)
    
    d.text((60, 60), "SIGNALIST", font=_load_font("Bold", 40), fill=C_MID_GRAY)
    d.text((60, 110), "DAILY BRIEF", font=_load_font("Bold", 80), fill=C_BLACK)
    d.text((820, 130), ref_date, font=_load_font("Medium", 32), fill=C_MID_GRAY)
    d.line((60, 220, 1020, 220), fill=theme_color, width=5)
    
    # Section: RADAR TOP 3 (위치 중앙으로 조정)
    y = 270
    d.text((60, y), "🚀 RADAR PICKS", font=_load_font("Bold", 45), fill=C_DARK_GRAY)
    y += 80
    
    if top_radars:
        for item in top_radars:
            name = item['name']
            close = int(item.get('close', 0))
            chg = float(item.get('chg', 0))
            sigma = float(item.get('tv_z', 0))
            label = item.get('_label', '')
            color = item.get('_color', C_BLACK)
            
            d.rectangle((60, y, 1020, y+120), fill=C_BG_BOX, outline=None)
            
            d.text((100, y+25), name, font=_load_font("Bold", 50), fill=C_BLACK)
            d.text((100, y+80), f"{close:,}원", font=_load_font("Medium", 28), fill=C_MID_GRAY)
            
            chg_txt = f"{chg:+.2f}%"
            chg_col = C_RED if chg > 0 else C_BLUE
            _draw_text_right(d, chg_txt, _load_font("Bold", 40), 1000, y+25, chg_col)
            _draw_text_right(d, f"({sigma:+.1f}σ)", _load_font("Medium", 32), 1000, y+80, C_DARK_GRAY)
            
            _draw_text_centered(d, f"[{label}]", _load_font("Bold", 30), 540, y+40, color)
            
            y += 140
    else:
        _draw_text_centered(d, "특이 종목 없음", _load_font("Medium", 30), 540, y+50, C_MID_GRAY)
        y += 140

    # Section: Market Temperature
    d.line((60, y, 1020, y), fill=(220,220,220), width=2)
    y += 40
    d.text((60, y), "🌡️ MARKET TEMP", font=_load_font("Bold", 45), fill=C_DARK_GRAY)
    y += 80
    d.text((100, y), market_temp['status'], font=_load_font("Bold", 40), fill=theme_color)
    d.text((100, y + 55), market_temp['gauge'], font=_load_font("Medium", 32), fill=C_DARK_GRAY)
    d.text((100, y + 105), f"“{market_temp['comment']}”", font=_load_font("Medium", 28), fill=C_MID_GRAY)

    img.save(todays_out / f"card_{card_idx:02d}.png")
    card_idx += 1

    # -----------------------------------------------------
    # [NEW Card 2] AI Briefing
    # -----------------------------------------------------
    if ai_summary:
        img = create_base_image("body")
        d = ImageDraw.Draw(img)
        d.text((60, 60), "AI MARKET BRIEF", font=_load_font("Bold", 60), fill=C_BLACK)
        d.line((60, 140, 420, 140), fill=C_PURPLE, width=4)
        
        y_text = 350
        wrapped_text = textwrap.wrap(f"“{ai_summary}”", width=25)
        for line in wrapped_text:
            # AI 요약은 가독성을 위해 Bold 대신 Medium 폰트 사용
            _draw_text_centered(d, line, _load_font("Medium", 52), 540, y_text, C_DARK_GRAY)
            y_text += 80
        
        _draw_text_centered(d, "- AI Analyst -", _load_font("Medium", 30), 540, y_text + 50, C_MID_GRAY)
        
        img.save(todays_out / f"card_{card_idx:02d}.png")
        card_idx += 1


    # -----------------------------------------------------
    # [Card 3 ~ N] Radar Stocks Detail
    # -----------------------------------------------------
    if top_radars:
        for i, item in enumerate(top_radars):
            img = create_base_image("body")
            d = ImageDraw.Draw(img)
            
            d.text((60, 60), f"RADAR PICK #{i+1}", font=_load_font("Bold", 45), fill=C_MID_GRAY)
            
            name = item['name']
            close_str = item.get('close', '0')
            change_str = item.get('change', '0.00%')
            sigma_str = item.get('sigma', '0.0σ')
            sentiment = item.get('sentiment', '')
            keyword = item.get('keyword', '-')
            
            color = C_RED if '매수' in sentiment else (C_BLUE if '매도' in sentiment else C_BLACK)
            label = sentiment.split('(')[0].strip()
            
            _draw_text_centered(d, name, _load_font("Bold", 90), 540, 250, color)
            
            # 등락률에서 숫자만 추출하여 색상 결정
            chg_val = float(re.findall(r"[-+]?\d*\.\d+", change_str)[0])
            info_price = f"{close_str}원 ({change_str})"
            _draw_text_centered(d, info_price, _load_font("Medium", 45), 540, 380, C_BLACK)
            
            # [NEW] 주도 테마 뱃지
            is_leading = top_theme and name in top_theme.get('stocks', [])
            if is_leading:
                badge_text = "🔥 주도 테마"
                badge_font = _load_font("Bold", 30)
                bbox = d.textbbox((0,0), badge_text, font=badge_font)
                badge_w, badge_h = bbox[2] - bbox[0], bbox[3] - bbox[1]
                badge_x, badge_y = 80, 150
                d.rounded_rectangle((badge_x, badge_y, badge_x + badge_w + 30, badge_y + badge_h + 15), fill=C_RED, radius=10)
                d.text((badge_x + 15, badge_y + 7), badge_text, font=badge_font, fill=C_WHITE)

            box_top = 460
            d.rectangle((100, box_top, 980, box_top+280), fill=C_BG_BOX, outline=None)
            
            d.text((140, box_top+40), "SIGNAL", font=_load_font("Bold", 35), fill=C_MID_GRAY)
            d.text((140, box_top+90), f"[{label}]", font=_load_font("Bold", 50), fill=color)
            
            d.text((600, box_top+40), "STRENGTH", font=_load_font("Bold", 35), fill=C_MID_GRAY)
            d.text((600, box_top+90), sigma_str, font=_load_font("Bold", 50), fill=C_PURPLE)
            
            if keyword and keyword != "-":
                d.line((140, box_top+170, 940, box_top+170), fill=(220,220,220), width=2)
                d.text((140, box_top+200), "ISSUE", font=_load_font("Bold", 35), fill=C_MID_GRAY)
                d.text((140, box_top+240), textwrap.shorten(keyword, width=35, placeholder="..."), font=_load_font("Medium", 40), fill=C_BLACK)
            
            # [NEW] 스파크라인
            stock_code = stock_code_map.get(name)
            if stock_code:
                price_history = load_price_history(stock_code, ref_dt, days=7)
                if price_history:
                    draw_sparkline(d, price_history, box=(140, 800, 940, 900), color=C_MID_GRAY, width=5)
            
            img.save(todays_out / f"card_{card_idx:02d}.png")
            card_idx += 1

    # -----------------------------------------------------
    # [Card N+1] General News Brief (독립 페이지)
    # -----------------------------------------------------
    img = create_base_image("body")
    d = ImageDraw.Draw(img)
    
    d.text((60, 60), "NEWS BRIEF", font=_load_font("Bold", 60), fill=C_BLACK)
    d.line((60, 140, 320, 140), fill=C_BLUE, width=4)
    
    y_news = 220
    f_news_t = _load_font("Bold", 38)
    f_news_s = _load_font("Medium", 24)
    
    if news_list:
        for item in news_list[:4]: # 최대 4개
            title = item.get('title', '')
            source = item.get('source', '')
            
            # 각 뉴스 아이템을 위한 박스
            d.rectangle((60, y_news, 1020, y_news + 160), fill=C_BG_BOX)
            
            wrapped = textwrap.wrap(title, width=35)
            
            y_line = y_news + 30
            for line in wrapped[:2]: # 최대 2줄
                d.text((90, y_line), line, font=f_news_t, fill=C_BLACK)
                y_line += 50
            
            if source:
                _draw_text_right(d, source, f_news_s, 990, y_news + 120, C_MID_GRAY)
            
            y_news += 190 # 박스 높이 + 간격
            if y_news > 900: break
    else:
        _draw_text_centered(d, "주요 뉴스 집계중", _load_font("Medium", 50), 540, 500, C_MID_GRAY)

    img.save(todays_out / f"card_{card_idx:02d}.png")
    print(f"✅ 카드뉴스 생성 완료 (총 {card_idx-1}장): {todays_out}")

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) >= 2 else date.today().isoformat()
    
    # 1. MD 파일 로드
    md_path = OUT_DIR / f"Signalist_Daily_{target_date}.md"
    if not md_path.exists():
        # 개발 환경용 접미사 시도
        md_path_dev = OUT_DIR / f"Signalist_Daily_{target_date}-dev.md"
        if md_path_dev.exists():
            md_path = md_path_dev
        else:
            print(f"❌ 뉴스레터 파일을 찾을 수 없습니다: {md_path}")
            sys.exit(1)
            
    md_content = md_path.read_text(encoding='utf-8')
    parsed_data = MarkdownParser(md_content).parse()
    generate_cardnews(target_date, parsed_data)