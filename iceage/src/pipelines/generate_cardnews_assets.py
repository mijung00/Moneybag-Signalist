# iceage/src/pipelines/generate_cardnews_assets.py
# -*- coding: utf-8 -*-
"""
[Signalist CardNews Generator v6.1 - Clean Cover]
- [Card 1] 뉴스 섹션 제거 -> 레이더 종목 집중형 표지로 변경
- [Layout] 표지 여백 확보로 가독성 증대
- [Font] 주군이 픽한 Bold/Medium 폰트 최적화 유지
"""
import sys
import json
import textwrap
import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta
from PIL import Image, ImageDraw, ImageFont

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
except:
    PROJECT_ROOT = Path.cwd()

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from iceage.src.pipelines.final_strategy_selector import StrategySelector
# [수정] yfinance 직접 호출 대신, 안정화된 뉴스레터의 데이터 로더를 사용
from iceage.src.pipelines.morning_newsletter import get_market_overview_safe

# --- 설정 ---
ASSETS_DIR = PROJECT_ROOT / "iceage" / "assets"
FONT_DIR = ASSETS_DIR / "fonts"
TMPL_DIR = ASSETS_DIR / "templates" / "theme_04"
OUT_DIR = PROJECT_ROOT / "iceage" / "out" / "social" / "cardnews"

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

def _get_strategy_label(strategy_name):
    if strategy_name in ['panic_buying']: return "과매도 반등", C_RED
    if strategy_name in ['fallen_angel']: return "낙폭과대", C_RED
    if strategy_name in ['kings_shadow']: return "눌림목 공략", C_RED
    if strategy_name in ['overheat_short']: return "과열 매도", C_BLUE
    return "특이 수급", C_BLACK

def _load_stock_news_map(ref_date: str) -> dict:
    news_map = {}
    path = PROJECT_ROOT / "iceage" / "data" / "raw" / f"kr_stock_event_news_{ref_date}.jsonl"
    if path.exists():
        try:
            with path.open(encoding='utf-8') as f:
                for line in f:
                    item = json.loads(line)
                    name = item.get('stock_name', '')
                    title = item.get('title', '')
                    if name and title:
                        if name not in news_map or len(title) > len(news_map[name]):
                            news_map[name] = title
        except: pass
    return news_map

def generate_cardnews(ref_date: str):
    print(f"🎨 카드뉴스 생성 시작 (Clean Cover): {ref_date}")
    
    # --- 1. 데이터 로딩 ---
    try:
        selector = StrategySelector(ref_date)
        res = selector.select_targets()
        radar_list = []
        for strat, rows in res.items():
            label, color = _get_strategy_label(strat)
            for r in rows:
                r['_label'] = label
                r['_color'] = color
                radar_list.append(r)
        radar_list.sort(key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)
        top_radars = radar_list[:3]
    except: top_radars = []

    stock_news = _load_stock_news_map(ref_date)

    news_list = []
    n_path = PROJECT_ROOT / "iceage" / "data" / "processed" / f"kr_news_cleaned_{ref_date}.jsonl"
    if n_path.exists():
        try:
            with n_path.open(encoding='utf-8') as f:
                for line in f: news_list.append(json.loads(line))
            news_list = news_list[:4] 
        except: pass

    ref_dt = date.fromisoformat(ref_date)
    try: snap = get_market_overview(ref_dt)
    except: snap = {}
    indices = snap.get("indices", {})
    commodities = snap.get("commodities", {})
    crypto = snap.get("crypto", {})

    theme_path = PROJECT_ROOT / "iceage" / "data" / "processed" / f"kr_sector_themes_{ref_date}.json"
    top_theme = None
    if theme_path.exists():
        try:
            themes = json.loads(theme_path.read_text(encoding='utf-8'))
            if themes: top_theme = themes[0]
        except: pass

    # --- 2. 이미지 생성 ---
    todays_out = OUT_DIR / ref_date
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
    d.line((60, 220, 1020, 220), fill=C_BLACK, width=3)
    
    # Section: RADAR TOP 3 (위치 중앙으로 조정)
    y = 290
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
            
            y += 150
    else:
        _draw_text_centered(d, "특이 종목 없음", _load_font("Medium", 30), 540, y+50, C_MID_GRAY)

    # [뉴스 섹션 제거됨]

    img.save(todays_out / f"card_{card_idx:02d}.png")
    card_idx += 1


    # -----------------------------------------------------
    # [Card 2] Global Market Watch
    # -----------------------------------------------------
    img = create_base_image("body")
    d = ImageDraw.Draw(img)
    
    d.text((60, 60), "MARKET WATCH", font=_load_font("Bold", 60), fill=C_BLACK)
    d.line((60, 140, 350, 140), fill=C_RED, width=4)
    
    y = 240; gap = 120
    targets = [
        ("KOSPI", "KOSPI", indices), ("KOSDAQ", "KOSDAQ", indices),
        ("USD/KRW", "USD/KRW", snap.get('fx', {})),
        ("S&P 500", "S&P 500", indices), ("NASDAQ", "NASDAQ", indices),
        ("Bitcoin", "BTC/USD", crypto)
    ]
    
    f_nm = _load_font("Bold", 40)
    f_vl = _load_font("Medium", 40)
    
    for lbl, key, source in targets:
        val, pct = 0.0, 0.0
        if key in source: val, pct = source[key]
        
        color = C_RED if pct > 0 else (C_BLUE if pct < 0 else C_MID_GRAY)
        icon = "▲" if pct > 0 else "▼"
        if pct == 0: icon = "-"
        
        d.text((80, y), lbl, font=f_nm, fill=C_DARK_GRAY)
        
        if val != 0:
            _draw_text_right(d, f"{val:,.2f}", f_vl, 750, y, C_BLACK)
            _draw_text_right(d, f"{icon} {abs(pct):.2f}%", f_vl, 1040, y, color)
        else:
             _draw_text_right(d, "-", f_vl, 1000, y, C_MID_GRAY)
        y += gap
        
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
            close = int(item.get('close', 0))
            chg = float(item.get('chg', 0))
            sigma = float(item.get('tv_z', 0))
            label = item.get('_label', '')
            color = item.get('_color', C_BLACK)
            
            _draw_text_centered(d, name, _load_font("Bold", 90), 540, 250, color)
            
            info_price = f"{close:,}원 ({chg:+.2f}%)"
            _draw_text_centered(d, info_price, _load_font("Medium", 45), 540, 380, C_BLACK)
            
            box_top = 480
            d.rectangle((100, box_top, 980, box_top+320), fill=C_BG_BOX, outline=None)
            
            d.text((140, box_top+40), "SIGNAL", font=_load_font("Bold", 35), fill=C_MID_GRAY)
            d.text((140, box_top+90), f"[{label}]", font=_load_font("Bold", 50), fill=color)
            
            d.text((600, box_top+40), "STRENGTH", font=_load_font("Bold", 35), fill=C_MID_GRAY)
            d.text((600, box_top+90), f"{sigma:+.1f}σ", font=_load_font("Bold", 50), fill=C_PURPLE)
            
            news_title = stock_news.get(name, "")
            if news_title:
                if len(news_title) > 28: news_title = news_title[:28] + "..."
                d.line((140, box_top+180, 940, box_top+180), fill=C_MID_GRAY, width=1)
                d.text((140, box_top+210), "ISSUE", font=_load_font("Bold", 35), fill=C_MID_GRAY)
                d.text((140, box_top+250), news_title, font=_load_font("Medium", 40), fill=C_BLACK)
            
            img.save(todays_out / f"card_{card_idx:02d}.png")
            card_idx += 1


    # -----------------------------------------------------
    # [Card N] Hot Theme
    # -----------------------------------------------------
    img = create_base_image("body")
    d = ImageDraw.Draw(img)
    
    d.text((60, 60), "HOT THEME", font=_load_font("Bold", 60), fill=C_BLACK)
    d.line((60, 140, 320, 140), fill=C_PURPLE, width=4)
    
    if top_theme:
        t_name = top_theme['sector']
        t_ret = top_theme['avg_return']
        t_stocks = top_theme.get('top_stocks', [])[:5]
        
        _draw_text_centered(d, t_name, _load_font("Bold", 80), 540, 250, C_BLACK)
        c_ret = C_RED if t_ret > 0 else C_BLUE
        _draw_text_centered(d, f"평균 수익률 {t_ret:+.2f}%", _load_font("Medium", 50), 540, 380, c_ret)
        
        y_st = 520
        f_st = _load_font("Medium", 42)
        d.line((200, 480, 880, 480), fill=C_MID_GRAY, width=1)
        
        for i, s in enumerate(t_stocks, 1):
            _draw_text_centered(d, f"{i}. {s}", f_st, 540, y_st, C_DARK_GRAY)
            y_st += 80
            
    img.save(todays_out / f"card_{card_idx:02d}.png")
    card_idx += 1
    

    # -----------------------------------------------------
    # [Card N+1] General News Brief (독립 페이지)
    # -----------------------------------------------------
    img = create_base_image("body")
    d = ImageDraw.Draw(img)
    
    d.text((60, 60), "NEWS BRIEF", font=_load_font("Bold", 60), fill=C_BLACK)
    d.line((60, 140, 320, 140), fill=C_BLUE, width=4)
    
    y_news = 250
    f_news_t = _load_font("Bold", 40)
    
    if news_list:
        for idx, item in enumerate(news_list, 1):
            title = item.get('title', '')
            wrapped = textwrap.wrap(title, width=24) 
            
            d.text((60, y_news), f"{idx}", font=f_news_t, fill=C_RED)
            
            for line in wrapped:
                d.text((110, y_news), line, font=f_news_t, fill=C_BLACK)
                y_news += 55
            
            y_news += 40
            if y_news > 950: break
    else:
        _draw_text_centered(d, "주요 뉴스 집계중", _load_font("Medium", 50), 540, 500, C_MID_GRAY)

    img.save(todays_out / f"card_{card_idx:02d}.png")
    print(f"✅ 카드뉴스 생성 완료 (총 {card_idx}장): {todays_out}")

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) >= 2 else date.today().isoformat()
    generate_cardnews(target_date)