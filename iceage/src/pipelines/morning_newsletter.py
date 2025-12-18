# iceage/src/pipelines/morning_newsletter.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import sys
import re  # [필수] 정규식 모듈 유지
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
import pandas as pd
from pathlib import Path
import csv
from typing import List
from datetime import date as _date, timedelta, datetime
from textwrap import dedent

# 프로젝트 루트 설정
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from iceage.src.llm.openai_driver import generate_newsletter_bundle
from iceage.src.analyzers.signalist_history_analyzer import build_signalist_history_markdown

try:
    from iceage.src.pipelines.final_strategy_selector import StrategySelector
except ImportError:
    pass

from iceage.src.data_sources.signalist_today import SignalRow
from iceage.src.signals.signal_volume_pattern import detect_signals_from_volume_anomaly_v2
from iceage.src.data_sources.market_themes import get_market_themes, MarketThemeSummary
from iceage.src.data_sources.sector_themes import get_sector_themes, SectorThemeSummary
from iceage.src.data_sources.investor_flow import load_investor_flow
from iceage.src.data_sources.kr_prices import load_normalized_prices
from iceage.src.data_sources.market_snapshot import get_market_overview
from iceage.src.utils.trading_days import (
    TradingCalendar,
    CalendarConfig,
    compute_reference_date,
    may_run_today,
)
from common.s3_manager import S3Manager


# LLM 캐시
_LLM_BUNDLE_CACHE: dict[str, dict] = {}

def _get_newsletter_env_suffix() -> str:
    env = os.getenv("NEWSLETTER_ENV", "prod").strip().lower()
    if env in ("", "prod"):
        return ""
    return f"-{env}"

# 1. LLM에게 보낼 재료를 풍성하게 만드는 함수
def _build_llm_payload(ref_date: str) -> dict:
    """LLM에게 보낼 재료 준비 (전략 의도 포함)"""
    ref = _date.fromisoformat(ref_date)
    
    # (1) 시장 요약
    try: snap = get_market_overview(ref)
    except: snap = {}
    
    headline_bits = []
    indices = snap.get("indices", {})
    if "KOSPI" in indices: headline_bits.append(f"코스피 {indices['KOSPI'][1]:+.2f}%")
    if "KOSDAQ" in indices: headline_bits.append(f"코스닥 {indices['KOSDAQ'][1]:+.2f}%")
    if "S&P 500" in indices: headline_bits.append(f"S&P500 {indices['S&P 500'][1]:+.2f}%")
    index_summary = " · ".join(headline_bits)

    # (2) 시그널 종목
    signal_items = []
    try:
        selector = StrategySelector(ref_date)
        results = selector.select_targets()
        
        candidates = []
        for r in results.get('panic_buying', []):
            r['_strat_hint'] = "투매가 과도하여 기술적 반등이 기대되는 구간"
            candidates.append(r)
        for r in results.get('fallen_angel', []):
            r['_strat_hint'] = "낙폭 과대 우량주의 저점 매수 기회"
            candidates.append(r)
        for r in results.get('kings_shadow', []):
            r['_strat_hint'] = "대형주 상승 추세 중 매물 소화 과정 (눌림목)"
            candidates.append(r)
        for r in results.get('overheat_short', []):
            r['_strat_hint'] = "단기 폭등으로 인한 피로감 누적, 차익 실현 매물 주의 (고점 징후)"
            candidates.append(r)
                         
        candidates.sort(key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)
        top_rows = candidates[:5]
        
        event_map = _get_internal_events(ref_date)
        
        for r in top_rows:
            name = r.get('name', '')
            item = {
                "name": name,
                "change_rate": f"{float(r.get('chg', 0)):+.2f}%",
                "volume_z": f"{float(r.get('tv_z', 0)):.1f}배",
                "strategy_intent": r.get('_strat_hint', ''),
                "keywords": event_map.get(name, ""),
                "is_bull": "매수" in r.get('_sentiment', '매수')
            }
            signal_items.append(item)
            
    except Exception as e:
        print(f"[WARN] LLM Payload 생성 오류: {e}")

    global_items = load_global_news(ref_date, limit=3)
    articles = [{"title": i.get("title_en",""), "snippet": i.get("summary_en","")} for i in global_items]

    return {
        "ref_date": ref_date,
        "index_summary_line": index_summary,
        "signals": signal_items,
        "global_news": articles,
    }

# 2. 테마 섹션
def section_themes(ref_date: str) -> str:
    ref = _date.fromisoformat(ref_date)
    themes = get_sector_themes(ref)
    if not themes: return "## Today’s Market Themes\n\n오늘은 섹터 기준으로 두드러진 테마 움직임이 크게 관찰되지 않았습니다."
    
    total_turnover = sum(max(getattr(t, "turnover_sum", 0.0), 0.0) for t in themes)
    
    lines = ["## Today’s Market Themes", f"기준일: {ref_date}", ""]
    
    for t in themes[:3]:
        lines.append(f"### {t.sector}")
        lines.append(f"- 섹터 평균 수익률: **{t.avg_return:+.2f}%**")
        
        if total_turnover > 0:
            share = max(getattr(t, "turnover_sum", 0.0), 0.0) / total_turnover * 100
            flame_count = min(5, int(share // 10))
            if flame_count == 0 and share > 1.0: flame_count = 1
            flames = "🔥" * flame_count
            
            if share > 30: comment = "돈이 이 섹터에 쏟아졌습니다."
            elif share > 20: comment = "주도 섹터의 모습을 보였습니다."
            elif share > 10: comment = "유의미한 자금이 유입되었습니다."
            else: comment = "특정 종목 위주로 움직였습니다."
            
            lines.append(f"- 💰 수급 집중도: {flames} **({share:.1f}%)** - _{comment}_")
        
        lines.append(f"- 대표 종목: {', '.join(t.top_stocks)}")
        lines.append("")
        
    return "\n".join(lines)

def _ensure_llm_bundle(ref_date: str) -> dict:
    if ref_date in _LLM_BUNDLE_CACHE:
        return _LLM_BUNDLE_CACHE[ref_date]

    payload = _build_llm_payload(ref_date)
    try:
        bundle = generate_newsletter_bundle(payload)
    except Exception as e:
        print("[WARN] LLM 번들 생성 실패:", repr(e))
        bundle = {}

    _LLM_BUNDLE_CACHE[ref_date] = bundle
    return bundle

ENABLE_INVESTOR_FLOW_SECTION = False 

def section_header_intro(ref_date: str) -> str:
    bundle = _ensure_llm_bundle(ref_date)
    title = bundle.get("title") or f"The Signalist Daily — {ref_date}"
    kicker = bundle.get("kicker") or ""
    market_summary = bundle.get("market_one_liner") or ""
    
    ref = _date.fromisoformat(ref_date)
    try:
        snap = get_market_overview(ref)
    except Exception as e:
        print(f"🚨 [ERROR] get_market_overview failed in section_header_intro: {e}")
        snap = {}
    
    indices = snap.get("indices", {})
    fx = snap.get("fx", {})
    commodities = snap.get("commodities", {})
    crypto = snap.get("crypto", {})

    def _fmt(key, label=None):
        if key in indices: val, pct = indices[key]
        elif key in fx: val, pct = fx[key]
        elif key in commodities: val, pct = commodities[key]
        elif key in crypto: val, pct = crypto[key]
        elif "WTI" in key and "WTI" in commodities: val, pct = commodities["WTI"]
        elif "WTI" in key and "WTI Crude" in commodities: val, pct = commodities["WTI Crude"]
        elif "BTC" in key and "BTC/USD" in crypto: val, pct = crypto["BTC/USD"]
        else: return ""
        
        icon = "🔺" if pct > 0 else ("🔹" if pct < 0 else "-")
        lbl = label if label else key
        return f"{lbl} {val:,.2f} ({icon} {pct:+.2f}%)"

    line_kr = []
    for k, l in [("KOSPI", "코스피"), ("KOSDAQ", "코스닥"), ("USD/KRW", "원/달러")]:
        r = _fmt(k, l)
        if r: line_kr.append(r)
        
    line_us = []
    for k, l in [("Dow Jones", "다우"), ("NASDAQ", "나스닥"), ("S&P 500", "S&P500")]:
        r = _fmt(k, l)
        if r: line_us.append(r)
        
    line_macro = []
    for k, l in [("WTI", "WTI유"), ("BTC", "비트코인")]:
        r = _fmt(k, l)
        if r: line_macro.append(r)

    lines = [f"# {title}", ""]
    if kicker:
        lines.append(f"_{kicker}_")
        lines.append("")
    
    lines.append("## 오늘의 시장 한눈에 보기")
    lines.append(f"기준일: {ref_date}")
    lines.append("")
    
    if market_summary:
        lines.append(market_summary)
        lines.append("")
    
    if line_kr: lines.append(f"**한국**: " + " │ ".join(line_kr)); lines.append("")
    if line_us: lines.append(f"**미국**: " + " │ ".join(line_us)); lines.append("")
    if line_macro: lines.append(f"**기타**: " + " │ ".join(line_macro)); lines.append("")

    return "\n".join(lines)

def _select_signalist_today_rows(ref: _date) -> List[SignalRow]:
    try: all_rows = detect_signals_from_volume_anomaly_v2(ref)
    except Exception: all_rows = []
    if not all_rows: return []

    def _vol(r):
        try: return abs(float(getattr(r, "vol_sigma", 0.0)))
        except Exception: return 0.0
    candidates = sorted(all_rows, key=_vol, reverse=True)

    def _sector(r):
        val = getattr(r, "sector", "") or getattr(r, "theme", "")
        return str(val).strip()

    pos_rows = [r for r in candidates if getattr(r, "vol_sigma", 0.0) > 0]
    neg_rows = [r for r in candidates if getattr(r, "vol_sigma", 0.0) < 0]
    TOP_N = 5; PER_SECTOR_LIMIT = 2
    selected: list = []; seen: set[tuple] = set(); sector_counts: dict[str, int] = {}

    def _can_add(r) -> bool:
        k = (r.name, getattr(r, "vol_sigma", 0.0))
        if k in seen: return False
        sec = _sector(r)
        if not sec: return True
        if sector_counts.get(sec, 0) >= PER_SECTOR_LIMIT: return False
        return True

    def _add(r):
        if not _can_add(r): return
        k = (r.name, getattr(r, "vol_sigma", 0.0))
        seen.add(k); selected.append(r)
        sec = _sector(r)
        if sec: sector_counts[sec] = sector_counts.get(sec, 0) + 1

    if pos_rows: _add(pos_rows[0])
    if neg_rows: _add(neg_rows[0])
    for r in candidates:
        if len(selected) >= TOP_N: break
        _add(r)
    if len(selected) < TOP_N:
        for r in candidates:
            if len(selected) >= TOP_N: break
            k = (r.name, getattr(r, "vol_sigma", 0.0))
            if k in seen: continue
            seen.add(k); selected.append(r)
    return selected

# ---------------------------------------------------------
# [핵심] 이슈 키워드 추출기 V2 (정규식 강화 버전 유지)
# ---------------------------------------------------------
def _extract_keyword_from_title(title: str, stock_name: str) -> str:
    if not title: return "-"
    
    # 1. 괄호 및 대괄호 안의 내용 제거 (예: [특징주], (속보))
    title = re.sub(r"\[.*?\]", " ", title)
    title = re.sub(r"\(.*?\)", " ", title)
    
    # 2. 종목명 제거 (정확도 향상을 위해 공백으로 치환)
    title = title.replace(stock_name, " ")
    
    # 3. 특수문자 제거 (따옴표, 점, 쉼표, 줄표 등 -> 공백)
    # 한글, 영문, 숫자 빼고 다 지움
    title = re.sub(r"[^가-힣a-zA-Z0-9\s]", " ", title)
    
    # 4. 불용어(Stopwords) 제거 - 뉴스 상투어
    stop_words = [
        "특징주", "급등", "상승", "하락", "약세", "강세", "주가", "전망", "이슈", 
        "공시", "체결", "규모", "종목", "관련주", "테마", "분석", "속보", "단독",
        "영향", "주목", "최고", "최저", "경신", "돌파", "마감", "출발", "오전", "오후",
        "포착", "체크", "주의", "비상", "기대", "우려", "쇼크", "서프라이즈", "실적",
        "발표", "공개", "개시", "성공", "체결", "확정", "진입", "확대", "축소", "상한가", "하한가"
    ]
    for w in stop_words:
        title = title.replace(w, " ")
        
    # 5. 숫자 제거 및 1글자 제거
    words = title.split()
    cleaned_words = []
    for w in words:
        if re.search(r"\d", w): continue
        if len(w) < 2: continue
        
        # 끝에 붙은 조사 제거 (간단한 규칙 기반)
        if len(w) >= 3 and w[-1] in ['에', '로', '을', '를', '가', '이', '은', '는', '의']:
             w = w[:-1]
        cleaned_words.append(w)
        
    if not cleaned_words: return "-"
        
    # 6. 가장 긴 단어 선택
    return max(cleaned_words, key=len)

def _get_internal_events(ref_date: str) -> dict[str, str]:
    news_path = PROJECT_ROOT / "iceage" / "data" / "raw" / f"kr_stock_event_news_{ref_date}.jsonl"
    event_map = {}
    if not news_path.exists(): return {}
    
    with news_path.open(encoding="utf-8") as f:
        for line in f:
            try:
                item = json.loads(line)
                name = item.get("stock_name")
                title = item.get("title")
                if name and title:
                    if name not in event_map:
                        keyword = _extract_keyword_from_title(title, name)
                        if keyword and keyword != "-":
                            event_map[name] = keyword
            except: continue
    return event_map

def section_market_thermometer(ref_date: str) -> str:
    ref = _date.fromisoformat(ref_date)
    try:
        snap = get_market_overview(ref)
        indices = snap.get("indices", {})
        changes = []
        if "KOSPI" in indices: changes.append(indices["KOSPI"][1])
        if "KOSDAQ" in indices: changes.append(indices["KOSDAQ"][1])
        if not changes: return ""
        avg_chg = sum(changes) / len(changes)
    except: return ""

    if avg_chg >= 1.5:
        status = "🔥 과열 (Extreme Greed)"; gauge = "[🟥🟥🟥🟥🟥]"; comment = "시장이 뜨겁습니다. 추격 매수보다는 차익 실현을 고려할 구간입니다."
    elif avg_chg >= 0.5:
        status = "☀️ 맑음 (Greed)"; gauge = "[🟥🟥🟥⬜⬜]"; comment = "투자 심리가 살아났습니다. 주도주 위주의 접근이 유효합니다."
    elif avg_chg >= -0.5:
        status = "☁️ 흐림 (Neutral)"; gauge = "[⬜⬜🟩⬜⬜]"; comment = "방향성 탐색 구간입니다. 개별 종목 이슈에 집중하세요."
    elif avg_chg >= -1.5:
        status = "☔ 비 (Fear)"; gauge = "[🟦🟦🟦⬜⬜]"; comment = "투심이 위축되었습니다. 보수적인 관점이 필요합니다."
    else:
        status = "❄️ 혹한 (Extreme Fear)"; gauge = "[🟦🟦🟦🟦🟦]"; comment = "공포 구간입니다. 투매 동참보다는 '패닉 바잉' 기회를 노리세요."

    return dedent(f"""
    ### 🌡️ 오늘의 시장 온도: {status}
    **{gauge}**
    > *"{comment}"*
    """).strip()

def section_signalist_today(ref_date: str) -> str:
    try:
        from iceage.src.pipelines.final_strategy_selector import StrategySelector
        selector = StrategySelector(ref_date)
        results = selector.select_targets()
        
        candidates = []
        for r in results.get('panic_buying', []) + results.get('fallen_angel', []) + results.get('kings_shadow', []):
            r['_sentiment'] = '📈 매수 우위'
            bucket = r.get('size_bucket', '')
            if bucket == 'large': r['_tone'] = "🔵 대형주 수급"
            elif bucket == 'mid': r['_tone'] = "🟡 중형주 반등"
            else: r['_tone'] = "🟢 소형주 급등"
            candidates.append(r)
            
        shorts = results.get('overheat_short', [])
        if shorts:
            shorts = sorted(shorts, key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)[:1]
            for r in shorts:
                r['_sentiment'] = '📉 매도 우위'
                r['_tone'] = "🚨 과열 경보"
                candidates.append(r)
            
        candidates.sort(key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)
        rows = candidates[:5]
        
    except Exception:
        rows = []

    if not rows:
        return "## 오늘의 레이더 포착 (The Signalist Radar)\n\n포착된 종목이 없습니다."

    event_map = _get_internal_events(ref_date)

    intro = dedent("""
    ## 오늘의 레이더 포착 (The Signalist Radar)
    **"데이터가 발견한 수급의 변곡점"**
    Signalist레이더는 시총별 특성과 거래대금 괴리율을 입체적 분석하여, **유의미한 흐름이 포착된 종목**을 선별합니다.
    단순한 가격 등락이 아닌, **평소 대비 비정상적인 거래 강도**를 기반으로 가능성이 높은 구간을 탐지했습니다.
    """).strip()

    header = "| 종목명 | 종가 | 등락 (폭) | 괴리율 | 수급 방향 | 이슈 키워드 |"
    sep = "|---|---|---|---|---|---|"
    
    body = []
    for r in rows:
        name = r.get('name', '')
        close_val = int(r.get('close', 0))
        close_str = f"{close_val:,}"
        chg_pct = float(r.get('chg', 0))
        prev_close = close_val / (1 + chg_pct/100)
        chg_won = int(close_val - prev_close)
        
        if chg_pct > 0: chg_str = f"**+{chg_pct:.2f}%**<br><small>(▲{chg_won:,})</small>"
        elif chg_pct < 0: chg_str = f"{chg_pct:.2f}%<br><small>(▼{abs(chg_won):,})</small>"
        else: chg_str = "0.00%"
            
        sigma = f"{float(r.get('tv_z', 0)):+.1f}σ"
        display_tone = f"{r.get('_sentiment', '-')}<br>({r.get('_tone', '')})"
        event_key = event_map.get(name, "-")

        body.append(f"| {name} | {close_str} | {chg_str} | {sigma} | {display_tone} | {event_key} |")

    table = "\n".join([header, sep] + body)
    
    memo_lines = ["\n### 🧐 종목별 관찰 메모"]
    bundle = _ensure_llm_bundle(ref_date)
    sig_comments = bundle.get("signal_comments") or {}
    
    for r in rows:
        name = r.get('name')
        comment = sig_comments.get(name) or "특이 수급 포착"
        memo_lines.append(f"- **{name}**: {comment}")

    memo_md = "\n".join(memo_lines)
    
    return f"{intro}\n\n기준일: {ref_date}\n\n{table}\n{memo_md}\n\n_위 리스트는 알고리즘 추출 결과이며, 투자 권유가 아닙니다._"

def section_signalist_history(ref_date: str, window_days: int = 90) -> str:
    ref = _date.fromisoformat(ref_date)
    return build_signalist_history_markdown(ref, lookback_days=window_days)

def load_kr_news_cleaned(ref_date: str, limit: int = 5) -> list[dict]:
    path = Path("iceage") / "data" / "processed" / f"kr_news_cleaned_{ref_date}.jsonl"
    if not path.exists(): return []
    items = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            try: items.append(json.loads(line))
            except: continue
            if len(items) >= limit: break
    return items

def load_global_news(ref_date: str, limit: int = 3) -> list[dict]:
    path = Path("iceage") / "data" / "processed" / f"global_news_{ref_date}.jsonl"
    if not path.exists(): return []
    items = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            try: items.append(json.loads(line))
            except: continue
            if len(items) >= limit: break
    return items

def section_news_digest(ref_date: str) -> str:
    kr_items = load_kr_news_cleaned(ref_date, limit=5)
    global_items = load_global_news(ref_date, limit=3)
    lines = ["## Today’s Top News"]

    if kr_items:
        lines.append("\n### 국내 주요 뉴스\n")
        for i, item in enumerate(kr_items, 1):
            lines.append(f"{i}. [{item.get('title')}]({item.get('link')}) ({item.get('source')})")
        lines.append("")

    if global_items:
        bundle = _ensure_llm_bundle(ref_date)
        llm_summary = bundle.get("global_summary")
        lines.append("\n### 해외 주요 뉴스\n")
        if isinstance(llm_summary, dict):
            if llm_summary.get("headline"): lines.append(f"**{llm_summary['headline']}**\n")
            if llm_summary.get("summary"): lines.append(f"{llm_summary['summary']}\n")
            for b in llm_summary.get("bullets", []): lines.append(f"- {b}")
            lines.append("")
        for i, item in enumerate(global_items, 1):
            t = item.get("title_en") or item.get("title")
            lines.append(f"{i}. [{t}]({item.get('link')}) ({item.get('source')})")

    return "\n".join(lines).strip()

def section_global_minute(ref_date: str) -> str:
    ref = _date.fromisoformat(ref_date)
    try:
        snap = get_market_overview(ref)
    except Exception as e:
        print(f"🚨 [ERROR] get_market_overview failed in section_global_minute: {e}")
        snap = {}
    indices = snap.get("indices", {})
    fx = snap.get("fx", {})
    commodities = snap.get("commodities", {})

    sp_level, sp_pct = indices.get("S&P 500", (None, None))
    ndq_level, ndq_pct = indices.get("NASDAQ", (None, None))
    dxy_level, dxy_pct = fx.get("DXY", (None, None))
    wti_level, wti_pct = commodities.get("WTI", commodities.get("WTI Crude", (None, None)))

    lines = ["## Global Minute", f"기준일: {ref_date}", ""]
    lines.append("### US")
    if sp_pct is not None:
        lines.append(f"- 이슈: S&P 500 {sp_level:,.2f} ({sp_pct:+.2f}%), NASDAQ {ndq_pct:+.2f}%")
        if sp_pct > 0.4: impact = "성장주·기술주 중심으로 위험선호가 강화된 흐름입니다."
        elif sp_pct < -0.4: impact = "금리·실적 부담으로 위험자산 회피 심리가 나타난 구간입니다."
        else: impact = "실적·매크로 이벤트를 소화하며 방향성을 탐색하는 조정 구간입니다."
        lines.append(f"- 해석: {impact}")
    else: lines.append("- 이슈: 데이터 부족")
    lines.append("")

    lines.append("### 달러/환율")
    if dxy_pct is not None:
        lines.append(f"- 이슈: 달러 인덱스(DXY) {dxy_level:,.2f} ({dxy_pct:+.2f}%)")
        if dxy_pct < -0.3: impact = "달러 약세 구간으로, 신흥국 자산과 위험자산에 상대적으로 우호적인 환경입니다."
        elif dxy_pct > 0.3: impact = "달러 강세로, 안전자산 선호 및 유동성 경계 심리가 반영된 흐름입니다."
        else: impact = "달러가 뚜렷한 방향성 없이 등락하며 단기 이벤트를 관망하는 구간입니다."
        lines.append(f"- 해석: {impact}")
    else: lines.append("- 이슈: 데이터 부족")
    lines.append("")

    lines.append("### 원자재/에너지")
    if wti_pct is not None:
        lines.append(f"- 이슈: WTI {wti_level:,.2f}달러 ({wti_pct:+.2f}%)")
        if wti_pct > 0.5: impact = "유가 상승으로 인플레이션·원가 부담에 대한 경계가 다시 부각될 수 있는 구간입니다."
        elif wti_pct < -0.5: impact = "유가 하락으로 물가 부담 완화 기대가 커지며 위험자산에 우호적인 환경입니다."
        else: impact = "유가가 박스권 등락을 이어가며 공급·수요 이슈를 소화하는 구간입니다."
        lines.append(f"- 해석: {impact}")
    else: lines.append("- 이슈: 데이터 부족")
    lines.append("")

    return "\n".join(lines)

def section_investors_mind(topic: str, body: str) -> str:
    if not topic or not body: return ""
    return dedent(f"""
    ## 🧘 Investor's Mind: {topic}
    {body}
    """).strip()

def _find_col(columns, candidates):
    cols = list(columns)
    for c in candidates:
        if c in columns: return c
    for c in candidates:
        for col in cols:
            if c in str(col): return col
    return None

def _load_turnover_by_market(ref: _date) -> dict[str, float]:
    """
    [수정] 거래대금 컬럼이 있어도 데이터가 0이면, 
    종가*거래량으로 강제 복구하여 반환하는 안전장치 추가
    """
    raw_path = Path("iceage") / "data" / "raw" / f"kr_prices_{ref.isoformat()}.csv"
    if not raw_path.exists(): return {}
    
    try: 
        df = pd.read_csv(raw_path)
    except: return {}
    
    # 숫자 변환 헬퍼
    def _clean(x):
        try: return float(str(x).replace(",", ""))
        except: return 0.0
        
    cols = set(df.columns)
    
    # 1. 시장 구분 컬럼 찾기
    market_col = _find_col(cols, ["market", "시장구분", "시장", "Market"])
    if not market_col: return {}
    
    # 2. 거래대금 우선 시도
    value_col = _find_col(cols, ["trading_value", "거래대금"])
    if value_col:
        df["_turnover_"] = df[value_col].apply(_clean)
    else:
        df["_turnover_"] = 0.0
        
    # 3. [핵심] 거래대금이 비어있거나 합계가 0이면 강제 계산 (심폐소생술)
    if df["_turnover_"].sum() == 0:
        close_col = _find_col(cols, ["close", "종가", "현재가"])
        vol_col = _find_col(cols, ["volume", "거래량"])
        
        if close_col and vol_col:
            df["_turnover_"] = df[close_col].apply(_clean) * df[vol_col].apply(_clean)

    return df.groupby(market_col)["_turnover_"].sum().to_dict()

def section_numbers_that_matter(ref_date: str) -> str:
    ref = _date.fromisoformat(ref_date)
    lines = ["## Numbers that Matter", f"기준일: {ref_date}", ""]
    
    by_market = _load_turnover_by_market(ref)
    if by_market:
        lines.append("### 오늘 국내 주식 거래대금 (조원 단위, 추정)")
        total = 0.0
        for market_name, v in by_market.items():
            total += float(v)
            trillions = float(v) / 1_000_000_000_000
            lines.append(f"- {market_name}: {trillions:,.1f}조")
        lines.append(f"- 합계: {total / 1_000_000_000_000:,.1f}조")
        lines.append("")
        
    fx_series = []
    for back in range(4, -1, -1):
        d = ref - timedelta(days=back)
        try:
            snap = get_market_overview(d)
            fx = snap.get("fx", {})
            if "USD/KRW" in fx: fx_series.append((d, fx["USD/KRW"][0]))
        except: continue
        
    if fx_series:
        lines.append("### USD/KRW 환율 (최근 일자)")
        prev = None
        for d, level in fx_series:
            diff = f"({level-prev:+.2f})" if prev else ""
            lines.append(f"- {d.isoformat()}: {level:,.2f} {diff}")
            prev = level
        lines.append("")
        
    if ENABLE_INVESTOR_FLOW_SECTION:
        flow_map = load_investor_flow(ref)
        if flow_map:
            lines.append("### 투자자별 매매 동향 (단위: 억원, 순매수 기준)")
            lines.append("| 시장 | 개인 | 외국인 | 기관 |")
            lines.append("|------|------|--------|------|")
            for m in ["KOSPI", "KOSDAQ"]:
                s = flow_map.get(m)
                if s:
                    p = s.net_by_investor.get("개인", 0)
                    f = s.net_by_investor.get("외국인", 0)
                    i = s.net_by_investor.get("기관", 0)
                    lines.append(f"| {m} | {p:,.1f} | {f:,.1f} | {i:,.1f} |")
            lines.append("")
            
    return "\n".join(lines)

def extract_first_sentence(text: str) -> str:
    if not text: return ""
    cleaned = " ".join(text.split())
    sentences = re.split(r'(?<=[\.!?])\s+', cleaned)
    return sentences[0].strip() if sentences else cleaned.strip()

def section_morning_quote(quote: str) -> str:
    return dedent(f"""
    ## Morning Quote
    > {quote}
    """).strip()

def section_footer() -> str:
    return dedent(f"""
    ---
    본 콘텐츠는 투자 권유 목적이 아닌 정보 제공용입니다.  
    The Signalist © 2025 All Rights Reserved.  [구독해지]  [의견보내기]
    """).strip()

MIND_TOPICS = ["확신보다 유연함", "손실을 대하는 태도", "과잉 확신의 함정", "복리와 기다림", "포지션 사이징"]
def pick_topic_and_body(ref_date: str) -> tuple[str, str]:
    import random
    fallback_topic = random.choice(MIND_TOPICS)
    fallback_body = "평정심을 유지하세요. 시장은 언제나 기회를 줍니다."
    try:
        bundle = _ensure_llm_bundle(ref_date)
        im = bundle.get("investor_mind") or {}
        return im.get("topic", fallback_topic), im.get("body", fallback_body)
    except: return fallback_topic, fallback_body

def render_newsletter(ref_date: str) -> str:
    topic, body = pick_topic_and_body(ref_date)
    parts = [
        section_header_intro(ref_date),
        section_market_thermometer(ref_date),
        section_signalist_today(ref_date),
        section_signalist_history(ref_date),
        section_themes(ref_date),
        # IPO 섹션 제거
        section_global_minute(ref_date),
        section_news_digest(ref_date),
        section_investors_mind(topic, body),
        section_numbers_that_matter(ref_date),
        section_footer()
    ]
    return "\n\n".join([p for p in parts if p])

def log_signalist_today(ref_date: str, rows: list, force: bool = True) -> None:
    if not rows: return
    out_dir = PROJECT_ROOT / "iceage" / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "signalist_today_log.csv"
    
    new_records = []
    for r in rows:
        if hasattr(r, '__dict__'):
            d = {
                "signal_date": ref_date, "code": getattr(r, "code", ""), "name": r.name,
                "close": r.close, "vol_sigma": float(getattr(r, "vol_sigma", 0.0)),
                "sentiment": getattr(r, "sentiment", ""), "insight": getattr(r, "insight", "")
            }
        else:
            d = {
                "signal_date": ref_date, "code": str(r.get('code', '')).zfill(6),
                "name": r.get('name', ''), "close": r.get('close', 0),
                "vol_sigma": float(r.get('tv_z', 0) or r.get('vol_sigma', 0)),
                "sentiment": r.get('_sentiment') or r.get('sentiment', ''),
                "insight": r.get('_insight') or r.get('insight', '')
            }
        new_records.append(d)
        
    new_records.sort(key=lambda x: abs(x['vol_sigma']), reverse=True)
    new_records = new_records[:5]
    df_new = pd.DataFrame(new_records)
    
    if out_path.exists():
        try:
            df_old = pd.read_csv(out_path, encoding="utf-8-sig")
            
            # [Fix] 컬럼 호환성 체크 (과거 백필 데이터 호환)
            if "date" in df_old.columns and "signal_date" not in df_old.columns:
                df_old.rename(columns={"date": "signal_date"}, inplace=True)
            
            if "tv_z" in df_old.columns and "vol_sigma" not in df_old.columns:
                df_old.rename(columns={"tv_z": "vol_sigma"}, inplace=True)
                
            if "signal_date" not in df_old.columns:
                print("[ERROR] 기존 로그 파일에 'signal_date' 컬럼이 없습니다. 덮어쓰기를 방지하기 위해 병합을 중단합니다.")
                return 

            df_old = df_old[df_old["signal_date"] != ref_date]
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        except Exception as e:
            print(f"[ERROR] 기존 로그 파일 병합 실패: {e}")
            print("   -> 기존 데이터를 보존하기 위해 새 데이터를 추가하지 않습니다.")
            return # 안전장치: 에러나면 그냥 리턴 (덮어쓰기 방지)
    else: 
        df_all = df_new
    
    df_all = df_all.sort_values("signal_date")
    df_all.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ [Log Saved] {ref_date} 시그널 {len(new_records)}개 저장 완료!")

def main():
    cal = TradingCalendar(CalendarConfig())
    if len(sys.argv) >= 2: ref_date = sys.argv[1]
    else: 
        ref = compute_reference_date(cal, datetime.now())
        ref_date = ref.isoformat()

    print(f"\n📅 Newsletter ref_date: {ref_date}")

    try:
        selector = StrategySelector(ref_date)
        results = selector.select_targets()
        candidates = []
        for r in results.get('panic_buying', []) + results.get('fallen_angel', []) + results.get('kings_shadow', []):
            r['_sentiment'] = '📈 매수 우위'
            b = r.get('size_bucket')
            if b == 'small': r['_insight'] = "소형주 수급 변곡점 포착"
            elif b == 'large': r['_insight'] = "대형주 추세 눌림목 포착"
            else: r['_insight'] = "중형주 낙폭 과대 포착"
            candidates.append(r)
        for r in results.get('overheat_short', []):
            r['_sentiment'] = '📉 매도 우위'
            r['_insight'] = "단기 과열권 도달 (고점 경고)"
            candidates.append(r)
            
        candidates.sort(key=lambda x: abs(float(x.get('tv_z', 0))), reverse=True)
        final_rows = candidates[:5]
        
        if final_rows:
            log_signalist_today(ref_date, final_rows)

    except Exception as e:
        print(f"[ERROR] 시그널 생성 중 오류: {e}")

    md = render_newsletter(ref_date)
    
    out_dir = PROJECT_ROOT / "iceage" / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = _get_newsletter_env_suffix()
    filename = f"Signalist_Daily_{ref_date}{suffix}.md"
    out_path = out_dir / filename
    
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"✅ 생성 완료: {out_path}")

if __name__ == "__main__":
    main()