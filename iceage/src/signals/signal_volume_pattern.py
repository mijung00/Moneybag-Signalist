from datetime import date
from datetime import timedelta as _timedelta, datetime as _dt
from typing import List, Optional, Dict, Any

import numpy as np
import pandas as pd

import json
from pathlib import Path

from iceage.src.data_sources.kr_prices import load_normalized_prices
from iceage.src.data_sources.signalist_today import SignalRow


def _find_col(df: pd.DataFrame, candidates) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


# --- 내부 이벤트(회사 이슈) 태깅용 헬퍼들 ---

# (태그명, [키워드들]) 리스트
EVENT_TAG_RULES: List[tuple[str, list[str]]] = [
    # 1) 실적/가이던스
    (
        "실적/가이던스",
        [
            "실적",
            "실적발표",
            "실적 발표",
            "잠정실적",
            "잠정 실적",
            "어닝 서프라이즈",
            "어닝 서프",
            "어닝 쇼크",
            "매출",
            "매출액",
            "영업이익",
            "순이익",
            "흑자 전환",
            "적자 전환",
            "guidance",
            "가이던스",
            "실적 전망",
            "실적 추정",
        ],
    ),

    # 2) 매출/수주 성장(탑라인 드라이버)
    (
        "매출/수주 성장",
        [
            "수주 증가",
            "수주 성장",
            "수주 확대",
            "수주 잔고",
            "backlog",
            "수주잔고",
            "매출 성장",
            "매출 증가",
            "매출 확대",
        ],
    ),

    # 3) 공급계약/장기 계약/공시
    (
        "공급계약/수주 공시",
        [
            "공급계약",
            "공급 계약",
            "장기 공급",
            "장기계약",
            "장기 계약",
            "수주",
            "대규모 수주",
            "수주 공시",
            "계약 체결",
            "계약을 체결",
            "계약을 맺",
            "양해각서",
            "mou 체결",
            "mou를 체결",
            "loi",
            "입찰",
            "낙찰",
            "공시",
        ],
    ),

    # 4) M&A/지분 인수·매각
    (
        "M&A/지분 거래",
        [
            "인수합병",
            "인수 합병",
            "m&a",
            "합병",
            "분할합병",
            "분할 합병",
            "흡수합병",
            "지분 인수",
            "지분 취득",
            "경영권 분쟁",
            "경영권 확보",
            "경영권 매각",
            "지분 매각",
            "지분 처분",
        ],
    ),

    # 5) 경영권/오너·지배구조
    (
        "경영권/지배구조 이슈",
        [
            "최대주주 변경",
            "최대 주주 변경",
            "오너 리스크",
            "오너 리스크",
            "지배구조 개선",
            "지배 구조 개선",
            "지배구조 개편",
            "지배 구조 개편",
            "경영 참여",
            "경영권 참여",
        ],
    ),

    # 6) 증자/CB/BW 등 자본 조달
    (
        "증자/CB/BW 자금조달",
        [
            "유상증자",
            "무상증자",
            "유상 증자",
            "무상 증자",
            "전환사채",
            "cb 발행",
            "전환 사채",
            "신주 발행",
            "신주인수권부사채",
            "신주인수권",
            "bw 발행",
            "사모채 발행",
            "회사채 발행",
        ],
    ),

    # 7) 자사주/배당/주주환원
    (
        "배당/자사주/주주환원",
        [
            "배당",
            "현금배당",
            "현금 배당",
            "중간배당",
            "분기배당",
            "특별배당",
            "배당금",
            "배당 정책",
            "자사주 취득",
            "자사주 매입",
            "자사주 소각",
            "주주환원",
            "주주 환원",
            "배당성향",
        ],
    ),

    # 8) 신사업/플랫폼/AI
    (
        "신사업/플랫폼/AI",
        [
            "신사업",
            "신규 사업",
            "새로운 사업",
            "플랫폼",
            "플랫폼 출시",
            "플랫폼 사업",
            "ai 사업",
            "인공지능 서비스",
            "ai 플랫폼",
            "데이터 플랫폼",
            "클라우드 사업",
        ],
    ),

    # 9) 신제품/서비스 출시
    (
        "신제품/서비스 출시",
        [
            "신제품",
            "신규 제품",
            "신규 서비스",
            "서비스 출시",
            "출시",
            "론칭",
            "런칭",
            "신규 라인업",
            "신규 라인 업",
            "업그레이드 버전",
        ],
    ),

    # 10) 임상/허가/의약품 이슈
    (
        "임상/허가 이슈",
        [
            "임상",
            "임상 1상",
            "임상1상",
            "임상 2상",
            "임상2상",
            "임상 3상",
            "임상3상",
            "임상시험",
            "임상 시험",
            "품목허가",
            "품목 허가",
            "허가 취득",
            "허가 신청",
            "승인 신청",
            "fda 승인",
            "fda 허가",
            "fda 심사",
            "신약 허가",
            "신약 승인",
            "신약 후보",
        ],
    ),

    # 11) 특허/기술 개발
    (
        "특허/기술 개발",
        [
            "특허 출원",
            "특허 등록",
            "특허 취득",
            "기술 이전",
            "기술이전",
            "공동 연구",
            "공동연구",
            "기술 제휴",
            "기술제휴",
            "알고리즘 개발",
            "플랫폼 기술",
        ],
    ),

    # 12) 규제/제재/행정조치
    (
        "규제/제재/행정조치",
        [
            "제재",
            "징계",
            "영업정지",
            "영업 정지",
            "과징금",
            "행정지도",
            "행정 제재",
            "제재 심의",
            "금융감독원",
            "금감원",
            "공정거래위원회",
            "공정위",
            "조사 착수",
        ],
    ),

    # 13) 소송/법적 리스크
    (
        "소송/법적 리스크",
        [
            "소송",
            "소송전",
            "소송 전",
            "집단소송",
            "집단 소송",
            "손해배상 청구",
            "소송 제기",
            "소송을 제기",
            "소송을 당",
            "소송 취하",
            "가처분",
            "법원",
        ],
    ),

    # 14) 생산/설비 증설·중단
    (
        "생산/설비 증설·중단",
        [
            "증설",
            "라인 증설",
            "공장 증설",
            "생산능력 확대",
            "증산",
            "생산 중단",
            "가동 중단",
            "가동 재개",
            "공장 가동",
            "라인 가동",
            "설비 투자",
            "capex",
        ],
    ),

    # 15) 원재료/공급망/가격 이슈
    (
        "원재료/공급망 이슈",
        [
            "원재료 가격",
            "원자재 가격",
            "원재료 상승",
            "원재료 하락",
            "공급망",
            "supply chain",
            "공급 차질",
            "공급 중단",
            "수급 불안",
            "수급 차질",
        ],
    ),

    # 16) 경영진/조직변경/인사
    (
        "경영진/조직개편",
        [
            "대표이사 교체",
            "대표이사 선임",
            "대표이사 해임",
            "대표이사 변경",
            "ceo 교체",
            "ceo 선임",
            "경영진 교체",
            "경영진 개편",
            "조직 개편",
            "조직개편",
            "임원 인사",
            "임원 인사 단행",
        ],
    ),

    # 17) ESG/안전/사고
    (
        "ESG/안전/사고 이슈",
        [
            "환경 오염",
            "환경오염",
            "환경 규제",
            "esg",
            "탄소중립",
            "탄소 중립",
            "온실가스",
            "산업재해",
            "산재",
            "화재",
            "공장 화재",
            "사고 발생",
        ],
    ),

    # 18) IR/컨퍼런스콜/투자자 소통
    (
        "IR/컨콜/투자자 소통",
        [
            "ir",
            "ir 행사",
            "ir 미팅",
            "컨퍼런스콜",
            "컨콜",
            "기업설명회",
            "기업 설명회",
            "ndr",
            "non deal roadshow",
            "애널리스트 미팅",
        ],
    ),

    # 19) 리포트/목표가 조정
    (
        "리포트/목표가 조정",
        [
            "투자의견",
            "투자 의견",
            "목표가 상향",
            "목표가 하향",
            "목표주가 상향",
            "목표주가 하향",
            "목표 주가",
            "증권사 리포트",
            "리포트 발표",
        ],
    ),

    # 20) 기타 테마/루머성 이슈
    (
        "테마/루머성 이슈",
        [
            "루머",
            "풍문",
            "시장 루머",
            "테마주",
            "관련주",
            "수혜주",
            "관련 주",
            "수혜 주",
        ],
    ),
]



def _parse_event_published_at(value: str) -> Optional[date]:
    """
    kr_news_cleaner 가 만들어 둔 published_at 문자열을 date 로 파싱한다.

    - ISO 형식(2025-11-09 또는 2025-11-09T...)이면 그대로 파싱
    - "11/09/2025, 02:20 AM, +0000 UTC" 같은 형식도 지원
    - 실패하면 None 반환 (이 경우 해당 뉴스는 날짜 기준 필터링에서 제외)
    """
    if not value:
        return None
    value = str(value).strip()
    from datetime import datetime as _dt

    # 1) ISO 형태 우선 시도
    try:
        v = value.replace("Z", "+00:00")
        return _dt.fromisoformat(v).date()
    except Exception:
        pass

    # 2) "11/09/2025, 02:20 AM, +0000 UTC" 형태
    try:
        if value.endswith(" UTC"):
            value2 = value[:-4]
        else:
            value2 = value
        dt = _dt.strptime(value2, "%m/%d/%Y, %I:%M %p, %z")
        return dt.date()
    except Exception:
        return None


def _load_stock_event_news(
    ref_date: date,
    window_days: int = 7,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
    """
    kr_news_cleaned_{date}.jsonl 에서 kind == 'stock_event' 인 뉴스만
    종목 code / name 기준으로 묶어서 반환.

    window_days:
        published_at 기준 ref_date ± window_days 안에 있는 기사만 사용.
        (날짜를 파싱할 수 없으면 품질 관점에서 과감히 스킵)
    """
    base = Path("iceage") / "data" / "processed"
    path = base / f"kr_news_cleaned_{ref_date.isoformat()}.jsonl"

    events_by_code: Dict[str, List[Dict[str, Any]]] = {}
    events_by_name: Dict[str, List[Dict[str, Any]]] = {}

    if not path.exists():
        return events_by_code, events_by_name

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj: Dict[str, Any] = json.loads(line)
            except Exception:
                continue

            if obj.get("kind") != "stock_event":
                continue

            pub_raw = obj.get("published_at") or ""
            pub_date = _parse_event_published_at(pub_raw)
            if pub_date is None:
                # 날짜를 알 수 없으면 품질 관점에서 과감히 스킵
                continue

            if abs((pub_date - ref_date).days) > window_days:
                # ref_date ± window_days 밖이면 사용하지 않음
                continue

            code = obj.get("code")
            name = obj.get("name")

            if code:
                events_by_code.setdefault(str(code), []).append(obj)
            if name:
                events_by_name.setdefault(str(name), []).append(obj)

    return events_by_code, events_by_name


def _infer_internal_event_tag(items: List[Dict[str, Any]]) -> str:
    """
    한 종목에 대해 모인 여러 개의 stock_event 뉴스(items)를 보고
    '내부 이벤트'용 태그 한 줄을 생성한다.

    기존: 태그가 하나라도 걸리면 True만 보고 EVENT_TAG_RULES 순서대로 사용
    변경: 각 태그가 기사들에서 몇 번 등장했는지 "빈도"를 세고,
          많이 등장한 태그 순으로 상위 2개만 선택.

    - 태그 스코어 = (해당 태그 키워드가 걸린 기사 개수)
    - 스코어가 동일하면 EVENT_TAG_RULES 정의 순서를 우선
    - 최종적으로는 상위 2개 태그를 " / "로 join
    - 아무 태그도 안 걸리면 "" 반환
    """
    if not items:
        return ""

    texts: List[str] = []
    for art in items:
        parts = [
            str(art.get("title", "")),
            str(art.get("snippet", "")),
            str(art.get("summary", "")),
        ]
        texts.append(" ".join(parts).lower())

    # 태그별 카운트 초기화
    tag_counts: Dict[str, int] = {tag: 0 for tag, _ in EVENT_TAG_RULES}

    # 기사 텍스트를 돌면서 각 태그가 몇 번 등장했는지 집계
    for doc in texts:
        for tag, keywords in EVENT_TAG_RULES:
            if any(kw.lower() in doc for kw in keywords):
                tag_counts[tag] += 1

    # 1번도 안 걸린 태그는 버림
    tag_items = [(tag, cnt) for tag, cnt in tag_counts.items() if cnt > 0]
    if not tag_items:
        return ""

    # EVENT_TAG_RULES 상의 원래 순서를 tie-breaker로 사용
    tag_index: Dict[str, int] = {tag: i for i, (tag, _) in enumerate(EVENT_TAG_RULES)}

    # 1) 많이 등장한 태그 우선  2) 그다음 규칙 정의 순서
    tag_items.sort(key=lambda x: (-x[1], tag_index[x[0]]))

    # 너무 길어지지 않게 상위 2개만
    top_tags = [tag for tag, _ in tag_items[:2]]
    return " / ".join(top_tags)



def _build_internal_event_map(
    ref_date: date,
    window_days: int = 7,
) -> Dict[str, str]:
    """
    code / name 기준으로 "내부 이벤트 태그"를 매핑한 dict를 만든다.

    반환 예:
        {
            "005930": "실적/어닝 이슈",
            "삼성전자": "실적/어닝 이슈",
            ...
        }
    """
    events_by_code, events_by_name = _load_stock_event_news(ref_date, window_days)
    tag_by_key: Dict[str, str] = {}

    # 코드 기준
    for code, items in events_by_code.items():
        tag = _infer_internal_event_tag(items)
        if tag:
            tag_by_key[code] = tag

    # 이름 기준 (code로 이미 채워진 건 덮어쓰지 않음)
    for name, items in events_by_name.items():
        tag = _infer_internal_event_tag(items)
        if tag and name not in tag_by_key:
            tag_by_key[name] = tag

    return tag_by_key



def detect_signals_from_prices(ref_date: date, top_n: int | None = None) -> List[SignalRow]:
    """
    ref_date 기준 한국 주식 시세에서
    '오늘 시장 내에서 상대적으로 거래대금과 가격 움직임이 큰 종목'을 선별한다.

    - 거래대금(가격×거래량)을 기준으로 단면 z-score(vol_sigma)를 계산
    - ETF/ETN/리츠/인버스/레버리지 등은 제외
    - 저유동성(거래대금 하위 구간)은 유니버스에서 제외

    top_n:
      None이면 조건을 통과한 모든 종목을 반환하고,
      정수가 주어지면 그 개수만큼 score 상위 종목만 반환한다.
    """

    df = load_normalized_prices(ref_date).copy()
    if df.empty:
        return []

    # 0) 기본 컬럼 탐색
    name_col = _find_col(df, ["name", "종목명"])
    code_col = _find_col(df, ["code", "종목코드", "ticker"])
    price_col = _find_col(df, ["close", "현재가", "종가"])
    vol_col = _find_col(df, ["volume", "거래량", "VOL", "vol"])
    value_col = _find_col(df, ["trading_value", "amount", "value", "거래대금"])

    if name_col is None or price_col is None or vol_col is None:
        return []

    df[name_col] = df[name_col].astype(str)

    # 1) ETF/ETN/리츠/인버스/레버리지/액티브 ETF 등 지수형 상품 제외
    etf_keywords = [
        "KODEX",
        "TIGER",
        "KINDEX",
        "ACE",
        "ARIRANG",
        "HANARO",
        "SOL ",
        "인버스",
        "레버리지",
        "선물",
        "TRF",
        "ETN",
        "리츠",
        "REITs",
        "ETF",
        "액티브",
        "TIMEFOLIO",
    ]
    etf_pattern = "|".join(etf_keywords)
    df = df[~df[name_col].str.contains(etf_pattern, regex=True, case=False, na=False)].copy()
    if df.empty:
        return []

    # 2) 거래대금 계산/정리
    if value_col is None:
        price_clean = df[price_col].astype(str).str.replace(",", "", regex=False)
        price_clean = pd.to_numeric(price_clean, errors="coerce")

        vol_clean = df[vol_col].astype(str).str.replace(",", "", regex=False)
        vol_clean = pd.to_numeric(vol_clean, errors="coerce")

        df["trading_value"] = price_clean * vol_clean
        value_col = "trading_value"
    else:
        df[value_col] = df[value_col].astype(str).str.replace(",", "", regex=False)
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

    df = df.dropna(subset=[value_col])
    if df.empty:
        return []

    # 3) 유동성 필터: 거래대금 너무 작은 종목 컷
    abs_floor = 5e8  # 5억
    q40 = df[value_col].quantile(0.4)
    liquidity_cut = max(abs_floor, q40)
    df = df[df[value_col] >= liquidity_cut].copy()
    if df.empty:
        return []

    # 4) 거래대금 단면 z-score 계산 (log scale)
    log_v = np.log1p(df[value_col].astype(float))
    mu = log_v.mean()
    sigma = log_v.std(ddof=0)
    if sigma <= 0:
        df["vol_sigma"] = 0.0
    else:
        df["vol_sigma"] = (log_v - mu) / sigma

    df["vol_sigma_abs"] = df["vol_sigma"].abs()

    # 5) 일간 등락률 계산
    ret_col = _find_col(
        df,
        [
            "ret_1d",
            "return_1d",
            "pct_change",
            "chg_pct",
            "change_pct",
            "등락률",
        ],
    )

    if ret_col is not None:
        if df[ret_col].dtype == "O":
            tmp = (
                df[ret_col]
                .astype(str)
                .str.replace("%", "", regex=False)
                .str.replace("+", "", regex=False)
                .str.replace(",", "", regex=False)
            )
            ret_series = pd.to_numeric(tmp, errors="coerce") / 100.0
        else:
            ret_series = pd.to_numeric(df[ret_col], errors="coerce")
    else:
        ret_series = pd.Series(0.0, index=df.index)

    df["__ret__"] = ret_series
    df = df.dropna(subset=["__ret__"])
    if df.empty:
        return []

    # 6) 가격/거래대금 움직임 필터 + 최소 5개 확보
    ret_abs = df["__ret__"].abs()
    vol_abs = df["vol_sigma_abs"]

    def _filter(r_min: float, v_min: float) -> pd.DataFrame:
        return df[(ret_abs >= r_min) & (vol_abs >= v_min)].copy()

    candidates = [
        (0.02, 1.0),  # 1차: 2% 이상 + 1σ 이상
        (0.015, 0.8),  # 2차: 1.5% 이상 + 0.8σ 이상
        (0.01, 0.5),  # 3차: 1% 이상 + 0.5σ 이상
        (0.005, 0.0),  # 4차: 0.5% 이상, vol_sigma 제한 없음
    ]
    df_sel = pd.DataFrame()
    for r_min, v_min in candidates:
        tmp = _filter(r_min, v_min)
        if len(tmp) >= 5:
            df_sel = tmp
            break
        if df_sel.empty or len(tmp) > len(df_sel):
            df_sel = tmp

    df = df_sel
    if df.empty:
        return []

    # 7) 수급 심리 점수/라벨
    # - 가격 등락률(__ret__)과 거래대금 z-score(vol_sigma)를 섞어서
    #   [-2, 2] 구간의 'sentiment_score'를 만들고,
    #   이를 5단계 라벨로 변환한다.
    ret = df["__ret__"].astype(float)
    z = df["vol_sigma"].astype(float)

    # 3% 등락 → 감정 1단계, 2σ → 감정 1단계 정도로 스케일링
    comp_ret = np.clip(ret / 0.03, -1.0, 1.0)
    comp_z = np.clip(z / 2.0, -1.0, 1.0)

    # 두 축을 반반 섞고, [-1, 1] → [-2, 2]로 확장
    sentiment_score = 2.0 * (0.5 * comp_ret + 0.5 * comp_z)
    df["sentiment_score"] = sentiment_score

    def _sentiment_label(s: float) -> str:
        if s >= 1.0:
            return "🔥 과열 유입"
        if s >= 0.3:
            return "⬆️ 유입 우세"
        if s > -0.3:
            return "⚖️ 관망"
        if s > -1.0:
            return "⬇️ 이탈 우세"
        return "❄️ 과열 이탈"

    df["sentiment"] = df["sentiment_score"].apply(_sentiment_label)


    # 8) 시그널 해석 문구
    def _insight(row) -> str:
        r = float(row["__ret__"])
        z = float(row["vol_sigma"])

        if r >= 0.02 and z >= 1.0:
            return (
                "가격과 거래대금이 동시에 평균을 뚜렷하게 상회하는 상승 패턴입니다. "
                "단기 수급이 과열될 수 있어 추격 매수 시 변동성 관리가 필요합니다."
            )
        if r <= -0.02 and z >= 1.0:
            return (
                "거래대금이 동반된 하락 구간입니다. 손절·투매 또는 악재 해석 구간일 수 있어 "
                "관련 뉴스·공시를 함께 확인하는 것이 좋습니다."
            )
        if r > 0 and z < 0:
            return (
                "거래대금 증가 없이 조용히 우상향하는 패턴입니다. 기관·외국인의 "
                "천천히 쌓이는 수급일 수 있어 중기적인 흐름을 점검할 필요가 있습니다."
            )
        if r < 0 and z < 0:
            return (
                "가격과 거래대금이 모두 위축된 구간입니다. 단기 관심도는 낮지만, "
                "과도한 저평가 구간이 아닌지 체크해볼 수 있습니다."
            )
        return (
            "가격과 거래대금이 평균 대비 의미 있는 수준으로 움직인 종목입니다. "
            "세부 재료와 수급 원인을 추가로 점검할 필요가 있습니다."
        )

    df["insight"] = df.apply(_insight, axis=1)

    # 9) 시그널 유형 태그 (패턴 태그) – 내부 이벤트와는 별개
    def _pattern_tag(row) -> str:
        r = float(row["__ret__"])
        z = float(row["vol_sigma"])

        if r >= 0.05 and z >= 2.0:
            return "강한 수급 상승"
        if r <= -0.05 and z >= 2.0:
            return "거래대금 동반 급락"
        if z >= 2.0:
            return "거래대금 급증"
        if r > 0.01 and z <= 0.0:
            return "조용한 상승"
        if r < -0.01 and z <= 0.0:
            return "조용한 하락"
        return "수급 패턴 유의"

    df["event_tag"] = df.apply(_pattern_tag, axis=1)

    # 10) 스코어
    df["score"] = df["vol_sigma_abs"] * 2.0 + df["__ret__"].abs()

    # 11) 가격 숫자화
    df[price_col] = df[price_col].astype(str).str.replace(",", "", regex=False)
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.dropna(subset=[price_col])
    if df.empty:
        return []

    # 12) 최종 정렬 + top_n 적용
    df_sorted = df.sort_values("score", ascending=False)
    if top_n is not None:
        df_sorted = df_sorted.head(top_n)

    # 13) 내부 이벤트 태그 (종목 이벤트 뉴스 기반: code 우선, name fallback)
    internal_event_map = _build_internal_event_map(ref_date)

    picks: List[SignalRow] = []
    for _, r in df_sorted.iterrows():
        name_val = str(r[name_col])
        code_str = None
        if code_col is not None and code_col in df.columns:
            code_str = str(r[code_col])

        # code -> name 순으로 내부 이벤트 태그 매칭
        internal_event = ""
        if code_str:
            internal_event = internal_event_map.get(code_str, "")
        if not internal_event:
            internal_event = internal_event_map.get(name_val, "")

        row_obj = SignalRow(
            name=name_val,
            close=int(r[price_col]),
            vol_sigma=float(r["vol_sigma"]),
            sentiment=str(r["sentiment"]),
            event=internal_event,  # 내부 이벤트
            insight=str(r["insight"]),
        )

        # 패턴 태그는 별도 속성으로 (시그널 유형)
        setattr(row_obj, "pattern_tag", str(r.get("event_tag", "")))

        # 코드도 같이 달아두기 (히스토리/뉴스 매핑용)
        if code_str:
            setattr(row_obj, "code", code_str)

        picks.append(row_obj)

    return picks


from typing import List

def select_featured_signals(signals: List["SignalRow"], k: int = 5) -> List["SignalRow"]:
    """
    Universe 시그널들 중에서 뉴스레터/공식 로그에 실릴 5개를 고른다.

    여기서는 detect_signals_from_prices가
    "이미 점수 순으로 정렬된 리스트"를 준다고 가정하고,

    - Universe: 전달받은 signals 전체 (정렬 순서 유지)
    - 1차 선택: 앞에서부터 k개
    - 전부 같은 방향(상승/하락)이면:
      * Universe 전체에서 반대 방향(ret_5d 반대 부호)인 종목을 하나 찾고
      * base 안의 '다수 방향' 종목 중 제일 뒤에 있는 애를 그걸로 교체

    점수 필드는 쓰지 않고, 정렬 순서 + ret_5d 방향만 쓴다.
    """
    if not signals:
        return []

    # Universe 순서는 detect_signals_from_prices가 정해준대로 사용
    universe = list(signals)
    base = universe[:k]

    def direction(row: "SignalRow") -> int:
        r = getattr(row, "ret_5d", 0.0)
        if r > 0:
            return 1
        if r < 0:
            return -1
        return 0  # 보합

    dirs = [direction(s) for s in base]
    ups = sum(1 for d in dirs if d > 0)
    downs = sum(1 for d in dirs if d < 0)

    # 이미 상승/하락이 섞여 있으면 그대로 사용
    if ups > 0 and downs > 0:
        return base

    # 한쪽으로 몰려 있는 경우
    majority_sign = 1 if ups > 0 else -1
    minority_sign = -majority_sign

    # Universe 전체에서 '반대 방향' 후보 찾기 (정렬 순서 기준으로 가장 강한 애)
    opposite_pick = None
    for s in universe:
        if direction(s) == minority_sign:
            opposite_pick = s
            break

    # 반대 방향이 아예 없으면(=Universe 10개 전부 같은 방향) → 그냥 base 반환
    if opposite_pick is None:
        return base

    # base 안에서 '다수 방향'인 애들 중 제일 뒤에 있는 애를 교체 대상로 선택
    majority_indices = [i for i, s in enumerate(base) if direction(s) == majority_sign]
    if not majority_indices:
        return base

    weakest_idx = majority_indices[-1]
    base[weakest_idx] = opposite_pick

    return base


# iceage/src/analyzers/signal_volume_pattern.py

from pathlib import Path
from datetime import date
import numpy as np
import pandas as pd

from iceage.src.data_sources.signalist_today import SignalRow

# 이미 위쪽에 EVENT_TAG_RULES, _build_internal_event_map 등 존재함
# :contentReference[oaicite:2]{index=2}

DATA_PROCESSED_DIR = Path("iceage") / "data" / "processed"


def detect_signals_from_volume_anomaly_v2(
    ref_date: date,
    use_top_bucket_only: bool = True,
) -> list[SignalRow]:
    """
    volume_anomaly_v2에서 뽑은 CSV를 기반으로
    뉴스레터용 SignalRow 리스트를 만들어 준다.
    """

    ref_str = ref_date.isoformat()
    path = DATA_PROCESSED_DIR / f"volume_anomaly_v2_{ref_str}.csv"
    if not path.exists():
        raise FileNotFoundError(f"{path} 가 없습니다. 먼저 volume_anomaly_v2를 돌려주세요.")

    df = pd.read_csv(path)

    # 1) 유니버스 선택: 체급별 상위 버킷만 사용
    if use_top_bucket_only and "is_top_bucket" in df.columns:
        df = df[df["is_top_bucket"] == True].copy()

    if df.empty:
        return []

    # 정렬 기준: 시장 대비 괴리(tv_z_rel)가 큰 순으로
    sort_col = "tv_z_rel" if "tv_z_rel" in df.columns else "tv_z"
    df = df.sort_values(sort_col, ascending=False)

    # 2) 수익률/거래대금 z-score 기반 sentiment_score 계산
    # change_rate는 보통 %라고 가정 → 100으로 나눠서 소수화
    ret = pd.to_numeric(df["change_rate"], errors="coerce") / 100.0
    z = pd.to_numeric(df["tv_z"], errors="coerce")  # v1의 vol_sigma 역할

    comp_ret = np.clip(ret / 0.03, -1.0, 1.0)  # 3% 등락을 기준 1단계로
    comp_z = np.clip(z / 2.0, -1.0, 1.0)       # 2σ를 기준 1단계로
    sentiment_score = 2.0 * (0.5 * comp_ret + 0.5 * comp_z)

    def _sentiment_label(s: float) -> str:
        if s >= 1.0:
            return "🔥 과열 유입"
        if s >= 0.3:
            return "⬆️ 유입 우세"
        if s > -0.3:
            return "⚖️ 관망"
        if s > -1.0:
            return "⬇️ 이탈 우세"
        return "❄️ 과열 이탈"

    df["sentiment_score"] = sentiment_score
    df["sentiment"] = df["sentiment_score"].apply(_sentiment_label)

    # 3) 패턴 태그 + insight 문구 (v1 로직을 그대로 옮김)
    def _insight(row) -> str:
        r = float(row.get("ret_1d", row["change_rate"] / 100.0))
        z = float(row["tv_z"])

        if r >= 0.02 and z >= 1.0:
            return (
                "가격과 거래대금이 동시에 평균을 뚜렷하게 상회하는 상승 패턴입니다. "
                "단기 수급이 과열될 수 있어 추격 매수 시 변동성 관리가 필요합니다."
            )
        if r <= -0.02 and z >= 1.0:
            return (
                "거래대금이 동반된 하락 구간입니다. 손절·투매 또는 악재 해석 구간일 수 있어 "
                "관련 뉴스·공시를 함께 확인하는 것이 좋습니다."
            )
        if r > 0 and z < 0:
            return (
                "거래대금 증가 없이 조용히 우상향하는 패턴입니다. 기관·외국인의 "
                "천천히 쌓이는 수급일 수 있어 중기적인 흐름을 점검할 필요가 있습니다."
            )
        if r < 0 and z < 0:
            return (
                "가격과 거래대금이 모두 위축된 구간입니다. 단기 관심도는 낮지만, "
                "과도한 저평가 구간이 아닌지 체크해볼 수 있습니다."
            )
        return (
            "가격과 거래대금이 평균 대비 의미 있는 수준으로 움직인 종목입니다. "
            "세부 재료와 수급 원인을 추가로 점검할 필요가 있습니다."
        )

    def _pattern_tag(row) -> str:
        r = float(row.get("ret_1d", row["change_rate"] / 100.0))
        z = float(row["tv_z"])

        if r >= 0.05 and z >= 2.0:
            return "강한 수급 상승"
        if r <= -0.05 and z >= 2.0:
            return "거래대금 동반 급락"
        if z >= 2.0:
            return "거래대금 급증"
        if r > 0.01 and z <= 0.0:
            return "조용한 상승"
        if r < -0.01 and z <= 0.0:
            return "조용한 하락"
        return "수급 패턴 유의"

    df["insight"] = df.apply(_insight, axis=1)
    df["event_tag"] = df.apply(_pattern_tag, axis=1)

    # 4) 내부 이벤트 태그 (뉴스 기반) 재사용
    internal_event_map = _build_internal_event_map(ref_date)

    # 5) SignalRow 리스트로 변환
    picks: list[SignalRow] = []
    for _, r in df.iterrows():
        name = str(r["name"])
        code = str(r["code"])
        close = int(r["close"])

        internal_event = internal_event_map.get(code) or internal_event_map.get(name, "")

        row_obj = SignalRow(
            name=name,
            close=close,
            vol_sigma=float(r["tv_z"]),   # v1에서 쓰던 vol_sigma 역할
            sentiment=str(r["sentiment"]),
            event=internal_event,
            insight=str(r["insight"]),
        )
        setattr(row_obj, "pattern_tag", str(r.get("event_tag", "")))
        setattr(row_obj, "code", code)

        picks.append(row_obj)

    return picks
