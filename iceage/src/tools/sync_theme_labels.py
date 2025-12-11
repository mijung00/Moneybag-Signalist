from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

# --- 설정값 (필요하면 나중에 숫자만 바꾸면 됨) ---

# 새 테마로 자동 추가하기 위한 최소 종목 수
MIN_STOCKS_FOR_NEW_THEME = 3

# 하루에 자동으로 추가할 테마 개수 상한 (거래대금 상위 N개)
TOP_K_THEMES_PER_DAY = 50


# --- 유틸 함수들 ---

def parse_ref_date(arg: str) -> date:
    """YYYY-MM-DD 형식 문자열을 date 로 변환."""
    try:
        return date.fromisoformat(arg)
    except ValueError:
        print(f"[ERROR] 잘못된 날짜 형식입니다: {arg} (예: 2025-11-12)")
        sys.exit(1)


def load_naver_theme_df(ref_date: date) -> pd.DataFrame:
    """네이버 테마 raw 파일을 읽어온다."""
    theme_path = Path("iceage") / "data" / "raw" / f"naver_themes_{ref_date.isoformat()}.csv"
    if not theme_path.exists():
        print(f"[ERROR] 네이버 테마 파일이 없습니다: {theme_path}")
        sys.exit(1)

    df = pd.read_csv(theme_path, dtype={"code": str})
    if "naver_label" not in df.columns or "code" not in df.columns:
        print("[ERROR] naver_themes CSV에 'naver_label' 또는 'code' 컬럼이 없습니다.")
        sys.exit(1)

    df["code"] = df["code"].astype(str).str.zfill(6)
    df["naver_label"] = df["naver_label"].astype(str).str.strip()
    df = df.dropna(subset=["naver_label"])

    return df


def pick_candidate_labels(df: pd.DataFrame) -> list[str]:
    """
    네이버 테마 중에서 '괜찮은' 테마만 뽑는다.

    기준:
    1) 해당 테마에 속한 종목 수가 MIN_STOCKS_FOR_NEW_THEME 이상
    2) 거래대금(또는 비슷한 컬럼)이 있으면, 그 합계 기준 상위 TOP_K_THEMES_PER_DAY 개만 선택
    """
    # 종목 수
    group_info = {"stock_count": ("code", "nunique")}

    # 거래대금 비슷한 컬럼 자동 탐지 시도
    turnover_col = None
    for c in df.columns:
        cl = c.lower()
        if cl in ("turnover", "trading_value", "amount", "value_traded", "거래대금"):
            turnover_col = c
            break

    if turnover_col:
        group_info["turnover_sum"] = (turnover_col, "sum")

    grouped = df.groupby("naver_label").agg(**group_info).reset_index()

    # 1차 필터: 종목 수 기준
    candidates = grouped[grouped["stock_count"] >= MIN_STOCKS_FOR_NEW_THEME]

    if candidates.empty:
        return []

    # 2차 필터: 거래대금 상위 N개만 (거래대금 컬럼이 있을 때만)
    if "turnover_sum" in candidates.columns:
        candidates = candidates.sort_values("turnover_sum", ascending=False).head(TOP_K_THEMES_PER_DAY)

    return candidates["naver_label"].tolist()


def load_theme_labels() -> pd.DataFrame:
    """기존 theme_labels.csv 를 읽어오거나, 없으면 빈 DataFrame을 만든다."""
    labels_path = Path("iceage") / "configs" / "theme_labels.csv"
    if labels_path.exists():
        df = pd.read_csv(labels_path, dtype=str)
        # 기본 컬럼이 없는 경우 대비
        for col in ["naver_label", "canonical_theme", "role", "status", "first_seen", "last_seen"]:
            if col not in df.columns:
                df[col] = ""
        return df
    else:
        return pd.DataFrame(columns=["naver_label", "canonical_theme", "role", "status", "first_seen", "last_seen"])


def save_theme_labels(df: pd.DataFrame) -> None:
    labels_path = Path("iceage") / "configs" / "theme_labels.csv"
    df_sorted = df.sort_values(["canonical_theme", "naver_label"], na_position="last")
    df_sorted.to_csv(labels_path, index=False, encoding="utf-8-sig")
    print(f"[INFO] theme_labels.csv 저장 완료: {labels_path}")


def sync_theme_labels_for_date(ref_date: date) -> None:
    print(f"[INFO] 기준일: {ref_date.isoformat()} 테마 라벨 동기화 시작")

    df_naver = load_naver_theme_df(ref_date)
    candidate_labels = pick_candidate_labels(df_naver)

    if not candidate_labels:
        print("[INFO] 조건을 만족하는 신규 테마 후보가 없습니다.")
        return

    print(f"[INFO] 오늘 후보 테마 수: {len(candidate_labels)}")

    df_labels = load_theme_labels()
    today_str = ref_date.isoformat()

    # 이미 존재하는 naver_label 목록
    existing_labels = set(df_labels["naver_label"].astype(str))

    new_rows = []
    updated_rows = 0

    for label in candidate_labels:
        if label in existing_labels:
            # 이미 있으면 last_seen만 업데이트
            mask = df_labels["naver_label"] == label
            df_labels.loc[mask, "last_seen"] = today_str
            updated_rows += 1
        else:
            # 새로 발견된 테마 -> 기본값으로 추가
            new_rows.append(
                {
                    "naver_label": label,
                    # 기본값: canonical_theme 를 naver_label 그대로 두고,
                    # 미정이가 나중에 묶고 싶은 것들은 직접 수정
                    "canonical_theme": label,
                    "role": "concept",      # 기본값: concept
                    "status": "active",     # 기본값: active
                    "first_seen": today_str,
                    "last_seen": today_str,
                }
            )

    if new_rows:
        df_labels = pd.concat([df_labels, pd.DataFrame(new_rows)], ignore_index=True)

    save_theme_labels(df_labels)
    print(f"[INFO] 신규 추가: {len(new_rows)}개, last_seen 업데이트: {updated_rows}개")


def main(argv: list[str]) -> None:
    if len(argv) < 2:
        print("사용법: python -m iceage.src.tools.sync_theme_labels YYYY-MM-DD")
        sys.exit(1)

    ref_date = parse_ref_date(argv[1])
    sync_theme_labels_for_date(ref_date)


if __name__ == "__main__":
    main(sys.argv)
