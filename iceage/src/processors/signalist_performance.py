# iceage/src/processors/signalist_performance.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from iceage.src.data_sources.kr_prices import load_normalized_prices


def _parse_date(arg: str | None) -> date:
    if arg:
        return date.fromisoformat(arg)
    return date.today()


def _find_col(df: pd.DataFrame, candidates) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def compute_performance(as_of: date, horizons=(1, 5, 20)) -> pd.DataFrame:
    """
    signalist_today_log.csv 에 기록된 시그널 이후
    D+1 / D+5 / D+20 수익률을 계산한다.

    - signal_date 기준으로 calendar day 를 더해서 target_date 계산
      (영업일이 아니면 그 날짜는 스킵)
    - target_date 의 정규화 시세 파일(kr_prices_YYYY-MM-DD.csv)에서
      해당 종목의 종가를 찾아 수익률 계산
    """
    log_path = Path("iceage") / "data" / "processed" / "signalist_today_log.csv"
    if not log_path.exists():
        print(f"[WARN] 로그 파일이 없습니다: {log_path}")
        return pd.DataFrame()

    df_log = pd.read_csv(log_path, dtype={"code": str})
    if df_log.empty:
        print("[WARN] 로그에 시그널이 없습니다.")
        return pd.DataFrame()

    # 날짜 파싱
    if "signal_date" not in df_log.columns:
        raise ValueError("signalist_today_log.csv 에 'signal_date' 컬럼이 없습니다.")

    df_log["signal_date"] = pd.to_datetime(df_log["signal_date"]).dt.date
    df_log["code"] = df_log.get("code", "").astype(str).str.zfill(6)

    results = []
    horizons = list(horizons)

    for _, row in df_log.iterrows():
        sig_date: date = row["signal_date"]
        code: str = row.get("code", "")
        name: str = str(row.get("name", ""))
        close_signal = float(row.get("close", 0.0))
        sentiment = str(row.get("sentiment", ""))
        insight = str(row.get("insight", ""))

        if close_signal <= 0:
            continue

        for h in horizons:
            target_date = sig_date + timedelta(days=h)
            if target_date > as_of:
                # 아직 미래인 구간은 계산하지 않음
                continue

            try:
                df_price = load_normalized_prices(target_date)
            except Exception:
                # 해당 날짜 시세 파일 없으면 스킵
                continue
            if df_price.empty:
                continue

            # 컬럼 탐색
            code_col = _find_col(df_price, ["code", "종목코드", "ticker"])
            name_col = _find_col(df_price, ["name", "종목명"])
            close_col = _find_col(df_price, ["close", "현재가", "종가"])

            if close_col is None:
                continue

            # 코드 우선, 없으면 종목명으로 매칭
            target_row = None
            if code and code_col and code_col in df_price.columns:
                df_price[code_col] = df_price[code_col].astype(str).str.zfill(6)
                matches = df_price[df_price[code_col] == code]
                if not matches.empty:
                    target_row = matches.iloc[0]
            if target_row is None and name and name_col and name_col in df_price.columns:
                matches = df_price[df_price[name_col].astype(str) == name]
                if not matches.empty:
                    target_row = matches.iloc[0]
            if target_row is None:
                continue

            try:
                close_target = float(
                    str(target_row[close_col]).replace(",", "")
                )
            except Exception:
                continue
            if close_target <= 0:
                continue

            ret = close_target / close_signal - 1.0

            results.append(
                {
                    "signal_date": sig_date.isoformat(),
                    "target_date": target_date.isoformat(),
                    "horizon_days": h,
                    "code": code,
                    "name": name,
                    "close_at_signal": close_signal,
                    "close_at_target": close_target,
                    "return": ret,
                    "sentiment_at_signal": sentiment,
                    "insight_at_signal": insight,
                }
            )

    if not results:
        print("[INFO] 계산된 성과 데이터가 없습니다.")
        return pd.DataFrame()

    df_result = pd.DataFrame(results)
    return df_result


def main():
    # 사용법:
    #   python -m iceage.src.processors.signalist_performance 2025-11-10
    # 인자를 생략하면 오늘 날짜 기준으로 계산
    as_of = _parse_date(sys.argv[1] if len(sys.argv) >= 2 else None)
    print(f"[INFO] Signalist 성과 계산 as_of={as_of.isoformat()}")

    df = compute_performance(as_of)
    if df.empty:
        print("[INFO] 저장할 성과 데이터가 없습니다.")
        return

    out_dir = Path("iceage") / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"signalist_performance_{as_of.isoformat()}.csv"

    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ 성과 리포트 저장 완료: {out_path}")


if __name__ == "__main__":
    main()
