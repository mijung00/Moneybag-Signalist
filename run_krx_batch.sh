#!/bin/bash
# -------------------------------------------------------------
# [Iceage] KRX 데이터 3종 세트 3일치 수집 스크립트
# -------------------------------------------------------------

# 1. 환경 변수 로드 (필수!)
. /opt/elasticbeanstalk/deployment/env

# 2. 프로젝트 폴더로 이동
cd /var/app/current

# 3. 최근 3일치 날짜에 대해 반복 (0=오늘, 1=어제, 2=그제)
for i in {0..2}
do
    # 날짜 계산 (YYYY-MM-DD 형식)
    TARGET_DATE=$(date -d "$i days ago" +%Y-%m-%d)
    
    echo "======================================================="
    echo "📅 날짜: $TARGET_DATE 데이터 수집 시작"
    echo "======================================================="

    # (1) 종목 리스트 갱신 (Listing)
    echo "   [1/3] 종목 리스트 수집 중..."
    /var/app/venv/*/bin/python3.14 -m iceage.src.collectors.krx_listing_collector $TARGET_DATE

    # (2) 지수 데이터 갱신 (Index)
    echo "   [2/3] 지수(Index) 수집 중..."
    /var/app/venv/*/bin/python3.14 -m iceage.src.collectors.krx_index_collector $TARGET_DATE

    # (3) 일별 시세 갱신 (Daily Prices)
    echo "   [3/3] 일별 시세(Prices) 수집 중..."
    /var/app/venv/*/bin/python3.14 -m iceage.src.collectors.krx_daily_price_collector $TARGET_DATE

    echo "   ✅ $TARGET_DATE 완료. API 보호를 위해 3초 대기..."
    sleep 3
done

echo "🎉 모든 KRX 배치 작업 완료!"