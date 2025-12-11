@echo off
chcp 65001
echo 🚀 [S3 Sync] AWS 서버의 데이터를 내 PC(C:\ubuntu)로 가져옵니다...

:: [설정] 여기에 정확한 버킷 이름을 넣으세요 (앞뒤 공백 없이!)
set BUCKET_NAME=fincore-output-storage

:: ----------------------------------------------------
:: aws s3 sync 명령어는 자동으로 '하위 폴더'까지 다 가져오고,
:: 이미 있는 파일은 건너뛰고 '새로운 파일'만 가져옵니다.
:: ----------------------------------------------------

:: 1. Moneybag (Data 폴더만)
echo.
echo 📂 [Moneybag] Data 동기화 중...
:: 서버의 moneybag/data 폴더 내용을 -> 내 PC moneybag/data 로
aws s3 sync s3://%BUCKET_NAME%/moneybag/data ./moneybag/data

:: 2. Iceage (Data + Out 폴더)
echo.
echo ❄️ [Iceage] Data 동기화 중...
:: 서버의 iceage/data 폴더 내용을 -> 내 PC iceage/data 로
aws s3 sync s3://%BUCKET_NAME%/iceage/data ./iceage/data

echo.
echo ❄️ [Iceage] Out (결과물) 동기화 중...
:: 서버의 iceage/out 폴더 내용을 -> 내 PC iceage/out 로
aws s3 sync s3://%BUCKET_NAME%/iceage/out ./iceage/out

echo.
echo ✅ 모든 데이터 동기화 완료! (새로 받은 파일만 다운로드됨)
pause