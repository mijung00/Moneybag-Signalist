@echo off
chcp 65001
echo 🚀 [S3 Sync] 로컬에서 생성/수정한 파일을 AWS 서버로 백업합니다...

:: [설정] 여기에 정확한 버킷 이름을 넣으세요 (앞뒤 공백 없이!)
set BUCKET_NAME=fincore-output-storage

:: ----------------------------------------------------
:: 로컬의 'processed' 폴더 내용을 S3로 업로드합니다.
:: 'aws s3 sync'는 변경된 파일만 알아서 업로드합니다.
:: ----------------------------------------------------

echo.
echo ❄️  [Iceage] Processed 데이터 업로드 중...
aws s3 sync ./iceage/data/processed s3://%BUCKET_NAME%/iceage/data/processed

echo.
echo ✅ 데이터 업로드 완료!
pause