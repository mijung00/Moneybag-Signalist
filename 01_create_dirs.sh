#!/bin/bash
set -e

# moralis_listener.py 및 다른 스크립트들이 사용할 데이터 디렉터리를 생성합니다.
# 애플리케이션 루트는 /var/app/staging 입니다.
DATA_DIR="/var/app/staging/moneybag/data/out"

echo "Creating application data directory at ${DATA_DIR}"
mkdir -p "${DATA_DIR}"
chown webapp:webapp -R "$(dirname ${DATA_DIR})"