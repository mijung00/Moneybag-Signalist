#!/bin/bash
set -euo pipefail

echo "🚀 [Post-Deploy Hook] Configuring and restarting custom daemons..."

# 1. 스크립트에 실행 권한 부여 (매우 중요)
chmod +x /var/app/current/run_moralis_listener.sh
chmod +x /var/app/current/run_watchdogs.sh

# 2. Systemd가 새로운 서비스 파일을 인식하도록 리로드
systemctl daemon-reload

# 3. 서비스 활성화 (인스턴스 재부팅 시 자동 시작되도록 설정)
systemctl enable moralis-listener.service watchdog.service

# 4. 서비스 재시작 (실패 시 로그를 남기도록)
if ! systemctl restart moralis-listener.service; then
    echo "⚠️ Failed to restart moralis-listener.service. Check 'journalctl -u moralis-listener.service' on the instance."
fi
if ! systemctl restart watchdog.service; then
    echo "⚠️ Failed to restart watchdog.service. Check 'journalctl -u watchdog.service' on the instance."
fi

echo "✅ [Post-Deploy Hook] Daemons restart command issued."