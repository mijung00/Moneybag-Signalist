# iceage/src/utils/slack_notifier.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import requests

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from common.env_loader import load_env
load_env(REPO_ROOT)


def send_slack_message(text: str, username: Optional[str] = None) -> None:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("[WARN] SLACK_WEBHOOK_URL 이 설정되어 있지 않습니다.")
        return

    payload = {"text": text}
    if username:
        payload["username"] = username

    headers = {"Content-Type": "application/json"}

    resp = requests.post(
        webhook_url,
        data=json.dumps(payload),
        headers=headers,
        timeout=5,
    )
    if resp.status_code >= 400:
        print(f"[WARN] Slack webhook 실패: {resp.status_code} {resp.text}")
    else:
        print("[OK] Slack 알림 전송 완료")
