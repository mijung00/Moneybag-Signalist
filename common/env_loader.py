# common/env_loader.py
from __future__ import annotations

from pathlib import Path

EB_ENV_FILE = Path("/opt/elasticbeanstalk/deployment/env")

def load_env(project_root: Path | str | None = None) -> None:
    """
    우선순위:
    1) EB 인스턴스: /opt/elasticbeanstalk/deployment/env (있으면)
    2) 로컬 개발: {project_root}/.env (있으면)
    - override=False: 이미 잡힌 OS 환경변수는 덮지 않음
    """
    try:
        from dotenv import load_dotenv
    except Exception:
        return

    if EB_ENV_FILE.exists():
        load_dotenv(EB_ENV_FILE, override=False)

    if project_root is not None:
        root = Path(project_root)
        env_path = root / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=False)
