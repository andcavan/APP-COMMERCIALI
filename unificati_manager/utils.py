from __future__ import annotations

import os
from datetime import datetime

from .config import DATE_FMT


def now_str() -> str:
    return datetime.now().strftime(DATE_FMT)


def normalize_upper(s: str) -> str:
    return (s or "").upper()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)
