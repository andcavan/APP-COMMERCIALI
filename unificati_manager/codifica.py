from __future__ import annotations

import re


# ---- Normati helpers ----
def normalize_mmm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^0-9]", "", s)
    return s[:3]


def normalize_gggg_normati(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^0-9]", "", s)
    return s[:4]


def is_valid_mmm(s: str) -> bool:
    return bool(re.fullmatch(r"[0-9]{3}", s or ""))


def is_valid_gggg_normati(s: str) -> bool:
    return bool(re.fullmatch(r"[0-9]{4}", s or ""))


# ---- Commerciali (non normati) helpers ----
def normalize_cccc(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^0-9]", "", s)
    return s[:4]


def normalize_ssss(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^0-9]", "", s)
    return s[:4]


def is_valid_cccc(s: str) -> bool:
    return bool(re.fullmatch(r"[0-9]{4}", s or ""))


def is_valid_ssss(s: str) -> bool:
    return bool(re.fullmatch(r"[0-9]{4}", s or ""))
