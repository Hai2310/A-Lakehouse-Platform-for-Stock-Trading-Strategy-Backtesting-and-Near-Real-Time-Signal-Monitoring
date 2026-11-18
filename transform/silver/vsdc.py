"""Extract typed corporate-action events from the VSDC Bronze notices."""

from __future__ import annotations

from decimal import Decimal
from html import unescape
from pathlib import Path
import re
from unicodedata import normalize

import pandas as pd

from transform.silver.reference import Identity, read_table, utc, venue

DATE = r"\d{1,2}/\d{1,2}/\d{4}"
AMOUNT = r"(\d[\d. ]*(?:,\d+)?)\s*(?:đồng|vnđ|vnd)\b"
RATE = r"(\d+(?:[.,]\d+)?)\s*%"
RATIO = r"(\d[\d.]*)\s*[:/]\s*(\d[\d.]*)"
REVISION = r"điều chỉnh|đính chính|h[ủuỷ]y|huỷ|thay đổi|hoãn"


def _lines(value: object) -> list[str]:
    if not isinstance(value, str):
        return []
    value = re.sub(r"<\s*(?:br\b[^>]*|/(?:p|div|tr|li|td|h[1-6]))\s*>", "\n", value, flags=re.I)
    text = unescape(re.sub(r"<[^>]+>", "", value))
    return [" ".join(normalize("NFC", line).lower().split()) for line in text.splitlines() if line.strip()]


def _event_types(title: str, tags: str) -> list[str]:
    types = []
    if "DIVIDEND" in tags or "cổ tức" in title:
        if "tiền" in title:
            types.append("CASH_DIVIDEND")
        if "bằng cổ phiếu" in title or re.search(r"cổ tức[^:;]*tiền[^:;]*và cổ phiếu", title):
            types.append("STOCK_DIVIDEND")
        if not types:
            types.append("DIVIDEND_UNSPECIFIED")
    for label in ("RIGHTS_ISSUE", "BONUS_SHARES", "SPLIT"):
        if label in tags:
            types.append(label)
    return types or ["UNCLASSIFIED_REVISION" if re.search(REVISION, title) else "OTHER"]
