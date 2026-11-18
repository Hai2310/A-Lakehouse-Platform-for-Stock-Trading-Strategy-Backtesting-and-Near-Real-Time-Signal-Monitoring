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


def _section_type(heading: str) -> str | None:
    if "quyền mua" in heading or "chào bán" in heading:
        return "RIGHTS_ISSUE"
    if "cổ tức" in heading:
        if "bằng cổ phiếu" in heading:
            return "STOCK_DIVIDEND"
        return "CASH_DIVIDEND" if "tiền" in heading else "DIVIDEND_UNSPECIFIED"
    if "cổ phiếu thưởng" in heading or "thưởng cổ phiếu" in heading:
        return "BONUS_SHARES"
    if "tách cổ phiếu" in heading or "gộp cổ phiếu" in heading:
        return "SPLIT"
    return None


def _section(lines: list[str], event_type: str, multiple: bool) -> list[str]:
    if not multiple:
        return lines
    headings = []
    for index, line in enumerate(lines):
        if re.match(r"^(?:[a-z0-9]+[)/.]\s+|\s*[-–]\s*)?(?:thực hiện quyền|trả cổ tức|chi trả cổ tức|phát hành cổ phiếu)", line):
            kind = _section_type(line)
            if kind:
                headings.append((index, kind))
    for index, (start, kind) in enumerate(headings):
        if kind == event_type:
            end = headings[index + 1][0] if index + 1 < len(headings) else len(lines)
            return lines[start:end]
    return []


def _match(lines: list[str], pattern: str) -> str | None:
    values = {m.group(1).strip() for line in lines if (m := re.search(pattern, line))}
    return next(iter(values)) if len(values) == 1 else None


def _date(lines: list[str], label: str):
    found = _match(lines, rf"^(?:[-+–]\s*)?(?:{label})\s*[:：]\s*(?:ngày\s+|từ ngày\s+)?({DATE})\b")
    return pd.to_datetime(found, format="%d/%m/%Y", errors="coerce") if found else pd.NaT
