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


def _money(value: str | None):
    return Decimal(value.replace(".", "").replace(" ", "").replace(",", ".")) if value else None


def parse_terms(lines: list[str], kind: str) -> dict:
    terms = dict(cash_per_share_vnd=None, subscription_price_vnd=None, rights_old_shares=None,
                 rights_new_shares=None, rate_percent=None, payment_date=pd.NaT)
    rate_lines = [line for line in lines if re.match(r"^(?:[-+–]\s*)?tỷ lệ\b", line)]
    if kind == "CASH_DIVIDEND":
        cash = _match(rate_lines, r"cổ phiếu\s+(?:được\s+)?nhận\s+" + AMOUNT)
        if cash is None:
            cash = _match(rate_lines, AMOUNT + r"\s*/\s*(?:cổ phiếu|cp)\b")
        terms["cash_per_share_vnd"] = _money(cash)
        terms["payment_date"] = _date(lines, "ngày thanh toán|thời gian thanh toán|ngày chi trả|thời gian thực hiện")
    elif kind == "RIGHTS_ISSUE":
        terms["subscription_price_vnd"] = _money(_match(lines, r"^(?:[-+–]\s*)?(?:giá phát hành|giá chào bán)\s*:\s*" + AMOUNT))
    if kind in {"CASH_DIVIDEND", "STOCK_DIVIDEND", "RIGHTS_ISSUE", "BONUS_SHARES", "SPLIT"}:
        rate = _match(rate_lines, RATE)
        terms["rate_percent"] = Decimal(rate.replace(",", ".")) if rate else None
    if kind in {"STOCK_DIVIDEND", "RIGHTS_ISSUE", "BONUS_SHARES", "SPLIT"}:
        explicit = r"(\d[\d.]*)\s+(?:cổ phiếu cũ|quyền nhận cổ tức|quyền mua)[^\d]{1,90}(\d[\d.]*)\s+cổ phiếu mới"
        ratio = {m.groups() for line in rate_lines if (m := re.search(explicit, line))}
        if not ratio and kind != "SPLIT":
            ratio = {m.groups() for line in rate_lines if (m := re.search(RATIO, line))}
        if len(ratio) == 1:
            old, new = (int(item.replace(".", "")) for item in next(iter(ratio)))
            if old > 0 and new > 0:
                terms.update(rights_old_shares=old, rights_new_shares=new)
    return terms


def build_events(bronze_root: Path, calendar: pd.DataFrame, identity: Identity) -> pd.DataFrame:
    notices = read_table(bronze_root / "notice_details")
    notices = notices.loc[notices.status.eq("OK"), ["notice_id", "article_html", "source_updated_at_text", "body_sha256", "bronze_file"]]
    merged = calendar.drop_duplicates(["notice_id", "symbol", "record_date"]).merge(
        notices.drop_duplicates("notice_id"), on="notice_id", how="left", suffixes=("", "_detail"))
    merged["exchange"] = venue(merged.market)
    merged["observed_at_utc"] = utc(merged.ingested_at)
    merged = identity.attach(merged, allow_venue_hint=False)
    rows = []
    for item in merged.itertuples(index=False):
        title = normalize("NFC", item.title).lower()
        kinds = _event_types(title, str(item.event_types))
        if kinds == ["OTHER"]:
            continue
        lines = _lines(item.article_html)
        notice_status = "REVISION_UNLINKED" if re.search(REVISION, title) else "ANNOUNCED"
        if re.match(r"^\s*(?:hủy|huỷ)\b", title.split(":", 1)[-1]):
            notice_status = "CANCEL_UNLINKED"
        for kind in kinds:
            event_lines = _section(lines, kind, len(kinds) > 1)
            terms = parse_terms(event_lines, kind)
            rows.append(dict(
                event_id=f"VSDC:{item.notice_id}:{item.symbol}:{item.record_date:%Y-%m-%d}:{kind}",
                revision_id=item.body_sha256 if isinstance(item.body_sha256, str) else "CALENDAR_ONLY",
                security_id=item.security_id, identity_status=item.identity_status, notice_id=item.notice_id,
                symbol=item.symbol, exchange=item.exchange, isin=item.isin, event_type=kind,
                status=notice_status, title=item.title, record_date=item.record_date,
                ex_date=_date(lines, "ngày giao dịch không hưởng quyền"),
                source_updated_at_text=item.source_updated_at_text, announced_at_utc=pd.NaT, available_at_utc=pd.NaT,
                observed_at_utc=item.observed_at_utc, source="vsdc", source_url=item.source_url,
                bronze_file=item.bronze_file, notice_bronze_file=item.bronze_file_detail,
                has_notice_body=bool(lines), **terms,
            ))
    result = pd.DataFrame(rows)
    updated = result.source_updated_at_text.astype("string").str.replace(r"\s*-\s*", " ", regex=True)
    result["source_updated_at_utc"] = utc(pd.to_datetime(updated, format="%d/%m/%Y %H:%M:%S", errors="coerce"))
    for name in ("announced_at_utc", "available_at_utc"):
        result[name] = result[name].dt.tz_localize("UTC")
    result["quality_status"] = "WARN"
    return result
