"""Backfill public VSDC corporate-action calendar records to local Bronze."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from hashlib import sha256
from math import ceil, isfinite
from pathlib import Path
import re
import unicodedata
from uuid import uuid4

import pandas as pd
import requests

from ingest.market.lake import PROJECT_ROOT, TZ
from ingest.vsdc.adapter import PAGE_SIZE, RequestPacer, VSDCCalendar

DEFAULT_SYMBOLS = PROJECT_ROOT / "data/bronze/market/_reports/vnstock_symbols_3_intervals.csv"
DEFAULT_ROOT = PROJECT_ROOT / "data/bronze/vsdc"
EVENT_PATTERNS = {
    "DIVIDEND": r"\bco tuc\b",
    "SPLIT": r"\bchia tach co phieu\b|\btach co phieu\b|\bgop co phieu\b",
    "RIGHTS_ISSUE": r"\bquyen mua\b|\bchao ban co phieu cho co dong hien huu\b",
    "BONUS_SHARES": r"\bco phieu thuong\b|\bthuong co phieu\b|\btang von co phan tu nguon von chu so huu\b",
}
FIELDS = ["notice_id", "source_url", "record_date", "symbol", "isin", "title", "security_type",
          "market", "management_area", "event_types", "source", "ingested_at"]


def classify(title: str) -> str:
    text = unicodedata.normalize("NFD", title.lower().replace("đ", "d"))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return "|".join(label for label, pattern in EVENT_PATTERNS.items() if re.search(pattern, text)) or "OTHER"


def atomic_write(path: Path, write) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{uuid4().hex}.tmp")
    try:
        write(temporary)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def crawl_year(start: date, end: date, symbols: set[str], pace: RequestPacer, path: Path) -> dict:
    rows = []
    seen = set()
    with requests.Session() as session:
        calendar = VSDCCalendar(session, pace)
        first_page, total = calendar.page(start, end, 1)
        pages = max(1, ceil(total / PAGE_SIZE))
        for page in range(1, pages + 1):
            incoming, page_total = (first_page, total) if page == 1 else calendar.page(start, end, page)
            offset = (page - 1) * PAGE_SIZE
            if page_total != total or len(incoming) != min(PAGE_SIZE, total - offset):
                raise ValueError(f"Page {page} does not match VSDC total {total}")
            for number, row in enumerate(incoming, offset + 1):
                if row["ordinal"] != number or not start <= row["record_date"] <= end:
                    raise ValueError(f"VSDC returned an unexpected row on page {page}")
                identity = (row["notice_id"], row["symbol"], row["record_date"])
                if identity in seen:
                    raise ValueError(f"Duplicate notice/stock/date across calendar pages: {identity}")
                seen.add(identity)
                if row["symbol"] in symbols:
                    rows.append(row)
            if page % 100 == 0:
                print(f"[{start.year}] {page}/{pages} pages", flush=True)

    if len(seen) != total:
        raise ValueError(f"Read {len(seen)} distinct rows but VSDC reported {total}")
    data = pd.DataFrame(rows).reindex(columns=FIELDS)
    data["event_types"] = data["title"].map(classify)
    data["record_date"] = pd.to_datetime(data["record_date"])
    data["source"] = "vsdc"
    data["ingested_at"] = datetime.now(TZ).isoformat()
    atomic_write(path, lambda temp: data.to_parquet(temp, index=False))
    return dict(status="OK", source_rows=total, matched_rows=len(data), symbols=data["symbol"].nunique(),
                pages=pages, first_record_date=data["record_date"].min(), last_record_date=data["record_date"].max(),
                local_path=str(path), error="")


def main() -> int:
    today = datetime.now(TZ).date()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat, default=today)
    parser.add_argument("--symbols-file", type=Path, default=DEFAULT_SYMBOLS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--pause", type=float, default=0.16, help="Minimum interval between HTTP requests")
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    start = args.start or (pd.Timestamp(args.end) - pd.DateOffset(years=10)).date()
    if start > args.end or args.end > today:
        parser.error("Use --start <= --end <= today")
    if args.workers < 1 or not isfinite(args.pause) or args.pause < 0:
        parser.error("Invalid --workers or --pause")
    universe = pd.read_csv(args.symbols_file, dtype={"symbol": "string"})["symbol"].dropna().str.strip().str.upper()
    symbols = set(universe.loc[universe.str.fullmatch(r"[A-Z0-9]+")])
    if not symbols:
        parser.error("The symbols file must contain a non-empty symbol column")

    key = sha256(f"{start}|{args.end}|{','.join(sorted(symbols))}".encode()).hexdigest()[:16]
    root = args.output_dir.resolve()
    output = root / "corporate_actions" / "source=vsdc" / f"request={key}"
    report_path = root / "_reports" / f"coverage_{key}.csv"
    report = pd.read_csv(report_path).set_index("year").to_dict("index") if report_path.is_file() else {}
    pace = RequestPacer(args.pause)
    print(f"VSDC: {len(symbols)} mã, ngày đăng ký cuối cùng {start} → {args.end}", flush=True)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        jobs = {}
        for year in range(start.year, args.end.year + 1):
            path = output / f"record_year={year}" / "part.parquet"
            if not args.refresh and report.get(year, {}).get("status") == "OK" and path.is_file():
                continue
            lower, upper = max(start, date(year, 1, 1)), min(args.end, date(year, 12, 31))
            jobs[pool.submit(crawl_year, lower, upper, symbols, pace, path)] = (year, lower, upper)

        for future in as_completed(jobs):
            year, lower, upper = jobs[future]
            try:
                result = future.result()
            except (requests.RequestException, ValueError) as exc:
                result = dict(status="FAILED", error=f"{type(exc).__name__}: {exc}", matched_rows=0)
            report[year] = dict(requested_start=lower, requested_end=upper, **result)
            records = pd.DataFrame.from_dict(report, orient="index").sort_index().rename_axis("year")
            atomic_write(report_path, lambda temp: records.to_csv(temp))
            print(f"[{result['status']}] {year}: {result['matched_rows']} rows {result['error']}", flush=True)

    print(f"Coverage: {report_path}", flush=True)
    print(f"Bronze: {output}", flush=True)
    return int(any(row["status"] != "OK" for row in report.values()))


if __name__ == "__main__":
    raise SystemExit(main())
