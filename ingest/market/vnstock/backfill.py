"""Backfill the local market Bronze tables through Vnstock 6."""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from functools import cache
from hashlib import sha256
import os
from pathlib import Path
from threading import Lock
import time

import pandas as pd
from dotenv import load_dotenv

from ingest.market.common.runner import table_for
from ingest.market.lake import MARKET_TABLES, PROJECT_ROOT, TZ, ParquetLake
from ingest.market.vnstock.table_probe import dnse_instrument, safe_message
from transform.silver.normalization import filter_dates

OHLCV = {"time", "open", "high", "low", "close", "volume"}
INTERVALS = ("1D", "5m", "1m")
EXCHANGES = {"HOSE": 0, "HNX": 1, "UPCOM": 2}
KEYS = {
    "dnse": ("DNSE_API_KEY", "DNSE_API_SECRET"),
    "ssi": ("SSI_API_KEY", "SSI_API_SECRET"),
    "fhsc": ("FHSC_API_KEY",),
}


class Backfill:
    def __init__(self, lake: ParquetLake, pause: float, refresh: bool) -> None:
        self.lake = lake
        self.pause = pause
        self.request_lock = Lock()
        self.next_request = 0.0
        self.write_lock = Lock()
        self.counts = {table: Counter() for table in MARKET_TABLES}
        self.completed = {}
        if not refresh and lake.report_path.is_file():
            report = pd.read_csv(lake.report_path, dtype=str, keep_default_na=False)
            for row in report.itertuples(index=False):
                if row.status in {"OK", "PARTIAL"} and row.local_path:
                    path = Path(row.local_path)
                    if path.is_file():
                        self.completed[path.parent.name] = row.status

    def fetch(self, fetch: Callable[[], pd.DataFrame]) -> pd.DataFrame:
        for retry in range(3):
            with self.request_lock:
                delay = self.next_request - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                self.next_request = time.monotonic() + self.pause
            try:
                return fetch()
            except Exception as exc:
                message = safe_message(exc).lower()
                transient = any(token in message for token in (
                    "429", "rate limit", "rate_limit", "timeout", "timed out", "502", "503", "504",
                ))
                if not transient or retry == 2:
                    raise
                time.sleep(30 * (retry + 1))

    def save(
        self, table: str, source: str, identity: str, fetch: Callable[[], pd.DataFrame],
        *, expected: tuple[str, ...] = (), partial: bool = False, note: str = "", **metadata: str,
    ) -> None:
        key = sha256(f"{table}|{source}|{identity}".encode()).hexdigest()[:24]
        checkpoint = f"request={key}"
        with self.write_lock:
            if checkpoint in self.completed:
                self.counts[table][f"RESUMED_{self.completed[checkpoint]}"] += 1
                return
        try:
            frame = self.fetch(fetch)
            if not isinstance(frame, pd.DataFrame):
                raise TypeError("Expected a Vnstock DataFrame")
            if expected and not frame.empty:
                observed = set(frame["symbol"].astype("string").str.upper())
                missing = sorted(set(expected) - observed)
                if missing:
                    partial = True
                    note += f" Missing symbols: {','.join(missing)}."
            if metadata.get("interval") and not frame.empty:
                if not OHLCV.issubset(frame.columns):
                    raise ValueError("Missing required OHLCV columns")
                start, end = metadata["start"], metadata["end"]
                frame = filter_dates(frame, start, end)
                if not frame.empty:
                    first = frame["time"].min().date()
                    last = frame["time"].max().date()
                    if first > date.fromisoformat(start) + timedelta(days=7) or last < date.fromisoformat(end) - timedelta(days=7):
                        partial = True
                        note += " Observed span does not reach both requested boundaries (7 calendar days tolerance)."
                    note += " Interior gaps have not been verified."
            status = "PARTIAL" if partial else "OK"
            with self.write_lock:
                path = self.lake.write(table, frame, source=source, batch_key=key, status=status,
                                       note=note.strip(), **metadata)
                if path is None:
                    status = "EMPTY"
                else:
                    self.completed[checkpoint] = status
        except Exception as exc:
            status = "FAILED"
            note = f"{type(exc).__name__}: {safe_message(exc)}"
            with self.write_lock:
                self.lake.record(table=table, source=source, status=status, note=note, **metadata)
        with self.write_lock:
            self.counts[table][status] += 1
        print(f"[{status}] {table} {source} {metadata.get('symbol', '')} {metadata.get('interval', '')}".rstrip(), flush=True)

    def missing(self, table: str, source: str) -> None:
        self.counts[table]["MISSING_CREDENTIALS"] += 1
        self.lake.record(table=table, source=source, status="MISSING_CREDENTIALS",
                         note=f"Set {', '.join(KEYS[source])} in .env.")


def select_catalog(catalog: pd.DataFrame, limit: int, requested: list[str]) -> pd.DataFrame:
    catalog = catalog.assign(
        symbol=catalog["symbol"].astype("string").str.strip().str.upper(),
        exchange=catalog["exchange"].astype("string").str.strip().str.upper(),
    )
    listed = catalog.loc[catalog["exchange"].isin(EXCHANGES) & catalog["symbol"].str.fullmatch(r"[A-Z0-9]+", na=False)]
    listed = listed.sort_values(["exchange", "symbol"], key=lambda col: col.map(EXCHANGES) if col.name == "exchange" else col)
    listed = listed.drop_duplicates("symbol")
    if requested:
        return listed.loc[listed["symbol"].isin(requested)]
    return listed.head(limit)


def csv_list(value: str) -> list[str]:
    return list(dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip()))


def run(args: argparse.Namespace, selected: set[str], today: date) -> int:
    from vnstock import Market, Reference

    creds = {source: all(os.getenv(key, "").strip() for key in keys) for source, keys in KEYS.items()}
    if not creds["dnse"]:
        raise SystemExit("Thiếu DNSE_API_KEY hoặc DNSE_API_SECRET trong .env.")

    lake = ParquetLake(data_root=args.output_dir, report_path=Path("_reports/vnstock_coverage.csv"))
    crawl = Backfill(lake, args.pause, args.refresh)
    snapshot = today.isoformat()
    dnse = Market("dnse")
    catalog = crawl.fetch(lambda: Reference("dnse").equity.list(type="stock"))
    catalog = select_catalog(catalog, args.limit, args.symbols)
    symbols = args.symbols or catalog["symbol"].tolist()
    if not symbols:
        raise SystemExit("Danh mục DNSE không trả mã cổ phiếu hợp lệ.")
    target = len(args.symbols) or args.limit
    if len(catalog) < target:
        print(f"Danh mục hiện tại khớp {len(catalog)}/{target} mã yêu cầu.", flush=True)
    print(f"Crawl {len(symbols)} mã (HOSE {sum(catalog.exchange.eq('HOSE'))}, HNX {sum(catalog.exchange.eq('HNX'))}, UPCoM {sum(catalog.exchange.eq('UPCOM'))})", flush=True)

    if "securities" in selected:
        crawl.save("securities", "dnse", f"{snapshot}|{','.join(symbols)}", lambda: catalog,
                   expected=tuple(symbols), asset="equity", note="Current listing snapshot.")

    if "index_list" in selected:
        source = "ssi" if creds["ssi"] else "fhsc" if creds["fhsc"] else "builtin"
        if source == "builtin":
            def groups() -> pd.DataFrame:
                frame = Reference().index.groups()
                return frame.loc[frame["category"].eq("Chỉ số VN")].rename(columns={"group_code": "symbol"})

            crawl.save("index_list", source, snapshot, groups, partial=True,
                       note="Built-in index groups. SSI or FHSC can supply the index catalogue.")
        else:
            crawl.save("index_list", source, snapshot, lambda: Reference(source).index.list())

    if "index_components" in selected:
        if creds["ssi"] or creds["fhsc"]:
            source = "ssi" if creds["ssi"] else "fhsc"
            for group in args.index_groups:
                index = Reference(source).index(group)
                fetch = index.members if source == "ssi" else index.constituents
                crawl.save("index_components", source, f"{snapshot}|{group}",
                           lambda: fetch().assign(index_symbol=group, snapshot_date=snapshot),
                           asset="index", note=f"Current members of {group}.")
        else:
            crawl.missing("index_components", "ssi")

    if creds["fhsc"]:
        if "securities_details" in selected:
            ref = Reference("fhsc")
            for symbol in symbols:
                crawl.save("securities_details", "fhsc", f"{snapshot}|{symbol}",
                           lambda: ref.company(symbol).info().assign(snapshot_date=snapshot),
                           asset="equity", symbol=symbol, note="Current company profile.")
        if "daily_stock_price" in selected:
            quote = Market("fhsc").equity.quote
            for offset in range(0, len(symbols), args.quote_batch_size):
                batch = symbols[offset:offset + args.quote_batch_size]
                tickers = ",".join(batch)
                crawl.save("daily_stock_price", "fhsc", f"{snapshot}|{tickers}",
                           lambda: quote(tickers).assign(snapshot_date=snapshot),
                           expected=tuple(batch), asset="equity", note="Current price-board snapshot.")
    elif selected & {"securities_details", "daily_stock_price"}:
        now = datetime.now(TZ).isoformat()
        for symbol in symbols:
            @cache
            def definition() -> pd.DataFrame:
                frame = dnse_instrument(symbol, now)
                if not frame.empty:
                    frame = frame.loc[frame["boardId"].eq("G1")]
                return frame.assign(snapshot_date=snapshot)

            for table, note in (
                ("securities_details", "Trading definition only. FHSC can supply the company profile."),
                ("daily_stock_price", "Reference/ceiling/floor snapshot only. FHSC can supply full quotes."),
            ):
                if table in selected:
                    crawl.save(table, "dnse", f"{snapshot}|{symbol}", definition,
                               asset="equity", symbol=symbol, partial=True, note=note)

    def history(job: tuple[str, str, str]) -> None:
        asset, symbol, interval = job
        table = table_for(asset, interval)
        crawl.save(table, "dnse", f"{asset}|{symbol}|{interval}|{args.start}|{args.end}",
                   lambda: getattr(dnse, asset)(symbol).ohlcv(start=args.start, end=args.end, interval=interval, compat="v5"),
                   asset=asset, symbol=symbol, interval=interval, start=args.start, end=args.end)

    jobs = [(asset, symbol, interval) for asset, universe in (("equity", symbols), ("index", args.indices))
            for symbol in universe for interval in INTERVALS if table_for(asset, interval) in selected]
    if args.workers == 1:
        for job in jobs:
            history(job)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for _ in pool.map(history, jobs):
                pass

    print(f"Coverage: {lake.report_path}")
    for table in MARKET_TABLES:
        if table in selected:
            print(f"  {table}: {dict(crawl.counts[table])}")
    incomplete = {"FAILED", "MISSING_CREDENTIALS", "EMPTY", "PARTIAL", "RESUMED_PARTIAL"}
    return int(len(catalog) < target or any(incomplete & counts.keys() for counts in crawl.counts.values()))


def main() -> int:
    today = datetime.now(TZ).date()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="Fetch data and write local Parquet. Default prints the plan.")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum number of auto-selected stocks.")
    parser.add_argument("--symbols", type=csv_list, default=[], help="Explicit stock tickers, comma-separated.")
    parser.add_argument("--indices", type=csv_list, default="VNINDEX,VN30,HNXINDEX,UPCOMINDEX")
    parser.add_argument("--index-groups", type=csv_list, default="VN30,VN100,HNX30")
    parser.add_argument("--tables", type=csv_list, default=",".join(MARKET_TABLES))
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat, default=today)
    parser.add_argument("--output-dir", type=Path, help="Override MARKET_DATA_DIR for local Bronze.")
    parser.add_argument("--pause", type=float, default=0.5, help="Minimum seconds between request starts across workers.")
    parser.add_argument("--workers", type=int, default=1, help="Maximum number of concurrent OHLCV requests.")
    parser.add_argument("--quote-batch-size", type=int, default=20)
    parser.add_argument("--refresh", action="store_true", help="Fetch already saved jobs again for the same range.")
    args = parser.parse_args()
    args.start = args.start or (pd.Timestamp(args.end) - pd.DateOffset(years=10)).date()
    if args.start > args.end or args.end > today:
        parser.error("Use --start <= --end <= today")
    if args.limit < 1 or not 0 <= args.pause < float("inf") or args.quote_batch_size < 1 or args.workers < 1:
        parser.error("Invalid --limit, --pause, --quote-batch-size or --workers")
    selected = {table.lower() for table in args.tables}
    if not selected or selected - set(MARKET_TABLES):
        parser.error("--tables must contain known Bronze market tables")
    if selected & {"daily_index", "intraday_index"} and not args.indices:
        parser.error("Specify at least one index for index OHLCV")
    if "index_components" in selected and not args.index_groups:
        parser.error("Specify at least one index group for index_components")
    args.start, args.end = args.start.isoformat(), args.end.isoformat()
    load_dotenv(PROJECT_ROOT / ".env")
    if not args.run:
        print(f"Vnstock 6 | {len(args.symbols) or args.limit} cổ phiếu | {args.start} → {args.end} | {' + '.join(INTERVALS)}")
        print(f"Tables: {', '.join(table for table in MARKET_TABLES if table in selected)}")
        print(f"Bronze: {args.output_dir or os.getenv('MARKET_DATA_DIR', 'data/bronze/market')}")
        print("Thêm --run để gọi API và ghi dữ liệu.")
        return 0
    return run(args, selected, today)


if __name__ == "__main__":
    raise SystemExit(main())
