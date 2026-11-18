"""Crawl financial statements (BCTC) from FHSC for the market stock universe.

FHSC is the only Vnstock 6 source with income statement, balance sheet and cash flow.
It needs FHSC_API_KEY in .env. The default mode only prints the plan; add --run to fetch.

Output: data/bronze/bctc/financial_statements/source=fhsc/asset=equity/interval=<statement>-<period>/
Coverage: data/bronze/bctc/_reports/fhsc_bctc_coverage.csv
"""

from __future__ import annotations

import argparse
import os
import re
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from ingest.market.lake import PROJECT_ROOT, ParquetLake

TABLE = "financial_statements"
SOURCE = "fhsc"
STATEMENTS = ("income-statement", "balance-sheet", "cash-flow")
PERIODS = ("year", "quarter")
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "bronze" / "bctc"
DEFAULT_UNIVERSE = PROJECT_ROOT / "data" / "bronze" / "market" / "_reports" / "vnstock_symbols_3_intervals.csv"
TRANSIENT = ("429", "rate limit", "timeout", "timed out", "502", "503", "504")


def _redact(text: str) -> str:
    for name, value in os.environ.items():
        if any(word in name.upper() for word in ("KEY", "SECRET", "TOKEN")) and len(value) > 5:
            text = text.replace(value, "[REDACTED]")
    return text


def load_universe(path: Path, limit: int, requested: list[str]) -> list[str]:
    """Stock symbols in the same order as the market crawl, or the explicit list if given."""
    if requested:
        return list(dict.fromkeys(requested))
    if not path.is_file():
        raise SystemExit(f"Missing symbol list: {path}. Run the market backfill first or pass --symbols.")
    symbols = pd.read_csv(path, dtype=str)["symbol"].str.strip().str.upper().tolist()
    return list(dict.fromkeys(symbols))[:limit]


def interval_label(statement: str, period: str) -> str:
    return f"{statement}-{period}"


def fetch_statement(symbol: str, statement: str, period: str) -> pd.DataFrame:
    """One statement for one symbol. Rows are returned as-is, with symbol/statement/period added."""
    import vnstock_oapi as oapi

    rows: list = []
    for item in oapi.source(SOURCE).financials(symbol, statement=statement, period=period):
        body = getattr(item, "body", item)
        if isinstance(body, list):
            rows.extend(body)
        elif isinstance(body, dict):
            rows.append(body)
        else:
            raise TypeError(f"Unexpected FHSC response type: {type(body).__name__}")
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.assign(symbol=symbol, statement=statement, period=period)
    return frame


def done_jobs(lake: ParquetLake) -> set[tuple[str, str]]:
    """(symbol, interval) pairs already written with a final status, so reruns resume."""
    if not lake.report_path.is_file():
        return set()
    report = pd.read_csv(lake.report_path, dtype=str, keep_default_na=False)
    report = report.loc[report["table"].eq(TABLE) & report["status"].isin(["OK", "EMPTY"])]
    return set(zip(report["symbol"], report["interval"]))


def crawl(
    lake: ParquetLake,
    symbols: list[str],
    *,
    pause: float,
    refresh: bool,
    fetch=fetch_statement,
) -> dict[str, int]:
    finished = set() if refresh else done_jobs(lake)
    jobs = [
        (symbol, statement, period)
        for symbol in symbols
        for statement in STATEMENTS
        for period in PERIODS
        if (symbol, interval_label(statement, period)) not in finished
    ]
    print(f"{len(jobs)} requests to run ({len(finished)} already done).", flush=True)
    counts = {"OK": 0, "EMPTY": 0, "FAILED": 0}

    for symbol, statement, period in jobs:
        interval = interval_label(statement, period)
        frame = None
        for attempt in range(3):
            try:
                frame = fetch(symbol, statement, period)
                break
            except Exception as exc:
                message = _redact(f"{type(exc).__name__}: {exc}")
                if attempt < 2 and any(token in message.lower() for token in TRANSIENT):
                    time.sleep(30 * (attempt + 1))
                    continue
                lake.record(table=TABLE, source=SOURCE, status="FAILED", asset="equity", symbol=symbol,
                            interval=interval, note=message)
                counts["FAILED"] += 1
                print(f"[FAILED] {symbol} {interval}: {message}", flush=True)
                break

        if frame is not None:
            status = "EMPTY" if frame.empty else "OK"
            lake.write(
                TABLE,
                frame,
                source=SOURCE,
                asset="equity",
                symbol=symbol,
                interval=interval,
                note=f"FHSC {statement}, {period}. Current report data; no point-in-time check yet.",
                batch_key=f"{symbol}|{interval}",
                status=status,
            )
            counts[status] += 1
            print(f"[{status}] {symbol} {interval}: {len(frame)} rows", flush=True)
        time.sleep(pause)

    print(f"Done: {counts}", flush=True)
    return counts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", action="store_true", help="Call FHSC and write Bronze. Default prints the plan.")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum symbols from the universe file.")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols; overrides the universe file.")
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE, help="CSV with a symbol column.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--pause", type=float, default=0.5, help="Seconds between requests.")
    parser.add_argument("--refresh", action="store_true", help="Fetch jobs again even if already written.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.limit < 1 or args.pause < 0:
        parser.error("--limit must be positive and --pause cannot be negative")
    load_dotenv(PROJECT_ROOT / ".env")

    requested = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    symbols = load_universe(args.universe, args.limit, requested)
    output = args.output_dir if args.output_dir.is_absolute() else PROJECT_ROOT / args.output_dir
    print(f"BCTC | {len(symbols)} symbols | {' + '.join(STATEMENTS)} | {' + '.join(PERIODS)}")
    print(f"Bronze: {output / TABLE}")
    if not args.run:
        print("Add --run to call FHSC and write Bronze.")
        return 0
    if not os.getenv("FHSC_API_KEY", "").strip():
        parser.exit(1, "Missing FHSC_API_KEY in .env. BCTC is only available through FHSC.\n")

    lake = ParquetLake(data_root=output, report_path=Path("_reports/fhsc_bctc_coverage.csv"))
    counts = crawl(lake, symbols, pause=args.pause, refresh=args.refresh)
    print(f"Coverage: {lake.report_path}")
    return int(counts["FAILED"] > 0)


if __name__ == "__main__":
    raise SystemExit(main())
