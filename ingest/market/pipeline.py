"""Coordinate provider-specific market ingestion into Bronze."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from ingest.market.lake import PROJECT_ROOT, ParquetLake
from ingest.market.mvp.ingestion import run_csv as run_mvp
from ingest.market.vnstock.adapter import VnstockAdapter
from ingest.market.vnstock.ingestion import (
    run_history as run_vnstock_history,
    run_index_catalog as run_vnstock_index_catalog,
    run_quotes as run_vnstock_quotes,
    run_reference as run_vnstock_reference,
)
from ingest.market.xno.adapter import XNOAdapter
from ingest.market.xno.ingestion import (
    run_history as run_xno_history,
    run_quotes as run_xno_quotes,
    run_reference as run_xno_reference,
)


TZ = ZoneInfo("Asia/Ho_Chi_Minh")
DEFAULT_INDICES = ["VNINDEX", "VN30", "HNXINDEX", "UPCOMINDEX"]


def _csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _canonical_interval(value: str) -> str:
    stripped = value.strip()
    if stripped.lower() == "1d":
        return "1D"
    if stripped.lower() == "5m":
        return "5m"
    raise ValueError(f"Unsupported interval {value!r}; use 1D or 5m.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--task",
        choices=["all", "reference", "history", "quotes", "xno", "mvp"],
        default="all",
    )
    parser.add_argument("--symbols", default="FPT", help="Comma-separated equity symbols")
    parser.add_argument("--futures", default="VN30F1M", help="Comma-separated futures symbols")
    parser.add_argument(
        "--indices", default=",".join(DEFAULT_INDICES), help="Comma-separated index symbols"
    )
    parser.add_argument(
        "--sources",
        default="vci,kbs,xno,mvp",
        help="Comma-separated sources (defaults to every configured market source)",
    )
    parser.add_argument("--intervals", default="1D,5m", help="Comma-separated intervals")
    parser.add_argument("--start", help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--end", help="Inclusive end date YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=14, help="Default calendar lookback")
    parser.add_argument(
        "--mvp-csv", help="Path to MVP 1-minute CSV (or set MVP_CSV_PATH in .env)"
    )
    parser.add_argument("--sleep", type=float, default=0.5, help="Pause between Vnstock/XNO calls")
    parser.add_argument("--upload-gcs", action="store_true", help="Mirror Bronze files to GCS")
    silver = parser.add_mutually_exclusive_group()
    silver.add_argument("--skip-silver", action="store_true", help="Only write Bronze (default)")
    silver.add_argument("--build-silver", action="store_true", help="Run Spark Silver from Bronze Market and VSDC")
    args = parser.parse_args()
    if args.days < 1 or args.sleep < 0:
        parser.error("--days must be positive and --sleep cannot be negative")

    load_dotenv(PROJECT_ROOT / ".env")
    end_date = args.end or datetime.now(TZ).date().isoformat()
    start_date = args.start or (
        datetime.fromisoformat(end_date).date() - timedelta(days=args.days)
    ).isoformat()
    if start_date > end_date:
        parser.error("--start must be on or before --end")

    symbols = [symbol.upper() for symbol in _csv_list(args.symbols)]
    futures = [symbol.upper() for symbol in _csv_list(args.futures)]
    indices = [symbol.upper() for symbol in _csv_list(args.indices)]
    intervals = [_canonical_interval(value) for value in _csv_list(args.intervals)]
    sources = [value.strip().lower() for value in _csv_list(args.sources)]
    valid_sources = {"vci", "kbs", "xno", "mvp"}
    unknown = sorted(set(sources) - valid_sources)
    if unknown:
        parser.error(f"Unknown source(s): {', '.join(unknown)}")

    if args.task == "xno":
        sources = ["xno"]
    elif args.task == "mvp":
        sources = ["mvp"]

    lake = ParquetLake(upload_gcs=args.upload_gcs)
    symbols_by_asset = {"equity": symbols, "index": indices, "futures": futures}

    vnstock_sources = [source for source in sources if source in {"vci", "kbs"}]
    if vnstock_sources:
        vnstock = VnstockAdapter()
        if args.task in {"all", "reference"}:
            run_vnstock_index_catalog(lake, vnstock)
        for source in vnstock_sources:
            if args.task in {"all", "reference"}:
                run_vnstock_reference(
                    lake, vnstock, source, symbols, futures, indices, args.sleep
                )
            if args.task in {"all", "history"}:
                run_vnstock_history(
                    lake,
                    vnstock,
                    source,
                    symbols_by_asset,
                    intervals,
                    start_date,
                    end_date,
                    args.sleep,
                )
            if args.task in {"all", "quotes"}:
                run_vnstock_quotes(lake, vnstock, source, symbols)

    if "xno" in sources:
        xno = XNOAdapter()
        if args.task in {"all", "reference", "xno"}:
            run_xno_reference(lake, xno, symbols)
        if args.task in {"all", "history", "xno"}:
            run_xno_history(
                lake,
                xno,
                symbols_by_asset,
                intervals,
                start_date,
                end_date,
                args.sleep,
            )
        if args.task in {"all", "quotes", "xno"}:
            run_xno_quotes(lake, xno, symbols)

    if "mvp" in sources and args.task in {"all", "history", "mvp"}:
        csv_path = args.mvp_csv or os.getenv("MVP_CSV_PATH", "input/mvp/vn30f1m_1m.csv")
        mvp_start, mvp_end = start_date, end_date
        if args.task == "mvp" and args.start is None and args.end is None:
            mvp_start = mvp_end = None
        run_mvp(lake, csv_path, mvp_start, mvp_end)

    if args.build_silver:
        from transform.silver.pipeline import run

        run(PROJECT_ROOT / "data/bronze", PROJECT_ROOT / "data/silver", PROJECT_ROOT / "data/quarantine/silver",
            market_root=lake.data_root)

    print(f"Coverage report: {lake.report_path}")


if __name__ == "__main__":
    main()
