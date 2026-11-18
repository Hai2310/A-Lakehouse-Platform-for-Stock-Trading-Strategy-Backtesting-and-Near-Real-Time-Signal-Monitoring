"""Shared helpers for writing provider fetches to the Bronze lake."""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from ingest.market.lake import ParquetLake


def table_for(asset: str, interval: str) -> str:
    if asset == "index":
        return "daily_index" if interval == "1D" else "intraday_index"
    return "daily_ohlc" if interval == "1D" else "intraday_ohlc"


def attempt(
    lake: ParquetLake,
    *,
    table: str,
    source: str,
    fetch: Callable[[], pd.DataFrame | None],
    asset: str = "",
    symbol: str = "",
    interval: str = "",
    start: str = "",
    end: str = "",
    note: str = "",
) -> None:
    try:
        frame = fetch()
        path = lake.write(
            table,
            frame,
            source=source,
            asset=asset,
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            note=note,
        )
        status = "EMPTY" if path is None else "OK"
        print(f"[{status}] {table} source={source} rows={0 if frame is None else len(frame)}")
    except Exception as exc:
        message = lake._redact(f"{type(exc).__name__}: {exc}")
        lake.record(
            table=table,
            source=source,
            status="FAILED",
            asset=asset,
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            note=message,
        )
        print(f"[FAILED] {table} source={source} symbol={symbol or '-'}: {message}")


def skip(lake: ParquetLake, table: str, source: str, note: str) -> None:
    lake.record(table=table, source=source, status="SKIPPED", note=note)
    print(f"[SKIPPED] {table} source={source}: {note}")
