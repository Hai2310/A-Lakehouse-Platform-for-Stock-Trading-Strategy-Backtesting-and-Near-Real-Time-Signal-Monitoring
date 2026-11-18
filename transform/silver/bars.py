"""Normalize and compact price bars in bounded local Parquet partitions."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from pyarrow.parquet import ParquetFile

from transform.silver.market import _validate_rows, _write_snapshot
from transform.silver.reference import INDEX_VENUES, Identity, utc

BAR_TABLES = ("daily_ohlc", "intraday_ohlc", "daily_index", "intraday_index")
PRICE_FIELDS = ("open", "high", "low", "close")
SOURCE_FIELDS = ("symbol", "source", "asset", "interval", "time", *PRICE_FIELDS, "volume", "ingested_at")
BATCH_ROWS = 120_000


def _read_batches(files):
    pending, count = [], 0
    for file in files:
        parquet = ParquetFile(file)
        columns = set(parquet.schema_arrow.names)
        required = set(SOURCE_FIELDS) - {"ingested_at"}
        if missing := required - columns:
            raise ValueError(f"{file} missing price columns {sorted(missing)}")
        frame = parquet.read(columns=[name for name in SOURCE_FIELDS if name in columns]).to_pandas()
        if "ingested_at" not in frame:
            frame["ingested_at"] = pd.NA
        frame["time"] = utc(frame.time)
        frame["bronze_file"] = str(file)
        pending.append(frame)
        count += len(frame)
        if count >= BATCH_ROWS:
            yield pd.concat(pending, ignore_index=True)
            pending, count = [], 0
    if pending:
        yield pd.concat(pending, ignore_index=True)


def _standardize(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    for name in ("symbol", "source", "asset", "interval"):
        data[name] = data[name].astype("string").str.strip().replace("", pd.NA)
    data["symbol"] = data.symbol.str.upper()
    data["source"] = data.source.str.lower()
    data["asset"] = data.asset.str.lower()
    data["interval"] = data.interval.str.lower().replace("1d", "1D")
    for name in (*PRICE_FIELDS, "volume"):
        data[name] = pd.to_numeric(data[name], errors="coerce").astype("float64")
    return data
