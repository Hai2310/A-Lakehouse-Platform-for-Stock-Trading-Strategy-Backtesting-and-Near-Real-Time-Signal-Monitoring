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


def _enrich(frame: pd.DataFrame, table: str, identity: Identity) -> pd.DataFrame:
    data = frame.copy()
    is_index = table.endswith("index")
    if is_index:
        data["index_id"] = "INDEX:" + data.symbol
        data["exchange"] = data.symbol.map(INDEX_VENUES).astype("string")
        data["identity_status"] = "SOURCE_INDEX_CODE"
    else:
        data = identity.attach(data)
    data["instrument_id"] = data["index_id" if is_index else "security_id"]
    data["instrument_type"] = "index" if is_index else data.asset
    data["time"] = data.time.dt.tz_convert("UTC")
    local_time = data.time.dt.tz_convert("Asia/Ho_Chi_Minh")
    data["trading_date"] = local_time.dt.tz_localize(None).dt.normalize()
    data["observed_at_utc"] = utc(data.ingested_at)
    data["bar_origin"] = pd.Series("UNVERIFIED", index=data.index, dtype="string").mask(
        data.source.eq("dnse"), "DIRECT").mask(data.source.eq("mvp"), "DERIVED_1M")
    dnse_equity = data.source.eq("dnse") & data.asset.eq("equity")
    points = data.asset.isin(["index", "futures"])
    data["source_price_unit"] = pd.Series("UNKNOWN", index=data.index, dtype="string").mask(points, "INDEX_POINT").mask(dnse_equity, "THOUSAND_VND")
    data["price_unit"] = data.source_price_unit.replace("THOUSAND_VND", "VND")
    data["price_basis"] = pd.Series("UNKNOWN", index=data.index, dtype="string").mask(points, "INDEX_POINT").mask(dnse_equity, "PROVIDER_ADJUSTED")
    for name in PRICE_FIELDS:
        data[f"{name}_source"] = data[name]
        data[name] = data[name].where(~dnse_equity, data[name] * 1000)
    data["volume_shares"] = data.volume.where(dnse_equity)
    is_regular_label = data.interval.isin(["1m", "5m"]) & data.source.eq("dnse") & local_time.dt.strftime("%H:%M").lt("14:45")
    data["bar_start_utc"] = data.time.where(is_regular_label)
    duration = pd.to_timedelta(data.interval.map({"1m": 1, "5m": 5}), unit="min")
    data["bar_end_utc"] = data.bar_start_utc + duration
    data["available_at_utc"] = pd.Series(pd.NaT, dtype="datetime64[ns, UTC]", index=data.index)
    data["quality_status"] = "WARN"
    return data


def _coverage(frame: pd.DataFrame) -> pd.DataFrame:
    keys = ["instrument_type", "instrument_id", "symbol", "exchange", "interval", "source", "bar_origin", "trading_date"]
    coverage = frame.groupby(keys, dropna=False, as_index=False, sort=False).agg(
        actual_bars=("time", "size"), first_time_utc=("time", "min"), last_time_utc=("time", "max"))
    return coverage.assign(
        calendar_version="BRONZE_OBSERVED_V1", expected_bars=pd.Series(pd.NA, index=coverage.index, dtype="Int64"),
        is_complete=pd.Series(pd.NA, index=coverage.index, dtype="boolean"), coverage_status="OBSERVED_ONLY")


def build_bars(bronze: Path, stage: Path, quarantine: Path, identity: Identity, table: str, run_id: str) -> tuple[dict, list[pd.DataFrame]]:
    files = sorted((bronze / table).rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Bronze files for {table}")
    counts = dict(input_rows=0, accepted_rows=0, rejected_rows=0, duplicate_rows=0)
    observations = []
    with TemporaryDirectory(prefix=f".{table}-", dir=stage) as temporary:
        buckets = defaultdict(list)
        for number, batch in enumerate(_read_batches(files)):
            data = _standardize(batch)
            counts["input_rows"] += len(data)
            labels = data.time.dt.tz_convert("Asia/Ho_Chi_Minh").dt.strftime("%Y-%m" if table.startswith("intraday") else "%Y")
            intervals = data.interval.where(data.interval.isin(["1D", "1m", "5m"]), "unknown")
            groups = data.groupby([intervals, labels.fillna("unparsed")], sort=False)
            for group_index, ((interval, period), group) in enumerate(groups):
                key = (interval, period)
                path = Path(temporary) / f"{number}-{group_index}.parquet"
                group.to_parquet(path, index=False)
                buckets[key].append(path)
