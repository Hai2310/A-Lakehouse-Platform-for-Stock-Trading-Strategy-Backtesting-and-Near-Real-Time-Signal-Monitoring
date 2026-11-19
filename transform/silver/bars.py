"""Distributed Spark transformations for the Bronze price bars."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pandas as pd
from pyarrow import types as arrow_types
from pyarrow.parquet import ParquetFile
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from transform.silver.market import TABLE_RULES
from transform.silver.reference import INDEX_VENUES, Identity

BAR_TABLES = ("daily_ohlc", "intraday_ohlc", "daily_index", "intraday_index")
PRICE_FIELDS = ("open", "high", "low", "close")
NUMBER_FIELDS = (*PRICE_FIELDS, "volume")
SOURCE_FIELDS = ("symbol", "source", "asset", "interval", "time", *NUMBER_FIELDS, "ingested_at")
COVERAGE_KEYS = ("instrument_type", "instrument_id", "symbol", "exchange", "interval",
                 "source", "bar_origin", "trading_date")
VN_TZ = "Asia/Ho_Chi_Minh"
CALENDAR_VERSION = "BRONZE_OBSERVED_V1"


def _timestamp(name, arrow_type):
    if arrow_types.is_timestamp(arrow_type):
        if arrow_type.unit == "ns":
            # Spark can expose Parquet nanos as a bigint. Convert to Spark's microsecond precision.
            stamp = F.timestamp_micros(F.expr(f"`{name}` DIV 1000"))
        else:
            stamp = F.col(name).cast("timestamp")
        return stamp if arrow_type.tz else F.to_utc_timestamp(stamp, VN_TZ)
    raw = F.trim(F.col(name).cast("string"))
    stamp = F.try_to_timestamp(raw)
    return F.when(raw.rlike(r"(?i)(?:Z|[+-][0-9]{2}:?[0-9]{2})$"), stamp).otherwise(
        F.to_utc_timestamp(stamp, VN_TZ)
    )


def _read_bars(spark: SparkSession, files: list[Path]) -> DataFrame:
    # Spark needs separate scans when a field has different physical Parquet types.
    # Inspect only footers on the driver. All row reads and conversions run in Spark.
    groups = defaultdict(list)
    for file in files:
        schema = ParquetFile(file).schema_arrow
        missing = set(SOURCE_FIELDS[:-1]) - set(schema.names)
        if missing:
            raise ValueError(f"{file} missing price columns {sorted(missing)}")
        types = tuple(schema.field(name).type if name in schema.names else None for name in SOURCE_FIELDS)
        groups[types].append(str(file))
    combined = None
    for types, paths in groups.items():
        raw = spark.read.option("recursiveFileLookup", "true").parquet(*paths)
        kind = dict(zip(SOURCE_FIELDS, types))
        if kind["ingested_at"] is None:
            raw = raw.withColumn("ingested_at", F.lit(None).cast("string"))
        labels = {name: F.nullif(F.trim(F.col(name).cast("string")), F.lit(""))
                  for name in ("symbol", "source", "asset", "interval")}
        interval = F.lower(labels["interval"])
        parsed = raw.select(
            F.upper(labels["symbol"]).alias("symbol"),
            F.lower(labels["source"]).alias("source"),
            F.lower(labels["asset"]).alias("asset"),
            F.when(interval == "1d", "1D").otherwise(interval).alias("interval"),
            _timestamp("time", kind["time"]).alias("time"),
            *(F.expr(f"try_cast(`{name}` AS DOUBLE)").alias(name) for name in NUMBER_FIELDS),
            F.col("ingested_at").cast("string").alias("ingested_at"),
            (_timestamp("ingested_at", kind["ingested_at"]) if kind["ingested_at"] is not None
             else F.lit(None).cast("timestamp")).alias("observed_at_utc"),
            F.url_decode(F.regexp_replace(F.regexp_replace(F.input_file_name(), "^file:(//)?", ""), r"\+", "%2B")).alias("bronze_file"),
        )
        combined = parsed if combined is None else combined.unionByName(parsed)
    return combined


def identity_lookup(spark: SparkSession, identity: Identity) -> DataFrame:
    symbols = set(identity.venues.index) | set(identity.by_pair.index.get_level_values(0)) | set(
        identity.ambiguous.get_level_values(0)
    )
    lookup = identity.attach(pd.DataFrame(
        [(symbol, asset) for asset in ("equity", "futures") for symbol in sorted(symbols)],
        columns=["symbol", "asset"],
    ))
    fields = ("symbol", "asset", "exchange", "isin", "security_id", "identity_status")
    values = lookup[list(fields)]
    values = values.astype(object).where(values.notna(), None)
    return spark.createDataFrame(list(values.itertuples(index=False, name=None)),
                                 ", ".join(f"{name} string" for name in fields))


def _check_rows(data: DataFrame, table: str, identities: DataFrame) -> DataFrame:
    col = F.col
    missing = {name: col(name).isNull() for name in TABLE_RULES[table]["required"]}
    missing.update({name: col(name).isNull() | F.isnan(name) for name in NUMBER_FIELDS})
    reasons = [(check, f"missing_{name}") for name, check in missing.items()]
    reasons.append((missing["time"], "invalid_time"))
    for name in NUMBER_FIELDS:
        reasons.extend([(missing[name], f"invalid_{name}"),
                        (F.abs(col(name)) == float("inf"), f"non_finite_{name}")])
    reasons.extend((~F.isnan(name) & (col(name) <= 0), f"non_positive_{name}") for name in PRICE_FIELDS)
    reasons.extend([
        (~F.isnan("volume") & (col("volume") < 0), "negative_volume"),
        (col("high") < col("low"), "high_below_low"),
        ((col("open") > col("high")) | (col("open") < col("low")), "open_outside_range"),
        ((col("close") > col("high")) | (col("close") < col("low")), "close_outside_range"),
    ])
    data = data.withColumn("rejection_reason", F.concat_ws("|", *(F.when(rule, label) for rule, label in reasons)))
    if table.endswith("index"):
        venues = F.create_map(*(F.lit(value) for pair in INDEX_VENUES.items() for value in pair))
        data = data.withColumns({
            "index_id": F.concat(F.lit("INDEX:"), col("symbol")),
            "exchange": venues[col("symbol")],
            "identity_status": F.lit("SOURCE_INDEX_CODE"),
        }).withColumns({"instrument_id": col("index_id"), "instrument_type": F.lit("index")})
    else:
        lookup_asset = F.when(col("asset") == "futures", "futures").otherwise("equity")
        data = data.withColumn("_lookup_asset", lookup_asset).join(
            F.broadcast(identities.withColumnRenamed("asset", "_lookup_asset")), ["symbol", "_lookup_asset"], "left"
        ).drop("_lookup_asset")
        exchange = F.when(col("asset") == "futures", "HNXDS").otherwise(col("exchange"))
        fallback = F.concat(F.lit("TICKER:"), F.coalesce(exchange, F.lit("UNKNOWN")), F.lit(":"), col("symbol"))
        data = data.withColumns({
            "exchange": exchange,
            "security_id": F.when(col("identity_status") != "AMBIGUOUS", F.coalesce(col("security_id"), fallback))
                            .when(col("identity_status").isNull(), fallback),
            "identity_status": F.coalesce(col("identity_status"), F.lit("TICKER_ONLY")),
        }).withColumns({"instrument_id": col("security_id"), "instrument_type": col("asset")})

    intraday = table.startswith("intraday")
    interval_ok = col("interval").isin("1m", "5m") if intraday else col("interval") == "1D"
    reason = col("rejection_reason")
    data = data.withColumn(
        "rejection_reason",
        F.when(reason != "", reason).when(col("instrument_id").isNull() | ~interval_ok,
                                          "unresolved_identity_or_interval").otherwise(""),
    )
    # Keep all invalid rows in quarantine, and rank only rows with a valid business key.
    window = Window.partitionBy(*TABLE_RULES[table]["keys"]).orderBy(
        col("observed_at_utc").desc_nulls_last(), col("ingested_at").desc_nulls_last(),
        col("bronze_file").desc(), *(col(name).desc() for name in NUMBER_FIELDS),
    )
    good = data.where(col("rejection_reason") == "").withColumn("_rank", F.row_number().over(window))
    good = good.withColumn("rejection_reason", F.when(col("_rank") > 1, "duplicate_key").otherwise("")).drop("_rank")
    checked = good.unionByName(data.where(col("rejection_reason") != ""))
    local_time = F.from_utc_timestamp("time", VN_TZ)
    return checked.withColumns({
        "trading_date": F.to_date(local_time),
        "timeframe": F.when(col("interval").isin("1D", "1m", "5m"), col("interval")).otherwise("unknown"),
        "period": F.coalesce(F.date_format(local_time, "yyyy-MM" if intraday else "yyyy"), F.lit("unparsed")),
    })


def _enrich(data: DataFrame, run_id: str) -> DataFrame:
    col = F.col
    dnse = col("source") == "dnse"
    equity = dnse & (col("asset") == "equity")
    points = col("asset").isin("index", "futures")
    regular = dnse & col("interval").isin("1m", "5m") & (
        F.date_format(F.from_utc_timestamp("time", VN_TZ), "HH:mm") < "14:45"
    )
    duration = F.when(col("interval") == "1m", F.expr("INTERVAL 1 MINUTE")).otherwise(F.expr("INTERVAL 5 MINUTES"))
    prices = {f"{name}_source": col(name) for name in PRICE_FIELDS}
    prices.update({name: F.when(equity, col(name) * 1000).otherwise(col(name)) for name in PRICE_FIELDS})
    return data.withColumns({
        **prices,
        "bar_origin": F.when(dnse, "DIRECT").when(col("source") == "mvp", "DERIVED_1M").otherwise("UNVERIFIED"),
        "source_price_unit": F.when(equity, "THOUSAND_VND").when(points, "INDEX_POINT").otherwise("UNKNOWN"),
        "price_unit": F.when(equity, "VND").when(points, "INDEX_POINT").otherwise("UNKNOWN"),
        "price_basis": F.when(equity, "PROVIDER_ADJUSTED").when(points, "INDEX_POINT").otherwise("UNKNOWN"),
        "volume_shares": F.when(equity, col("volume")),
        "bar_start_utc": F.when(regular, col("time")),
        "bar_end_utc": F.when(regular, col("time") + duration),
        "available_at_utc": F.lit(None).cast("timestamp"),
        "quality_status": F.lit("WARN"),
        "silver_run_id": F.lit(run_id),
        "schema_version": F.lit("1"),
    })


def _write_partitions(data: DataFrame, path: Path, split_minute: bool) -> None:
    # Spread high-volume minute partitions across tasks without adding a partition column.
    buckets = F.when(F.col("timeframe") == "1m", F.pmod(F.xxhash64("instrument_id"), F.lit(4))).otherwise(0)
    keys = ["timeframe", "period", buckets] if split_minute else ["timeframe", "period"]
    data.repartition(*keys).write.option("maxRecordsPerFile", 1_000_000).partitionBy(
        "timeframe", "period"
    ).parquet(str(path))


def build_bars(spark: SparkSession, bronze: Path, stage: Path, quarantine: Path,
               identities: DataFrame, table: str, run_id: str) -> dict:
    files = sorted((bronze / table).rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Bronze files for {table}")
    checked = _check_rows(_read_bars(spark, files), table, identities).persist(StorageLevel.DISK_ONLY)
    try:
        counts = {row.rejection_reason: row["count"] for row in checked.groupBy("rejection_reason").count().collect()}
        accepted = counts.get("", 0)
        duplicates = counts.get("duplicate_key", 0)
        rejected = sum(counts.values()) - accepted - duplicates
        if not accepted:
            raise ValueError(f"No valid bars remain for {table}")
        if rejected or duplicates:
            checked.where(F.col("rejection_reason") != "").write.partitionBy("timeframe", "period").parquet(
                str(quarantine / table)
            )
        bars = _enrich(checked.where(F.col("rejection_reason") == "").drop("rejection_reason"), run_id)
        _write_partitions(bars, stage / "market" / table, table.startswith("intraday"))
        coverage = bars.groupBy(*COVERAGE_KEYS, "timeframe", "period").agg(
            F.count("*").alias("actual_bars"), F.min("time").alias("first_time_utc"), F.max("time").alias("last_time_utc")
        ).withColumns({
            "calendar_version": F.lit(CALENDAR_VERSION),
            "expected_bars": F.lit(None).cast("long"),
            "is_complete": F.lit(None).cast("boolean"),
            "coverage_status": F.lit("OBSERVED_ONLY"),
            "silver_run_id": F.lit(run_id),
            "schema_version": F.lit("1"),
        })
        _write_partitions(coverage, stage / "quality" / "data_coverage" / f"bar_table={table}",
                          table.startswith("intraday"))
        return dict(table=table, bronze_files=len(files), input_rows=sum(counts.values()),
                    accepted_rows=accepted, rejected_rows=rejected, duplicate_rows=duplicates)
    finally:
        checked.unpersist()
