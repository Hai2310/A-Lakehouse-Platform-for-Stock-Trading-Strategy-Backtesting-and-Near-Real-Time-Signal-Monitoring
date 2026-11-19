"""Build and publish the Spark Silver snapshot for Market and VSDC."""

from __future__ import annotations

import argparse
from datetime import datetime, UTC
from pathlib import Path
from shutil import move, rmtree
from tempfile import TemporaryDirectory
from uuid import uuid4

import pandas as pd
from pyarrow.parquet import ParquetFile
from pyspark.sql import DataFrame, SparkSession, functions as F

from ingest.market.lake import PROJECT_ROOT
from transform.silver.bars import BAR_TABLES, CALENDAR_VERSION, build_bars, identity_lookup
from transform.silver.market import _resolve_path, _write_snapshot
from transform.silver.quality import validate_snapshot
from transform.silver.reference import CATALOG_TABLES, INDEX_VENUES, Identity, build_references, read_table, reference_frame
from transform.silver.vsdc import build_events


def _quality(status: dict) -> None:
    print(f"[{status['status']}] {status['table']}: {status['accepted_rows']:,} accepted", flush=True)


def _publish_table(frame, keys, table, domain, stage, quarantine, run_id) -> dict:
    missing = frame[keys].isna().any(axis=1)
    valid = frame.loc[~missing]
    repeated = valid.duplicated(keys, keep="last")
    bad = pd.concat([
        frame.loc[missing].assign(rejection_reason="missing_key"),
        valid.loc[repeated].assign(rejection_reason="duplicate_key"),
    ], ignore_index=True)
    good = valid.loc[~repeated]
    if good.empty:
        raise ValueError(f"No valid rows remain for {table}")
    _write_snapshot(good.assign(silver_run_id=run_id, schema_version="1"), stage / domain / table / "data.parquet")
    if not bad.empty:
        _write_snapshot(bad, quarantine / domain / table / "rejected.parquet")
    return dict(table=table, input_rows=len(frame), accepted_rows=len(good), rejected_rows=int(missing.sum()),
                duplicate_rows=int(repeated.sum()), status="WARN" if not bad.empty else "PASS")


def _calendar(coverage: DataFrame, run_id: str) -> DataFrame:
    daily = F.col("interval") == "1D"
    return coverage.where(F.col("exchange").isNotNull()).groupBy("exchange", "trading_date").agg(
        F.bool_or(daily).alias("has_daily_bars"),
        F.bool_or(~daily).alias("has_intraday_bars"),
        F.min(F.when(~daily, F.col("first_time_utc"))).alias("first_observed_bar_utc"),
        F.max(F.when(~daily, F.col("last_time_utc"))).alias("last_observed_bar_utc"),
    ).withColumns({
        "calendar_version": F.lit(CALENDAR_VERSION),
        "session_segment": F.lit("UNVERIFIED"),
        "is_official": F.lit(False),
        "session_start_utc": F.lit(None).cast("timestamp"),
        "session_end_utc": F.lit(None).cast("timestamp"),
        "source": F.lit("bronze_bar_observations"),
        "silver_run_id": F.lit(run_id),
        "schema_version": F.lit("1"),
    })


def _written_rows(path: Path) -> int:
    rows = sum(ParquetFile(file).metadata.num_rows for file in path.rglob("*.parquet"))
    if rows == 0:
        raise ValueError(f"No rows written to {path}")
    return rows


def _spark_session(master: str, driver_memory: str, shuffle_partitions: int) -> SparkSession:
    builder = (SparkSession.builder.appName("silver-market-vsdc").master(master)
               .config("spark.driver.memory", driver_memory)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", shuffle_partitions)
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.sql.legacy.parquet.nanosAsLong", "true")
               .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
               .config("spark.ui.enabled", "false"))
    if master.startswith("local"):
        builder.config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run(bronze: Path, output: Path, quarantine_root: Path, *,
        master: str = "local[2]", driver_memory: str = "1g", shuffle_partitions: int = 32,
        market_root: Path | None = None) -> Path:
    market, vsdc = market_root or bronze / "market", bronze / "vsdc"
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:8]
    runs = output / "_runs"
    runs.mkdir(parents=True, exist_ok=True)
    quarantine = quarantine_root / f"run={run_id}"
    summaries = []
    with TemporaryDirectory(prefix=".building-", dir=runs) as tmp:
        stage = Path(tmp)
        raw = {table: reference_frame(market, table) for table in CATALOG_TABLES}
        calendar = read_table(vsdc / "corporate_actions")
        identity = Identity(raw["securities"], raw["securities_details"], calendar)
        for table, (frame, keys) in build_references(raw, identity, INDEX_VENUES).items():
            domain = "reference" if table == "security_symbol_history" else "market"
            entry = _publish_table(frame, keys, table, domain, stage, quarantine, run_id)
            summaries.append(entry)
            _quality(entry)

        events = build_events(vsdc, calendar, identity)
        entry = _publish_table(events, ["event_id", "revision_id", "security_id"], "corporate_actions", "vsdc", stage, quarantine, run_id)
        entry.update(status="WARN", note="Verify terms, revision links and initial publication before PIT use.")
        summaries.append(entry)
        _quality(entry)

        spark = _spark_session(master, driver_memory, shuffle_partitions)
        try:
            identities = identity_lookup(spark, identity)
            for table in BAR_TABLES:
                entry = build_bars(spark, market, stage, quarantine / "market", identities, table, run_id)
                entry.update(status="WARN", note="Observed price coverage. Availability and historical identity not certified.")
                summaries.append(entry)
                _quality(entry)

            coverage_path = stage / "quality" / "data_coverage"
            coverage = spark.read.parquet(str(coverage_path))
            counts = {row.bar_table: row for row in coverage.groupBy("bar_table").agg(
                F.count("*").alias("coverage_rows"), F.sum("actual_bars").alias("bars")
            ).collect()}
            for entry in summaries:
                if entry["table"] in BAR_TABLES:
                    observed = counts.get(entry["table"])
                    if observed is None or observed.bars != entry["accepted_rows"]:
                        raise ValueError(f"Coverage counts differ from accepted bars for {entry['table']}")

            calendar_path = stage / "reference" / "trading_calendar"
            # A small calendar keeps the existing single-file path for consumers.
            calendar_stage = calendar_path / ".spark-output"
            _calendar(coverage, run_id).coalesce(1).write.parquet(str(calendar_stage))
            next(calendar_stage.glob("part-*.parquet")).rename(calendar_path / "data.parquet")
            rmtree(calendar_stage)
            calendar_rows = _written_rows(calendar_path)
            entry = dict(table="trading_calendar", input_rows=calendar_rows, accepted_rows=calendar_rows,
                         rejected_rows=0, duplicate_rows=0, status="WARN",
                         note="Observed trading dates from Bronze bars. Official expected sessions unavailable.")
            summaries.append(entry)
            _quality(entry)
            entry = dict(table="data_coverage", accepted_rows=sum(row.coverage_rows for row in counts.values()),
                         status="WARN", note="Observed days only. Expected bars and completeness are unknown.")
            summaries.append(entry)
            _quality(entry)
            validation_report = quarantine / "gx_validation.json"
            expected_counts = {entry["table"]: entry["accepted_rows"] for entry in summaries}
            validate_snapshot(spark, stage, expected_counts, run_id, validation_report)
            move(validation_report, stage / "gx_validation.json")
        finally:
            spark.stop()

        report = pd.DataFrame(summaries).assign(run_id=run_id, schema_version="1")
        report.to_csv(stage / "quality_report.csv", index=False)
        published = runs / run_id
        stage.rename(published)

    pointer = output / f".current-{run_id}"
    try:
        pointer.symlink_to(Path("_runs") / run_id, target_is_directory=True)
        pointer.replace(output / "current")
    finally:
        pointer.unlink(missing_ok=True)
    print(f"Silver: {output / 'current'}", flush=True)
    print(f"Quality: {published / 'quality_report.csv'}", flush=True)
    return published


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-root", type=Path, default=PROJECT_ROOT / "data/bronze")
    parser.add_argument("--silver-root", type=Path, default=PROJECT_ROOT / "data/silver")
    parser.add_argument("--quarantine-root", type=Path, default=PROJECT_ROOT / "data/quarantine/silver")
    parser.add_argument("--master", default="local[2]", help="Spark master")
    parser.add_argument("--driver-memory", default="1g", help="Spark driver heap, for example 2g")
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    args = parser.parse_args()
    if args.shuffle_partitions < 1:
        parser.error("--shuffle-partitions must be positive")
    run(_resolve_path(args.bronze_root), _resolve_path(args.silver_root), _resolve_path(args.quarantine_root),
        master=args.master, driver_memory=args.driver_memory, shuffle_partitions=args.shuffle_partitions)


if __name__ == "__main__":
    main()
