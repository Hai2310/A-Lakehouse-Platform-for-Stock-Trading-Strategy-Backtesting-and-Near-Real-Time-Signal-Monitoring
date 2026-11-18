"""Build and publish the local Silver snapshot for Market and VSDC."""

from __future__ import annotations

import argparse
from datetime import datetime, UTC
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pandas as pd

from ingest.market.lake import PROJECT_ROOT
from transform.silver.bars import BAR_TABLES, build_bars
from transform.silver.market import _resolve_path, _write_snapshot
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


def _calendar(parts):
    observations = pd.concat(parts, ignore_index=True)
    daily = observations.interval.eq("1D")
    observations["has_daily_bars"] = daily
    observations["has_intraday_bars"] = ~daily
    for name in ("first_observed_bar_utc", "last_observed_bar_utc"):
        observations[name] = observations[name].mask(daily)
    calendar = observations.groupby(["exchange", "trading_date"], as_index=False).agg(
        has_daily_bars=("has_daily_bars", "any"), has_intraday_bars=("has_intraday_bars", "any"),
        first_observed_bar_utc=("first_observed_bar_utc", "min"), last_observed_bar_utc=("last_observed_bar_utc", "max"))
    return calendar.assign(calendar_version="BRONZE_OBSERVED_V1", session_segment="UNVERIFIED", is_official=False,
                           session_start_utc=pd.NaT, session_end_utc=pd.NaT, source="bronze_bar_observations")


def run(bronze: Path, output: Path, quarantine_root: Path) -> Path:
    market, vsdc = bronze / "market", bronze / "vsdc"
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

        observations = []
        for table in BAR_TABLES:
            entry, parts = build_bars(market, stage, quarantine / "market", identity, table, run_id)
            entry.update(status="WARN", note="Observed price coverage. Availability and historical identity not certified.")
            observations.extend(parts)
            summaries.append(entry)
            _quality(entry)

        calendar = _calendar(observations)
        entry = _publish_table(calendar, ["exchange", "trading_date", "calendar_version"], "trading_calendar",
                               "reference", stage, quarantine, run_id)
        entry.update(status="WARN", note="Observed trading dates from Bronze bars. Official expected sessions unavailable.")
        summaries.append(entry)
        _quality(entry)
        from pyarrow.parquet import ParquetFile

        coverage_rows = sum(ParquetFile(p).metadata.num_rows for p in (stage / "quality" / "data_coverage").rglob("*.parquet"))
        entry = dict(table="data_coverage", accepted_rows=coverage_rows, status="WARN",
                     note="Observed days only. Expected bars and completeness are unknown.")
        summaries.append(entry)
        _quality(entry)

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
