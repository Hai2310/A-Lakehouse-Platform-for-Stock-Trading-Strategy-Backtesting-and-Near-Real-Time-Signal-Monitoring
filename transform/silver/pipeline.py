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
