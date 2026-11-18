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
