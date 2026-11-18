"""Observed instrument identities and the small market reference tables."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from transform.silver.market import _canonicalize

VENUES = {"STO": "HOSE", "STX": "HNX", "UPX": "UPCOM", "DVX": "HNXDS", "HSX": "HOSE"}
INDEX_VENUES = {"VNINDEX": "HOSE", "VN30": "HOSE", "VN100": "HOSE", "HNXINDEX": "HNX",
                "HNX30": "HNX", "UPCOMINDEX": "UPCOM"}
CATALOG_TABLES = ("securities", "securities_details", "daily_stock_price", "index_list", "index_components")


def read_table(path: Path) -> pd.DataFrame:
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Bronze Parquet in {path}")
    return pd.concat([pd.read_parquet(file).assign(bronze_file=str(file)) for file in files], ignore_index=True)
