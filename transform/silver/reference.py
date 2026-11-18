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


def series(frame: pd.DataFrame, column: str) -> pd.Series:
    return frame[column] if column in frame else pd.Series(pd.NA, index=frame.index, dtype="string")


def venue(values: pd.Series) -> pd.Series:
    return values.astype("string").str.strip().str.upper().replace(VENUES).replace("", pd.NA)


def utc(values: pd.Series) -> pd.Series:
    """Interpret timezone-less source timestamps as exchange-local time."""
    if isinstance(values.dtype, pd.DatetimeTZDtype):
        return values.dt.tz_convert("UTC")
    values = values.astype("string")
    has_zone = values.str.contains(r"(?:Z|[+-][0-9]{2}:?[0-9]{2})$", case=False, na=False)
    result = pd.to_datetime(values.where(has_zone), utc=True, format="mixed", errors="coerce")
    local = pd.to_datetime(values.where(~has_zone), format="mixed", errors="coerce")
    return result.fillna(local.dt.tz_localize("Asia/Ho_Chi_Minh", ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC"))


def reference_frame(root: Path, table: str) -> pd.DataFrame:
    frame = _canonicalize(read_table(root / table), table)
    observed = utc(series(frame, "snapshot_at")).fillna(utc(series(frame, "ingested_at")))
    exchange = venue(series(frame, "exchange")).fillna(venue(series(frame, "marketid")))
    frame["exchange"] = exchange
    frame["observed_at_utc"] = observed
    frame["evidence_date"] = observed.dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None).dt.normalize()
    return frame
