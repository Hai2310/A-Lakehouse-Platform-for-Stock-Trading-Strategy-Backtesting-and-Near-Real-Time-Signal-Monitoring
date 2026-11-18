"""Common timestamp and OHLCV normalization for secondary data sources."""

from __future__ import annotations

import re
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def _column_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def _first_column(columns: dict[str, Any], *names: str) -> Any | None:
    for name in names:
        if name in columns:
            return columns[name]
    return None


def _parse_timestamp(values: pd.Series) -> pd.Series:
    first = next((value for value in values.dropna().tolist()), None)
    if first is None:
        return pd.Series(pd.NaT, index=values.index, dtype=f"datetime64[ns, {TZ.key}]")

    if pd.api.types.is_numeric_dtype(values):
        number = pd.to_numeric(values, errors="coerce")
        maximum = number.dropna().abs().max()
        if pd.isna(maximum):
            parsed = pd.to_datetime(values, errors="coerce")
            if isinstance(parsed.dtype, pd.DatetimeTZDtype):
                return parsed.dt.tz_convert(TZ)
            return parsed.dt.tz_localize(TZ)
        if maximum >= 100_000_000_000_000_000:
            parsed = pd.to_datetime(number, unit="ns", errors="coerce", utc=True)
            return parsed.dt.tz_convert(TZ)
        if maximum >= 100_000_000_000_000:
            parsed = pd.to_datetime(number, unit="us", errors="coerce", utc=True)
            return parsed.dt.tz_convert(TZ)
        if maximum >= 100_000_000_000:
            parsed = pd.to_datetime(number, unit="ms", errors="coerce", utc=True)
            return parsed.dt.tz_convert(TZ)
        if maximum >= 1_000_000_000:
            parsed = pd.to_datetime(number, unit="s", errors="coerce", utc=True)
            return parsed.dt.tz_convert(TZ)
        if maximum >= 10_000_000:
            parsed = pd.to_datetime(number.astype("Int64").astype(str), format="%Y%m%d", errors="coerce")
            return parsed.dt.tz_localize(TZ)

    first_text = str(first)
    has_zone = bool(re.search(r"(?:Z|[+-]\d{2}:?\d{2})$", first_text))
    if has_zone:
        return pd.to_datetime(values, errors="coerce", utc=True).dt.tz_convert(TZ)

    parsed = pd.to_datetime(values, errors="coerce")
    if isinstance(parsed.dtype, pd.DatetimeTZDtype):
        return parsed.dt.tz_convert(TZ)
    return parsed.dt.tz_localize(TZ, ambiguous="NaT", nonexistent="NaT")


def normalize_ohlcv(frame: pd.DataFrame) -> pd.DataFrame:
    """Map common vendor column names to time/open/high/low/close/volume."""
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    raw = frame.copy()
    columns: dict[str, Any] = {}
    for original in raw.columns:
        columns.setdefault(_column_key(original), original)

    date_col = _first_column(columns, "date", "trading_date", "trade_date", "tradingdate")
    time_col = _first_column(columns, "time", "trading_time", "trade_time", "bar_time")
    timestamp_col = _first_column(
        columns, "timestamp", "datetime", "date_time", "trading_datetime", "trade_datetime", "t"
    )

    if timestamp_col is not None:
        timestamps = raw[timestamp_col]
    elif date_col is not None and time_col is not None:
        time_values = raw[time_col].astype(str)
        if time_values.str.contains(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", regex=True).any():
            timestamps = raw[time_col]
        else:
            timestamps = raw[date_col].astype(str) + " " + time_values
    elif time_col is not None:
        timestamps = raw[time_col]
    elif date_col is not None:
        timestamps = raw[date_col]
    elif not isinstance(raw.index, pd.RangeIndex):
        timestamps = pd.Series(raw.index, index=raw.index)
    else:
        raise ValueError(f"No timestamp/date column found in OHLCV columns: {list(raw.columns)}")

    aliases = {
        "open": ("open", "o", "open_price", "openprice"),
        "high": ("high", "h", "high_price", "highprice"),
        "low": ("low", "l", "low_price", "lowprice"),
        "close": ("close", "c", "close_price", "closeprice"),
        "volume": (
            "volume",
            "vol",
            "v",
            "trading_volume",
            "match_volume",
            "matched_volume",
            "matchvol",
        ),
    }

    out = pd.DataFrame(index=raw.index)
    out["time"] = _parse_timestamp(pd.Series(timestamps, index=raw.index))
    for name, candidates in aliases.items():
        column = _first_column(columns, *candidates)
        if column is None and name == "volume":
            out[name] = 0
        elif column is None:
            raise ValueError(f"Missing required OHLCV field '{name}' in {list(raw.columns)}")
        else:
            out[name] = pd.to_numeric(raw[column], errors="coerce")

    trade_count_col = _first_column(
        columns, "trade_count", "trades", "number_of_trades", "num_trades", "match_count"
    )
    if trade_count_col is not None:
        out["trade_count"] = pd.to_numeric(raw[trade_count_col], errors="coerce").fillna(0)

    contract_col = _first_column(columns, "tickersymbol", "ticker_symbol", "contract_symbol")
    if contract_col is not None:
        out["source_contract"] = raw[contract_col].astype("string")
    contract_month_col = _first_column(columns, "contract_month")
    if contract_month_col is not None:
        out["contract_month"] = raw[contract_month_col].astype("string")

    out = out.dropna(subset=["time", "open", "high", "low", "close"])
    out["volume"] = out["volume"].fillna(0)
    return out.sort_values("time").reset_index(drop=True)


def filter_dates(frame: pd.DataFrame, start: str | None, end: str | None) -> pd.DataFrame:
    """Filter normalized OHLCV on inclusive local-market dates."""
    if frame.empty or (start is None and end is None):
        return frame
    result = frame.copy()
    start_at = pd.Timestamp(start) if start else None
    end_at = pd.Timestamp(end) + pd.Timedelta(days=1) if end else None
    if start_at is not None:
        start_at = start_at.tz_localize(TZ) if start_at.tzinfo is None else start_at.tz_convert(TZ)
        result = result[result["time"] >= start_at]
    if end_at is not None:
        end_at = end_at.tz_localize(TZ) if end_at.tzinfo is None else end_at.tz_convert(TZ)
        result = result[result["time"] < end_at]
    return result.reset_index(drop=True)


def resample_ohlcv(frame: pd.DataFrame, interval: str) -> pd.DataFrame:
    """Aggregate minute/hour bars to 5-minute or daily OHLCV bars."""
    if frame.empty:
        return frame.copy()
    if interval not in {"5m", "1D"}:
        raise ValueError("Resampling supports only '5m' and '1D'.")

    rule = "5min" if interval == "5m" else "1D"
    indexed = frame.set_index("time").sort_index()
    aggregations = {
        "open": ("open", "first"),
        "high": ("high", "max"),
        "low": ("low", "min"),
        "close": ("close", "last"),
        "volume": ("volume", "sum"),
        "source_rows": ("close", "count"),
    }
    if "trade_count" in indexed.columns:
        aggregations["trade_count"] = ("trade_count", "sum")
    for column in ("source_contract", "contract_month"):
        if column in indexed.columns:
            aggregations[column] = (column, "last")

    resample_options = {
        "origin": "start_day",
        "label": "left",
        "closed": "left",
    }
    if interval == "5m":
        resample_options["offset"] = "9h"
    bars = indexed.resample(rule, **resample_options).agg(**aggregations)
    bars = bars.dropna(subset=["open", "high", "low", "close"]).reset_index()
    if interval == "5m":
        bars["is_complete_window"] = bars["source_rows"] >= 5
    return bars
