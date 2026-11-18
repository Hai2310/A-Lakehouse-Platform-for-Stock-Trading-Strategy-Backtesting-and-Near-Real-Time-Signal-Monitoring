"""Probe documented KBS minute bars without writing market data."""

from datetime import date, timedelta
import re

import pandas as pd
import requests

BASE_URL = "https://kbbuddywts.kbsec.com.vn/iis-server/investment/stocks"
FIELDS = dict(t="time", o="open", h="high", l="low", c="close", v="volume")
RESOLUTIONS = {"1m": "1P", "5m": "5P"}


def probe_minute_history(symbol: str, start: str, end: str, *, interval: str = "1m"):
    """Return a request summary and bars within the requested local dates."""
    symbol = symbol.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{1,15}", symbol):
        raise ValueError("Invalid stock symbol")
    if interval not in RESOLUTIONS:
        raise ValueError(f"Unsupported KBS interval: {interval}")
    data_key = f"data_{RESOLUTIONS[interval]}"
    lower, upper = date.fromisoformat(start), date.fromisoformat(end)
    if upper < lower:
        raise ValueError("End date must be on or after start date")

    report = dict(symbol=symbol, interval=interval, start=start, end=end,
                  http_status=None, status="", returned_rows=None,
                  matched_rows=None, first_time=None, last_time=None)
    empty = pd.DataFrame(columns=[*FIELDS.values(), "symbol", "source", "interval"])
    try:
        response = requests.get(
            f"{BASE_URL}/{symbol}/{data_key}",
            params={"sdate": lower.strftime("%d-%m-%Y"), "edate": upper.strftime("%d-%m-%Y")},
            headers={"Accept": "application/json"},
            timeout=(5, 15),
        )
    except requests.RequestException as exc:
        return dict(report, status="NETWORK_ERROR", detail=type(exc).__name__), empty
    report["http_status"] = response.status_code
    if not response.ok:
        blocked = "request blocked" in response.text[:2048].lower()
        return dict(report, status="BLOCKED" if blocked else "HTTP_ERROR"), empty

    try:
        payload = response.json()
    except ValueError:
        return dict(report, status="INVALID_JSON"), empty
    rows = payload.get(data_key) if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return dict(report, status="UNEXPECTED_SCHEMA"), empty
    report["returned_rows"] = len(rows)
    if not rows:
        return dict(report, status="EMPTY", matched_rows=0), empty

    bars = pd.DataFrame(rows).rename(columns=FIELDS)
    if not set(FIELDS.values()).issubset(bars.columns):
        return dict(report, status="UNEXPECTED_SCHEMA"), empty
    times = pd.to_datetime(bars["time"], format="%Y-%m-%d %H:%M", errors="coerce")
    start_at = pd.Timestamp(lower)
    end_at = pd.Timestamp(upper + timedelta(days=1))
    matched = times.ge(start_at) & times.lt(end_at)
    report.update(first_time=times.min(), last_time=times.max(),
                  matched_rows=int(matched.sum()), invalid_timestamps=int(times.isna().sum()))
    if not matched.any():
        return dict(report, status="INVALID_TIMESTAMPS" if times.isna().all() else "OUTSIDE_RANGE"), empty
    bars = bars.loc[matched, list(FIELDS.values())].copy()
    bars["time"] = times.loc[matched].dt.tz_localize("Asia/Ho_Chi_Minh")
    for column in ("open", "high", "low", "close", "volume"):
        bars[column] = pd.to_numeric(bars[column], errors="coerce")
    bars = bars.assign(symbol=symbol, source="kbs", interval=interval).sort_values("time")
    return dict(report, status="MATCHED_DATES"), bars.reset_index(drop=True)
