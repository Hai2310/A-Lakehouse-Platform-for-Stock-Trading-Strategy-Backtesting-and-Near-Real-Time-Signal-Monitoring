"""Read 5-minute bars from the public chart endpoint used by quant-agent."""

from datetime import date, timedelta
import re

import pandas as pd
import requests

CHART_URL = "https://services.entrade.com.vn/chart-api/v2/ohlcs/stock"
TIMEZONE = "Asia/Ho_Chi_Minh"
FIELDS = dict(t="time", o="open", h="high", l="low", c="close", v="volume")


def ohlcv_5m(symbol: str, start: str, end: str) -> pd.DataFrame:
    """Return OHLCV in source units, with Vietnam-time timestamps."""
    symbol = symbol.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{1,15}", symbol):
        raise ValueError("Invalid stock symbol")
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if last < first:
        raise ValueError("End date must be on or after start date")
    lower = pd.Timestamp(first, tz=TIMEZONE)
    upper = pd.Timestamp(last + timedelta(days=1), tz=TIMEZONE)

    response = requests.get(
        CHART_URL,
        params={"symbol": symbol, "resolution": "5", "from": int(lower.timestamp()),
                "to": int(upper.timestamp()) - 1},
        headers={"Accept": "application/json"},
        timeout=(5, 15),
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or not FIELDS.keys() <= payload.keys():
        raise ValueError("Unexpected Entrade OHLCV response")
    values = {name: payload[key] for key, name in FIELDS.items()}
    if all(value is None for value in values.values()):
        values = {name: [] for name in values}
    if not all(isinstance(value, list) for value in values.values()):
        raise ValueError("Unexpected Entrade OHLCV columns")
    bars = pd.DataFrame(values)
    bars["time"] = pd.to_datetime(bars["time"], unit="s", utc=True, errors="raise").dt.tz_convert(TIMEZONE)
    for column in ("open", "high", "low", "close", "volume"):
        bars[column] = pd.to_numeric(bars[column], errors="coerce")
    bars = bars.loc[bars["time"].ge(lower) & bars["time"].lt(upper)]
    return bars.assign(symbol=symbol, source="entrade_chart", interval="5m").sort_values("time").reset_index(drop=True)
