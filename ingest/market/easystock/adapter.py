"""Read daily OHLCV from the public EasyStock downloads page."""

import re

import pandas as pd
import requests

BASE_URL = "https://easystock.vn/stock-data/"


def _read_json(filename):
    response = requests.get(BASE_URL + filename, timeout=(10, 30))
    response.raise_for_status()
    return response.json()


def catalog():
    frame = pd.DataFrame(_read_json("_index.json")).rename(
        columns={"sym": "symbol", "n": "reported_rows", "from": "first_date", "to": "last_date"}
    )
    for column in ("first_date", "last_date"):
        frame[column] = pd.to_datetime(frame[column], format="%Y-%m-%d")
    return frame


def daily(symbol):
    symbol = symbol.strip().upper()
    if not re.fullmatch(r"[A-Z0-9]+", symbol):
        raise ValueError(f"Invalid EasyStock symbol: {symbol!r}")
    frame = pd.DataFrame(_read_json(f"{symbol}.json"))
    if not frame.empty:
        frame["time"] = pd.to_datetime(frame["time"], format="%Y-%m-%d")
    return frame.assign(symbol=symbol, interval="1D", source="easystock")
