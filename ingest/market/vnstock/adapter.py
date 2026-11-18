"""Vnstock adapter for VCI/KBS reference and market data."""

from __future__ import annotations

import pandas as pd
from vnstock import Market, Reference

from transform.silver.normalization import normalize_ohlcv


class VnstockAdapter:
    def __init__(self) -> None:
        self.market = Market(show_log=False)
        self.reference = Reference()

    def securities(self, source: str) -> pd.DataFrame:
        equities = self.reference.equity.list_by_exchange(source=source.lower()).copy()
        equities["asset"] = "equity"
        futures = self.reference.futures.list(source=source.lower()).copy()
        futures["asset"] = "futures"
        return pd.concat([equities, futures], ignore_index=True, sort=False)

    def index_list(self) -> pd.DataFrame:
        # Vnstock currently routes the index catalogue through its built-in source.
        return self.reference.index.list()

    def index_members(self, symbol: str, source: str) -> pd.DataFrame:
        frame = self.reference.index(symbol).members(source=source.lower()).copy()
        frame["index_symbol"] = symbol.upper()
        return frame

    def security_details(self, symbol: str, asset: str, source: str) -> pd.DataFrame:
        if asset == "equity":
            frame = self.reference.company(symbol).info(source=source.lower()).copy()
        elif asset == "futures":
            frame = self.reference.futures(symbol).info().copy()
        else:
            raise ValueError(f"Unsupported security asset: {asset}")
        if not frame.empty and "symbol" not in frame.columns:
            frame["symbol"] = symbol.upper()
        return frame

    def ohlcv(
        self,
        asset: str,
        symbol: str,
        source: str,
        interval: str,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        if asset == "equity":
            market_asset = self.market.equity(symbol)
        elif asset == "index":
            market_asset = self.market.index(symbol)
        elif asset == "futures":
            market_asset = self.market.future(symbol)
        else:
            raise ValueError(f"Unsupported market asset: {asset}")
        frame = market_asset.ohlcv(
            start=start,
            end=end,
            interval=interval,
            source=source.lower(),
        )
        return normalize_ohlcv(frame)

    def stock_price(self, symbols: list[str], source: str) -> pd.DataFrame:
        frame = self.market.price_board(symbols_list=symbols, source=source.lower()).copy()
        if isinstance(frame.columns, pd.MultiIndex):
            flat_columns = [
                "_".join(
                    str(part).strip()
                    for part in column
                    if str(part).strip() and not str(part).startswith("Unnamed")
                ) or "column"
                for column in frame.columns
            ]
            counts: dict[str, int] = {}
            for position, name in enumerate(flat_columns):
                counts[name] = counts.get(name, 0) + 1
                if counts[name] > 1:
                    flat_columns[position] = f"{name}_{counts[name]}"
            frame.columns = flat_columns
        if not frame.empty:
            frame["snapshot_date"] = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").date().isoformat()
        return frame
