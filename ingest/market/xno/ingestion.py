"""XNO-specific fetch orchestration."""

from __future__ import annotations

import time

from ingest.market.common.runner import attempt, skip, table_for
from ingest.market.lake import ParquetLake
from ingest.market.xno.adapter import XNOAdapter


def run_reference(lake: ParquetLake, adapter: XNOAdapter, symbols: list[str]) -> None:
    if not adapter.configured:
        for table in ("securities", "index_list", "index_components"):
            skip(lake, table, "xno", "XNO_API_KEY is empty.")
        for symbol in symbols:
            skip(
                lake,
                "securities_details",
                "xno",
                f"XNO_API_KEY is empty; details for {symbol} were not requested.",
            )
        return

    attempt(lake, table="securities", source="xno", fetch=adapter.securities)
    attempt(lake, table="index_list", source="xno", fetch=adapter.index_list)
    skip(
        lake,
        "index_components",
        "xno",
        "The installed XNO API exposes the index catalogue but not constituent membership.",
    )
    for symbol in symbols:
        attempt(
            lake,
            table="securities_details",
            source="xno",
            fetch=lambda symbol=symbol: adapter.security_details(symbol),
            asset="equity",
            symbol=symbol,
        )


def run_history(
    lake: ParquetLake,
    adapter: XNOAdapter,
    symbols_by_asset: dict[str, list[str]],
    intervals: list[str],
    start: str,
    end: str,
    pause: float,
) -> None:
    if not adapter.configured:
        for asset, symbols in symbols_by_asset.items():
            for interval in intervals:
                skip(
                    lake,
                    table_for(asset, interval),
                    "xno",
                    f"XNO_API_KEY is empty; {asset} history was not requested.",
                )
        return

    for asset, symbols in symbols_by_asset.items():
        if asset == "index":
            for interval in intervals:
                skip(
                    lake,
                    table_for(asset, interval),
                    "xno",
                    "The installed XNO API has no historical OHLCV route for indices.",
                )
            continue
        for symbol in symbols:
            for interval in intervals:
                attempt(
                    lake,
                    table=table_for(asset, interval),
                    source="xno",
                    fetch=lambda asset=asset, symbol=symbol, interval=interval: adapter.ohlcv(
                        asset, symbol, interval, start, end
                    ),
                    asset=asset,
                    symbol=symbol,
                    interval=interval,
                    start=start,
                    end=end,
                    note=(
                        "XNO stock history uses minute bars for 5m and hourly bars for 1D, then filters dates."
                        if asset == "equity"
                        else "XNO derivative history uses the requested interval."
                    ),
                )
                time.sleep(pause)


def run_quotes(lake: ParquetLake, adapter: XNOAdapter, symbols: list[str]) -> None:
    if not adapter.configured:
        skip(lake, "daily_stock_price", "xno", "XNO_API_KEY is empty.")
        return
    attempt(
        lake,
        table="daily_stock_price",
        source="xno",
        fetch=lambda: adapter.stock_price(symbols),
        asset="equity",
        note="Current XNO top-price snapshot where the endpoint returns data.",
    )
