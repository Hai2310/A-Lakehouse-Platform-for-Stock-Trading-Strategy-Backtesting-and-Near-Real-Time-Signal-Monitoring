"""Vnstock-specific fetch orchestration."""

from __future__ import annotations

import time

from ingest.market.common.runner import attempt, skip, table_for
from ingest.market.lake import ParquetLake
from ingest.market.vnstock.adapter import VnstockAdapter


def run_index_catalog(lake: ParquetLake, adapter: VnstockAdapter) -> None:
    attempt(
        lake,
        table="index_list",
        source="builtin",
        fetch=adapter.index_list,
        note="Vnstock index catalogue from the built-in reference source.",
    )


def run_reference(
    lake: ParquetLake,
    adapter: VnstockAdapter,
    source: str,
    symbols: list[str],
    futures: list[str],
    indices: list[str],
    pause: float,
) -> None:
    attempt(
        lake,
        table="securities",
        source=source,
        fetch=lambda: adapter.securities(source),
        note="Equity listings and currently listed index futures.",
    )
    time.sleep(pause)

    for index_symbol in indices:
        attempt(
            lake,
            table="index_components",
            source=source,
            fetch=lambda index_symbol=index_symbol: adapter.index_members(index_symbol, source),
            asset="index",
            symbol=index_symbol,
            note="Current index members; constituent availability depends on the index/source.",
        )
        time.sleep(pause)

    for symbol in symbols:
        attempt(
            lake,
            table="securities_details",
            source=source,
            fetch=lambda symbol=symbol: adapter.security_details(symbol, "equity", source),
            asset="equity",
            symbol=symbol,
        )
        time.sleep(pause)

    for symbol in futures:
        skip(
            lake,
            "securities_details",
            "vnstock",
            f"Detailed futures profile for {symbol} requires a Vnstock tier that exposes future_profile; contract listings are in securities.",
        )


def run_history(
    lake: ParquetLake,
    adapter: VnstockAdapter,
    source: str,
    symbols_by_asset: dict[str, list[str]],
    intervals: list[str],
    start: str,
    end: str,
    pause: float,
) -> None:
    for asset, symbols in symbols_by_asset.items():
        for symbol in symbols:
            for interval in intervals:
                attempt(
                    lake,
                    table=table_for(asset, interval),
                    source=source,
                    fetch=lambda asset=asset, symbol=symbol, interval=interval: adapter.ohlcv(
                        asset, symbol, source, interval, start, end
                    ),
                    asset=asset,
                    symbol=symbol,
                    interval=interval,
                    start=start,
                    end=end,
                )
                time.sleep(pause)


def run_quotes(
    lake: ParquetLake,
    adapter: VnstockAdapter,
    source: str,
    symbols: list[str],
) -> None:
    attempt(
        lake,
        table="daily_stock_price",
        source=source,
        fetch=lambda: adapter.stock_price(symbols, source),
        asset="equity",
        note="Current price-board snapshot; this is not a historical daily table.",
    )
