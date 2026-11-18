"""XNO API adapter. It is imported lazily because XNO has optional chart dependencies."""

from __future__ import annotations

import os

import pandas as pd

from transform.silver.normalization import filter_dates, normalize_ohlcv, resample_ohlcv


class XNOAdapter:
    def __init__(self) -> None:
        self.api_key = os.getenv("XNO_API_KEY", "").strip()
        self._api = None
        self._history_cache: dict[tuple[str, str], pd.DataFrame] = {}

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    def _load_api(self):
        if not self.configured:
            raise RuntimeError("XNO_API_KEY is empty; add it to the ignored .env file.")
        if self._api is None:
            from xnoapi.vn.data import (
                client,
                get_derivatives_hist,
                get_indices,
                get_stock_hist,
                get_stock_info,
                get_stock_top_price,
                list_liquid_asset,
            )

            client(apikey=self.api_key, verify=False)
            self._api = {
                "derivatives": get_derivatives_hist,
                "indices": get_indices,
                "stock_history": get_stock_hist,
                "stock_info": get_stock_info,
                "stock_top_price": get_stock_top_price,
                "liquid_assets": list_liquid_asset,
            }
        return self._api

    def securities(self) -> pd.DataFrame:
        return self._load_api()["liquid_assets"]()

    def index_list(self) -> pd.DataFrame:
        return self._load_api()["indices"]()

    def security_details(self, symbol: str) -> pd.DataFrame:
        return self._load_api()["stock_info"](symbol.upper())

    def stock_price(self, symbols: list[str]) -> pd.DataFrame:
        api = self._load_api()
        frames = [api["stock_top_price"](symbol.upper()) for symbol in symbols]
        frames = [frame for frame in frames if frame is not None and not frame.empty]
        return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()

    def ohlcv(
        self,
        asset: str,
        symbol: str,
        interval: str,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        api = self._load_api()
        if asset == "futures":
            raw = api["derivatives"](symbol.upper(), interval)
            if isinstance(raw, dict) and raw.get("error"):
                raise RuntimeError(str(raw["error"]))
            frame = normalize_ohlcv(raw)
        elif asset == "equity":
            key = symbol.upper()
            resolution = "m" if interval == "5m" else "h"
            cache_key = (key, resolution)
            if cache_key not in self._history_cache:
                raw = api["stock_history"](key, resolution=resolution)
                self._history_cache[cache_key] = normalize_ohlcv(raw)
            frame = resample_ohlcv(self._history_cache[cache_key], interval)
        else:
            raise ValueError("XNO OHLCV supports equity and futures assets only.")
        return filter_dates(frame, start, end)
