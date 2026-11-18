"""MVP-IntradayMomentum CSV adapter for VN30F1M minute data."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from transform.silver.normalization import filter_dates, normalize_ohlcv, resample_ohlcv


class MVPAdapter:
    def load_minute_data(
        self, csv_path: str | Path, start: str | None = None, end: str | None = None
    ) -> pd.DataFrame:
        path = Path(csv_path)
        if not path.is_file():
            raise FileNotFoundError(f"MVP minute CSV not found: {path}")
        frame = normalize_ohlcv(pd.read_csv(path))
        frame = filter_dates(frame, start, end)
        if frame.empty:
            raise ValueError(f"MVP CSV contains no rows between {start} and {end}.")
        return frame

    def daily_and_intraday(
        self, csv_path: str | Path, start: str | None = None, end: str | None = None
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        minute_data = self.load_minute_data(csv_path, start, end)
        return (
            resample_ohlcv(minute_data, "1D"),
            resample_ohlcv(minute_data, "5m"),
        )
