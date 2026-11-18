"""MVP CSV-specific ingestion routine."""

from __future__ import annotations

from pathlib import Path

from ingest.market.common.runner import skip
from ingest.market.lake import PROJECT_ROOT, ParquetLake
from ingest.market.mvp.adapter import MVPAdapter


def run_csv(
    lake: ParquetLake,
    csv_path: str | Path,
    start: str | None,
    end: str | None,
) -> None:
    path = Path(csv_path).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.is_file():
        skip(lake, "intraday_ohlc", "mvp", f"Minute CSV is not present: {path}")
        skip(lake, "daily_ohlc", "mvp", f"Minute CSV is not present: {path}")
        return

    try:
        daily, intraday = MVPAdapter().daily_and_intraday(path, start, end)
        lake.write(
            "daily_ohlc",
            daily,
            source="mvp",
            asset="futures",
            symbol="VN30F1M",
            interval="1D",
            start=start or "",
            end=end or "",
            note=f"Derived from 1-minute CSV {path.name}.",
        )
        lake.write(
            "intraday_ohlc",
            intraday,
            source="mvp",
            asset="futures",
            symbol="VN30F1M",
            interval="5m",
            start=start or "",
            end=end or "",
            note="Derived from 1-minute CSV; source_rows and is_complete_window identify short windows.",
        )
        print(f"[OK] MVP VN30F1M daily={len(daily)} intraday_5m={len(intraday)}")
    except Exception as exc:
        message = lake._redact(f"{type(exc).__name__}: {exc}")
        lake.record(
            table="intraday_ohlc",
            source="mvp",
            status="FAILED",
            asset="futures",
            symbol="VN30F1M",
            interval="5m",
            start=start or "",
            end=end or "",
            note=message,
        )
        print(f"[FAILED] MVP VN30F1M: {message}")
