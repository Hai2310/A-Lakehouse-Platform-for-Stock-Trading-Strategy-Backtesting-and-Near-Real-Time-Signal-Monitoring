"""Local Parquet writer, optional GCS mirror, and ingestion coverage report."""

from __future__ import annotations

import csv
import os
import re
from datetime import datetime
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv

from ingest.storage.gcs import GCSStore


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TZ = ZoneInfo("Asia/Ho_Chi_Minh")
REPORT_FIELDS = [
    "recorded_at",
    "table",
    "source",
    "asset",
    "symbol",
    "interval",
    "requested_start",
    "requested_end",
    "actual_start",
    "actual_end",
    "rows",
    "status",
    "local_path",
    "gcs_uri",
    "note",
]
MARKET_TABLES = (
    "securities",
    "securities_details",
    "index_list",
    "index_components",
    "daily_ohlc",
    "intraday_ohlc",
    "daily_index",
    "intraday_index",
    "daily_stock_price",
)


def _slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value.strip()).strip("_").lower()


class ParquetLake:
    def __init__(
        self, *, data_root: str | Path | None = None, upload_gcs: bool = False,
        report_path: Path | None = None,
    ) -> None:
        load_dotenv(PROJECT_ROOT / ".env")
        configured_root = data_root or os.getenv("MARKET_DATA_DIR", "data/bronze/market")
        root = Path(configured_root).expanduser()
        self.data_root = root if root.is_absolute() else PROJECT_ROOT / root
        for table in MARKET_TABLES:
            (self.data_root / table).mkdir(parents=True, exist_ok=True)
        self.report_path = self.data_root / report_path if report_path else PROJECT_ROOT / "data" / "reports" / "coverage.csv"
        self.gcs = GCSStore() if upload_gcs else None

    def _redact(self, value: object) -> str:
        text = str(value)
        for key in ("XNO_API_KEY", "DNSE_API_KEY", "DNSE_API_SECRET"):
            secret = os.getenv(key, "")
            if secret:
                text = text.replace(secret, "[REDACTED]")
        return text

    def _append_report(self, record: dict[str, object]) -> None:
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        exists = self.report_path.exists()
        with self.report_path.open("a", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=REPORT_FIELDS, extrasaction="ignore")
            if not exists:
                writer.writeheader()
            writer.writerow({field: record.get(field, "") for field in REPORT_FIELDS})

    def record(
        self,
        *,
        table: str,
        source: str,
        status: str,
        asset: str = "",
        symbol: str = "",
        interval: str = "",
        start: str = "",
        end: str = "",
        rows: int = 0,
        local_path: str = "",
        gcs_uri: str = "",
        note: str = "",
    ) -> None:
        self._append_report(
            {
                "recorded_at": datetime.now(TZ).isoformat(),
                "table": table,
                "source": source.lower(),
                "asset": asset,
                "symbol": symbol,
                "interval": interval,
                "requested_start": start,
                "requested_end": end,
                "rows": rows,
                "status": status,
                "local_path": local_path,
                "gcs_uri": gcs_uri,
                "note": self._redact(note),
            }
        )

    @staticmethod
    def _actual_range(frame: pd.DataFrame) -> tuple[str, str]:
        for column in ("time", "timestamp", "datetime", "date", "trading_date"):
            if column in frame.columns:
                values = frame[column].dropna()
                if not values.empty:
                    return str(values.min()), str(values.max())
        return "", ""

    def write(
        self,
        table: str,
        frame: pd.DataFrame | None,
        *,
        source: str,
        asset: str = "",
        symbol: str = "",
        interval: str = "",
        start: str = "",
        end: str = "",
        note: str = "",
        batch_key: str = "",
        status: str = "OK",
    ) -> Path | None:
        if frame is None or frame.empty:
            self.record(
                table=table,
                source=source,
                status="EMPTY",
                asset=asset,
                symbol=symbol,
                interval=interval,
                start=start,
                end=end,
                note=note or "Source returned no rows.",
            )
            return None

        data = frame.copy()
        if isinstance(data.columns, pd.MultiIndex):
            flattened = []
            counts: dict[str, int] = {}
            for column in data.columns:
                name = "_".join(
                    str(part).strip()
                    for part in column
                    if str(part).strip() and not str(part).startswith("Unnamed")
                ) or "column"
                counts[name] = counts.get(name, 0) + 1
                flattened.append(name if counts[name] == 1 else f"{name}_{counts[name]}")
            data.columns = flattened
        else:
            data.columns = [str(column) for column in data.columns]
        if symbol and "symbol" not in data.columns:
            data["symbol"] = symbol.upper()
        data["source"] = source.lower()
        data["table"] = table
        if asset and "asset" not in data.columns:
            data["asset"] = asset
        if interval and "interval" not in data.columns:
            data["interval"] = interval
        data["ingested_at"] = datetime.now(TZ).isoformat()

        partition = self.data_root / table / f"source={_slug(source)}"
        if asset:
            partition /= f"asset={_slug(asset)}"
        if interval:
            partition /= f"interval={_slug(interval)}"
        if batch_key:
            partition /= f"request={_slug(batch_key)}"
            filename = "part.parquet"
        else:
            partition /= f"ingestion_date={datetime.now(TZ).date().isoformat()}"
            filename = f"part-{datetime.now(TZ).strftime('%H%M%S%f')}-{uuid4().hex[:8]}.parquet"
        partition.mkdir(parents=True, exist_ok=True)
        output_path = partition / filename
        temporary = partition / f".{uuid4().hex}.tmp"
        try:
            data.to_parquet(temporary, index=False)
            temporary.replace(output_path)
        finally:
            temporary.unlink(missing_ok=True)

        gcs_uri = ""
        if self.gcs is not None:
            relative = output_path.relative_to(self.data_root).as_posix()
            gcs_uri = self.gcs.upload_file(output_path, f"market/{relative}")

        actual_start, actual_end = self._actual_range(data)
        self._append_report(
            {
                "recorded_at": datetime.now(TZ).isoformat(),
                "table": table,
                "source": source.lower(),
                "asset": asset,
                "symbol": symbol,
                "interval": interval,
                "requested_start": start,
                "requested_end": end,
                "actual_start": actual_start,
                "actual_end": actual_end,
                "rows": len(data),
                "status": status,
                "local_path": str(output_path),
                "gcs_uri": gcs_uri,
                "note": self._redact(note),
            }
        )
        return output_path
