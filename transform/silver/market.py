"""Build local Silver snapshots from Bronze Parquet and record data-quality results."""

from __future__ import annotations

import argparse
import csv
import os
import re
from datetime import datetime
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv

from ingest.market.lake import MARKET_TABLES, PROJECT_ROOT


TZ = ZoneInfo("Asia/Ho_Chi_Minh")
QUALITY_FIELDS = [
    "run_id",
    "processed_at",
    "table",
    "bronze_files",
    "input_rows",
    "accepted_rows",
    "rejected_rows",
    "duplicate_rows",
    "status",
    "silver_path",
    "quarantine_path",
    "note",
]
OHLC_TABLES = {"daily_ohlc", "intraday_ohlc", "daily_index", "intraday_index"}

# Silver keeps vendor-specific columns but maps commonly used names to a stable
# snake_case schema. Table-specific keys are declared below.
ALIASES = {
    "ticker": "symbol",
    "ticker_symbol": "symbol",
    "stock_code": "symbol",
    "security_code": "symbol",
    "symbol_code": "symbol",
    "code": "symbol",
    "trading_symbol": "symbol",
    "timestamp": "time",
    "datetime": "time",
    "date_time": "time",
    "trading_datetime": "time",
    "trade_datetime": "time",
    "trading_date": "time",
    "trade_date": "time",
    "bar_time": "time",
    "open_price": "open",
    "openprice": "open",
    "high_price": "high",
    "highprice": "high",
    "low_price": "low",
    "lowprice": "low",
    "close_price": "close",
    "closeprice": "close",
    "vol": "volume",
    "trading_volume": "volume",
    "matched_volume": "volume",
    "match_volume": "volume",
    "data_source": "source",
    "asset_type": "asset",
    "timeframe": "interval",
    "resolution": "interval",
}

TABLE_RULES: dict[str, dict[str, list[str]]] = {
    "securities": {"keys": ["source", "asset", "symbol"], "required": ["source", "asset", "symbol"]},
    "securities_details": {"keys": ["source", "symbol"], "required": ["source", "symbol"]},
    "index_list": {"keys": ["source", "symbol"], "required": ["source", "symbol"]},
    "index_components": {
        "keys": ["source", "index_symbol", "symbol"],
        "required": ["source", "index_symbol", "symbol"],
    },
    "daily_stock_price": {
        "keys": ["source", "symbol", "snapshot_date"],
        "required": ["source", "symbol", "snapshot_date"],
    },
}
for _table in OHLC_TABLES:
    TABLE_RULES[_table] = {
        "keys": ["source", "asset", "symbol", "interval", "time"],
        "required": ["source", "asset", "symbol", "interval", "time", "open", "high", "low", "close", "volume"],
    }


def _column_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_") or "column"


def _canonicalize(frame: pd.DataFrame, table: str) -> pd.DataFrame:
    data = frame.copy()
    if isinstance(data.columns, pd.MultiIndex):
        names = []
        for column in data.columns:
            names.append("_".join(str(part).strip() for part in column if str(part).strip()))
        data.columns = names

    columns: list[str] = []
    counts: dict[str, int] = {}
    for original in data.columns:
        name = _column_key(original)
        if name in ALIASES:
            name = ALIASES[name]
        if table == "index_list" and name in {"index", "index_code", "index_symbol"}:
            name = "symbol"
        elif table == "index_components" and name in {"index", "index_code"}:
            name = "index_symbol"
        counts[name] = counts.get(name, 0) + 1
        columns.append(name if counts[name] == 1 else f"{name}_{counts[name]}")
    data.columns = columns

    # xnoapi's liquid-asset list currently returns two unnamed columns (ticker,
    # liquidity); preserve the second value while giving the ticker a useful name.
    if table == "securities" and "symbol" not in data.columns and "0" in data.columns:
        data = data.rename(columns={"0": "symbol"})
        if "1" in data.columns:
            data = data.rename(columns={"1": "liquidity_metric"})

    for column in ("symbol", "index_symbol", "source", "asset", "interval"):
        if column in data.columns:
            data[column] = data[column].astype("string").str.strip()
    if "symbol" in data.columns:
        data["symbol"] = data["symbol"].str.upper()
    if "index_symbol" in data.columns:
        data["index_symbol"] = data["index_symbol"].str.upper()
    if "source" in data.columns:
        data["source"] = data["source"].str.lower()
    if "asset" in data.columns:
        data["asset"] = data["asset"].str.lower()
    if "interval" in data.columns:
        data["interval"] = data["interval"].replace({"1d": "1D", "1D": "1D", "5M": "5m"})

    if table == "securities" and "asset" not in data.columns:
        data["asset"] = "equity"
    if table == "daily_stock_price" and "snapshot_date" not in data.columns:
        if "ingested_at" in data.columns:
            data["snapshot_date"] = pd.to_datetime(data["ingested_at"], errors="coerce").dt.strftime("%Y-%m-%d")
    if table == "daily_stock_price" and "snapshot_date" in data.columns:
        data["snapshot_date"] = pd.to_datetime(data["snapshot_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return data


def _localize_times(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    if isinstance(parsed.dtype, pd.DatetimeTZDtype):
        return parsed.dt.tz_convert(TZ)
    return parsed.dt.tz_localize(TZ, ambiguous="NaT", nonexistent="NaT")


def _add_reason(reasons: pd.Series, mask: pd.Series, code: str) -> None:
    mask = mask.fillna(False)
    if not mask.any():
        return
    current = reasons.loc[mask]
    reasons.loc[mask] = current.mask(current.eq(""), code).where(current.eq(""), current + "|" + code)


def _validate_rows(frame: pd.DataFrame, table: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    rules = TABLE_RULES[table]
    missing_columns = [column for column in rules["required"] if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

    data = frame.copy().reset_index(drop=True)
    reasons = pd.Series("", index=data.index, dtype="string")
    for column in rules["required"]:
        missing = data[column].isna()
        if pd.api.types.is_string_dtype(data[column].dtype) or data[column].dtype == object:
            missing |= data[column].astype("string").str.strip().eq("").fillna(False)
        _add_reason(reasons, missing, f"missing_{column}")

    if table in OHLC_TABLES:
        data["time"] = _localize_times(data["time"])
        _add_reason(reasons, data["time"].isna(), "invalid_time")
        for column in ("open", "high", "low", "close", "volume"):
            data[column] = pd.to_numeric(data[column], errors="coerce")
            _add_reason(reasons, data[column].isna(), f"invalid_{column}")
            _add_reason(
                reasons,
                data[column].isin([float("inf"), -float("inf")]),
                f"non_finite_{column}",
            )
        for column in ("open", "high", "low", "close"):
            _add_reason(reasons, data[column].le(0), f"non_positive_{column}")
        _add_reason(reasons, data["volume"].lt(0), "negative_volume")
        _add_reason(reasons, data["high"].lt(data["low"]), "high_below_low")
        _add_reason(reasons, data["open"].gt(data["high"]) | data["open"].lt(data["low"]), "open_outside_range")
        _add_reason(reasons, data["close"].gt(data["high"]) | data["close"].lt(data["low"]), "close_outside_range")
        if "trade_count" in data.columns:
            data["trade_count"] = pd.to_numeric(data["trade_count"], errors="coerce")
            _add_reason(reasons, data["trade_count"].isna(), "invalid_trade_count")
            _add_reason(
                reasons,
                data["trade_count"].isin([float("inf"), -float("inf")]),
                "non_finite_trade_count",
            )
            _add_reason(reasons, data["trade_count"].lt(0), "negative_trade_count")

    rejected_mask = reasons.ne("")
    rejected = data.loc[rejected_mask].copy()
    rejected["rejection_reason"] = reasons.loc[rejected_mask].to_numpy()
    accepted = data.loc[~rejected_mask].copy()

    key_columns = rules["keys"]
    order_column = f"_silver_order_{uuid4().hex}"
    accepted[order_column] = range(len(accepted))
    sort_columns = [order_column]
    if "ingested_at" in accepted.columns:
        ingest_sort_column = f"_silver_ingested_at_{uuid4().hex}"
        accepted[ingest_sort_column] = pd.to_datetime(
            accepted["ingested_at"], errors="coerce", utc=True
        )
        sort_columns = [ingest_sort_column, order_column]
    ordered = accepted.sort_values(sort_columns, kind="stable", na_position="first")
    duplicate_mask = ordered.duplicated(subset=key_columns, keep="last")
    duplicates = ordered.loc[duplicate_mask].copy()
    if not duplicates.empty:
        duplicates["rejection_reason"] = "duplicate_key"
    accepted = ordered.loc[~duplicate_mask].copy().sort_values(order_column, kind="stable")
    helper_columns = [column for column in accepted.columns if column.startswith("_silver_")]
    accepted = accepted.drop(columns=helper_columns)
    duplicates = duplicates.drop(
        columns=[column for column in helper_columns if column in duplicates.columns]
    )
    rejected = pd.concat([rejected, duplicates], ignore_index=True, sort=False)
    return accepted.reset_index(drop=True), rejected.reset_index(drop=True)


def _resolve_path(path: str | Path) -> Path:
    result = Path(path).expanduser()
    return result if result.is_absolute() else PROJECT_ROOT / result


def _append_quality_report(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=QUALITY_FIELDS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow({field: record.get(field, "") for field in QUALITY_FIELDS})


def _write_snapshot(data: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.stem}-{uuid4().hex}.tmp.parquet")
    try:
        data.to_parquet(temporary, index=False)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def run_silver(
    *,
    bronze_root: str | Path | None = None,
    silver_root: str | Path = "data/silver/market",
    quarantine_root: str | Path = "data/quarantine/market",
    quality_report: str | Path = "data/reports/silver_quality.csv",
    tables: list[str] | None = None,
) -> list[dict[str, object]]:
    """Rebuild selected local Silver table snapshots from all matching Bronze files."""
    load_dotenv(PROJECT_ROOT / ".env")
    configured_bronze = bronze_root or os.getenv("MARKET_DATA_DIR", "data/bronze/market")
    bronze = _resolve_path(configured_bronze)
    silver = _resolve_path(silver_root)
    quarantine = _resolve_path(quarantine_root)
    report_path = _resolve_path(quality_report)
    selected = tables or list(MARKET_TABLES)
    unknown = sorted(set(selected) - set(MARKET_TABLES))
    if unknown:
        raise ValueError(f"Unknown table(s): {', '.join(unknown)}")

    run_id = datetime.now(TZ).strftime("%Y%m%dT%H%M%S%f")
    summaries: list[dict[str, object]] = []
    for table in selected:
        files = sorted((bronze / table).rglob("*.parquet")) if (bronze / table).exists() else []
        summary: dict[str, object] = {
            "run_id": run_id,
            "processed_at": datetime.now(TZ).isoformat(),
            "table": table,
            "bronze_files": len(files),
            "input_rows": 0,
            "accepted_rows": 0,
            "rejected_rows": 0,
            "duplicate_rows": 0,
            "status": "NO_DATA",
            "silver_path": "",
            "quarantine_path": "",
            "note": "No Bronze Parquet files found.",
        }
        if not files:
            _append_quality_report(report_path, summary)
            summaries.append(summary)
            print(f"[NO_DATA] {table}: no Bronze Parquet files")
            continue

        frames: list[pd.DataFrame] = []
        read_errors: list[str] = []
        for path in files:
            try:
                frames.append(pd.read_parquet(path))
            except Exception as exc:
                read_errors.append(f"{path.name}: {type(exc).__name__}: {exc}")
        if not frames:
            summary.update(status="READ_FAILED", note="; ".join(read_errors))
            _append_quality_report(report_path, summary)
            summaries.append(summary)
            print(f"[READ_FAILED] {table}: could not read Bronze Parquet")
            continue

        bronze_data = pd.concat(frames, ignore_index=True, sort=False)
        summary["input_rows"] = len(bronze_data)
        try:
            normalized = _canonicalize(bronze_data, table)
            accepted, rejected = _validate_rows(normalized, table)
        except Exception as exc:
            summary.update(status="SCHEMA_ERROR", note=f"{type(exc).__name__}: {exc}")
            if read_errors:
                summary["note"] += f"; unreadable files: {len(read_errors)}"
            _append_quality_report(report_path, summary)
            summaries.append(summary)
            print(f"[SCHEMA_ERROR] {table}: {exc}")
            continue

        duplicate_rows = int(rejected["rejection_reason"].eq("duplicate_key").sum()) if not rejected.empty else 0
        rejected_rows = len(rejected) - duplicate_rows
        summary["accepted_rows"] = len(accepted)
        summary["rejected_rows"] = rejected_rows
        summary["duplicate_rows"] = duplicate_rows

        if not rejected.empty:
            quarantine_path = quarantine / table / f"run={run_id}" / "rejected.parquet"
            _write_snapshot(rejected, quarantine_path)
            summary["quarantine_path"] = str(quarantine_path)

        if not accepted.empty:
            silver_path = silver / table / "data.parquet"
            _write_snapshot(accepted, silver_path)
            summary["silver_path"] = str(silver_path)
        else:
            summary["note"] = "No valid rows remained; existing Silver snapshot was left untouched."

        notes = []
        if rejected_rows:
            notes.append(f"{rejected_rows} rows failed row-level quality checks")
        if duplicate_rows:
            notes.append(f"{duplicate_rows} duplicate keys removed")
        if read_errors:
            notes.append(f"{len(read_errors)} unreadable Bronze file(s)")
        if not notes:
            notes.append("All rows passed quality checks.")
        summary["note"] = "; ".join(notes)
        summary["status"] = "WARN" if rejected_rows or duplicate_rows or read_errors else "PASS"
        _append_quality_report(report_path, summary)
        summaries.append(summary)
        print(
            f"[{summary['status']}] {table}: input={summary['input_rows']} "
            f"accepted={summary['accepted_rows']} rejected={rejected_rows} duplicates={duplicate_rows}"
        )
    print(f"Silver quality report: {report_path}")
    return summaries


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-root", help="Bronze root (defaults to MARKET_DATA_DIR or data/bronze/market)")
    parser.add_argument("--silver-root", default="data/silver/market")
    parser.add_argument("--quarantine-root", default="data/quarantine/market")
    parser.add_argument("--quality-report", default="data/reports/silver_quality.csv")
    parser.add_argument("--tables", help="Comma-separated table names; defaults to every market table")
    args = parser.parse_args()
    selected = [item.strip() for item in args.tables.split(",") if item.strip()] if args.tables else None
    run_silver(
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        quarantine_root=args.quarantine_root,
        quality_report=args.quality_report,
        tables=selected,
    )


if __name__ == "__main__":
    main()
