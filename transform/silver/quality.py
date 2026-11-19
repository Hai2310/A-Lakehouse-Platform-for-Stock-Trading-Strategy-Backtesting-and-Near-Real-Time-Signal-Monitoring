"""Validate written Silver datasets with Great Expectations before publication."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import great_expectations as gx
from great_expectations import expectations as gxe
from pyspark.sql import SparkSession

from ingest.market.lake import PROJECT_ROOT
from transform.silver.bars import BAR_TABLES, COVERAGE_KEYS, NUMBER_FIELDS, PRICE_FIELDS
from transform.silver.market import TABLE_RULES, _resolve_path

TABLE_KEYS = {
    "securities": ("security_id",),
    "securities_details": ("security_id", "source", "snapshot_kind", "snapshot_id"),
    "daily_stock_price": ("security_id", "source", "snapshot_kind", "snapshot_id"),
    "index_list": ("index_id",),
    "index_components": ("index_id", "security_id", "source", "snapshot_id"),
    "security_symbol_history": ("security_id", "symbol", "exchange", "source", "identity_status"),
    "corporate_actions": ("event_id", "revision_id", "security_id"),
    **{table: tuple(TABLE_RULES[table]["keys"]) for table in BAR_TABLES},
    "trading_calendar": ("exchange", "trading_date", "calendar_version"),
    "data_coverage": ("bar_table", *COVERAGE_KEYS),
}
DOMAINS = {
    "corporate_actions": "vsdc",
    "security_symbol_history": "reference",
    "trading_calendar": "reference",
    "data_coverage": "quality",
}


def table_path(root: Path, table: str) -> Path:
    return root / DOMAINS.get(table, "market") / table


def _expectations(table: str, expected_rows: int, run_id: str):
    keys = TABLE_KEYS[table]
    # Coverage includes observations whose source did not identify the exchange.
    nullable = {"exchange"} if table == "data_coverage" else set()
    required = set(keys) | {"silver_run_id", "schema_version"}
    invalid = [f"`{column}` IS NULL" for column in sorted(required - nullable)]
    invalid.extend([f"silver_run_id <> '{run_id}'", "schema_version <> '1'"])
    if table in BAR_TABLES:
        bar_columns = {*NUMBER_FIELDS, "instrument_id", "trading_date", "bronze_file", "timeframe", "period"}
        required.update(bar_columns)
        invalid.extend(f"`{column}` IS NULL" for column in sorted(bar_columns))
        invalid.extend(f"isnan(`{column}`) OR abs(`{column}`) = double('Infinity')" for column in NUMBER_FIELDS)
        invalid.extend(f"`{column}` <= 0" for column in PRICE_FIELDS)
        invalid.extend([
            "volume < 0", "high < low", "open NOT BETWEEN low AND high", "close NOT BETWEEN low AND high",
            "timeframe <> interval",
        ])
        valid_intervals = "'1m', '5m'" if table.startswith("intraday") else "'1D'"
        invalid.append(f"interval NOT IN ({valid_intervals})")
    if table == "data_coverage":
        required.update(("actual_bars", "first_time_utc", "last_time_utc"))
        invalid.extend(["actual_bars IS NULL OR actual_bars <= 0", "first_time_utc IS NULL",
                        "last_time_utc IS NULL OR first_time_utc > last_time_utc"])

    # Return only a count, even on failure. Spark checks the entire dataset without collecting row data.
    grouping = ", ".join(f"`{column}`" for column in keys)
    condition = " OR ".join(invalid)
    query = (
        "SELECT count(*) AS failing_keys FROM ("
        "SELECT 1 FROM {batch} "
        f"GROUP BY {grouping} HAVING count(*) > 1 OR count_if({condition}) > 0"
        ") AS failures HAVING count(*) > 0"
    )
    return {
        "required_columns": gxe.ExpectTableColumnsToMatchSet(column_set=sorted(required), exact_match=False),
        "row_count": gxe.ExpectTableRowCountToEqual(value=expected_rows),
        "unique_keys_and_valid_values": gxe.UnexpectedRowsExpectation(unexpected_rows_query=query),
    }


def validate_snapshot(spark: SparkSession, snapshot: Path, expected_counts: dict[str, int],
                      run_id: str, report_path: Path) -> None:
    if set(expected_counts) != set(TABLE_KEYS) or any(count < 1 for count in expected_counts.values()):
        raise ValueError("Silver quality report must contain a positive row count for every table")
    if len(run_id) != 24 or any(char not in "0123456789abcdefT-" for char in run_id):
        raise ValueError(f"Invalid Silver run ID: {run_id!r}")

    context = gx.get_context(mode="ephemeral")
    context.enable_analytics(False)
    source = context.data_sources.add_spark(name="silver_parquet", persist=False)
    report = {}
    for table, expected in expected_counts.items():
        asset = source.add_dataframe_asset(name=table)
        definition = asset.add_batch_definition_whole_dataframe("snapshot")
        frame = spark.read.parquet(str(table_path(snapshot, table)))
        checks = _expectations(table, expected, run_id)
        suite = context.suites.add(gx.ExpectationSuite(name=f"{table}_quality", expectations=list(checks.values())))
        validation = context.validation_definitions.add(
            gx.ValidationDefinition(name=f"{table}_validation", data=definition, suite=suite)
        )
        result = validation.run(batch_parameters={"dataframe": frame}, result_format="BASIC")
        checks_report = {}
        for name, check in zip(checks, result.results, strict=True):
            details = check.to_json_dict()
            checks_report[name] = {key: details[key] for key in ("success", "result", "exception_info")}
        report[table] = {"success": result.success, "checks": checks_report}
        print(f"[GX {'PASS' if result.success else 'FAIL'}] {table}", flush=True)

    success = all(table["success"] for table in report.values())
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({"run_id": run_id, "success": success, "tables": report}, indent=2) + "\n",
                           encoding="utf-8")
    if not success:
        failed = ", ".join(name for name, table in report.items() if not table["success"])
        raise ValueError(f"Great Expectations rejected Silver tables {failed}. See {report_path}")


def main() -> None:
    from transform.silver.pipeline import _spark_session

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, default=PROJECT_ROOT / "data/silver/current")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    snapshot = _resolve_path(args.snapshot).resolve(strict=True)
    with (snapshot / "quality_report.csv").open(encoding="utf-8", newline="") as report:
        rows = list(csv.DictReader(report))
    run_id = snapshot.name
    if any(row["run_id"] != run_id for row in rows) or len(rows) != len(TABLE_KEYS):
        raise ValueError("Silver quality report does not match the selected snapshot")
    counts = {row["table"]: int(row["accepted_rows"]) for row in rows}
    report_path = _resolve_path(args.report) if args.report else snapshot / "gx_validation.json"
    spark = _spark_session("local[2]", "1g", 32)
    try:
        validate_snapshot(spark, snapshot, counts, run_id, report_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
