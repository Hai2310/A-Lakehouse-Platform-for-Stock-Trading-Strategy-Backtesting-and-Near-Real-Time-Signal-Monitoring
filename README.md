# A-Lakehouse-Platform-for-Stock-Trading-Strategy-Backtesting-and-Near-Real-Time-Signal-Monitoring

## Google Cloud Storage

The project includes a small GCS helper at `ingest/storage/gcs.py`. It uses
Google Application Default Credentials (ADC), keeping credentials out of the
repository and `.env`.

1. Select the Google Cloud project that owns your bucket:

   ```bash
   gcloud config set project YOUR_PROJECT_ID
   ```

2. Configure local ADC for the Google account that should access GCS:

   ```bash
   gcloud auth application-default login
   ```

3. Add the project and an existing bucket to your ignored `.env` file, using
   `.env.example` as a template:

   ```dotenv
   GOOGLE_CLOUD_PROJECT=your-gcp-project-id
   GCS_BUCKET=your-existing-bucket-name
   ```

4. Install the Python dependencies:

   ```bash
   python -m pip install -r requirement.txt
   ```

The authenticated identity needs permission to access objects in the bucket
(for example, `roles/storage.objectAdmin` for upload, download, and listing).
The bucket must already exist and the Cloud Storage API must be enabled.

Example:

```python
from ingest.storage.gcs import GCSStore

gcs = GCSStore()
uri = gcs.upload_file("data/prices.parquet", "raw/prices.parquet")
print(uri)
```

## Market data ingestion

The MARKET pipeline is run from the repository root and writes local Parquet
files under `data/bronze/market/`. Each output row contains source and ingest
metadata. A run also appends success, empty, skipped, and failed fetches to
`data/reports/coverage.csv`.

Provider code is kept in separate packages: `ingest/market/vnstock/`,
`ingest/market/xno/`, and `ingest/market/mvp/`. Each contains its adapter and
provider-specific ingestion routines. `ingest/market/common/` holds shared
Bronze-writing helpers, while `ingest/market/pipeline.py` parses options and
dispatches work to the providers. Data transformations live beside `ingest/`:
`transform/silver/` contains normalization and quality checks, and
`transform/gold/` is reserved for business-level datasets built from Silver.

| Table | Content | Supported sources |
| --- | --- | --- |
| `securities` | Listed equities and futures | VCI, KBS, XNO |
| `securities_details` | Profiles for requested securities | VCI, KBS, XNO |
| `index_list` | Index catalogue | Vnstock builtin, XNO |
| `index_components` | Current members for requested indices | VCI, KBS |
| `daily_ohlc` | Equity/futures 1D bars | VCI, KBS, XNO, MVP futures CSV |
| `intraday_ohlc` | Equity/futures 5m bars | VCI, KBS, XNO, MVP futures CSV |
| `daily_index` | Index 1D bars | VCI, KBS |
| `intraday_index` | Index 5m bars | VCI, KBS |
| `daily_stock_price` | Current quote/reference-price snapshot, where available | VCI, KBS, XNO |

VCI and KBS use Vnstock's `source` option. The Vnstock index catalogue uses its
built-in reference source. Community-tier Vnstock may restrict detailed futures
profiles; currently listed contracts are still captured in `securities`. XNO
needs `XNO_API_KEY` in the
ignored `.env`; its stock history endpoint returns a full minute/hour series,
which the adapter filters to the requested dates and aggregates to 5m or 1D.
XNO supplies index listings but the installed API has no historical index OHLCV
or index-constituent route, so those coverage entries are marked skipped.
MVP is a supplied 1-minute VN30F1M CSV, not a live API. Download
`vn30f_front_month_1m_clean_raw.csv` from the [MVP project's shared data
folder](https://drive.google.com/drive/folders/1GLcpnhK_eUb9Qnp_T-4Z_VrZWR1185Ta),
save it as `input/mvp/vn30f1m_1m.csv`, or set `MVP_CSV_PATH`. The adapter
preserves a trade-count column when present and flags incomplete 5-minute
windows.

Install the market dependencies (Vnstock is also distributed through its
additional package index):

```bash
python -m pip install --upgrade pip
python -m pip install --extra-index-url https://vnstocks.com/api/packages -r requirement.txt
```

Fetch a small sample for FPT, VN30F1M, and the default indices over the last 14
calendar days. The default source list includes VCI, KBS, XNO, and MVP; sources
without configured credentials or input files are recorded as skipped.

```bash
python -m ingest.market.pipeline --task all --symbols FPT --futures VN30F1M --days 14
```

Useful focused runs:

```bash
python -m ingest.market.pipeline --task history --sources vci,kbs --symbols FPT,HPG --intervals 1D,5m --days 14
python -m ingest.market.pipeline --task xno --symbols FPT --futures VN30F1M --intervals 1D,5m --days 14
python -m ingest.market.pipeline --task mvp --mvp-csv input/mvp/vn30f1m_1m.csv
```

The standalone MVP task imports the full date range in the CSV unless `--start`
or `--end` is provided. The shared dataset currently covers 2021-01-15 through
2026-06-23; a default 14-day `--task all` run does not overlap that range.

To mirror each resulting Parquet file into the configured GCS bucket, add
`--upload-gcs` after completing ADC and setting `GCS_BUCKET` in `.env`.

### Local Silver and data quality

Build Silver from existing Bronze Market and VSDC data with PySpark 4.0.1
and Java 17 or later. Set `JAVA_HOME` if Java is outside your `PATH`.

```bash
source .venv/bin/activate
python -m pip install pyspark==4.0.1 great-expectations==1.25.0
python -m transform.silver.pipeline
```

Spark validates, deduplicates and partitions the four OHLCV tables, then
aggregates coverage and observed trading dates. The small reference tables
and VSDC notice extraction use the existing pandas and PyArrow transforms.
Price datasets are partitioned by timeframe and period (year for daily bars,
month for intraday). The full 13-table snapshot is published under
`data/silver/current/` after the Great Expectations checks and coverage totals pass.
Use `data/silver/current/quality_report.csv` for counts and
`data/quarantine/silver/` for rejected rows and duplicates. The snapshot also
contains `gx_validation.json` with the results for each table. Run
`python -m transform.silver.quality` to validate the current snapshot again.

Market ingestion writes Bronze by default. Pass `--build-silver` to also run
the Spark pipeline when the Bronze Market and VSDC inputs are ready.

### Vnstock 6 notebook table coverage

`ingest/market/vnstock/test.ipynb` tests historical stock bars for up to 1,000
symbols in memory. Its final section uses
`ingest.market.vnstock.table_probe.run_remaining_tables` for the other seven
market tables. It does not export CSV or Parquet. Existing CLI adapter examples
above describe the older provider integration; the notebook uses Vnstock 6
and explicit DNSE/SSI/FHSC routing.

Configure credentials privately in `.env` (names are in `.env.example`):

- DNSE: `DNSE_API_KEY`, `DNSE_API_SECRET` for OHLCV, listings and trading definitions.
- SSI: `SSI_API_KEY`, `SSI_API_SECRET` for the index catalogue and current members.
- FHSC: `FHSC_API_KEY` for company profiles and full quote snapshots.

Without SSI/FHSC, reports explicitly mark `index_components` as
`MISSING_CREDENTIALS` and `index_list`, `securities_details`, and
`daily_stock_price` as `PARTIAL`. DNSE trading definitions are not company
profiles; reference/ceiling/floor prices are not full quote data. Builtin index
groups are not a complete index catalogue. Snapshots and current constituents
must not be treated as historical point-in-time reference data.

See the [Vnstock source-key setup](https://vnstocks.com/docs/vnstock/v6/cai-dat)
and [Reference route coverage](https://vnstocks.com/docs/vnstock/v6/tham-chieu-reference).

The notebook can also read existing Bronze reference snapshots. To test
constituents from exchange notices without broker credentials, place a
transcribed CSV at `input/market/index_components.csv` with the columns
`index_symbol,symbol,effective_from,source_url`. Use the notice's effective
date in `YYYY-MM-DD` format and keep a row per constituent per effective date.
The notebook keeps the published data in memory and checks for duplicate keys.
