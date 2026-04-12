# price-oracle-lf

A periodic market data service that incrementally pulls 1-minute OHLCV candles from the Binance REST API and stores them in a Railway S3 BUCKET.

It utilizes the [`groai-fi-datastore-shared`](https://github.com/groai-fi/datastore.shared) library as the underlying data layer to efficiently manage Hive-partitioned Parquet files on S3.

## Features
- **S3-Native**: Never relies on local volume storage; data goes directly to S3.
- **Incremental Sync**: Automatically tracks the `MAX(date)` of data already in S3 and resumes downloads from there.
- **Auto-Merge**: Merges Parquet files when reaching 50+ fragments for optimization.
- **Extensible Registry**: Tokens are tracked via an `assets.json` configuration file, which maps exchanges, symbols, and start dates.

## Architecture & Flow
Triggered natively by a Railway cron job (defaulting to every 8 hours).
1. `service/assets.py` loads `assets.json`
2. Connects to S3 to discover what symbols are currently tracked
3. For existing symbols: incremental load based on max date
4. For new symbols: initial seeding from the `start_date` specified in `assets.json`
5. Optional Parquet parts compaction (if >50 fragments are detected)

## Local Development
1. Create a `.env` file by copying the template:
   ```bash
   cp .env.example .env
   ```
2. Fill out the `BINANCE_API_KEY`, `BINANCE_API_SECRET`, and BUCKET values.
3. Install dependencies:
   ```bash
   pip install -e .[dev]
   ```
4. Run tests:
   ```bash
   pytest tests/
   ```
5. Trigger a sync locally:
   ```bash
   python main.py
   ```

## Data Usage Example (Python & DuckDB)

With `groai-fi-datastore-shared>=0.2.2`, you can easily interact with the Parquet data directly from the S3 bucket using the built-in DuckDB integration. Railway provides `BUCKET_` environment variables which need to be mapped to the `S3_` variables expected by the library.

Here is a Python example that connects directly to S3 and resamples the 1m OHLCV data to 10m intervals:

```python
import os
import duckdb
from dotenv import load_dotenv
from groai_fi_datastore_shared.Binance.cli.s3_utils import configure_duckdb_s3

# 1. Load credentials from your Railway environment equivalent
load_dotenv(".env.railway.production")

# 2. Map Railway's BUCKET_ vars to the S3_ vars expected by the library
os.environ["S3_ENDPOINT_URL"] = os.environ.get("BUCKET_ENDPOINT", "")
os.environ["S3_BUCKET_NAME"] = os.environ.get("BUCKET_NAME", "")
os.environ["S3_ACCESS_KEY_ID"] = os.environ.get("BUCKET_ACCESS_KEY_ID", "")
os.environ["S3_SECRET_ACCESS_KEY"] = os.environ.get("BUCKET_SECRET_ACCESS_KEY", "")

# 3. Initialize DuckDB and automatically configure HTTPFS + S3 keys
con = duckdb.connect()
configure_duckdb_s3(con)

# 4. Query and resample 1m data to 10m candles (e.g. BTCUSDT)
bucket = os.environ["S3_BUCKET_NAME"]
path = f"s3://{bucket}/prices_v3.parquet/exchange=Binance/symbol=BTCUSDT/**/*.parquet"

query = f"""
    WITH base AS (
        SELECT *
        FROM read_parquet('{path}', hive_partitioning=true)
        ORDER BY date
    ),
    bucketed AS (
        SELECT
            epoch_ms((epoch_ms(date) // 600000) * 600000) AS bucket_time,
            open, high, low, close, volume
        FROM base
    )
    SELECT
        bucket_time,
        first(open  ORDER BY bucket_time) AS open,
        max(high)                         AS high,
        min(low)                          AS low,
        last(close  ORDER BY bucket_time) AS close,
        sum(volume)                       AS volume
    FROM bucketed
    GROUP BY bucket_time
    ORDER BY bucket_time DESC
    LIMIT 5
"""

resampled_df = con.execute(query).df()
print(resampled_df)

con.close()
```

### Parquet Schema

When checking the dataset, you'll encounter the following schema:

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `date` | `TIMESTAMP WITH TIME ZONE` | YES | The candle open time in UTC |
| `yymm` | `VARCHAR` | YES | Year-month partitioning key (e.g., '2604') |
| `exchange` | `VARCHAR` | YES | Partitioning key for the exchange (e.g., 'Binance') |
| `symbol` | `VARCHAR` | YES | Partitioning key for the asset symbol (e.g., 'BTCUSDT') |
| `open` | `DOUBLE` | YES | Opening price for the candle |
| `high` | `DOUBLE` | YES | Highest price during the candle |
| `low` | `DOUBLE` | YES | Lowest price during the candle |
| `close` | `DOUBLE` | YES | Closing price for the candle |
| `volume` | `DOUBLE` | YES | Trading volume during the candle |

### Read Example Result (10m Resampled DataFrame)

Using the example query above returns a Pandas DataFrame matching this structure:

| bucket_time | open | high | low | close | volume |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 2026-04-12 00:00:00 | 73043.16 | 73043.17 | 72939.03 | 72946.90 | 81.35 |
| 2026-04-11 23:50:00 | 73062.00 | 73067.73 | 73009.45 | 73043.16 | 62.93 |
| 2026-04-11 23:40:00 | 73133.96 | 73133.96 | 73060.31 | 73062.00 | 36.31 |
| 2026-04-11 23:30:00 | 73136.00 | 73176.40 | 73082.20 | 73133.96 | 70.79 |
| 2026-04-11 23:20:00 | 73123.64 | 73210.00 | 73123.64 | 73136.00 | 33.57 |
