# price-oracle-lf

A periodic service responsible for storing market data on S3.


## Prerequisites

- Library: `groai-fi-datastore-shared` (`pip install groai-fi-datastore-shared`)
- Focus only on Railway BUCKET (S3) — no local parquet files


## Functionality

### Incremental price update

Pulls 1m OHLCV bars from the Binance REST API using `groai-fi-datastore-shared` and
writes them to the Railway BUCKET at the S3 path:

```
s3://<bucket>/prices_v3.parquet/exchange=Binance/symbol=<SYMBOL>/part.<ts>.parquet
```

Internally this calls `download_price_binance_s3.run_for_symbol()`, which:
- Reads `MAX(date)` from existing S3 files and resumes from there
- Uploads new rows as a new `part.<unix_ts>.parquet` file
- Triggers a merge (via `merge_parquet_prices_s3.run_for_symbol()`) when part count > 50


### Asset discovery

`assets.json` defines the universe of tracked tokens (exchange, symbol, start_date).
On each cron run:

1. Load `assets.json`
2. Call `list_s3_symbols()` (from `groai_fi_datastore_shared.Binance.cli.s3_utils`)
   to discover symbols already tracked on S3
3. Symbols present in both → incremental update (auto-resume from MAX date)
4. Symbols in `assets.json` only (new) → initial seed download from `start_date`

Even though only Binance is supported now, `assets.json` is designed to be
extensible to other exchanges in the future.


## Environment Variables

Required by `groai-fi-datastore-shared` and the Railway BUCKET:

```
S3_ENDPOINT_URL
S3_BUCKET_NAME
S3_ACCESS_KEY_ID
S3_SECRET_ACCESS_KEY
BINANCE_API_KEY
BINANCE_API_SECRET
```


## Scheduling

Railway-native cron job (`railway.json`) — runs every 8 hours.
No long-running process required; Railway starts a fresh container on each tick.


## Deployment

The GitHub repo is already connected to Railway with cron configured.
Service deploys automatically on push to `main`.