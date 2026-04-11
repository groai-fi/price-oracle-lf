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
