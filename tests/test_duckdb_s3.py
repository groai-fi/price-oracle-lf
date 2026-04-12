"""
tests/test_duckdb_s3.py

Integration test to verify DuckDB can successfully connect to the Railway S3 bucket,
read the partitioned Parquet files, and resample the 1m OHLCV data into 10m intervals.

These tests require live S3 credentials via `.env.railway.production` or similar, 
and will be skipped if the necessary environment variables are not found.
"""
import os
import duckdb
import pytest
from dotenv import load_dotenv
from groai_fi_datastore_shared.Binance.cli.s3_utils import configure_duckdb_s3

# Attempt to load Railway config if running locally
load_dotenv(".env.railway.production")

# Check if we have S3 credentials in the environment
HAS_S3_CREDS = (
    "BUCKET_ENDPOINT" in os.environ and 
    "BUCKET_NAME" in os.environ and 
    "BUCKET_ACCESS_KEY_ID" in os.environ
)

@pytest.mark.skipif(not HAS_S3_CREDS, reason="Requires Railway S3 bucket credentials")
def test_duckdb_s3_resample_10m():
    """Verify DuckDB can connect, read parquet, and perform 10m resampling."""
    # 1. Map Railway's BUCKET_ vars to the S3_ vars expected by the library
    os.environ["S3_ENDPOINT_URL"] = os.environ.get("BUCKET_ENDPOINT", "")
    os.environ["S3_BUCKET_NAME"] = os.environ.get("BUCKET_NAME", "")
    os.environ["S3_ACCESS_KEY_ID"] = os.environ.get("BUCKET_ACCESS_KEY_ID", "")
    os.environ["S3_SECRET_ACCESS_KEY"] = os.environ.get("BUCKET_SECRET_ACCESS_KEY", "")

    # 2. Initialize DuckDB and automatically configure HTTPFS + S3 keys
    con = duckdb.connect()
    configure_duckdb_s3(con)

    # 3. Query and resample 1m data to 10m candles (e.g. BTCUSDT)
    bucket = os.environ["S3_BUCKET_NAME"]
    # We use limit 5 to ensure the test is fast and doesn't pull the whole historical dataset
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
    con.close()

    # 4. Asserts
    assert not resampled_df.empty, "DataFrame should not be empty"
    assert len(resampled_df) <= 5, "Should have maximum 5 rows due to LIMIT 5"
    
    # Check expected schema
    expected_columns = ["bucket_time", "open", "high", "low", "close", "volume"]
    for col in expected_columns:
        assert col in resampled_df.columns, f"Missing expected column: {col}"
        
    # Check that volume is positive and high is >= low
    assert (resampled_df["volume"] >= 0).all(), "Volume should be non-negative"
    assert (resampled_df["high"] >= resampled_df["low"]).all(), "High should be >= Low"
