## Production Solution: Cache by Fixed Chunks (Per Day)

Break the data into **daily chunks** and cache each day independently.

```
# Instead of one key per query range...
"AAPL:10m:2024-01-01:2024-01-05"   ❌

# Cache one key per day per timeframe
"AAPL:10m:20240101"   ✅
"AAPL:10m:20240102"   ✅
"AAPL:10m:20240103"   ✅
```

Now **any query that touches that day reuses the same cache entry** — regardless of the exact start/end time.

---

## How It Works End-to-End

```
Query: AAPL, 10m, 2024-01-01 to 2024-01-03
              │
              ▼
   Break into daily chunks
   ┌──────────────────────┐
   │ AAPL:10m:20240101    │──► Redis HIT  ✅ → use cached df
   │ AAPL:10m:20240102    │──► Redis MISS ❌ → query DuckDB → cache it
   │ AAPL:10m:20240103    │──► Redis HIT  ✅ → use cached df
   └──────────────────────┘
              │
              ▼
      Combine all 3 days
              │
              ▼
   Trim to exact [start, end]
              │
              ▼
          Return ✅
```

---

## Full Production Code

```python
import duckdb
import redis
import pandas as pd

r = redis.Redis(host='localhost', port=6379, db=0)

def get_resampled_price(symbol: str, timeframe: str, start: str, end: str) -> pd.DataFrame:

    start_dt = pd.Timestamp(start)
    end_dt   = pd.Timestamp(end)

    # 1️⃣ Break query range into daily chunks
    dates = (
        pd.date_range(start_dt.normalize(), end_dt.normalize(), freq='D')
        .strftime('%Y%m%d')
        .tolist()
    )

    dfs          = []
    missing_dates = []

    # 2️⃣ Check Redis for each day chunk
    for date in dates:
        key    = f"{symbol}:{timeframe}:{date}"
        cached = r.get(key)
        if cached:
            dfs.append(pd.read_json(cached, orient='split'))
        else:
            missing_dates.append(date)

    # 3️⃣ Batch query DuckDB for ALL missing days in one shot
    if missing_dates:
        conditions = []
        for date in missing_dates:
            day_start = pd.Timestamp(date).strftime('%Y-%m-%d')
            day_end   = (pd.Timestamp(date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            conditions.append(f"(timestamp >= '{day_start}' AND timestamp < '{day_end}')")

        where_clause = " OR ".join(conditions)

        df_missing = duckdb.query(f"""
            SELECT
                time_bucket(INTERVAL '{timeframe}', timestamp) AS ts,
                symbol,
                FIRST(open  ORDER BY timestamp) AS open,
                MAX(high)                        AS high,
                MIN(low)                         AS low,
                LAST(close  ORDER BY timestamp)  AS close,
                SUM(volume)                      AS volume
            FROM read_parquet('data/prices_1m/**/*.parquet', hive_partitioning=true)
            WHERE symbol = '{symbol}'
              AND ({where_clause})
            GROUP BY 1, 2
            ORDER BY 1
        """).df()

        df_missing['ts'] = pd.to_datetime(df_missing['ts'])
        df_missing = df_missing.set_index('ts')

        # 4️⃣ Split result by day → cache each day independently
        for day, group in df_missing.groupby(pd.Grouper(freq='D')):
            if group.empty:
                continue
            date_str = day.strftime('%Y%m%d')
            key      = f"{symbol}:{timeframe}:{date_str}"
            group    = group.reset_index()
            ttl      = _get_ttl(date_str)
            r.setex(key, ttl, group.to_json(orient='split'))
            dfs.append(group)

    # 5️⃣ Combine + trim to exact requested range
    if not dfs:
        return pd.DataFrame()

    combined      = pd.concat(dfs, ignore_index=True)
    combined['ts'] = pd.to_datetime(combined['ts'])
    mask          = (combined['ts'] >= start_dt) & (combined['ts'] <= end_dt)

    return combined[mask].sort_values('ts').reset_index(drop=True)


def _get_ttl(date_str: str) -> int:
    date_dt  = pd.Timestamp(date_str)
    days_ago = (pd.Timestamp.now() - date_dt).days
    if days_ago > 1:
        return 60 * 60 * 24   # 24h — historical, stable
    else:
        return 60 * 5         # 5min — today, still updating


def invalidate_cache(symbol: str, date: str):
    """ Call this when new 1m data arrives for a given day """
    date_str = date.replace('-', '')
    pattern  = f"{symbol}:*:{date_str}"   # catches all timeframes for that day
    keys     = r.keys(pattern)
    if keys:
        r.delete(*keys)
        print(f"🗑️ Invalidated {len(keys)} keys for {symbol} on {date_str}")
```

---

## Why This Is Production-Standard

| Problem | Old Approach | Chunk Approach |
|---|---|---|
| Key combinations | `O(start × end)` — explosive | `O(symbols × timeframes × days)` — linear |
| Cache reuse | Almost zero overlap | Any query sharing a day reuses it |
| Partial hits | All-or-nothing | Per-day granularity |
| Invalidation | Hard — which keys to delete? | Easy — delete `symbol:*:YYYYMMDD` |
| DuckDB calls | 1 per unique range | 1 batch call for all missing days only |

---

## Cache Key Space Comparison

```
# Old: AAPL × 10m × 365 possible starts × 365 possible ends
= 133,000+ combinations for just 1 symbol, 1 timeframe, 1 year  ❌

# New: AAPL × 10m × 365 days
= 365 keys for 1 symbol, 1 timeframe, 1 year  ✅
# And each key is REUSED by every query that touches that day
```

---

## Summary

> **The golden rule in production caching:**
> Cache at the **natural unit of your data** (a day),
> not at the **query boundary** (start to end).

This is exactly how systems like **Bloomberg Terminal data cache**, **financial data vendors (Refinitiv, Polygon.io internal caches)**, and **large quant fund data layers** handle it — chunk by the smallest stable unit, assemble on read.