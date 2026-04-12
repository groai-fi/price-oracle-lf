"""
service/sync.py

Core orchestration for the price-oracle-lf cron run.

On each invocation:
  1. Load assets.json → list of tracked tokens
  2. Per exchange: call list_s3_symbols() once to discover what's already on S3
  3. Per symbol:
       - If NEW (not yet on S3) → seed download from asset.start_date
       - If EXISTING           → incremental update (auto-resumes from MAX date)
  4. After download: check part count; merge if > MERGE_THRESHOLD
  5. Return a summary dict: { "success": N, "failed": N, "new_rows": N }

Error isolation: a failure on one symbol does not abort others.
Merge failures are logged as warnings only — the downloaded data is still safe.
"""
import logging
import os
from datetime import datetime
from typing import Dict

from groai_fi_datastore_shared.Binance.cli.s3_utils import (
    list_s3_symbols,
    count_parts_s3,
)
from groai_fi_datastore_shared.Binance.cli.download_price_binance_s3 import (
    run_for_symbol as download_symbol,
)
from groai_fi_datastore_shared.Binance.cli.merge_parquet_prices_s3 import (
    run_for_symbol as merge_symbol,
)

from service.assets import load_assets

log = logging.getLogger(__name__)

# S3 path root — matches groai-fi-datastore-shared convention
PRICE_ROOT = "prices_v3.parquet"

# Part count threshold above which a merge is triggered
MERGE_THRESHOLD = 50


def run_sync(assets_path: str = "assets.json") -> Dict[str, int]:
    """
    Execute one full sync cycle.

    Returns:
        dict with keys: "success", "failed", "new_rows"
    """
    bucket = os.environ["BUCKET_NAME"]
    assets = load_assets(assets_path)

    total_new_rows = 0
    success = 0
    failed = 0

    # Group by exchange so we only call list_s3_symbols() once per exchange
    by_exchange: Dict[str, list] = {}
    for asset in assets:
        by_exchange.setdefault(asset.exchange, []).append(asset)

    for exchange, exchange_assets in by_exchange.items():

        # ── Discover symbols already on S3 (one S3 call per exchange) ────────
        try:
            existing_on_s3 = set(list_s3_symbols(bucket, PRICE_ROOT, exchange))
            log.info(
                f"[{exchange}] {len(existing_on_s3)} symbol(s) already on S3"
            )
        except Exception as exc:
            log.error(
                f"[{exchange}] Failed to list S3 symbols — skipping entire exchange: {exc}"
            )
            failed += len(exchange_assets)
            continue

        for asset in exchange_assets:
            symbol = asset.symbol
            is_new = symbol not in existing_on_s3
            label = "seed" if is_new else "update"

            log.info(f"[{symbol}] Starting {label} (exchange={exchange})")

            # ── Download ──────────────────────────────────────────────────────
            try:
                start_dt = datetime.strptime(asset.start_date, "%Y/%m/%d")
                rows = download_symbol(
                    symbol=symbol,
                    tframe="1m",
                    bucket=bucket,
                    price_root=PRICE_ROOT,
                    start_date_fallback=start_dt,
                    exchange=exchange,
                )
                log.info(f"[{symbol}] {label} complete — {rows} new row(s)")
                total_new_rows += rows
            except Exception as exc:
                log.error(f"[{symbol}] Download failed: {exc}")
                failed += 1
                continue

            # ── Merge check ───────────────────────────────────────────────────
            try:
                part_count = count_parts_s3(bucket, PRICE_ROOT, exchange, symbol)
                log.info(
                    f"[{symbol}] Part count: {part_count} (threshold: {MERGE_THRESHOLD})"
                )
                if part_count > MERGE_THRESHOLD:
                    log.info(f"[{symbol}] Merging {part_count} parts...")
                    merge_symbol(
                        symbol=symbol,
                        bucket=bucket,
                        price_root=PRICE_ROOT,
                        exchange=exchange,
                    )
                    log.info(f"[{symbol}] Merge complete → part.00000.parquet")
            except Exception as exc:
                # Data was already downloaded successfully — only merge failed.
                # Log as warning; do not count as a symbol failure.
                log.warning(f"[{symbol}] Merge failed (data is safe): {exc}")

            success += 1

    return {"success": success, "failed": failed, "new_rows": total_new_rows}
