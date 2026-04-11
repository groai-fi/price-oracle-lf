"""
main.py — price-oracle-lf entrypoint

Called directly by the Railway cron job on each tick.
Loads environment variables (no-op on Railway where vars are injected),
runs one full sync cycle, prints a summary, and exits.

Exit codes:
    0 — all symbols processed successfully
    1 — one or more symbols failed
"""
import logging
import sys

from dotenv import load_dotenv

from service.sync import run_sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def main() -> None:
    # load_dotenv is a no-op on Railway (vars already injected);
    # useful for local development with a .env file.
    load_dotenv()

    log.info("price-oracle-lf: sync starting")

    try:
        result = run_sync()
    except Exception as exc:
        log.exception(f"price-oracle-lf: unexpected error — {exc}")
        sys.exit(1)

    log.info(
        f"price-oracle-lf: sync complete — "
        f"success={result['success']} "
        f"failed={result['failed']} "
        f"new_rows={result['new_rows']}"
    )

    sys.exit(0 if result["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
