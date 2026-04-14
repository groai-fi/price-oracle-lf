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
import tomllib
from pathlib import Path

from dotenv import load_dotenv

from service.sync import run_sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def get_version() -> str:
    try:
        pyproject_path = Path(__file__).parent / "pyproject.toml"
        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)
            return data.get("project", {}).get("version", "unknown")
    except Exception:
        return "unknown"


def main() -> None:
    # load_dotenv is a no-op on Railway (vars already injected);
    # useful for local development with a .env file.
    load_dotenv()

    version = get_version()
    log.info(f"price-oracle-lf v{version}: sync starting")

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
