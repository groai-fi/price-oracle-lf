"""
service/assets.py

Loads and validates assets.json — the registry of tracked tokens.

Schema (per entry):
    exchange   : str   — exchange label (e.g. "Binance")
    symbol     : str   — trading pair (e.g. "BTCUSDT")
    start_date : str   — fallback start for initial seed, format "YYYY/MM/DD"

Designed to be exchange-agnostic so future exchanges can be added without
changing the sync logic — just extend assets.json.
"""
import json
from dataclasses import dataclass
from pathlib import Path
from typing import List


_REQUIRED_FIELDS = ("exchange", "symbol", "start_date")


@dataclass
class Asset:
    exchange: str
    symbol: str
    start_date: str   # "YYYY/MM/DD" — matches Binance CLI convention


def load_assets(path: str = "assets.json") -> List[Asset]:
    """
    Load and validate assets.json.

    Raises:
        FileNotFoundError: if the file does not exist.
        ValueError: if the JSON is malformed or an entry is missing a required field.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"assets.json not found at: {p.resolve()}")

    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Malformed assets.json: {exc}") from exc

    if not isinstance(data, list):
        raise ValueError(
            f"assets.json must be a JSON array, got {type(data).__name__}"
        )

    assets: List[Asset] = []
    for i, entry in enumerate(data):
        for field in _REQUIRED_FIELDS:
            if field not in entry:
                raise ValueError(
                    f"assets.json entry #{i} is missing required field: '{field}'"
                )
        assets.append(
            Asset(
                exchange=str(entry["exchange"]),
                symbol=str(entry["symbol"]),
                start_date=str(entry["start_date"]),
            )
        )

    return assets
