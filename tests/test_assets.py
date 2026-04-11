"""
tests/test_assets.py

Unit tests for service/assets.py.
No external dependencies required — all tests use tmp_path fixtures.
"""
import json

import pytest

from service.assets import Asset, load_assets


# ── Happy path ────────────────────────────────────────────────────────────────

def test_load_two_valid_assets(tmp_path):
    data = [
        {"exchange": "Binance", "symbol": "BTCUSDT", "start_date": "2019/01/01"},
        {"exchange": "Binance", "symbol": "ETHUSDT", "start_date": "2019/01/01"},
    ]
    f = tmp_path / "assets.json"
    f.write_text(json.dumps(data))

    assets = load_assets(str(f))

    assert len(assets) == 2
    assert assets[0] == Asset(exchange="Binance", symbol="BTCUSDT", start_date="2019/01/01")
    assert assets[1] == Asset(exchange="Binance", symbol="ETHUSDT", start_date="2019/01/01")


def test_load_single_asset(tmp_path):
    data = [{"exchange": "Binance", "symbol": "SOLUSDT", "start_date": "2021/06/01"}]
    f = tmp_path / "assets.json"
    f.write_text(json.dumps(data))

    assets = load_assets(str(f))

    assert len(assets) == 1
    assert assets[0].symbol == "SOLUSDT"
    assert assets[0].start_date == "2021/06/01"


def test_fields_are_coerced_to_strings(tmp_path):
    """Numeric-looking values in JSON should be coerced to str."""
    data = [{"exchange": "Binance", "symbol": "BTCUSDT", "start_date": "2019/01/01"}]
    f = tmp_path / "assets.json"
    f.write_text(json.dumps(data))

    assets = load_assets(str(f))
    assert isinstance(assets[0].exchange, str)
    assert isinstance(assets[0].symbol, str)
    assert isinstance(assets[0].start_date, str)


def test_extra_fields_are_ignored(tmp_path):
    """Additional keys in an entry should be silently ignored."""
    data = [
        {
            "exchange": "Binance",
            "symbol": "BTCUSDT",
            "start_date": "2019/01/01",
            "notes": "primary pair",
        }
    ]
    f = tmp_path / "assets.json"
    f.write_text(json.dumps(data))

    assets = load_assets(str(f))
    assert len(assets) == 1


# ── Error cases ───────────────────────────────────────────────────────────────

def test_missing_file_raises_file_not_found():
    with pytest.raises(FileNotFoundError, match="assets.json not found"):
        load_assets("/nonexistent/path/assets.json")


def test_malformed_json_raises_value_error(tmp_path):
    f = tmp_path / "assets.json"
    f.write_text("{not: valid json")

    with pytest.raises(ValueError, match="Malformed assets.json"):
        load_assets(str(f))


def test_root_is_object_not_array_raises(tmp_path):
    f = tmp_path / "assets.json"
    f.write_text(json.dumps({"exchange": "Binance"}))

    with pytest.raises(ValueError, match="JSON array"):
        load_assets(str(f))


def test_missing_exchange_field_raises(tmp_path):
    data = [{"symbol": "BTCUSDT", "start_date": "2019/01/01"}]
    f = tmp_path / "assets.json"
    f.write_text(json.dumps(data))

    with pytest.raises(ValueError, match="'exchange'"):
        load_assets(str(f))


def test_missing_symbol_field_raises(tmp_path):
    data = [{"exchange": "Binance", "start_date": "2019/01/01"}]
    f = tmp_path / "assets.json"
    f.write_text(json.dumps(data))

    with pytest.raises(ValueError, match="'symbol'"):
        load_assets(str(f))


def test_missing_start_date_field_raises(tmp_path):
    data = [{"exchange": "Binance", "symbol": "BTCUSDT"}]
    f = tmp_path / "assets.json"
    f.write_text(json.dumps(data))

    with pytest.raises(ValueError, match="'start_date'"):
        load_assets(str(f))


def test_error_message_includes_entry_index(tmp_path):
    """Validation error should tell us which entry number failed."""
    data = [
        {"exchange": "Binance", "symbol": "BTCUSDT", "start_date": "2019/01/01"},
        {"exchange": "Binance", "symbol": "ETHUSDT"},          # entry #1 — missing start_date
    ]
    f = tmp_path / "assets.json"
    f.write_text(json.dumps(data))

    with pytest.raises(ValueError, match="#1"):
        load_assets(str(f))
