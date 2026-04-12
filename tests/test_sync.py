"""
tests/test_sync.py

Unit tests for service/sync.py.

All external calls (S3, Binance API, DuckDB) are mocked so these tests run
without credentials or network access. The patch targets are in the
`service.sync` namespace (where the names are imported).
"""
import json
from unittest.mock import MagicMock, call, patch

import pytest

from service.sync import MERGE_THRESHOLD, run_sync

# ── Helpers ───────────────────────────────────────────────────────────────────

MOCK_BUCKET = "test-bucket"

TWO_ASSETS = [
    {"exchange": "Binance", "symbol": "BTCUSDT", "start_date": "2019/01/01"},
    {"exchange": "Binance", "symbol": "ETHUSDT", "start_date": "2019/01/01"},
]


@pytest.fixture()
def assets_file(tmp_path):
    f = tmp_path / "assets.json"
    f.write_text(json.dumps(TWO_ASSETS))
    return str(f)


@pytest.fixture(autouse=True)
def set_bucket_env(monkeypatch):
    monkeypatch.setenv("BUCKET_NAME", MOCK_BUCKET)


# ── New-symbol path ───────────────────────────────────────────────────────────

def test_new_symbols_use_asset_start_date(assets_file):
    """When S3 is empty, both symbols are seeded from their start_date."""
    with (
        patch("service.sync.list_s3_symbols", return_value=[]) as mock_list,
        patch("service.sync.download_symbol", return_value=100) as mock_dl,
        patch("service.sync.count_parts_s3", return_value=1),
        patch("service.sync.merge_symbol") as mock_merge,
    ):
        result = run_sync(assets_file)

    assert result["success"] == 2
    assert result["failed"] == 0
    assert result["new_rows"] == 200  # 100 rows × 2 symbols

    # list_s3_symbols should be called once per exchange (not once per symbol)
    mock_list.assert_called_once()
    # merge should NOT be triggered (part_count=1 < threshold)
    mock_merge.assert_not_called()


# ── Existing-symbol path ──────────────────────────────────────────────────────

def test_existing_symbols_still_call_download(assets_file):
    """Symbols already on S3 are passed to download_symbol (which auto-resumes)."""
    with (
        patch("service.sync.list_s3_symbols", return_value=["BTCUSDT", "ETHUSDT"]),
        patch("service.sync.download_symbol", return_value=5) as mock_dl,
        patch("service.sync.count_parts_s3", return_value=1),
        patch("service.sync.merge_symbol"),
    ):
        result = run_sync(assets_file)

    assert result["success"] == 2
    assert result["failed"] == 0
    assert result["new_rows"] == 10  # 5 rows × 2 symbols
    assert mock_dl.call_count == 2


# ── Merge threshold ───────────────────────────────────────────────────────────

def test_merge_triggered_when_parts_exceed_threshold(assets_file):
    """Merge is called when part count is strictly above MERGE_THRESHOLD."""
    with (
        patch("service.sync.list_s3_symbols", return_value=[]),
        patch("service.sync.download_symbol", return_value=10),
        patch("service.sync.count_parts_s3", return_value=MERGE_THRESHOLD + 1),
        patch("service.sync.merge_symbol") as mock_merge,
    ):
        result = run_sync(assets_file)

    assert result["success"] == 2
    assert mock_merge.call_count == 2   # once per symbol


def test_merge_not_triggered_at_threshold(assets_file):
    """Merge is NOT called when part count equals MERGE_THRESHOLD exactly."""
    with (
        patch("service.sync.list_s3_symbols", return_value=[]),
        patch("service.sync.download_symbol", return_value=10),
        patch("service.sync.count_parts_s3", return_value=MERGE_THRESHOLD),
        patch("service.sync.merge_symbol") as mock_merge,
    ):
        run_sync(assets_file)

    mock_merge.assert_not_called()


def test_merge_not_triggered_below_threshold(assets_file):
    """Merge is NOT called when part count is below MERGE_THRESHOLD."""
    with (
        patch("service.sync.list_s3_symbols", return_value=[]),
        patch("service.sync.download_symbol", return_value=10),
        patch("service.sync.count_parts_s3", return_value=5),
        patch("service.sync.merge_symbol") as mock_merge,
    ):
        run_sync(assets_file)

    mock_merge.assert_not_called()


# ── Error isolation ───────────────────────────────────────────────────────────

def test_download_failure_for_one_symbol_does_not_abort_others(assets_file):
    """A failed download increments 'failed' but the next symbol is still processed."""
    call_count = {"n": 0}

    def flaky_download(**kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("Binance API timeout")
        return 50

    with (
        patch("service.sync.list_s3_symbols", return_value=[]),
        patch("service.sync.download_symbol", side_effect=flaky_download),
        patch("service.sync.count_parts_s3", return_value=1),
        patch("service.sync.merge_symbol"),
    ):
        result = run_sync(assets_file)

    assert result["failed"] == 1   # first symbol failed
    assert result["success"] == 1  # second symbol succeeded
    assert result["new_rows"] == 50


def test_merge_failure_does_not_count_as_symbol_failure(assets_file):
    """Merge errors are warnings only — the symbol still counts as success."""
    with (
        patch("service.sync.list_s3_symbols", return_value=[]),
        patch("service.sync.download_symbol", return_value=10),
        patch("service.sync.count_parts_s3", return_value=MERGE_THRESHOLD + 1),
        patch("service.sync.merge_symbol", side_effect=RuntimeError("merge error")),
    ):
        result = run_sync(assets_file)

    # Both symbols downloaded successfully — merge failure is non-fatal
    assert result["success"] == 2
    assert result["failed"] == 0


def test_list_s3_symbols_failure_skips_entire_exchange(tmp_path, monkeypatch):
    """If S3 listing fails for an exchange, all its symbols are counted as failed."""
    data = [
        {"exchange": "Binance", "symbol": "BTCUSDT", "start_date": "2019/01/01"},
        {"exchange": "Binance", "symbol": "ETHUSDT", "start_date": "2019/01/01"},
    ]
    f = tmp_path / "assets.json"
    f.write_text(json.dumps(data))

    with (
        patch("service.sync.list_s3_symbols", side_effect=Exception("S3 unreachable")),
        patch("service.sync.download_symbol") as mock_dl,
    ):
        result = run_sync(str(f))

    # No downloads should have been attempted
    mock_dl.assert_not_called()
    assert result["failed"] == 2
    assert result["success"] == 0


# ── All failures → exit code 1 ────────────────────────────────────────────────

def test_all_failures_returns_nonzero_failed(assets_file):
    """run_sync result['failed'] > 0 signals main() to exit(1)."""
    with (
        patch("service.sync.list_s3_symbols", return_value=[]),
        patch("service.sync.download_symbol", side_effect=Exception("fatal")),
        patch("service.sync.count_parts_s3", return_value=1),
        patch("service.sync.merge_symbol"),
    ):
        result = run_sync(assets_file)

    assert result["failed"] == 2
    assert result["success"] == 0
    assert result["failed"] > 0   # main() would exit(1)


# ── S3 call efficiency ────────────────────────────────────────────────────────

def test_list_s3_symbols_called_once_per_exchange_not_per_symbol(tmp_path):
    """list_s3_symbols() must be called once per exchange group, not once per symbol."""
    data = [
        {"exchange": "Binance", "symbol": "BTCUSDT", "start_date": "2019/01/01"},
        {"exchange": "Binance", "symbol": "ETHUSDT", "start_date": "2019/01/01"},
        {"exchange": "Binance", "symbol": "SOLUSDT", "start_date": "2021/01/01"},
    ]
    f = tmp_path / "assets.json"
    f.write_text(json.dumps(data))

    with (
        patch("service.sync.list_s3_symbols", return_value=[]) as mock_list,
        patch("service.sync.download_symbol", return_value=0),
        patch("service.sync.count_parts_s3", return_value=1),
        patch("service.sync.merge_symbol"),
    ):
        run_sync(str(f))

    # 3 symbols, all same exchange → only 1 S3 list call
    assert mock_list.call_count == 1
