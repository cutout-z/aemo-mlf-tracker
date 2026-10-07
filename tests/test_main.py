"""src/main.py run policy (every network step stubbed)."""

import pandas as pd
import pytest

from conftest import detail_rows
from src import main


@pytest.fixture
def stubbed_run(tmp_path, monkeypatch):
    """main.run() against tmp_path with the AEMO fetches replaced."""
    monkeypatch.setattr(main, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(main, "get_latest_available_month", lambda: (2026, 8))
    monkeypatch.setattr(main, "download_dudetailsummary",
                        lambda y, m, d: detail_rows(("GEN1", "2025-07-01", "2026-07-01", 0.95)))
    monkeypatch.setattr(main, "fetch_mlf_workbooks", lambda *a, **k: (None, None))
    return tmp_path


def test_metadata_failure_stops_the_run_before_publishing(stubbed_run, monkeypatch):
    def broken(*a, **k):
        raise ValueError("Excel file format cannot be determined")   # e.g. a challenge page cached as the list
    monkeypatch.setattr(main, "fetch_generator_metadata", broken)
    with pytest.raises(SystemExit) as exit_info:
        main.run(full_refresh=True)
    assert exit_info.value.code == 1
    assert not (stubbed_run / "outputs" / "summary.csv").exists()
