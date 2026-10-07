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


def _workbooks(cache_dir, detail_df, full_refresh=False, status=None):
    status["final_workbook"] = {"fy": "2026-27", "state": "published", "last_modified": "Wed, 22 Jul 2026 05:12:00 GMT"}
    status["draft_workbook"] = {"fy": "2027-28", "state": "not_published", "detail": "not on AEMO (redirected to /404)"}
    return None, None


def _metadata(cache_dir, mmsdm_year=None, mmsdm_month=None, refresh=False, status=None):
    status["registration_list"] = {"state": "downloaded", "file_date": "2026-10-07"}
    status["mmsdm_tables"] = {"STATION": "downloaded", "GENUNITS": "downloaded", "DUALLOC": "downloaded"}
    meta = pd.DataFrame({"DUID": ["GEN1"], "FUEL_CATEGORY": ["Solar"], "CAPACITY_MW": [50.0], "DUID_TYPE": ["Generator"]})
    return meta, pd.Series(dtype=str)


def test_run_writes_the_run_status_record(stubbed_run, monkeypatch):
    import json
    monkeypatch.setattr(main, "fetch_mlf_workbooks", _workbooks)
    monkeypatch.setattr(main, "fetch_generator_metadata", _metadata)
    main.run(full_refresh=True)
    status = json.loads((stubbed_run / "outputs" / "run_status.json").read_text())
    assert status["mmsdm_month"] == "2026-08" and status["dudetailsummary_source"] == "download"
    assert status["draft_workbook"]["state"] == "not_published"
    assert status["final_workbook"]["last_modified"].startswith("Wed, 22 Jul 2026")
    assert status["registration_list"]["state"] == "downloaded"
    assert len(status["run_date"]) == 10

    # An incremental run reuses the cache and still knows which month it came from
    main.run(full_refresh=False)
    status = json.loads((stubbed_run / "outputs" / "run_status.json").read_text())
    assert status["mmsdm_month"] == "2026-08" and status["dudetailsummary_source"] == "cache"
    assert status["registration_list"]["state"] == "cached"
