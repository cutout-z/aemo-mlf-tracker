"""Unit tests for the cross-checks in tests/validate_outputs.py."""

import pandas as pd
import pytest

import validate_outputs as vo
from conftest import GEN_HEADER, write_mlf_workbook
from test_indicative import BDU_HEADER


@pytest.fixture(autouse=True)
def fresh_errors(monkeypatch):
    monkeypatch.setattr(vo, "errors", [])
    return vo.errors


def _summary(n=20, prev=0.95, step=0.01, cur=None):
    """n generators whose MLF moves by `step` into the current FY (or `cur` for all)."""
    return pd.DataFrame({
        "DUID": [f"G{i}" for i in range(n)],
        "FY25-26": [prev] * n,
        "FY26-27": [cur if cur is not None else prev - step] * n,
    })


def test_current_fy_passes_when_values_move():
    vo.check_current_fy(_summary())
    assert vo.errors == []


def test_current_fy_copied_forward_fails():
    vo.check_current_fy(_summary(step=0.0))
    assert any("copied forward" in e for e in vo.errors)


def test_current_fy_blank_fails():
    vo.check_current_fy(_summary(cur=float("nan")))
    assert any("workbook missing" in e for e in vo.errors)


@pytest.fixture
def cached_workbook(tmp_path, monkeypatch):
    monkeypatch.setattr(vo, "DATA_DIR", tmp_path)
    write_mlf_workbook(tmp_path / "final_mlf_2026-27.xlsx", {
        "NSW Gen": [GEN_HEADER] + [[f"Farm {i}", 132, f"G{i}", "CP", "TNI", 0.94, 0.95] for i in range(20)],
        "ACT Gen": [GEN_HEADER, BDU_HEADER] + [
            [f"Battery {i}", 132, f"B{i}", "CP", "TNI", 1.0263, 0.9783, 1.0389, 0.9774] for i in range(5)
        ],
    })


def _with_batteries(export, import_):
    df = _summary()
    bdu = pd.DataFrame({
        "DUID": [f"B{i}" for i in range(5)],
        "FY25-26": [export] * 5, "FY25-26 Import": [import_] * 5,
        "FY26-27": [0.9783] * 5, "FY26-27 Import": [1.0263] * 5,
    })
    return pd.concat([df, bdu], ignore_index=True)


def test_workbook_cross_check_passes(cached_workbook):
    vo.check_against_final_workbook(_with_batteries(0.9774, 1.0389))
    assert vo.errors == []


def test_workbook_cross_check_catches_swapped_batteries(cached_workbook):
    vo.check_against_final_workbook(_with_batteries(1.0389, 0.9774))
    assert any("swapped" in e for e in vo.errors)


def test_workbook_cross_check_skipped_without_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(vo, "DATA_DIR", tmp_path)
    vo.check_against_final_workbook(_with_batteries(1.0389, 0.9774))
    assert vo.errors == []


# --- Input age: checked against the run date in outputs/run_status.json ------------

def _status(run_date="2026-10-07", month="2026-08", reg_date="2026-10-07", reg_state="downloaded"):
    return {"run_date": run_date, "mmsdm_month": month,
            "registration_list": {"state": reg_state, "file_date": reg_date}}


def test_fresh_inputs_pass():
    vo.check_input_freshness(_summary(), _status())
    assert vo.errors == []


def test_archive_month_75_days_old_still_passes():
    # 2026_08 ended 31 Aug; 14 Nov is 75 days on (2026_09 would be out by then, but late months happen)
    vo.check_input_freshness(_summary(), _status(run_date="2026-11-14", reg_date="2026-11-14"))
    assert vo.errors == []


def test_old_archive_month_fails():
    vo.check_input_freshness(_summary(), _status(run_date="2026-11-15", reg_date="2026-11-15"))
    assert any("MMSDM archive 2026-08" in e for e in vo.errors)


def test_unknown_archive_month_fails():
    vo.check_input_freshness(_summary(), _status(month=None))
    assert any("which MMSDM archive month" in e for e in vo.errors)


def test_latest_final_fy_may_be_current_or_next():
    vo.check_input_freshness(_summary(), _status(run_date="2027-05-01", month="2027-03", reg_date="2027-05-01"))
    assert vo.errors == []       # FY26-27 is current until 30 June


def test_latest_final_fy_a_year_behind_fails():
    vo.check_input_freshness(_summary(), _status(run_date="2027-08-20", month="2027-06", reg_date="2027-08-20"))
    assert any("Latest final MLF year is FY26-27" in e for e in vo.errors)


def test_old_registration_list_fails():
    vo.check_input_freshness(_summary(), _status(reg_date="2026-07-01", reg_state="refresh_failed"))
    assert any("registration list in use was fetched on 2026-07-01" in e and "refresh_failed" in e
               for e in vo.errors)


def test_missing_run_status_fails(tmp_path):
    assert vo.load_run_status(tmp_path / "run_status.json") is None
    assert any("run_status.json does not exist" in e for e in vo.errors)


# --- Generator metadata: a run without it must not pass ----------------------------

def _with_metadata(n=20, fuel="Solar", capacity=50.0, n_blank=0):
    df = _summary(n)
    df["DUID_TYPE"] = "Generator"
    df["STATUS"] = "Active"
    df["FUEL_CATEGORY"] = [fuel] * (n - n_blank) + [None] * n_blank
    df["CAPACITY_MW"] = capacity
    return df


def test_metadata_coverage_passes():
    vo.check_metadata_coverage(_with_metadata(n_blank=1))     # 95%, as in the 2026-10 output
    assert vo.errors == []


def test_missing_fuel_fails():
    vo.check_metadata_coverage(_with_metadata(n_blank=5))
    assert any("fuel category" in e for e in vo.errors)


def test_no_metadata_at_all_fails():
    # what build_summary produces when generators is None: every DUID typed by name or Unknown
    df = _summary()
    df["DUID_TYPE"] = "Unknown"
    vo.check_metadata_coverage(df)
    assert any("generator metadata missing" in e for e in vo.errors)
