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
