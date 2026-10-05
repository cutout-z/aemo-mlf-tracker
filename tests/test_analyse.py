"""Unit tests for src/analyse.py FY extraction and summary building."""

import pandas as pd
import pytest

from conftest import OPEN_ENDED, detail_rows
from src.analyse import build_summary, extract_fy_mlfs


def _fy(fy_mlfs: pd.DataFrame, duid: str, fy: str) -> pd.Series:
    row = fy_mlfs[(fy_mlfs["DUID"] == duid) & (fy_mlfs["FY"] == fy)]
    assert len(row) == 1, f"expected one {duid} {fy} row, got {len(row)}"
    return row.iloc[0]


# --- Battery (BIDIRECTIONAL) orientation ---------------------------------------

# CAPBES1 as it appears in DUDETAILSUMMARY (Aug 2026). AEMO's final 2026-27 workbook
# lists 2025-26 Import 1.0389 / Export 0.9774 and 2026-27 Import 1.0263 / Export 0.9783.
CAPBES1 = [
    ("CAPBES1", "2024-10-02", "2025-07-01", 1.0084, 0.9337, "BIDIRECTIONAL"),
    ("CAPBES1", "2025-07-01", "2025-09-09", 1.0389, 0.9774, "BIDIRECTIONAL"),
    ("CAPBES1", "2025-09-09", "2025-12-05", 1.0389, 0.9774, "BIDIRECTIONAL"),
    ("CAPBES1", "2025-12-05", "2026-07-01", 1.0389, 0.9774, "BIDIRECTIONAL"),
    ("CAPBES1", "2026-07-01", OPEN_ENDED, 1.0263, 0.9783, "BIDIRECTIONAL"),
]


def test_bidirectional_export_is_secondary_tlf(fy_range):
    fy_range(2024, 2026)
    fy = extract_fy_mlfs(detail_rows(*CAPBES1))
    rec = _fy(fy, "CAPBES1", "FY25-26")
    assert rec["MLF"] == pytest.approx(0.9774)
    assert rec["IMPORT_MLF"] == pytest.approx(1.0389)


def test_generator_keeps_tlf_as_export(fy_range):
    # Pumped-hydro style GENERATOR record carrying a SECONDARY_TLF (pump side)
    fy_range(2025, 2025)
    fy = extract_fy_mlfs(detail_rows(("SHGEN", "2025-07-01", "2026-07-01", 0.9737, 0.9877)))
    rec = _fy(fy, "SHGEN", "FY25-26")
    assert rec["MLF"] == pytest.approx(0.9737)
    assert rec["IMPORT_MLF"] == pytest.approx(0.9877)


def test_battery_yoy_matches_final_workbook_orientation(fy_range):
    # FY26-27 comes from the final workbook (export/import the right way round);
    # FY25-26 from DUDETAILSUMMARY must use the same orientation.
    fy_range(2024, 2026)
    final = pd.DataFrame({"DUID": ["CAPBES1"], "FINAL_MLF": [0.9783], "FINAL_IMPORT_MLF": [1.0263]})
    summary = build_summary(extract_fy_mlfs(detail_rows(*CAPBES1)), final_excel=final).set_index("DUID")
    assert summary.loc["CAPBES1", "YOY_CHANGE"] == pytest.approx(0.0009)
    assert summary.loc["CAPBES1", "IMPORT_YOY_CHANGE"] == pytest.approx(-0.0126)
