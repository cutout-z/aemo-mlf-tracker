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


# --- Value in effect for most of the FY -----------------------------------------

def test_mid_year_starter_takes_majority_value(fy_range):
    # QPSFB1 FY25-26: 1.0190 for 14 days, corrected to 0.9176 for the remaining 148.
    fy_range(2025, 2025)
    fy = extract_fy_mlfs(detail_rows(
        ("QPSFB1", "2026-01-20", "2026-02-03", 1.0190, 0.9176),
        ("QPSFB1", "2026-02-03", "2026-07-01", 0.9176),
    ))
    assert _fy(fy, "QPSFB1", "FY25-26")["MLF"] == pytest.approx(0.9176)


def test_days_are_summed_across_split_records(fy_range):
    # 0.95 runs 1 Jul–30 Sep then 1 Mar–30 Jun (≈ 214 days) in two records;
    # 0.97 runs 1 Oct–28 Feb (151 days) in one record: the longer total wins.
    fy_range(2025, 2025)
    fy = extract_fy_mlfs(detail_rows(
        ("SPLIT1", "2025-07-01", "2025-10-01", 0.95),
        ("SPLIT1", "2025-10-01", "2026-03-01", 0.97),
        ("SPLIT1", "2026-03-01", "2026-07-01", 0.95),
    ))
    assert _fy(fy, "SPLIT1", "FY25-26")["MLF"] == pytest.approx(0.95)


def test_tie_goes_to_later_value(fy_range):
    # Two values in effect for 90 days each: the later-effective one wins.
    fy_range(2025, 2025)
    fy = extract_fy_mlfs(detail_rows(
        ("TIE1", "2026-01-01", "2026-04-01", 0.98),
        ("TIE1", "2026-04-01", "2026-06-30", 0.99),
    ))
    assert _fy(fy, "TIE1", "FY25-26")["MLF"] == pytest.approx(0.99)


# --- Current FY never repeats last year's MLF -----------------------------------

# RACOMIL1 as DUDETAILSUMMARY showed it in April 2026 (FY25-26 record still
# open-ended) and in August 2026 (FY25-26 closed, FY26-27 loaded from 1 July).
RACOMIL1_APR = [
    ("RACOMIL1", "2024-07-01", "2025-07-01", 0.9922),
    ("RACOMIL1", "2025-07-01", OPEN_ENDED, 1.0026),
]
RACOMIL1_AUG = [
    ("RACOMIL1", "2024-07-01", "2025-07-01", 0.9922),
    ("RACOMIL1", "2025-07-01", "2026-07-01", 1.0026),
    ("RACOMIL1", "2026-07-01", OPEN_ENDED, 0.8765),
]


def test_open_ended_previous_record_does_not_fill_current_fy(fy_range):
    fy_range(2024, 2026)
    fy = extract_fy_mlfs(detail_rows(*RACOMIL1_APR))
    assert fy[fy["DUID"] == "RACOMIL1"]["FY"].tolist() == ["FY24-25", "FY25-26"]

    summary = build_summary(fy).set_index("DUID")
    assert "FY26-27" in summary.columns
    assert pd.isna(summary.loc["RACOMIL1", "FY26-27"])
    assert pd.isna(summary.loc["RACOMIL1", "YOY_CHANGE"])
    assert summary.loc["RACOMIL1", "PREV_MLF"] == pytest.approx(1.0026)


def test_current_fy_from_final_workbook_only_for_listed_duids(fy_range):
    fy_range(2024, 2026)
    fy = extract_fy_mlfs(detail_rows(
        *RACOMIL1_APR,
        ("OTHER1", "2025-07-01", OPEN_ENDED, 0.9500),
    ))
    final = pd.DataFrame({"DUID": ["OTHER1"], "FINAL_MLF": [0.9400]})
    summary = build_summary(fy, final_excel=final).set_index("DUID")
    assert summary.loc["OTHER1", "FY26-27"] == pytest.approx(0.9400)
    assert summary.loc["OTHER1", "YOY_CHANGE"] == pytest.approx(-0.0100)
    assert pd.isna(summary.loc["RACOMIL1", "FY26-27"])


def test_current_fy_record_effective_from_1_july_is_used(fy_range):
    fy_range(2024, 2026)
    summary = build_summary(extract_fy_mlfs(detail_rows(*RACOMIL1_AUG))).set_index("DUID")
    assert summary.loc["RACOMIL1", "FY26-27"] == pytest.approx(0.8765)
    assert summary.loc["RACOMIL1", "YOY_CHANGE"] == pytest.approx(-0.1261)


def test_mid_year_starter_in_current_fy_is_used(fy_range):
    fy_range(2025, 2026)
    fy = extract_fy_mlfs(detail_rows(("NEWSF1", "2026-09-15", OPEN_ENDED, 0.9300)))
    assert _fy(fy, "NEWSF1", "FY26-27")["MLF"] == pytest.approx(0.9300)


# --- DUIDs only in the final workbook ---------------------------------------------

def test_workbook_only_duid_takes_region_from_its_sheet(fy_range):
    # KIDSPHL1 (Kidston pump side) is in the final workbook's QLD Gen sheet but in neither
    # DUDETAILSUMMARY nor the registration list; SHPUMP is registered (NSW1).
    fy_range(2025, 2026)
    fy = extract_fy_mlfs(detail_rows(("OTHER1", "2025-07-01", OPEN_ENDED, 0.95)))
    final = pd.DataFrame({"DUID": ["OTHER1", "KIDSPHL1", "SHPUMP"], "REGIONID": ["NSW1", "QLD1", "NSW1"],
                          "FINAL_MLF": [0.94, 1.0394, 0.9892]})
    gens = pd.DataFrame({"DUID": ["SHPUMP"], "REGION": ["NSW1"], "DUID_TYPE": ["Scheduled Load"]})
    summary = build_summary(fy, gens, final_excel=final).set_index("DUID")
    assert summary.loc["KIDSPHL1", "REGIONID"] == "QLD1"
    assert summary.loc["SHPUMP", "REGIONID"] == "NSW1"
    assert summary.loc["KIDSPHL1", "FY26-27"] == pytest.approx(1.0394)


# --- DUIDs only in the draft workbook --------------------------------------------

def test_draft_only_duid_gets_a_row(fy_range):
    fy_range(2025, 2026)
    fy = extract_fy_mlfs(detail_rows(("OTHER1", "2026-07-01", OPEN_ENDED, 0.95)))
    draft = pd.DataFrame({"DUID": ["OTHER1", "NEWBESS1"], "REGIONID": ["NSW1", "VIC1"],
                          "INDICATIVE_MLF": [0.96, 0.97], "INDICATIVE_IMPORT_MLF": [None, 1.01]})
    summary = build_summary(fy, indicative=draft).set_index("DUID")
    assert summary.loc["NEWBESS1", "FY27-28 (Draft)"] == pytest.approx(0.97)
    assert summary.loc["NEWBESS1", "FY27-28 (Draft) Import"] == pytest.approx(1.01)
    assert summary.loc["NEWBESS1", "REGIONID"] == "VIC1"
    assert summary.loc["NEWBESS1", "STATUS"] == "Active"
    assert summary.loc["OTHER1", "FY27-28 (Draft)"] == pytest.approx(0.96)


# --- Retired status -------------------------------------------------------------

def test_unit_with_no_current_fy_mlf_is_retired(fy_range):
    # WESTCBT1's last record ran 1-15 July 2025: it has an FY25-26 MLF but none for FY26-27.
    fy_range(2023, 2026)
    fy = extract_fy_mlfs(detail_rows(
        ("WESTCBT1", "2024-07-01", "2025-07-01", 0.9954),
        ("WESTCBT1", "2025-07-01", "2025-07-15", 0.9961),
        ("LIVE1", "2025-07-01", "2026-07-01", 0.95),
        ("LIVE1", "2026-07-01", OPEN_ENDED, 0.94),
        ("NEXTYR1", "2025-07-01", OPEN_ENDED, 0.97),          # not in the final workbook, but in the draft
    ))
    draft = pd.DataFrame({"DUID": ["NEXTYR1"], "REGIONID": ["NSW1"], "INDICATIVE_MLF": [0.96]})
    summary = build_summary(fy, indicative=draft).set_index("DUID")
    assert summary["STATUS"].to_dict() == {"WESTCBT1": "Retired", "LIVE1": "Active", "NEXTYR1": "Active"}
