"""Property tests: MLF orientation and year-on-year change (synthetic data only).

Dev-only (Hypothesis); skipped visibly where it is not installed.
"""

import math
from unittest import mock

import pandas as pd
import pytest

pytest.importorskip("hypothesis", reason="hypothesis is a dev-only dependency (requirements-dev.txt)")

import hypothesis_gate  # noqa: E402,F401  (loads the fixed profile)
from hypothesis import assume, given, strategies as st  # noqa: E402

from conftest import OPEN_ENDED, detail_rows  # noqa: E402
from src import config  # noqa: E402
from src.analyse import _orient_mlfs, build_summary, extract_fy_mlfs  # noqa: E402

mlf = st.floats(min_value=0.5, max_value=1.5, allow_nan=False).map(lambda v: round(v, 4))
opt_mlf = st.one_of(st.none(), mlf)
dispatch = st.sampled_from(["GENERATOR", "BIDIRECTIONAL", "LOAD"])


def _same(a, b):
    return (pd.isna(a) and pd.isna(b)) or a == b


# ----------------------------------------------------------------------------- orientation
@given(recs=st.lists(st.tuples(mlf, opt_mlf, dispatch), min_size=1, max_size=15))
def test_orient_mlfs_orientation_invariant(recs):
    rows = [(f"DUID{i}", "2020-07-01", OPEN_ENDED, tlf, sec, kind) for i, (tlf, sec, kind) in enumerate(recs)]
    out = _orient_mlfs(detail_rows(*rows))
    for (tlf, sec, kind), (_, r) in zip(recs, out.iterrows()):
        sec = float("nan") if sec is None else sec
        if kind == "BIDIRECTIONAL":
            # batteries: TLF is the Import (load) MLF, SECONDARY_TLF the Export (generation) MLF
            assert _same(r["EXPORT_MLF"], sec) and _same(r["IMPORT_MLF"], tlf)
        else:
            assert _same(r["EXPORT_MLF"], tlf) and _same(r["IMPORT_MLF"], sec)


@given(tlf=mlf, sec=mlf)
def test_bidirectional_export_reaches_fy_row(tlf, sec):
    with mock.patch.object(config, "FY_START", 2024), mock.patch.object(config, "FY_END", 2024):
        fy = extract_fy_mlfs(detail_rows(("BAT1", "2024-07-01", OPEN_ENDED, tlf, sec, "BIDIRECTIONAL")))
    row = fy.iloc[0]
    assert row["MLF"] == sec and row["IMPORT_MLF"] == tlf


# ----------------------------------------------------------------------------- YoY
def _fy_frame(per_duid):
    """per_duid: [(export y0, y1, y2, import y0, y1, y2)] for FY23-24, FY24-25, FY25-26."""
    rows = []
    for i, t in enumerate(per_duid):
        for k, fy_year in enumerate((2023, 2024, 2025)):
            rows.append({
                "DUID": f"DUID{i}", "REGIONID": "NSW1", "CONNECTIONPOINTID": f"CP{i}", "STATIONID": f"ST{i}",
                "FY": f"FY{fy_year % 100:02d}-{(fy_year + 1) % 100:02d}", "FY_START_YEAR": fy_year,
                "MLF": t[k], "IMPORT_MLF": t[3 + k],
            })
    return pd.DataFrame(rows)


@given(per_duid=st.lists(st.tuples(*[opt_mlf] * 6), min_size=1, max_size=15))
def test_yoy_change_is_latest_minus_previous(per_duid):
    nan = float("nan")
    per_duid = [tuple(nan if v is None else v for v in t) for t in per_duid]
    # An all-NaN PREVIOUS FY is dropped by the pivot, so no YoY column exists: not the case under test.
    # (An all-NaN latest FY still yields NaN YOY_CHANGE: the current FY column is always added.)
    assume(any(not math.isnan(t[1]) for t in per_duid))
    with mock.patch.object(config, "FY_START", 2023), mock.patch.object(config, "FY_END", 2025):
        summary = build_summary(_fy_frame(per_duid)).set_index("DUID")
    # latest = FY25-26 (index 2), previous = FY24-25 (index 1); FY23-24 must not be used
    for i, t in enumerate(per_duid):
        r = summary.loc[f"DUID{i}"]
        prev, latest, prev_imp, latest_imp = t[1], t[2], t[4], t[5]
        if math.isnan(prev) or math.isnan(latest):
            assert pd.isna(r["YOY_CHANGE"]) and pd.isna(r["YOY_PCT_CHANGE"])
        else:
            change = round(latest - prev, 4)
            assert r["YOY_CHANGE"] == pytest.approx(change, abs=5.1e-5)
            assert r["YOY_PCT_CHANGE"] == pytest.approx(round(change / prev * 100, 2), abs=0.0101)
    both_import = [t for t in per_duid if not (math.isnan(t[4]) or math.isnan(t[5]))]
    if both_import:
        assert "IMPORT_YOY_CHANGE" in summary.columns, "import YoY column missing though import MLFs exist"
    if "IMPORT_YOY_CHANGE" in summary.columns:
        for i, t in enumerate(per_duid):
            r = summary.loc[f"DUID{i}"]
            if math.isnan(t[4]) or math.isnan(t[5]):
                assert pd.isna(r["IMPORT_YOY_CHANGE"])
            else:
                assert r["IMPORT_YOY_CHANGE"] == pytest.approx(round(t[5] - t[4], 4), abs=5.1e-5)
