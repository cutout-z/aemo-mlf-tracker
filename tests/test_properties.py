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
    rows = []
    for i, (prev, latest, prev_imp, latest_imp) in enumerate(per_duid):
        for fy_year, m, imp in ((2024, prev, prev_imp), (2025, latest, latest_imp)):
            rows.append({
                "DUID": f"DUID{i}", "REGIONID": "NSW1", "CONNECTIONPOINTID": f"CP{i}", "STATIONID": f"ST{i}",
                "FY": f"FY{fy_year % 100:02d}-{(fy_year + 1) % 100:02d}", "FY_START_YEAR": fy_year,
                "MLF": m, "IMPORT_MLF": imp,
            })
    return pd.DataFrame(rows)


@given(per_duid=st.lists(st.tuples(opt_mlf, opt_mlf, opt_mlf, opt_mlf), min_size=1, max_size=15))
def test_yoy_change_is_latest_minus_previous(per_duid):
    nan = float("nan")
    per_duid = [tuple(nan if v is None else v for v in t) for t in per_duid]
    # An FY column with no MLF at all is dropped by the pivot and no YoY column exists: not the case under test.
    assume(any(not math.isnan(t[0]) for t in per_duid) and any(not math.isnan(t[1]) for t in per_duid))
    with mock.patch.object(config, "FY_START", 2024), mock.patch.object(config, "FY_END", 2025):
        summary = build_summary(_fy_frame(per_duid)).set_index("DUID")
    for i, (prev, latest, prev_imp, latest_imp) in enumerate(per_duid):
        r = summary.loc[f"DUID{i}"]
        if math.isnan(prev) or math.isnan(latest):
            assert pd.isna(r["YOY_CHANGE"])
        else:
            assert r["YOY_CHANGE"] == pytest.approx(latest - prev, abs=0.001)
        if "IMPORT_YOY_CHANGE" in summary.columns:
            if math.isnan(prev_imp) or math.isnan(latest_imp):
                assert pd.isna(r["IMPORT_YOY_CHANGE"])
            else:
                assert r["IMPORT_YOY_CHANGE"] == pytest.approx(latest_imp - prev_imp, abs=0.001)
