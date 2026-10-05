"""Shared fixtures for the pipeline unit tests (synthetic data, no network)."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import config  # noqa: E402

OPEN_ENDED = "2099-12-31"  # download.py clamps AEMO's 2999/12/31 sentinel to this


def detail_rows(*records) -> pd.DataFrame:
    """Build a DUDETAILSUMMARY-shaped frame from (DUID, start, end, TLF[, SECONDARY_TLF[, DISPATCHTYPE]])."""
    rows = []
    for rec in records:
        duid, start, end, tlf, *rest = rec
        secondary = rest[0] if len(rest) > 0 else None
        dispatch_type = rest[1] if len(rest) > 1 else "GENERATOR"
        rows.append({
            "DUID": duid,
            "START_DATE": pd.Timestamp(start),
            "END_DATE": pd.Timestamp(end),
            "DISPATCHTYPE": dispatch_type,
            "CONNECTIONPOINTID": f"CP_{duid}",
            "REGIONID": "NSW1",
            "STATIONID": f"ST_{duid}",
            "TRANSMISSIONLOSSFACTOR": tlf,
            "SECONDARY_TLF": float("nan") if secondary is None else secondary,
        })
    return pd.DataFrame(rows)


@pytest.fixture
def fy_range(monkeypatch):
    """Pin the tracked FY range so tests don't depend on today's date."""
    def _set(start: int, end: int):
        monkeypatch.setattr(config, "FY_START", start)
        monkeypatch.setattr(config, "FY_END", end)
    return _set
