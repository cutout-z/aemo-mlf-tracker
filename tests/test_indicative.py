"""Unit tests for src/indicative.py (MLF workbook download and parsing; no network)."""

import pytest
import requests

from conftest import GEN_HEADER, write_mlf_workbook
from src import config, indicative


class _Resp:
    def __init__(self, status_code: int, content: bytes = b""):
        self.status_code = status_code
        self.content = content


@pytest.fixture
def fake_get(monkeypatch):
    """Replace requests.get in src.indicative with a canned response (or exception)."""
    monkeypatch.setattr(config, "FY_END", 2026)
    monkeypatch.setattr(config, "RETRY_BACKOFF", 0)
    calls = []

    def _install(response):
        def _get(url, **kwargs):
            calls.append(url)
            if isinstance(response, Exception):
                raise response
            return response
        monkeypatch.setattr(indicative.requests, "get", _get)
        return calls
    return _install


def _xlsx_bytes(tmp_path) -> bytes:
    path = tmp_path / "src.xlsx"
    write_mlf_workbook(path, {"NSW Gen": [GEN_HEADER, ["Solar Farm", 132, "SOLAR1", "NCP1", "NCP", 0.95, 0.96]]})
    return path.read_bytes()


# --- Final workbook: required from April, so any failure stops the run -----------

@pytest.mark.parametrize("status", [403, 404, 500])
def test_final_workbook_http_error_fails(fake_get, tmp_path, status):
    fake_get(_Resp(status))
    with pytest.raises(RuntimeError, match=f"HTTP {status}"):
        indicative.download_final_mlfs(str(tmp_path))
    assert not (tmp_path / "final_mlf_2026-27.xlsx").exists()


def test_final_workbook_challenge_page_fails(fake_get, tmp_path):
    fake_get(_Resp(200, b"<!DOCTYPE html><title>Just a moment...</title>"))
    with pytest.raises(RuntimeError, match="not an xlsx"):
        indicative.download_final_mlfs(str(tmp_path))
    assert not (tmp_path / "final_mlf_2026-27.xlsx").exists()


def test_final_workbook_network_error_fails(fake_get, tmp_path):
    calls = fake_get(requests.ConnectionError("boom"))
    with pytest.raises(RuntimeError, match="Could not download"):
        indicative.download_final_mlfs(str(tmp_path))
    assert len(calls) == config.MAX_RETRIES


def test_final_workbook_downloads_and_parses(fake_get, tmp_path):
    fake_get(_Resp(200, _xlsx_bytes(tmp_path)))
    final = indicative.download_final_mlfs(str(tmp_path))
    assert final.set_index("DUID").loc["SOLAR1", "FINAL_MLF"] == pytest.approx(0.95)


# --- Draft workbook: optional, but only a genuine 404 means "not published" ------

def test_draft_workbook_404_is_not_yet_published(fake_get, tmp_path):
    fake_get(_Resp(404))
    assert indicative.download_draft_mlfs(str(tmp_path)) is None


def test_draft_workbook_403_fails(fake_get, tmp_path):
    fake_get(_Resp(403))
    with pytest.raises(RuntimeError, match="HTTP 403"):
        indicative.download_draft_mlfs(str(tmp_path))


# --- Duplicate DUIDs: keep the row that applies from 1 July ---------------------

BDU_HEADER = ["Generator", "Voltage (kV)", "DUID", "Connection Point ID", "TNI code",
              "2026-27 Import MLF", "2026-27 Export MLF", "2025-26 Import MLF", "2025-26 Export MLF"]


def _parse(tmp_path, sheets):
    path = tmp_path / "final.xlsx"
    write_mlf_workbook(path, sheets)
    return indicative._parse_mlf_excel(path, "2026-27", "FINAL_MLF").set_index("DUID")


def test_superseded_row_is_dropped(tmp_path):
    # As in AEMO's final 2026-27 workbook (QLD Gen rows 123-124 / NSW Gen rows 34-35)
    final = _parse(tmp_path, {
        "QLD Gen": [
            GEN_HEADER,
            ["Wandoan South Solar Farm 1 (as published on 01/04/2026)", 275, "WANDSF1", "QWSR1W", "QWSR", 0.9904, 0.9975],
            ["Wandoan South Solar Farm 1 (effective from 01/07/2026)", 275, "WANDSF1", "QWSR1W", "QWSR", 0.9850, 0.9975],
        ],
        "NSW Gen": [
            GEN_HEADER,
            ["Corowa Solar Farm (as published on 01/04/2026))", 132, "CRWASF1", "NAL11C", "NAL1", 0.8899, 0.9345],
            ["Corowa Solar Farm (effective from 01/07/2026)", 132, "CRWASF1", "NAL11C", "NAL1", 0.8912, 0.9345],
            ["Plain Solar Farm", 132, "PLAIN1", "NPL1", "NPL", 0.9700, 0.9800],
        ],
    })
    assert final.index.is_unique
    assert final.loc["WANDSF1", "FINAL_MLF"] == pytest.approx(0.9850)
    assert final.loc["CRWASF1", "FINAL_MLF"] == pytest.approx(0.8912)
    assert final.loc["PLAIN1", "FINAL_MLF"] == pytest.approx(0.9700)


def test_revision_order_in_sheet_does_not_matter(tmp_path):
    final = _parse(tmp_path, {"VIC Gen": [
        GEN_HEADER,
        ["Farm (effective from 01/07/2026)", 66, "FARM1", "V1", "V", 0.9800, 0.99],
        ["Farm (as published on 01/04/2026)", 66, "FARM1", "V1", "V", 0.9900, 0.99],
    ]})
    assert final.loc["FARM1", "FINAL_MLF"] == pytest.approx(0.9800)


def test_mid_year_revision_does_not_replace_1_july_value(tmp_path):
    final = _parse(tmp_path, {"SA Gen": [
        GEN_HEADER,
        ["Wind Farm", 275, "WIND1", "S1", "S", 0.9600, 0.97],
        ["Wind Farm (effective from 01/10/2026)", 275, "WIND1", "S1", "S", 0.9500, 0.97],
    ]})
    assert final.loc["WIND1", "FINAL_MLF"] == pytest.approx(0.9600)


def test_undated_duplicates_keep_first(tmp_path):
    # Several TAS stations share one aggregated DUID with the same MLF
    final = _parse(tmp_path, {"TAS Gen": [
        GEN_HEADER,
        ["Catagunya", 220, "LI_WY_CA", "TLI11", "TLI1", 1.0052, 0.9855],
        ["Liapootah", 220, "LI_WY_CA", "TLI11", "TLI1", 1.0052, 0.9855],
    ]})
    assert len(final) == 1
    assert final.loc["LI_WY_CA", "FINAL_MLF"] == pytest.approx(1.0052)


def test_battery_section_keeps_export_and_import(tmp_path):
    final = _parse(tmp_path, {"ACT Gen": [
        GEN_HEADER,
        BDU_HEADER,
        ["Capital ESS", 132, "CAPBES1", "NQBC3C", "NQBC", 1.0263, 0.9783, 1.0389, 0.9774],
    ]})
    assert final.loc["CAPBES1", "FINAL_MLF"] == pytest.approx(0.9783)
    assert final.loc["CAPBES1", "FINAL_IMPORT_MLF"] == pytest.approx(1.0263)
