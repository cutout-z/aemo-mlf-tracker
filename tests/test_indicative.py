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
