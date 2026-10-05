"""Metadata caches are refreshed on --full-refresh and survive a failed refresh (no network)."""

import pandas as pd
import pytest
import requests

from src import config, generators


class _Resp:
    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self):
        pass


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(config, "RETRY_BACKOFF", 0)


def test_cached_registration_list_is_reused_without_refresh(tmp_path, monkeypatch):
    (tmp_path / "NEM-Registration-and-Exemption-List.xls").write_bytes(b"old")
    monkeypatch.setattr(generators.requests, "get", lambda *a, **k: pytest.fail("should not fetch"))
    path = generators._download_xls(str(tmp_path))
    assert path.read_bytes() == b"old"


def test_refresh_replaces_a_stale_registration_list(tmp_path, monkeypatch):
    (tmp_path / "NEM-Registration-and-Exemption-List.xls").write_bytes(b"old")
    monkeypatch.setattr(generators.requests, "get", lambda *a, **k: _Resp(b"new"))
    path = generators._download_xls(str(tmp_path), refresh=True)
    assert path.read_bytes() == b"new"


def test_failed_refresh_keeps_the_cached_list(tmp_path, monkeypatch, caplog):
    (tmp_path / "NEM-Registration-and-Exemption-List.xls").write_bytes(b"old")

    def boom(*a, **k):
        raise requests.ConnectionError("blocked")
    monkeypatch.setattr(generators.requests, "get", boom)
    with caplog.at_level("WARNING"):
        path = generators._download_xls(str(tmp_path), refresh=True)
    assert path.read_bytes() == b"old" and "keeping the cached copy" in caplog.text


def test_failed_refresh_with_no_cache_raises(tmp_path, monkeypatch):
    def boom(*a, **k):
        raise requests.ConnectionError("blocked")
    monkeypatch.setattr(generators.requests, "get", boom)
    with pytest.raises(RuntimeError, match="registration list"):
        generators._download_xls(str(tmp_path), refresh=True)


def test_mmsdm_tables_refetched_on_refresh_and_cache_kept_on_failure(tmp_path, monkeypatch):
    pd.DataFrame({"STATIONID": ["OLD"], "STATIONNAME": ["Old station"]}).to_feather(tmp_path / "mmsdm_station.feather")
    pd.DataFrame({"GENSETID": ["G1"], "STATIONID": ["OLD"], "REGISTEREDCAPACITY": [10.0],
                  "CO2E_ENERGY_SOURCE": ["Solar"], "DISPATCHTYPE": ["GENERATOR"]}).to_feather(tmp_path / "mmsdm_genunits.feather")
    calls = []
    monkeypatch.setattr(generators, "_download_mmsdm_zip", lambda url: calls.append(url))  # returns None: failed
    names, units = generators.fetch_mmsdm_participant_metadata(str(tmp_path), 2026, 8, refresh=True)
    assert len(calls) == 2                       # both tables were re-fetched
    assert names.to_dict() == {"OLD": "Old station"} and list(units["GENSETID"]) == ["G1"]   # cache kept
    calls.clear()
    generators.fetch_mmsdm_participant_metadata(str(tmp_path), 2026, 8)
    assert calls == []                           # no refresh: cache reused, nothing fetched
