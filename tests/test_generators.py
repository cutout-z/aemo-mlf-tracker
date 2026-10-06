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


# --- MMSDM GENUNITS tier: gensets are rolled up to DUIDs through DUALLOC ----------

import io
import zipfile

from openpyxl import Workbook

REG_HEADER = ["Participant", "Station Name", "Region", "Dispatch Type", "Classification",
              "Fuel Source - Primary", "Fuel Source - Descriptor", "Technology Type - Descriptor",
              "DUID", "Reg Cap generation (MW)"]


def write_registration(path, rows) -> None:
    """A registration list with the primary sheet only (rows follow REG_HEADER)."""
    wb = Workbook()
    ws = wb.active
    ws.title = generators.PRIMARY_SHEET
    ws.append(REG_HEADER)
    for row in rows:
        ws.append(list(row))
    wb.save(path)


def mmsdm_zip(table: str, cols: list[str], rows: list[list]) -> bytes:
    """An MMSDM archive zip: I row then D rows (D, group, table, version, values...)."""
    lines = [f"C,SETP.WORLD,DVD_{table}", "I,PARTICIPANT_REGISTRATION," + table + ",1," + ",".join(cols)]
    lines += ["D,PARTICIPANT_REGISTRATION," + table + ",1," + ",".join(str(v) for v in r) for r in rows]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(f"PUBLIC_ARCHIVE#{table}#FILE01#202608010000.CSV", "\n".join(lines) + "\n")
    return buf.getvalue()


def genunit(gensetid, capacity, source="", gensettype="GENERATOR"):
    row = dict.fromkeys(generators.GENUNITS_COLS, "")
    row.update(GENSETID=gensetid, REGISTEREDCAPACITY=capacity, CO2E_ENERGY_SOURCE=source,
               GENSETTYPE=gensettype, DISPATCHTYPE="NET/NET")
    return [row[c] for c in generators.GENUNITS_COLS]


def serve_mmsdm(monkeypatch, genunits_rows, dualloc_rows, station_rows=(("QUERIVE", "Que River"),)):
    tables = {
        "GENUNITS": mmsdm_zip("GENUNITS", generators.GENUNITS_COLS, genunits_rows),
        "DUALLOC": mmsdm_zip("DUALLOC", generators.DUALLOC_COLS, dualloc_rows),
        "STATION": mmsdm_zip("STATION", generators.STATION_COLS, [
            [sid, name] + [""] * (len(generators.STATION_COLS) - 2) for sid, name in station_rows]),
    }
    monkeypatch.setattr(generators, "_download_mmsdm_zip",
                        lambda url: next((z for t, z in tables.items() if f"%23{t}%23" in url), None))


# QUERIVE1 (Que River, TAS; retired 2016) as MMSDM has it: three 2016 allocations, the last
# one of gensets QUERIVE1 and QUERIVE2 (24 MW each); QUERIVE3 (12 MW) dropped out on 18 Jun.
QUERIVE_GENUNITS = [genunit("QUERIVE1", 24, "Diesel oil"), genunit("QUERIVE2", 24, "Diesel oil"),
                    genunit("QUERIVE3", 12, "Diesel oil")]
QUERIVE_DUALLOC = [
    ["2016/04/14 00:00:00", 1, "QUERIVE1", "QUERIVE1", ""],
    ["2016/04/14 00:00:00", 1, "QUERIVE1", "QUERIVE2", ""],
    ["2016/05/03 00:00:00", 1, "QUERIVE1", "QUERIVE1", ""],
    ["2016/05/03 00:00:00", 1, "QUERIVE1", "QUERIVE2", ""],
    ["2016/05/03 00:00:00", 1, "QUERIVE1", "QUERIVE3", ""],
    ["2016/06/18 00:00:00", 1, "QUERIVE1", "QUERIVE1", ""],
    ["2016/06/18 00:00:00", 1, "QUERIVE1", "QUERIVE2", ""],
]


def test_genunits_are_rolled_up_to_duids_through_dualloc(tmp_path, monkeypatch):
    write_registration(tmp_path / "NEM-Registration-and-Exemption-List.xls", [])
    serve_mmsdm(monkeypatch, QUERIVE_GENUNITS, QUERIVE_DUALLOC)
    meta, _ = generators.fetch_generator_metadata(str(tmp_path), 2026, 8)
    meta = meta.set_index("DUID")
    assert meta.loc["QUERIVE1", "CAPACITY_MW"] == pytest.approx(48)   # not genset QUERIVE1's 24 MW
    assert meta.loc["QUERIVE1", "FUEL_CATEGORY"] == "Fossil"
    assert "QUERIVE2" not in meta.index                               # a genset, not a DUID


def test_unallocated_genset_and_missing_dualloc_fall_back_to_genset_ids(tmp_path, monkeypatch):
    write_registration(tmp_path / "NEM-Registration-and-Exemption-List.xls", [])
    serve_mmsdm(monkeypatch, QUERIVE_GENUNITS + [genunit("LONE1", 5, "Solar")], QUERIVE_DUALLOC)
    meta, _ = generators.fetch_generator_metadata(str(tmp_path), 2026, 8)
    assert meta.set_index("DUID").loc["LONE1", "CAPACITY_MW"] == pytest.approx(5)
    rolled = generators.units_by_duid(
        pd.DataFrame({"GENSETID": ["A1"], "REGISTEREDCAPACITY": [7.0]}), pd.DataFrame(columns=["DUID", "GENSETID"]))
    assert rolled.to_dict("records") == [{"DUID": "A1", "REGISTEREDCAPACITY": 7.0}]
