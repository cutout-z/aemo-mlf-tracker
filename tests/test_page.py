"""index.html logic run under node against synthetic rows (skipped when node is absent).

Only the named functions are lifted out of the page's script, with a minimal `document` stub,
so these tests pin the arithmetic of the page, not its layout (the design gates cover that).
"""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

PAGE = Path(__file__).resolve().parent.parent / "index.html"
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="node not installed")


def page_function(name: str) -> str:
    src = PAGE.read_text()
    m = re.search(rf"^function {name}\(.*?^}}$", src, re.S | re.M)
    assert m, f"function {name} not found in index.html"
    return m.group(0)


def run_page(functions: list[str], setup: str, expr: str):
    script = "\n".join([
        "const els = {};",
        "const document = { getElementById: id => (els[id] = els[id] || { value: '', checked: false, innerHTML: '' }) };",
        "const REGIONS = {NSW1:'NSW',QLD1:'QLD',VIC1:'VIC',SA1:'SA',TAS1:'TAS'};",
        "let activeRegion = 'ALL';",
        *[page_function(f) for f in functions],
        setup,
        f"process.stdout.write(JSON.stringify({expr}));",
    ])
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


def tiles(html: str) -> list[str]:
    """The text of the four stat tiles: value, label, scope line."""
    cells = re.findall(r'<div class="kpi-value">(.*?)</div><div class="kpi-label">(.*?)</div><div[^>]*>(.*?)</div>', html)
    return [" | ".join(c) for c in cells]


ROWS = [
    # two generators, a pump (load MLF on the price it PAYS), a dummy generator at 1.0000
    {"DUID": "GEN1", "DUID_TYPE": "Generator", "REGIONID": "NSW1", "STATUS": "Active", "FY25-26": "0.9600", "FY26-27": "0.9500", "YOY_CHANGE": "-0.0100", "FUEL_CATEGORY": "Solar", "STATION_NAME": "Gen one"},
    {"DUID": "GEN2", "DUID_TYPE": "Generator", "REGIONID": "NSW1", "STATUS": "Active", "FY25-26": "0.9700", "FY26-27": "0.9700", "YOY_CHANGE": "0.0000", "FUEL_CATEGORY": "Wind", "STATION_NAME": "Gen two"},
    {"DUID": "PUMP1", "DUID_TYPE": "Scheduled Load", "REGIONID": "QLD1", "STATUS": "Active", "FY25-26": "0.9000", "FY26-27": "0.8000", "YOY_CHANGE": "-0.1000", "FUEL_CATEGORY": "Other", "STATION_NAME": "Wivenhoe pump"},
    {"DUID": "DG_SA1", "DUID_TYPE": "Dummy Generator", "REGIONID": "SA1", "STATUS": "Active", "FY25-26": "1.0000", "FY26-27": "1.0000", "YOY_CHANGE": "0.0000", "FUEL_CATEGORY": "", "STATION_NAME": "SA1 Dummy Generator"},
]


def render_stats(rows):
    setup = f"let allData = {json.dumps(rows)}; let fyCols = ['FY25-26', 'FY26-27']; renderStats();"
    return run_page(["getBaseRows", "renderStats"], setup, "{stats: els.stats.innerHTML, foot: els.statsFoot.innerHTML}")


def test_mlf_tiles_read_generating_units_only():
    out = render_stats(ROWS)
    t = tiles(out["stats"])
    assert t[0].startswith("4 | DUIDs live")                               # the count tile covers every live DUID
    assert t[1] == "0.9600 | Average MLF, FY26-27 | 4.0% of price lost · simple mean of 2 live generating units"
    assert t[2].startswith("0.9500 | Deepest loss, FY26-27 | GEN1")        # not the pump at 0.8000
    assert t[3] == "1 | Worse than FY25-26 | of 2 generating units with both years · 0 better · 1 unchanged"
    assert "2 live loads, network load points, dummy generators and interconnector units" in out["foot"]


def test_search_finds_a_battery_by_its_previous_duid():
    rows = [
        {"DUID": "HPR1", "PREVIOUS_DUIDS": "HPRG1", "STATION_NAME": "Hornsdale Power Reserve", "STATUS": "Active", "REGIONID": "SA1"},
        {"DUID": "LBB1", "PREVIOUS_DUIDS": "", "STATION_NAME": "Lake Bonney BESS", "STATUS": "Active", "REGIONID": "SA1"},
    ]
    setup = f"let allData = {json.dumps(rows)}; document.getElementById('search').value = 'hprg1';"
    assert run_page(["getBaseRows", "getFiltered"], setup, "getFiltered().map(r => r.DUID)") == ["HPR1"]


def run_status_parts(status):
    return run_page(["fmtDay", "runStatusParts"], f"const s = {json.dumps(status)};", "runStatusParts(s)")


STATUS = {
    "run_date": "2026-10-07", "mmsdm_month": "2026-08",
    "final_workbook": {"fy": "2026-27", "state": "published", "last_modified": "Wed, 22 Jul 2026 05:12:00 GMT"},
    "draft_workbook": {"fy": "2027-28", "state": "not_published"},
    "registration_list": {"state": "downloaded", "file_date": "2026-10-07"},
}


def test_footer_states_what_the_data_was_built_from():
    assert run_status_parts(STATUS) == [
        "Data as of the 2026-08 MMSDM archive",
        "final 2026-27 MLFs: AEMO workbook dated 22 Jul 2026",
        "draft 2027-28 not yet published",
        "refreshed 7 Oct 2026",
    ]


def test_footer_names_a_blocked_draft_and_a_failed_registration_refresh():
    status = {**STATUS, "draft_workbook": {"fy": "2027-28", "state": "blocked"},
              "registration_list": {"state": "refresh_failed", "file_date": "2026-09-22"}}
    parts = run_status_parts(status)
    assert "draft 2027-28 could not be downloaded (blocked), so it is left out" in parts
    assert "registration list refresh failed: fuel and capacity from the copy fetched 22 Sep 2026" in parts


def test_footer_says_a_draft_blocked_in_march_is_out_but_missing():
    status = {**STATUS, "run_date": "2027-03-08", "draft_workbook": {"fy": "2027-28", "state": "blocked"}}
    assert ("draft 2027-28 is out (AEMO publishes it early in March) but could not be downloaded (blocked), "
            "so it is left out") in run_status_parts(status)
