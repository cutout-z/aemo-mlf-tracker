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
