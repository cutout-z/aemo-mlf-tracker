#!/usr/bin/env python3
"""Behaviour check for the AEMO MLF Tracker — every interaction a design pass must NOT lose:
the region tabs, the type/fuel/region filters, search, the retired-DUID toggle, column sorting,
select-all, the XLSX export of the selection, the sticky header and the six Excel/CSV downloads.

    cd ~/Design/"AEMO MLF Tracker" && python3 -m http.server 9380 --bind 127.0.0.1 &
    /opt/anaconda3/bin/python3 scripts/verify-interactions.py

Green TODAY, on the unstyled page, and it must still be green at handback. Every expected number is
read from outputs/summary.csv, so a restyle that re-maps a column fails. Reads only; writes nothing
(the XLSX is captured, never saved). Exit 1 on any breakage.
"""
from __future__ import annotations

import csv
import pathlib
import sys

from playwright.sync_api import sync_playwright

ROOT = pathlib.Path(__file__).resolve().parent.parent
URL = "http://127.0.0.1:9380/index.html"
CSV = ROOT / "outputs" / "summary.csv"

fails: list[str] = []


def check(ok: bool, name: str, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not ok:
        fails.append(name)


rows = list(csv.DictReader(CSV.open()))
fy_cols = sorted({c for c in rows[0] if c.startswith("FY") and "Import" not in c and "Draft" not in c})
latest = fy_cols[-1]

# the page hides retired DUIDs until the toggle is on, and splits batteries into their own table
live = [r for r in rows if r["STATUS"] != "Retired"]
live_gen = [r for r in live if r["FUEL_CATEGORY"] != "Battery"]
live_batt = [r for r in live if r["FUEL_CATEGORY"] == "Battery"]
region = lambda code: [r for r in live if r["REGIONID"] == code]


def first_by_sort_key(rs):
    """The page's default sort is the latest FY ascending, with blanks pushed to the end."""
    have = [r for r in rs if r[latest].strip()]
    return min(have, key=lambda r: float(r[latest]))


n_visible = len(live)
top = first_by_sort_key(live_gen)
duid_min = min(r["DUID"] for r in live_gen if r["DUID"].strip())
duid_max = max(r["DUID"] for r in live_gen if r["DUID"].strip())
n_wind = sum(1 for r in live if r["FUEL_CATEGORY"] == "Wind")
n_solar = sum(1 for r in live if r["FUEL_CATEGORY"] == "Solar")
n_type = sum(1 for r in live if (r["DUID_TYPE"] or "Unknown") == "Unknown")
n_retired = len(rows) - n_visible
print(f"csv: {len(rows)} DUIDs · {n_visible} live ({n_retired} retired) · "
      f"generators {len(live_gen)} · batteries {len(live_batt)} · latest {latest}")

with sync_playwright() as pw:
    br = pw.chromium.launch()
    pg = br.new_page(viewport={"width": 1440, "height": 900})
    errors: list[str] = []
    pg.on("pageerror", lambda e: errors.append(str(e)))
    pg.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
    pg.goto(URL, wait_until="networkidle", timeout=60000)
    pg.wait_for_timeout(1800)
    # Count only rows the user can actually see: the page hides a section that has no rows
    # (`section.style.display='none'; return`) WITHOUT clearing its tbody, so stale rows from the
    # previous filter stay in the DOM. Counting them is how a gate reports 169 rows for a 107-row filter.
    vis = "e => e.filter(x => x.offsetParent !== null).length"
    gen_rows = lambda: pg.eval_on_selector_all("#genTbody tr", vis)
    batt_rows = lambda: pg.eval_on_selector_all("#battTbody tr", vis)
    total = lambda: gen_rows() + batt_rows()
    gen_duids = lambda: pg.eval_on_selector_all(
        "#genTbody tr", "e => e.filter(x => x.offsetParent !== null)"
                        ".map(x => x.getAttribute('data-duid') || (x.children[1] || {}).innerText || '')")

    print("data")
    check(total() == n_visible, f"all {n_visible} live assets render across the two tables", f"{total()} rows")
    check(gen_rows() == len(live_gen) and batt_rows() == len(live_batt),
          "the two tables split generators from batteries by fuel",
          f"generators {gen_rows()}/{len(live_gen)} · batteries {batt_rows()}/{len(live_batt)}")
    stats = pg.inner_text("#stats")
    check(str(n_solar) in stats and str(n_wind) in stats,
          "the stat tiles quote the CSV fuel counts", " ".join(stats.split())[:100])
    check(gen_duids()[0] == top["DUID"],
          f"the default sort ({latest}, ascending) puts the deepest loss first",
          f"{gen_duids()[0]} vs {top['DUID']} ({top[latest]})")

    print("tabs and the region filter")
    for code in ("NSW1", "VIC1", "TAS1"):
        pg.evaluate("""(label) => { const b = [...document.querySelectorAll('#tabs button, #tabs .tab')]
            .find(x => x.innerText.trim() === label); b.click(); }""", {"NSW1": "NSW", "VIC1": "VIC", "TAS1": "TAS"}[code])
        pg.wait_for_timeout(500)
        check(total() == len(region(code)), f"the {code} tab shows its {len(region(code))} live assets", f"{total()} rows")
    pg.evaluate("""(() => { const b = [...document.querySelectorAll('#tabs button, #tabs .tab')]
        .find(x => x.innerText.trim() === 'All'); b.click(); })()""")
    pg.wait_for_timeout(500)
    check(total() == n_visible, "the All tab restores every row", f"{total()} rows")
    pg.select_option("#regionFilter", "SA1")
    pg.wait_for_timeout(500)
    check(total() == len(region("SA1")), "the region dropdown filters in All mode", f"{total()} rows")
    pg.select_option("#regionFilter", "")
    pg.wait_for_timeout(400)

    print("filters")
    pg.select_option("#fuelFilter", "Wind")
    pg.wait_for_timeout(500)
    check(total() == n_wind, f"fuel=Wind filters to {n_wind}", f"{total()} rows")
    pg.select_option("#fuelFilter", "")
    pg.wait_for_timeout(400)
    pg.select_option("#typeFilter", "Unknown")
    pg.wait_for_timeout(500)
    check(total() == n_type, f"type=Unknown filters to {n_type}", f"{total()} rows")
    pg.select_option("#typeFilter", "")
    pg.wait_for_timeout(400)
    pg.fill("#search", top["DUID"])
    pg.wait_for_timeout(500)
    check(0 < total() <= 6, "search by DUID narrows the table", f"{total()} rows for {top['DUID']!r}")
    pg.fill("#search", "zzzz-no-such-asset")
    pg.wait_for_timeout(500)
    check(total() == 0, "a search with no matches renders no rows", f"{total()} rows")
    pg.fill("#search", "")
    pg.wait_for_timeout(500)
    check(total() == n_visible, "clearing the search restores every row", f"{total()} rows")

    print("the retired-DUID toggle")
    pg.check("#showRetired")
    pg.wait_for_timeout(600)
    check(total() == len(rows), f"showing retired DUIDs adds the {n_retired} retired assets", f"{total()} rows")
    pg.uncheck("#showRetired")
    pg.wait_for_timeout(500)
    check(total() == n_visible, "hiding them again restores the live count", f"{total()} rows")

    print("sorting")
    click_header = """(label) => { const th = [...document.querySelectorAll('#genThead th')]
        .find(x => x.innerText.trim() === label); th.click(); }"""
    pg.evaluate(click_header, "DUID")
    pg.wait_for_timeout(500)
    first_asc = gen_duids()[0]
    pg.evaluate(click_header, "DUID")
    pg.wait_for_timeout(500)
    first_desc = gen_duids()[0]
    check(first_asc == duid_min and first_desc == duid_max,
          "clicking a header sorts, clicking again reverses",
          f"{first_asc} (want {duid_min}) -> {first_desc} (want {duid_max})")

    print("selection and export")
    pg.evaluate("document.querySelector('[data-selectall=\"gen\"]').click()")
    pg.wait_for_timeout(800)
    checked = pg.eval_on_selector_all(
        "#genTbody input.cb:checked, #genTbody .cb:checked",
        "e => e.filter(x => x.closest('tr').offsetParent !== null).length")
    check(checked == gen_rows(), "select-all selects every row in the generator table", f"{checked} of {gen_rows()}")
    check(pg.get_attribute("#exportSelected", "disabled") is None,
          "the export button enables once something is selected")
    check(bool(pg.inner_text("#selCount").strip()), "the selection count is stated", pg.inner_text("#selCount"))
    with pg.expect_download(timeout=20000) as dl:
        pg.click("#exportSelected")
    fname = dl.value.suggested_filename
    check(fname.endswith(".xlsx") and f"MLF_{gen_rows()}_assets" in fname,
          "the XLSX export downloads one workbook for the selection", fname)
    pg.evaluate("document.getElementById('clearSelection').click()")
    pg.wait_for_timeout(500)
    check(pg.get_attribute("#exportSelected", "disabled") is not None, "clearing disables the export button")
    check(total() == n_visible, "clearing the selection leaves every row rendered", f"{total()} rows")

    print("downloads, footer and the pinned header")
    hrefs = pg.eval_on_selector_all("a[download]", "e => e.map(x => x.getAttribute('href'))")
    check(len(hrefs) == 6 and any(h.endswith("summary.csv") for h in hrefs),
          "the 5 regional workbooks + the CSV stay wired", f"{hrefs}")
    foot = pg.inner_text("#footer")
    check("Source:" in foot and "tracked" in foot, "the footer states the source and the count",
          " ".join(foot.split())[:140])
    sticky = pg.evaluate("""(() => { const w = document.querySelector('.table-wrap');
        const wt = w.getBoundingClientRect().top;
        w.scrollTop = 300;
        const th = document.querySelector('#genThead th').getBoundingClientRect().top;
        return {offset: Math.round(th - wt), scrolled: w.scrollTop}; })()""")
    check(0 <= sticky["offset"] <= 40 and sticky["scrolled"] > 0,
          "the header stays pinned while the table scrolls", f"{sticky}")

    print("phone")
    phone = br.new_page(viewport={"width": 390, "height": 844}, device_scale_factor=2)
    phone.goto(URL, wait_until="networkidle", timeout=60000)
    phone.wait_for_timeout(1500)
    wrap = phone.evaluate("""(() => { const w = document.querySelector('.table-wrap');
        w.scrollLeft = 9999; return {sw: w.scrollWidth, cw: w.clientWidth, sl: w.scrollLeft}; })()""")
    check(wrap["sw"] > wrap["cw"] and wrap["sl"] > 0,
          "the dense table scrolls inside its wrapper at 390px", f"{wrap}")

    check(not errors, "no JS/console errors", "; ".join(errors[:3]))
    br.close()

print(f"\n{len(fails)} check(s) failed" if fails else "\nall interactions intact")
sys.exit(1 if fails else 0)
