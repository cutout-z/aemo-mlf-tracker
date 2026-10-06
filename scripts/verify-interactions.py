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
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

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
TYPE_PICK = "Unknown" if any((r["DUID_TYPE"] or "Unknown") == "Unknown" for r in live) else "Scheduled Load"   # the metadata fix leaves no "Unknown" type; pick a type the data has
n_type = sum(1 for r in live if (r["DUID_TYPE"] or "Unknown") == TYPE_PICK)
n_retired = len(rows) - n_visible
print(f"csv: {len(rows)} DUIDs · {n_visible} live ({n_retired} retired) · "
      f"generators {len(live_gen)} · batteries {len(live_batt)} · latest {latest}")

# ── The page's numbers, recomputed from the csv alone ──────────────────────────────────────────
# Mirrors renderStats / renderTables / renderOneTable in index.html (retired hidden, no filters),
# rounding half-up on the exact decimals in the file rather than on binary floats.
REGIONS = {"NSW1": "NSW", "QLD1": "QLD", "VIC1": "VIC", "SA1": "SA", "TAS1": "TAS"}
MLF_EDGES = [Decimal(e) for e in ("1.00", "0.98", "0.96", "0.94", "0.92", "0.90", "0.85")]
FUEL_ORDER = ["Solar", "Wind", "Battery", "Hydro", "Fossil", "Other Renewable"]
ALL_FY = sorted(k for k in rows[0] if k.startswith("FY"))
FY_STD = [k for k in ALL_FY if "Draft" not in k and "Import" not in k]
FY_IMP = [k for k in ALL_FY if "Import" in k]
FY_DFT = [k for k in ALL_FY if "Draft" in k]
HALF_UP = lambda d, q: d.quantize(Decimal(q), ROUND_HALF_UP)


def num(v: str | None) -> Decimal | None:
    try:
        d = Decimal((v or "").strip())
    except InvalidOperation:
        return None
    return d if d.is_finite() else None


def signed(d: Decimal, q: str) -> str:
    return ("+" if d > 0 else "−" if d < 0 else "") + str(HALF_UP(abs(d), q))


def table_columns(battery: bool) -> list[str]:
    cols = FY_STD + FY_DFT if not battery else \
        [c for fy in FY_STD for c in (fy, fy + " Import") if c == fy or c in FY_IMP] + FY_DFT
    scope = [r for r in live if (r["FUEL_CATEGORY"] == "Battery") == battery]
    return [c for c in cols if any(num(r[c]) is not None for r in scope)]   # all-blank columns are dropped


def expected_row(r: dict[str, str], fy_cols: list[str], extra: list[str], show_region: bool) -> list:
    # Text cells: the page prints the csv's text (blank -> its stated fallback); innerText trims it.
    text = lambda v, blank: v.strip() if v else blank
    out = [r["DUID"], text(r["DUID_TYPE"], "Unknown"), text(r["STATION_NAME"], "Not stated")]
    if show_region:
        out.append(text(REGIONS.get(r["REGIONID"], r["REGIONID"]), "Not stated"))
    out.append(text(r["FUEL_CATEGORY"], "Not stated"))
    cap = num(r["CAPACITY_MW"])
    out.append(str(HALF_UP(cap, "1")) if cap is not None else "N/A")
    for c in fy_cols:
        v = num(r[c])
        if v is None:
            out.append(["N/A", "seq-none"])
        elif "Import" in c:
            out.append([str(HALF_UP(v, "0.0001")), None])           # import MLFs stay off the loss ramp
        else:
            out.append([str(HALF_UP(v, "0.0001")), f"seq-{next((i for i, e in enumerate(MLF_EDGES) if v >= e), 7)}"])
    for c in extra:
        v = num(r[c])
        out.append("N/A" if v is None else signed(v, "0.01") + "%" if "PCT" in c else signed(v, "0.0001"))
    return out


def expected_stats(code: str) -> tuple[list[list[str]], list[str]]:
    scope_rows = rows if code == "ALL" else [r for r in rows if r["REGIONID"] == code]
    lv = [r for r in scope_rows if r["STATUS"] != "Retired"]
    where = "all regions" if code == "ALL" else REGIONS[code]
    lost = lambda v: f"{HALF_UP((1 - v) * 100, '0.1')}% lost"
    gu = [r for r in lv if (r["DUID_TYPE"] or "Unknown") == "Generator"]   # the MLF tiles read generating units only
    pub = [(r, v) for r in gu if (v := num(r[latest])) is not None]
    if pub:
        avg = sum(v for _, v in pub) / len(pub)
        deep_r, deep_v = pub[0]
        for r, v in pub[1:]:                     # first-wins on ties, as the page's reduce does
            if v < deep_v:
                deep_r, deep_v = r, v
        t2 = [str(HALF_UP(avg, "0.0001")), f"Average MLF, {latest}",
              f"{HALF_UP((1 - avg) * 100, '0.1')}% of price lost · simple mean of {len(pub)} live generating units"]
        t3 = [str(HALF_UP(deep_v, "0.0001")), f"Deepest loss, {latest}",
              f"{deep_r['DUID']} · {deep_r['STATION_NAME'] or 'station not stated'} · {lost(deep_v)}"]
    else:
        t2 = ["N/A", f"Average MLF, {latest}", "no MLF published for this scope"]
        t3 = ["N/A", f"Deepest loss, {latest}", "no MLF published for this scope"]
    yoy = [v for r in gu if (v := num(r["YOY_CHANGE"])) is not None]
    down, up = sum(v < 0 for v in yoy), sum(v > 0 for v in yoy)
    t4 = ([str(down), f"Worse than {FY_STD[-2]}",
           f"of {len(yoy)} generating units with both years · {up} better · {len(yoy) - down - up} unchanged"] if yoy else
          ["N/A", f"Worse than {FY_STD[-2]}", "no year-on-year change in this scope"])
    t1 = [str(len(lv)), "DUIDs live", f"of {len(scope_rows)} in the file for {where} · {len(scope_rows) - len(lv)} retired"]
    counts: dict[str, int] = {}
    for r in lv:
        counts[r["FUEL_CATEGORY"]] = counts.get(r["FUEL_CATEGORY"], 0) + 1
    named = [f for f in counts if f]
    order = [f for f in FUEL_ORDER if counts.get(f)] + sorted(f for f in named if f not in FUEL_ORDER)
    badges = [f"{f} {counts[f]}" for f in order] + ([f"Fuel not stated {counts['']}"] if counts.get("") else [])
    return [t1, t2, t3, t4], badges

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
    check(gen_duids()[0] == top["DUID"],
          f"the default sort ({latest}, ascending) puts the deepest loss first",
          f"{gen_duids()[0]} vs {top['DUID']} ({top[latest]})")

    print("values match the csv (every tab: the four tiles, the fuel badges, every cell of both tables)")
    read_rows = """tb => [...document.querySelectorAll(tb + ' tr')].filter(x => x.offsetParent !== null).map(tr =>
        [tr.dataset.duid, ...[...tr.children].slice(1).map(td => td.classList.contains('mlf-cell')
            ? [td.innerText.trim(), (td.className.match(/\\bseq-(\\d|none)\\b/) || [null])[0]] : td.innerText.trim())])"""
    for code, tab in [("ALL", "All")] + list(REGIONS.items()):
        pg.evaluate("""(label) => { const b = [...document.querySelectorAll('#tabs button, #tabs .seg-item')]
            .find(x => x.innerText.trim() === label); b.click(); }""", tab)
        pg.wait_for_timeout(500)
        want_tiles, want_badges = expected_stats(code)
        tiles = pg.eval_on_selector_all("#stats .grid > div", "e => e.map(t => [...t.children].map(c => c.innerText.trim()))")
        check(tiles == want_tiles, f"{tab}: the four tiles equal the figures recomputed from the csv",
              f"page {tiles} vs csv {want_tiles}")
        badges = pg.eval_on_selector_all("#stats .badge", "e => e.map(x => x.innerText.replace(/\\s+/g, ' ').trim())")
        check(badges == want_badges, f"{tab}: the fuel badges count the csv's live DUIDs", f"{badges} vs {want_badges}")
        for battery, tb, th, extra in [
                (False, "#genTbody", "#genThead", ["YOY_CHANGE", "YOY_PCT_CHANGE"]),
                (True, "#battTbody", "#battThead", ["YOY_CHANGE", "YOY_PCT_CHANGE", "IMPORT_YOY_CHANGE", "IMPORT_YOY_PCT_CHANGE"])]:
            noun = "batteries" if battery else "generators"
            fy_cols = table_columns(battery)
            scope = [r for r in live if (r["FUEL_CATEGORY"] == "Battery") == battery and (code == "ALL" or r["REGIONID"] == code)]
            want = {r["DUID"]: expected_row(r, fy_cols, extra, code == "ALL") for r in scope}
            page = {r[0]: r[1:] for r in pg.evaluate(read_rows, tb)}
            heads = pg.eval_on_selector_all(f"{th} th[data-col]", "e => e.map(x => x.dataset.col)")
            meta = ["DUID", "DUID_TYPE", "STATION_NAME"] + (["REGIONID"] if code == "ALL" else []) + ["FUEL_CATEGORY", "CAPACITY_MW"]
            check(not scope or heads == meta + fy_cols + extra, f"{tab}: the {noun} columns are the csv's non-empty years",
                  f"{heads[len(meta):]} vs {fy_cols + extra}")
            missing = [d for d in want if d not in page]
            extra_rows = [d for d in page if d not in want]
            bad = [(d, i, page[d][i] if i < len(page[d]) else None, w) for d in want if d in page
                   for i, w in enumerate(want[d]) if i >= len(page[d]) or page[d][i] != w]
            check(not missing and not extra_rows and not bad and len(page) == len(want),
                  f"{tab}: every {noun} cell equals summary.csv ({len(want)} rows x {len(meta) + len(fy_cols) + len(extra)} columns)",
                  f"missing {missing[:2]}, extra {extra_rows[:2]}, {len(bad)} wrong, e.g. {bad[:3]}")
    pg.evaluate("""(() => { const b = [...document.querySelectorAll('#tabs button, #tabs .seg-item')]
        .find(x => x.innerText.trim() === 'All'); b.click(); })()""")
    pg.wait_for_timeout(500)

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
    pg.select_option("#typeFilter", TYPE_PICK)
    pg.wait_for_timeout(500)
    check(total() == n_type, f"type={TYPE_PICK} filters to {n_type}", f"{total()} rows")
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
