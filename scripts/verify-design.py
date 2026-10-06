#!/usr/bin/env python3
"""Render + token check for the AEMO MLF Tracker — the gate this pass must leave green.

    cd ~/Design/"AEMO MLF Tracker" && python3 -m http.server 9380 --bind 127.0.0.1 &
    /opt/anaconda3/bin/python3 scripts/verify-design.py            # checks only
    /opt/anaconda3/bin/python3 scripts/verify-design.py --screens  # + design/screens/after-*.png

Why a script and not an eyeball: the failures this page actually produces are invisible in a diff —
`mlfColor()` writes a six-stop red→yellow→green gradient as an inline `style="background:rgb(...)"`
on every cell (so the page cannot flip theme, and a low MLF reads as "red = bad" while the family's
ramp means "more lost"); the draft/import column markers are literal hexes; there is no legend, no
theme toggle and no state panel at all; blank MLF cells render `-` with no stated reason.

The DOM contract this checks is written down in AGENTS.md (section "DOM contract").
Exit 1 = fix it.
"""
from __future__ import annotations

import argparse
import csv
import io
import pathlib
import re
import sys

import openpyxl
from playwright.sync_api import sync_playwright

ROOT = pathlib.Path(__file__).resolve().parent.parent
URL = "http://127.0.0.1:9380/index.html"
SCREENS = ROOT / "design" / "screens"
TOKEN_SRC = ROOT / "assets" / "css" / "tailwind.src.css"
PAGE = ROOT / "index.html"
CSV = ROOT / "outputs" / "summary.csv"

fails: list[str] = []


def check(ok: bool, name: str, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not ok:
        fails.append(name)


def rgb(value: str):
    v = (value or "").strip()
    if v.startswith("#"):
        v = v.lstrip("#")
        if len(v) == 3:
            v = "".join(c * 2 for c in v)
        try:
            return tuple(int(v[i:i + 2], 16) for i in (0, 2, 4))
        except ValueError:
            return None
    if v.startswith("rgb"):
        parts = v[v.index("(") + 1:v.index(")")].split(",")
        try:
            return tuple(int(float(p)) for p in parts[:3])
        except ValueError:
            return None
    return None


def token_sets() -> tuple[dict[str, str], dict[str, str]]:
    """Read both theme roots out of the token source.

    Two hazards, both already paid for in this family: the source explains itself in `/* … */` comments
    and one of them names a token (the `--faint` line's "4.9:1 on --surface"), and the heat ramp lives in
    a SECOND `:root` block further down the file — so a single split on the first `[data-theme="light"]`
    leaves `seq-*` unread. Strip comments, then take every declaration block whose selector is a theme
    root (`:root` = dark, `[data-theme="light"]` = light), later blocks overriding earlier ones."""
    css = re.sub(r"/\*.*?\*/", " ", TOKEN_SRC.read_text(), flags=re.S)
    grab = lambda s: dict(re.findall(r"--([a-z0-9-]+)\s*:\s*([^;]+);", s))
    dark: dict[str, str] = {}
    light: dict[str, str] = {}
    for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", css):
        sel, body = m.group(1), m.group(2)
        if "data-theme" in sel:
            light.update(grab(body))
        elif ":root" in sel:
            dark.update(grab(body))
    return dark, light


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--screens", action="store_true", help="write design/screens/after-*.png")
    args = ap.parse_args()
    dark, light = token_sets()
    csv_rows = list(csv.DictReader(CSV.open()))
    fy_cols = sorted({c for c in csv_rows[0] if c.startswith("FY")
                      and "Import" not in c and "Draft" not in c})
    latest = fy_cols[-1]
    blank_cells = sum(1 for r in csv_rows for c in fy_cols if not r[c].strip())

    print("static")
    page = PAGE.read_text()
    check('href="assets/css/app.css"' in page, "index.html links assets/css/app.css",
          "no <link> to the compiled token layer")
    hexes = re.findall(r"#[0-9a-fA-F]{3,8}\b", page)
    check(not hexes, "no raw hex colour in index.html", f"found {len(set(hexes))}: {sorted(set(hexes))[:8]}")
    check("data-theme" in page, "index.html carries the theme attribute",
          "no theme toggle and no attribute to flip")

    with sync_playwright() as pw:
        br = pw.chromium.launch()
        pg = br.new_page(viewport={"width": 1440, "height": 900})
        errors: list[str] = []
        pg.on("pageerror", lambda e: errors.append(str(e)))
        pg.goto(URL, wait_until="networkidle", timeout=60000)
        pg.wait_for_timeout(1500)

        print("tokens")
        check(pg.evaluate("async () => (await fetch('assets/css/app.css')).status") == 200,
              "assets/css/app.css is served (200)")
        check(rgb(pg.evaluate("getComputedStyle(document.body).backgroundColor")) == rgb(dark["bg"]),
              "body background is the --bg token",
              pg.evaluate("getComputedStyle(document.body).backgroundColor") + f" vs {dark['bg']}")
        n_cards = pg.eval_on_selector_all(".card", "e => e.length")
        check(n_cards >= 2, "the page is composed of .card panels", f"{n_cards} found")
        if n_cards:
            check(rgb(pg.evaluate("getComputedStyle(document.querySelector('.card')).backgroundColor")) == rgb(dark["surface"]),
                  ".card background is the --surface token")

        print("shell")
        kpi = pg.eval_on_selector_all(".kpi-value", "e => e.map(x => (x.innerText || '').trim())")
        check(len(kpi) >= 4 and all(kpi), "a KPI row renders (>= 4 valued tiles)", f"{len(kpi)} tiles: {kpi}")
        labels = pg.eval_on_selector_all(".kpi-label", "e => e.map(x => x.innerText.trim())")
        check(len(labels) >= 4 and all(labels), "every KPI carries a label", f"{labels}")
        keep = pg.eval_on_selector_all("#typeFilter, #fuelFilter, #search, #showRetired", "e => e.length")
        check(keep == 4, "the four filters survive (#typeFilter, #fuelFilter, #search, #showRetired)",
              f"{keep} of 4 found")

        print("the two tables")
        gen = pg.eval_on_selector_all("#genTbody tr", "e => e.length")
        batt = pg.eval_on_selector_all("#battTbody tr", "e => e.length")
        visible = sum(1 for r in csv_rows if r["STATUS"] != "Retired")
        check(gen + batt == visible, f"all {visible} live assets render across the two tables",
              f"generators {gen} + batteries {batt}")
        check(gen > 0 and batt > 0, "both the generator and the battery table render",
              f"{gen} / {batt}")
        ramped = pg.eval_on_selector_all(
            "#genTbody td.mlf-cell:not(.imp), #battTbody td.mlf-cell:not(.imp)",
            "e => e.map(x => ({cls: x.className, txt: x.innerText.trim(), bg: getComputedStyle(x).backgroundColor}))")
        imported = pg.eval_on_selector_all(
            "#battTbody td.mlf-cell.imp",
            "e => e.map(x => ({cls: x.className, txt: x.innerText.trim(), bg: getComputedStyle(x).backgroundColor,"
                 " bl: getComputedStyle(x).borderLeftStyle}))")
        # Exact counts from the csv: live rows x the year columns each table shows (a column empty for
        # every live row of that table is dropped and named in its foot, so it is not counted here).
        def shown(battery: bool, imports: bool) -> int:
            scope = [r for r in csv_rows if r["STATUS"] != "Retired" and (r["FUEL_CATEGORY"] == "Battery") == battery]
            cols = [c for c in csv_rows[0] if c.startswith("FY") and ("Import" in c) == imports
                    and (battery or not imports) and any(r[c].strip() for r in scope)]
            return len(scope) * len(cols)
        want_ramped = shown(False, False) + shown(True, False)
        want_imported = shown(True, True)
        check(len(ramped) == want_ramped, f"the MLF cells render ({want_ramped}: live rows x shown year columns)",
              f"{len(ramped)} ramped cells")
        # `.seq-none` is the token file's own step for "no value"; a stated N/A is on the ramp system, not off it.
        on_ramp = r"\bseq-(\d|none)\b"
        seq = [c for c in ramped if re.search(on_ramp, c["cls"] or "")]
        check(len(seq) == len(ramped), "every export MLF cell uses the .seq-* ramp",
              f"{len(ramped) - len(seq)} cells not on the ramp")
        check(not [c for c in ramped if "rgb" in (c["bg"] or "") and not re.search(on_ramp, c["cls"] or "")],
              "no MLF cell carries an inline rgb() colour")
        # An import MLF multiplies the price a LOAD pays, so a lower one is cheaper charging, not a bigger
        # loss — 124 of the 159 published values here are below 1.00. On the shared ramp they read as the
        # deepest losses on the page, which is the one thing they are not. The number stays, the dashed
        # marking stays, the fill stays off.
        check(len(imported) == want_imported,
              f"the battery table's import MLF cells render ({want_imported}: live batteries x shown import years)",
              f"{len(imported)} cells")
        check(not [c for c in imported if re.search(r"\bseq-[0-7]\b", c["cls"] or "")],
              "import MLF cells are NOT on the loss ramp (lower = cheaper, not more lost)",
              f"{[c['txt'] for c in imported if re.search(r'seq-[0-7]', c['cls'] or '')][:3]}")
        check(all(c["bl"] == "dashed" for c in imported),
              "import MLF cells keep their dashed marking", sorted({c["bl"] for c in imported}))
        bad_val = [c["txt"] for c in imported
                   if not (len(c["txt"]) == 6 and c["txt"][1] == ".") and c["txt"].upper() not in ("N/A", "NA")]
        check(not bad_val, "import MLF cells state their value", f"{bad_val[:3]}")
        # A class can be present and still lose the cascade (a page rule out-ranking `.seq-*`), which
        # leaves the cell unfilled while every name-based check passes. Compare the pixels to the tokens.
        ramp_rgb = {rgb(dark[f"seq-{i}"]) for i in range(8) if f"seq-{i}" in dark}
        numeric = [c for c in ramped if re.fullmatch(r"\d*\.?\d+", c["txt"].replace(",", ""))]
        unfilled = [c for c in numeric if rgb(c["bg"]) not in ramp_rgb]
        check(not unfilled, "every numeric export MLF cell is actually filled from the ramp",
              f"{len(unfilled)} of {len(numeric)} unfilled, e.g. {unfilled[:2]}")
        steps = {int(re.search(r"\bseq-(\d)\b", c["cls"]).group(1)) for c in seq if re.search(r"\bseq-(\d)\b", c["cls"])}
        check(len(steps) >= 4, "the ramp is graded, not one flat step", f"steps used: {sorted(steps)}")
        na = [c for c in ramped if c["txt"].upper() in ("N/A", "NA")]
        check(len(na) >= blank_cells * 0.5,
              "unpublished MLF values are stated, not left blank",
              f"{len(na)} stated vs {blank_cells} blank cells in the CSV today")
        # The import change columns carry the sign only: a RISING import MLF is dearer charging, so the
        # generator good/bad colours are inverted there. Read the column by index and check the colour.
        iyoy = pg.evaluate("""() => {
            const labels = [...document.querySelectorAll('#battThead tr:last-child th')].map(x => x.innerText.trim());
            const off = 7, idx = [];
            labels.forEach((l, i) => { if (/^Import YoY/.test(l)) idx.push(i + off); });
            const out = [];
            for (const r of document.querySelectorAll('#battTbody tr')) {
              const tds = r.querySelectorAll('td');
              for (const i of idx) { const td = tds[i]; if (!td) continue;
                const t = td.innerText.trim(); if (!t || t.toUpperCase() === 'N/A') continue;
                out.push({t: t, c: getComputedStyle(td).color, cls: td.className}); } }
            return out; }""")
        good_bad = {rgb(dark[k]) for k in ("good", "bad") if k in dark}
        coloured = [c for c in iyoy if rgb(c["c"]) in good_bad]
        check(bool(iyoy) and not coloured, "import YoY carries its sign without the good/bad colour",
              f"{len(coloured)} of {len(iyoy)} coloured, e.g. {coloured[:2]}")
        check(bool(iyoy) and all(re.match(r"^[+\u2212]", c["t"]) or re.match(r"^0(\.0+)?%?$", c["t"]) for c in iyoy),
              "import YoY still prints its sign (a zero change is unsigned by design)",
              f"{[c['t'] for c in iyoy[:4]]}")
        legend = pg.eval_on_selector_all("[data-heat-legend]",
                                        "e => e.map(x => x.innerText.trim())")
        check(bool(legend) and bool(" ".join(legend).strip()),
              "a scale is stated on screen ([data-heat-legend])", f"{legend[:1]}")
        if legend:
            txt = " ".join(legend)
            stops = re.findall(r"\d\.\d{2,4}", txt)
            check(len(stops) >= 3, "the stated scale names its steps", f"{len(stops)} stops: {stops[:5]}")
            check(bool(re.search(r"lost|loss|below", txt, re.I)),
                  "the stated scale says which direction is worse", txt[:120])
        yoy = pg.eval_on_selector_all(
            "#genTbody td",
            "e => e.filter(x => /^[+\\u2212-]\\d/.test(x.innerText.trim())).map(x => x.innerText.trim())")
        check(bool(yoy), "year-on-year changes keep an explicit sign (colour is not the only signal)",
              f"{len(yoy)} signed cells, e.g. {yoy[:3]}")

        print("open items")
        # 1. The served shell must reserve the layout the app is about to fill, at EVERY width. This page used
        #    to grow ~1,600 px the moment the CSV landed, shoving everything below the skeleton down the
        #    screen. Read with JS off: that is literally what the browser paints first, so the comparison is
        #    deterministic rather than a race.
        #    Measuring only 1440 is how a desktop-only reservation passed as "reserved" while the phone page
        #    still grew 1,086 px (28 %) — so the widths below are not decoration. Keep the phone rung.
        shell_shapes = {}
        for w in (1440, 900, 600, 390):
            shell_ctx = br.new_context(viewport={"width": w, "height": 900}, java_script_enabled=False)
            shell = shell_ctx.new_page()
            shell.goto(URL, wait_until="load", timeout=60000)
            shell.wait_for_timeout(600)
            shell_shapes[w] = {
                "bars": shell.eval_on_selector_all(".skeleton", "e => e.length"),
                "h": shell.evaluate("document.documentElement.scrollHeight"),
            }
            shell_ctx.close()
            live_ctx = br.new_context(viewport={"width": w, "height": 900})
            live = live_ctx.new_page()
            live.goto(URL, wait_until="networkidle", timeout=60000)
            live.wait_for_timeout(1500)
            shell_shapes[w]["loaded"] = live.evaluate("document.documentElement.scrollHeight")
            live_ctx.close()
        check(min(s["bars"] for s in shell_shapes.values()) >= 15,
              "the served shell renders a skeleton, not a blank page",
              f"{ {w: s['bars'] for w, s in shell_shapes.items()} }")
        for w, s in shell_shapes.items():
            gap = abs(s["loaded"] - s["h"])
            check(gap <= 0.2 * max(s["loaded"], 1),
                  f"the shell reserves the page height at {w}px, so loading does not jump the layout",
                  f"shell {s['h']} px vs loaded {s['loaded']} px ({gap / max(s['loaded'], 1):.0%})")

        # 2. Sticky chrome must stay ONE row. A wrapped control bar pinned over the table it is meant to
        #    help you read costs 104 px of a 900 px viewport. Below lg stacking is allowed — it is not
        #    sticky there, so it scrolls away. Both halves of that rule are checked.
        bar = {}
        for w in (1440, 1280, 1100, 900, 768, 390):
            pg.set_viewport_size({"width": w, "height": 900})
            pg.wait_for_timeout(400)
            bar[w] = pg.evaluate("""(() => { const el = document.getElementById('controls');
                if (!el) return null; const r = el.getBoundingClientRect();
                return {h: Math.round(r.height), pos: getComputedStyle(el).position}; })()""")
        tall = {w: b for w, b in bar.items() if b and b["pos"] == "sticky" and b["h"] > 72}
        check(not tall, "the sticky control bar is never more than one row", f"{tall}")
        one_row = {w: bar[w]["h"] for w in (1440, 1280, 1100) if bar[w]}
        check(len(one_row) == 3 and all(h <= 72 for h in one_row.values()),
              "the control bar stays one row down to 1100 (it scrolls, it does not wrap)", f"{one_row}")
        check(all(b["pos"] != "sticky" or b["h"] <= 72 for b in bar.values() if b),
              "wherever it is sticky it is one row", f"{ {w: b and (b['pos'], b['h']) for w, b in bar.items()} }")
        pg.set_viewport_size({"width": 1440, "height": 900})
        pg.wait_for_timeout(300)

        # 3. A year column empty for every asset on screen is dropped from the table and NAMED in that
        #    card's foot (the battery card was a wall of ten N/A columns). Expected from the CSV, so a
        #    data change cannot quietly hide a column that only looks empty — and expected per table,
        #    because the generator table draws no Import columns at all.
        #
        #    Scope: the retired toggle counts (retired batteries do have early-year MLFs, so turning it on
        #    must bring those columns back), and the region/type/fuel filters must NOT reshape the table.
        #    Both halves are checked below.
        def blanks_for(pred, keep, live_only):
            rows = [r for r in csv_rows if (r["STATUS"] != "Retired" or not live_only) and pred(r)]
            cols = sorted({c for r in rows for c in r if c.startswith("FY") and keep(c)})
            return [c for c in cols if all(not (r.get(c) or "").strip() for r in rows)]
        def live_batt(r):
            return r["FUEL_CATEGORY"] == "Battery"
        blank_gen = blanks_for(lambda r: r["FUEL_CATEGORY"] != "Battery", lambda c: "Import" not in c, True)
        blank_batt = blanks_for(live_batt, lambda c: True, True)
        drawn = {k: pg.eval_on_selector_all(f"#{k}Thead tr:last-child th", "e => e.map(x => x.innerText.trim())")
                 for k in ("gen", "batt")}
        feet = {k: pg.eval_on_selector(f"#{k}Foot", "e => e.innerText").strip() for k in ("gen", "batt")}
        for key, blanks, noun in (("gen", blank_gen, "generator"), ("batt", blank_batt, "battery")):
            still = [c for c in blanks if c in drawn[key]]
            check(not still, f"the {key} table draws no year column that is empty for every {noun}",
                  f"still drawn: {still[:6]}")
            check(not blanks or all(c in feet[key] for c in blanks),
                  f"the {key} card names what it hid in its own foot", feet[key][:160] or "(foot empty)")
        check(bool(blank_gen) or bool(blank_batt),
              "the empty-column rule is exercised by today's file (else this check proves nothing)",
              f"generators {blank_gen} · batteries {len(blank_batt)}")

        #    The scope rule has two halves. Retired ON must bring back the columns only retired assets
        #    fill (their early-year MLFs are real data); a region filter must NOT reshape the table.
        blank_batt_all = blanks_for(live_batt, lambda c: True, False)
        pg.evaluate("document.getElementById('showRetired').click()")
        pg.wait_for_timeout(1000)
        drawn_retd = pg.eval_on_selector_all("#battThead tr:last-child th", "e => e.map(x => x.innerText.trim())")
        check(len(drawn_retd) > len(drawn["batt"]) or blank_batt_all == blank_batt,   # superseded batteries merge into their successor, so no column may be retired-only
              "turning on retired DUIDs brings back the columns only retired batteries fill",
              f"{len(drawn['batt'])} -> {len(drawn_retd)} columns; still hidden {blank_batt_all}")
        check(all(c not in drawn_retd for c in blank_batt_all),
              "and no column blank for every asset in the file is drawn",
              f"still drawn: {[c for c in blank_batt_all if c in drawn_retd]}")
        pg.evaluate("""(() => { for (const b of document.querySelectorAll('#tabs button'))
            if (b.innerText.trim() === 'NSW') b.click(); })()""")
        pg.wait_for_timeout(1000)
        drawn_nsw = pg.eval_on_selector_all("#battThead tr:last-child th", "e => e.map(x => x.innerText.trim())")
        check(drawn_nsw == drawn_retd, "a region filter does not reshape the column set",
              f"{len(drawn_nsw)} vs {len(drawn_retd)} columns")
        pg.evaluate("document.getElementById('showRetired').click()")
        pg.evaluate("""(() => { for (const b of document.querySelectorAll('#tabs button'))
            if (b.innerText.trim() === 'All') b.click(); })()""")
        pg.wait_for_timeout(900)

        # 4. The Draft column path had never been rendered: today's CSV has no Draft key, so nothing
        #    proved the marker works. Serve a fixture that adds one and read the result off the page.
        #    The column names are the pipeline's own (src/analyse.py: "FY27-28 (Draft)" and its
        #    "… Import"), and a draft import is included: it matches both "Draft" and "Import", which
        #    is how it once reached the generator table and the export twice.
        raw = list(csv.reader(CSV.open()))
        head, pick = raw[0] + ["FY27-28 (Draft)", "FY27-28 (Draft) Import"], {}
        for row in raw[1:]:
            d = dict(zip(raw[0], row))
            if not d.get("DUID"):
                continue
            key = "batt" if d.get("FUEL_CATEGORY") == "Battery" else "gen"
            pick.setdefault(key, row)
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(head)
        for key, row in pick.items():
            w.writerow(row + ["0.9750", "0.9900" if key == "batt" else ""])
        draft = br.new_page(viewport={"width": 1440, "height": 900})
        draft.route("**/outputs/summary.csv*",
                    lambda route: route.fulfill(status=200, content_type="text/csv", body=buf.getvalue()))
        draft.goto(URL, wait_until="networkidle", timeout=60000)
        draft.wait_for_timeout(1800)
        dh = draft.eval_on_selector_all(
            "#genThead th.dft, #battThead th.dft",
            "e => e.map(x => ({t: x.innerText.trim(), bl: getComputedStyle(x).borderLeftStyle,"
            " blc: getComputedStyle(x).borderLeftColor}))")
        db = draft.eval_on_selector_all(
            "#genTbody td.dft, #battTbody td.dft",
            "e => e.map(x => ({t: x.innerText.trim(), bl: getComputedStyle(x).borderLeftStyle}))")
        check(len(dh) >= 2 and all("Draft" in d["t"] for d in dh),
              "a Draft column renders a marked header in both tables", f"{[d['t'] for d in dh]}")
        check(bool(dh) and all(d["bl"] == "dashed" and rgb(d["blc"]) == rgb(dark["warn"]) for d in dh),
              "the Draft header carries the warn marking", f"{dh[:1]} vs --warn {dark.get('warn')}")
        check(sorted(d["t"] for d in db) == ["0.9750", "0.9750", "0.9900"],
              "the Draft cells render their values (gen and battery draft, battery draft import)",
              f"{[d['t'] for d in db]}")
        gen_heads = draft.eval_on_selector_all("#genThead th[data-col]", "e => e.map(x => x.dataset.col)")
        batt_heads = draft.eval_on_selector_all("#battThead th[data-col]", "e => e.map(x => x.dataset.col)")
        check(not [h for h in gen_heads if "Import" in h],
              "no import column reaches the generator table (draft import included)", f"{[h for h in gen_heads if 'Import' in h]}")
        check(batt_heads.count("FY27-28 (Draft) Import") == 1
              and batt_heads.index("FY27-28 (Draft) Import") == batt_heads.index("FY27-28 (Draft)") + 1,
              "the battery table shows the draft import once, next to its draft", f"{batt_heads[-8:]}")
        dimp = draft.eval_on_selector_all("#battTbody td.dft.imp", "e => e.map(x => x.className)")
        check(len(dimp) == 1 and not re.search(r"\bseq-[0-7]\b", dimp[0]),
              "the draft import cell stays off the loss ramp", f"{dimp}")
        draft.evaluate("selected = new Set(allData.map(r => r.DUID)); updateSelectionUI();")
        with draft.expect_download() as dl:
            draft.click("#exportSelected")
        xl = openpyxl.load_workbook(io.BytesIO(pathlib.Path(dl.value.path()).read_bytes()), read_only=True)
        xl_heads = [c.value for c in next(xl.active.iter_rows(max_row=1))]
        dup = sorted({h for h in xl_heads if xl_heads.count(h) > 1})
        check(not dup and "FY27-28 (Draft) Import" in xl_heads,
              "the selection export carries each column once, draft import included", f"duplicated: {dup}")
        check(bool(db) and all(d["bl"] == "dashed" for d in db),
              "the Draft cells carry the warn marking too", f"{[d['bl'] for d in db][:3]}")
        dlegend = " ".join(draft.eval_on_selector_all("[data-heat-legend]", "e => e.map(x => x.innerText)"))
        dtxt = draft.evaluate("document.body.innerText")
        says = re.search(r"[^.]*[Dd]raft[^.]*\.", dtxt)
        check(re.search(r"draft[^.]*indicative|indicative[^.]*draft", dtxt, re.I) is not None,
              "the page states that a Draft column is indicative, not final",
              says.group(0)[:160] if says else "(no Draft sentence found)")
        check("Draft" not in dlegend, "the ramp legend keeps to the MLF tokens (Draft is stated in the foot)",
              dlegend[:80])
        draft.close()

        print("themes and phone")
        flip = pg.evaluate("""(() => { const r = document.documentElement;
            const before = getComputedStyle(document.body).backgroundColor;
            r.setAttribute('data-theme','light');
            const after = getComputedStyle(document.body).backgroundColor;
            r.setAttribute('data-theme','dark'); return {before, after}; })()""")
        check(rgb(flip["after"]) == rgb(light["bg"]),
              "the light theme flips body to the light --bg token", f"{flip['after']} vs {light['bg']}")
        check(bool(pg.eval_on_selector_all("#themeToggle, [data-theme-toggle]",
                                           "e => e.map(x => x.id || x.className)")),
              "a theme toggle control exists")

        phone = br.new_page(viewport={"width": 390, "height": 844}, device_scale_factor=2)
        phone.goto(URL, wait_until="networkidle", timeout=60000)
        phone.wait_for_timeout(1200)
        over = phone.evaluate("document.documentElement.scrollWidth - document.documentElement.clientWidth")
        check(over <= 1, "no page-level horizontal overflow at 390px", f"{over}px over")
        small = phone.eval_on_selector_all(
            "#tabs button, #tabs .seg-item, .action-btn, input[type=checkbox], select, #search",
            "e => e.map(x => ({t: (x.innerText||x.id||x.type).slice(0,18), h: Math.round(x.getBoundingClientRect().height)}))"
                 ".filter(x => x.h > 0 && x.h < 32)")
        check(not small, "every interactive target is >= 32px tall at 390px", f"{small[:4]}")
        # NB: the "MLF by financial year" group label cannot be pinned for a sideways scroll — measured
        # on a 390px phone, the sticky clamp caps it at the colspan cell's own right edge (21px of strip
        # at full scroll), and pinning behind the pinned columns hides it instead. The meaning is carried
        # by the card head and the legend, which stay visible because the table scrolls in its own box.

        state = br.new_page(viewport={"width": 1440, "height": 900})
        state.route("**/outputs/summary.csv", lambda r: r.abort())
        state.goto(URL, wait_until="domcontentloaded", timeout=60000)
        state.wait_for_timeout(1800)
        state_rows = state.eval_on_selector_all(
            ".state", "e => e.map(x => ({txt: x.innerText, h: x.getBoundingClientRect().height}))")
        check(bool(state_rows) and max(s["h"] for s in state_rows) > 0,
              "a missing data file renders a VISIBLE .state panel, not a bare error string",
              f"{len(state_rows)} .state element(s), tallest {max([s['h'] for s in state_rows], default=0):.0f}px")
        check(not errors, "no JS errors on load", "; ".join(errors[:3]))

        if args.screens:
            SCREENS.mkdir(parents=True, exist_ok=True)
            for fname, page_obj, full in (("after-top.png", pg, False), ("after-full.png", pg, True),
                                          ("after-phone.png", phone, True)):
                page_obj.screenshot(path=str(SCREENS / fname), full_page=full)
            state.screenshot(path=str(SCREENS / "after-nodata.png"))
            print(f"  wrote screenshots to {SCREENS}")

        br.close()

    print(f"\n{len(fails)} check(s) failed" if fails else "\nall checks passed")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
