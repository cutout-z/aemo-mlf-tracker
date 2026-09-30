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
import pathlib
import re
import sys

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
        check(len(ramped) >= 400, "the MLF cells render", f"{len(ramped)} ramped cells")
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
        check(len(imported) >= 100, "the battery table's import MLF cells render", f"{len(imported)} cells")
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
