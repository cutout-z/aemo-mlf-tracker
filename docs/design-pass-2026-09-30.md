# Design pass 1 — AEMO MLF Tracker (2026-09-30)

Branch `design/2026-10`. Presentation only: the only files changed are `index.html`,
`assets/css/app.css` (rebuilt), `design/screens/after-*.png` and this note.

## What changed

| Surface | Before | After |
|---|---|---|
| Frame / header | five unaligned bands, inline `:root` palette overriding the tokens | `<main>` frame on the token layer (`assets/css/app.css` linked before the inline `<style>`; inline palette and reset deleted, preflight is the only reset); header states the question and how to read an MLF |
| KPI strip | nine raw fuel-count cards incl. "UNKNOWN 43", "OTHER 5"; footer "736 generators tracked" | four tiles with label + scope from the CSV (DUIDs live of file total; average MLF, latest FY; deepest loss, named DUID; how many are worse than the prior FY) + one labelled fuel-badge row; card foot states scope, blanks and the publication rhythm |
| Controls | native controls, action row separate | one sticky `.card` bar (>= 640px): region `.seg`, region/type/fuel selects, search, retired `.switch`, "N of M DUIDs shown", selection count + Clear + Export together |
| Tables | inline `rgb()` per cell, centred numerics, accent-fill header | one `.card` each: head (what columns mean, what Import is) -> stated legend -> table -> foot (source, rhythm, count). Two-row sticky header, right-aligned tabular 4 dp numerics, `aria-sort` + arrow sort (keyboard reachable), checkbox + DUID pinned |
| Heat | diverging red-yellow-green via `mlfColor()`, no legend | MLF read as price lost (1 - MLF) on the one `--seq-0..7` ramp via `td.seq-N`; edges/labels identical to the Renewable Generator dashboard: `>=1.00, 0.98, 0.96, 0.94, 0.92, 0.90, 0.85, <0.85`; direction stated ("Stronger fill = more lost") |
| Unpublished values | bare `-` | `N/A` with the reason (cell title, legend, card foot); blank region / fuel / station are "Not stated" |
| States | "Loading data..." / a bare sentence / a hidden section keeping stale rows | skeleton card; `.state` naming `outputs/summary.csv` for missing/blocked/404/empty file (and for a failed PapaParse CDN load); an empty table states why and its tbody/thead are cleared |
| Themes | none | dark default, `#themeToggle` remembered per viewer, `?theme=light|dark` forces one (not stored); heat steps flip with the tokens |
| Phone (390px) | 269 px page overflow, 28-31 px targets | 0 px overflow, every interactive target >= 32 px, tables scroll inside their cards |

## Decisions the brief did not cover

- **One ramp was enough.** No second scale was defined; the token file is untouched.
- **KPI scope.** Tiles and the fuel row always cover *live* DUIDs in the chosen region; the retired
  toggle changes the tables only (before, the fuel counts followed the toggle). Average MLF is a simple
  mean, not MW-weighted, and the tile says so. "Worse than FY25-26" counts `YOY_CHANGE < 0` (verified
  equal to FY26-27 minus FY25-26 for all 622 rows that have it).
- **Blank rows.** 43 live DUIDs with no `FUEL_CATEGORY` are counted as "Fuel not stated" (dashed badge)
  and are not a filter option; 4 live DUIDs with no `REGIONID` are stated in the KPI foot as sitting under
  All and on no region tab. "Other" (5 DUIDs: pumps and two BLNK placeholders) is a real value in the
  file and stays a fuel badge. The type filter's "Unknown" option stays: it is AEMO's own value for 12
  DUIDs (and the interactions gate depends on it).
- **Import columns.** Distinguished by `--accent-wash` header + dashed `--accent` left edge on header and
  cells, and explained in the battery card head. They are on the same ramp (the gate requires every
  numeric MLF cell on it) with a stated caveat that they are a factor on the price paid. Draft columns
  have a `--warn` path with the same treatment.
- **YoY colour.** Sign is printed (U+2212 for minus); colour (`--good`/`--bad`) is applied to the export
  YoY only. Import YoY carries its sign without colour because the good/bad direction differs for a load.
- **Behaviour fix.** The region dropdown started hidden in All mode until All was re-clicked; it is now
  visible on load.
- **Empty tables stay visible with a stated reason** instead of being hidden (the old behaviour hid the
  section and left its rows in the DOM).
- **Stats card and control bar are hidden until data has loaded**, so a failed load shows only the state.

## How it was verified

Playwright/Chromium against `python3 -m http.server 9380`, real `outputs/summary.csv`:

- `scripts/verify-design.py` exit 0, 28 checks pass (18 of 25 checks failed before the pass; the gate counts conditional checks when they apply) — `--screens` written to `design/screens/after-{top,full,phone,nodata}.png`.
- `scripts/verify-interactions.py` exit 0, all 28 checks, incl. the downloaded XLSX.
- `tests/validate_outputs.py` exit 0.
- By hand in a browser: KPI values re-computed from the CSV with pandas (638 / 0.9637 / 0.8206 LIMOSF11 /
  340 of 622); XLSX export driven by keyboard only (battery table, select-all -> Enter on Export ->
  `MLF_62_assets.xlsx`, opened with openpyxl): 62 rows, headers are a superset of what the table shows;
  theme toggle persistence and `?theme=` override; missing (aborted), 404, and empty-file states;
  phone overflow and target sizes in light and dark.

## Deliberately left alone

`outputs/**`, `data/**`, `src/**`, `deploy/**`, `tests/**`, `.github/**`, `requirements.txt`; the token
file and `tailwind.config.js` (frozen); the XLSX export code and filename shape; PapaParse/SheetJS; the
`<title>` ("... NEM Generator Loss Factors"); the sort logic (blank values still sort last).

## Known issues / things I suspect I bent (circle back)

1. **Import semantics need a domain check.** The battery head says the Import MLF is "a factor on the
   price it pays, not the price it receives, so a low import MLF is not a loss". That is my reading of
   how loads are settled; please confirm before it goes to `main`.
2. **The ramp saturates at the deep end.** With the shared edges, FY26-27 lands 93/196/121/81/49/29/56/10
   assets on steps 0-7; steps 6-7 (< 0.90) hold 66 of 635 and the two deepest steps are hard to tell
   apart in dark (light blue vs lighter blue). Matches the sibling page deliberately; changing it is a
   family decision.
3. **Dark ramp direction is "lighter = more lost"** (token design), which is the opposite of the old
   red-is-bad intuition; the legend states it but it is a change of habit.
4. **Battery table is mostly `N/A`** for FY15-16 to FY23-24 and the Import columns before FY24-25: this
   is the data (new assets), not a bug, but it reads as a wall of N/A.
5. **Sticky bar is two rows tall at 1440px** (~96px) because the export group wraps; a single-row bar
   needs shorter labels.
6. **Draft-column path is untested with data** — the CSV has no `Draft` columns today. The code path
   and its `--warn` styling exist but were never rendered.
7. **Not reviewed visually:** the Import/retired/empty-state combinations in light theme beyond one
   battery screenshot; Safari and Firefox (only Chromium was run; `color-mix()` and `:has()` need a
   modern browser).
8. **Layout jump on load:** the stats card and bar appear when data arrives, below a skeleton card.
9. **The group header label** ("MLF by financial year ...") is clipped at the left edge on a phone
   once you scroll the table sideways (it does not stick to the viewport).
10. **Export is a superset and writes blanks as empty cells** (as before), not "N/A"; it includes
    `Technology` and every FY column regardless of what the table shows.
11. `tools/tailwindcss` is gitignored; `assets/css/app.css` must be rebuilt (`./scripts/build-css.sh`)
    after any class change.

## Follow-up (Hermes, 2026-09-30): the Import columns off the loss ramp

Item 1 above is confirmed, and it changed more than the wording. In the NEM the MLF is a multiplier on
the regional reference price to give the local price at each connection point — the same form in both
directions, which is why loads "tend to have MLFs greater than one: the price paid by the load is
increased" (AEMC, Application of Dual Marginal Loss Factors, quoting NER 3.6.2(b)(2) — the clause that
exists so a bidirectional point gets two MLFs). A **low** import MLF therefore means the battery charges
cheaply. Two corrections followed:

- **Import MLF cells are off the loss ramp.** 124 of the 159 published values are below 1.00, so on the
  shared ramp the cheapest charge prices drew the strongest "more lost" fill — QPSFB2 FY25-26 showed
  export 1.0190 with no fill beside import 0.9176 at `seq-6`. They keep the number (right-aligned, 4 dp),
  the dashed accent marking and the `imp` class; they no longer carry `seq-*`. The card-head sentence
  saying they "use the same ramp so the two can be compared" was corrected, and the battery legend
  gained a stated import rule (`data-heat-legend="import"`).
- **Import YoY is sign-only.** The intent was already in the code (`tone = imp ? '' : …`) but `isImp()`
  matched only `FY… Import` and never the CSV's `IMPORT_YOY_*` keys, so `imp` was false and the
  generator good/bad colours were applied — a **rising** import MLF (dearer charging) rendered green.
  `isImp()` now covers both naming conventions, and the import YoY header/cells pick up the dashed
  marking for free.

`scripts/verify-design.py` gained three checks so neither can come back: import cells must not be on the
ramp, must keep their dashed marking, and import YoY must print its sign with no good/bad colour. Proven
by reverting `index.html` to `9961657` and watching exactly those two go red. Gates on the fixed page:
data exit 0 · behaviour 28/28 · render 34/34.

Measured, not fixed — **the "MLF by financial year" group label cannot be pinned for a sideways scroll.**
The colspan cell is ~924 px wide, so the sticky clamp holds its left edge at the containing block's right
edge; a nested sticky label clamps to the same edge (at 390 px, full scroll leaves a 21 px strip for a
235 px label), and pinning it behind the pinned columns hides it instead. The meaning is carried by the
card head and the legend, which stay visible because the table scrolls inside its own box — so it is left
as-is deliberately, with a comment in the gate where a check would otherwise have gone.
