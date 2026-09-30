# BRIEF — AEMO MLF Tracker redesign, pass 1

**Read `CLAUDE.md` → `AGENTS.md` (the contract) before writing code.** The first line of your session
should be `git status` and `git rev-parse --short HEAD` — if git fails here, stop and say so rather
than working around it.

*Handover prompt (if the session is started fresh, this is all that needs saying):*
> Project folder `~/Design/AEMO MLF Tracker`. Read `CLAUDE.md`, then `AGENTS.md`, then execute
> `BRIEF.md` — and stop after step 1 to report.

## The goal, stated as an outcome

This page answers one question — *how much of the regional price does each asset lose on the way in,
and is it getting worse?* — and it answers it badly: a 2019-era table app that paints every MLF cell
with its own inline gradient and explains nothing. Make it **look like it was designed by someone who
cares** and like it belongs to the same product family as the other AEMO dashboards — modern,
restrained, scannable.

**Definition of done = the real page, rendering real data, looking right when screenshotted.**
Adopting a token file, adding a stylesheet, "styling centralised", or a theme module is NOT done.

## Hard constraints

- **It stays static.** GitHub Pages serves the repo root — one page, no server, no build step at
  deploy, no self-hosting. Everything must work as plain files a browser loads.
- **Interactivity is client-side only**, and it is the product: the region tabs and dropdown, the
  type/fuel filters, search, the retired-DUID toggle, column sorting, select-all, the client-side XLSX
  export of the selection, the six downloads and the sticky header. All of it must still work.
- **One page.** `index.html`. Do not split it, do not add a framework.
- **No new runtime dependencies.** PapaParse and SheetJS stay the only scripts. No chart library: the
  heat is HTML cells.
- **Presentation only** — `AGENTS.md` lists the forbidden trees.

## The design language — adopt it, do not invent a second one

| Where | What |
|---|---|
| `assets/css/tailwind.src.css` | **The tokens.** Surfaces, text, status, radii, shadows, both themes, and the sequential ramp `--seq-0…7` / `.seq-0…7`. The only file with literal colours. |
| `design/design-tokens.md` | The seven rules (colour = entity, contrast floor, heat needs a stated scale, …) |
| `design/tokens.html` | **The proof page** — palette, type scale, controls, a dense table with grouped columns, the ramp, the states. Open it first (`http://127.0.0.1:9380/design/tokens.html`). |
| `~/Design/aemo-credit-design`, `~/Design/AEMO Renewable Generator Dashboard` | The sibling AEMO passes, already live (same tokens). Match their language; do not copy their markup. |

The rules that matter most here: **colour belongs to the metric family, not to a column index**;
**status colour is never the only signal** (a cell prints its number, a change prints its sign, a
column group carries words); **the heat scale is stated on screen, including which direction is
worse**; **`--faint` is the floor for 12px text**; **`.card` is the unit of composition and its foot
carries provenance — including which publication rhythm the panel shows**.

**The one design decision the brief does force:** MLF is a ratio centred on 1.0000, not a magnitude,
so a *diverging* scale looks natural — and it is wrong for this family. The sibling pass already
solved it: **render MLF as price lost (1 − MLF) on the one sequential ramp**, so 1.0000 = no fill and a
deeper loss = a stronger step, with the mapping and the stops printed above the table (the renewable
page carries the same scale as `>= 1.00, 0.98, 0.96, 0.94, 0.92, 0.90, 0.85, < 0.85`). Match it. If
you are convinced one ramp cannot carry both meanings, define the second scale **in the token file**
and say why in the commit message — do not add a second inline colour function.

## Baseline — measured on this clone, 2026-09-30, before the pass

`python3 scripts/verify-design.py`: **18 of 25 checks fail**; `scripts/verify-interactions.py`: 28
checks, exit 0. Page facts: **1,863 px tall at 1440×900**, **638 live assets** across two tables
(576 generators + 62 batteries; 98 retired DUIDs behind the toggle), 21 header cells in the generator
table and 30 in the battery table, 9 fuel-count stat cards, 3 selects + 1 search + 1 retired checkbox,
**2,630 blank MLF cells** across the 12 financial years, footer reading "736 generators tracked ·
FY15-16 to FY26-27". Phone 390px: **the document is 659 px wide — 269 px of page-level sideways
overflow**, and the filter controls are 29–31 px tall.

The failures, and the visible problems behind them:

1. **No shell.** Five unaligned bands (title, stat cards, tabs+filters, action buttons, table boxes,
   footer), each with its own margins, and not a `.card` among them. Nothing states what the page is
   for or what a "good" MLF looks like.
2. **The stat cards quote raw fuel counts** (`.stat-card` / `.stat-label` / `.stat-value`) and two of
   them are artefacts of blank data: **"UNKNOWN 43"** (rows with an empty `FUEL_CATEGORY`) and
   **"OTHER 5"**. The footer's "736 generators tracked" is likewise 736 *DUIDs* of mixed types, 638 live.
3. **21 literal hex colours in the page** — the inline `:root` palette (`#0f1117`, `#1a1d27`,
   `#4472C4` …), the draft/import header fills (`#5a4a8a`, `#4a6a3a` + hovers), the type badges, and
   `change-pos`/`change-neg`. The page's own palette also **overrides the token layer**, which is why
   the body is still `rgb(15,17,23)` and a theme flip would change nothing.
4. **`mlfColor()` writes a diverging red→yellow→green as an inline `rgb()` on every cell**
   (0.80 → 0.95 → 1.05) with a `textColor()` contrast guess — so "red" means both *a low MLF* and the
   family's bad status, the page cannot follow a theme, and there is **no legend and no stated stop**
   anywhere.
5. **Every numeric cell is centred**, MLF at 4 dp, no right alignment down the column; the header is a
   solid accent fill with sort indicated only by a ▲/▼ appended to the sorted header.
6. **The `Import` columns are a mystery**: a dashed border and a green-ish header with nothing on the
   page saying what an import MLF is — and the battery table interleaves 7 of them with the plain FYs.
7. **The filters are unstyled native controls** pushed to the right of the tabs; the action row is
   plain buttons; the selection state is the string "N assets selected".
8. **2,630 unpublished MLF values render a bare `-`**, with no reason anywhere on the page.
9. **No light theme, no toggle, no `?theme=` forcing.**
10. **Phone:** 269 px of page-level overflow, 29–31 px targets.
11. **No states at all**: no skeleton (just "Loading data..."), no panel when `outputs/summary.csv` is
    missing (a bare sentence), no panel when a filter matches nothing — `renderOneTable()` hides an
    empty section and **keeps its stale rows**, so "nothing here" is never actually stated.

## Order of work

One commit per step. **Stop after step 1 and report before scaling to the rest.**

1. **Wire the token layer and the shell.** Add the `assets/css/app.css` link *before* the existing
   inline `<style>` so nothing shifts by accident, then convert the page chrome: frame, background,
   typography, the title/subtitle block, the stat row (`.kpi-value` / `.kpi-label`), the tab row
   (`.seg` / `.seg-item`), the controls (`.input`, `.btn`), the footer (`.card-foot`). **Delete the
   inline `:root` palette as you go** (the tokens replace it) and, once the old inline rules for those
   elements are gone, delete the inline reset — `preflight` is already ON in `tailwind.config.js` and
   must end up as the page's *only* reset — then rebuild with `./scripts/build-css.sh` and commit
   `assets/css/app.css`. Report here.
2. **Header + KPI strip.** Four to six tiles, each with its label and its scope, all computed from the
   CSV — e.g. DUIDs tracked (live, of the total), the five regions, the **average MLF for a named FY**,
   the deepest loss in that FY (named), how many assets deteriorated year-on-year. State the
   publication rhythm (final MLFs in April, indicative/draft in October) in the card foot. Keep the
   fuel breakdown as a labelled row rather than nine loose cards, and **decide what the blank
   `REGIONID` / `FUEL_CATEGORY` rows do** — they must never inflate a count or become a filter option
   without saying so.
3. **One control bar.** Region as `.seg`, search as `.input`, the type/fuel/region selects restyled,
   the retired toggle as a proper switch — **keeping `#tabs`, `#regionFilter`, `#typeFilter`,
   `#fuelFilter`, `#search`, `#showRetired` and every behaviour** — counts as `.badge`s, and the
   selection count surfaced where the export lives. It should stay put while the tables scroll.
4. **The two tables.** `.card` per table with head (title + what the columns mean) → body (the table) →
   foot (provenance). Numerals right-aligned in tabular figures at 4 dp so columns scan; a real sort
   affordance (`aria-sort` set, arrow visible) that still sorts on click; both header rows sticky; the
   DUID pinned on phone. Heat cells become `td.seq-0…7` with **the scale stated on screen** (the
   stops *and* the direction, per the mapping above). Distinguish the `Import` columns with tokens
   **and explain them in the head or the foot**. Every unpublished value is stated (`N/A`) with the
   reason — "no MLF published for that financial year" — never a bare `-`.
5. **Selection and export.** Row + select-all checkboxes, `.btn` export/clear, keyboard reachable, the
   SheetJS export still writing the same columns under the same filename shape (`MLF_<n>_assets.xlsx`).
6. **States.** Loading (skeleton rows), missing file (`.state` naming `outputs/summary.csv`), and a
   filter combination with no matches — stated in the table area, and **no stale rows left behind in a
   hidden section**.
7. **Themes.** Dark default; a toggle remembered per viewer; `?theme=light` / `?theme=dark` force one.
   Every surface, label and **heat step** must flip from the tokens alone.
8. **Phone (390px).** No page-level sideways scroll (269 px today), controls wrap, both tables scroll
   inside their cards, tap targets ≥ 32px.
9. **The interactions you must not lose** — the 28 checks in `scripts/verify-interactions.py`.

## Evidence (part of done, not optional)

- `scripts/verify-design.py` exits **0** (25 checks). `--screens` writes `design/screens/after-*.png`.
- `scripts/verify-interactions.py` still exits **0**, same check count — including the downloaded XLSX.
- `/opt/anaconda3/bin/python3 tests/validate_outputs.py` exits 0 ("All validations passed").
- Screenshots for every surface you changed, desktop **and** phone, at the same positions as
  `before-*.png` (top, full, phone, no-data). Evidence lives in `design/screens/` — never next to the
  page, never a new root-level folder.
- Re-verify the export by hand once: select all → Export → open the workbook and confirm the columns
  are the ones the table shows.
- `./scripts/build-css.sh` run after the last class change, `assets/css/app.css` committed.
- `git diff --stat main` shows no `outputs/**`, `data/**`, `src/**`, `deploy/**`, `tests/**`,
  `.github/**`.

## Report back

Surfaces changed of the ones that exist; what is half-done; any decision the brief did not cover (in
particular: how you mapped MLF onto the ramp and what the legend says, how the `Import` columns are
distinguished, and what you did with the blank region/fuel rows). Do not claim a surface is done
because the HTML changed — it is done when a browser renders it with real data and the gate says so.
