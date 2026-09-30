# Design-pass contract — AEMO MLF Tracker

Read this before touching anything. `BRIEF.md` says *what* to build; this says how work here is
allowed to happen.

## The five hard rules

1. **Work on the branch, never on `main`.** This copy is on `design/2026-10`. `main` is published:
   GitHub Pages serves this repository and the NAS lane pushes `outputs/**` to it. Nothing lands on
   `main` until Zalen has looked at the rendered page.
2. **Presentation only.** Never touch the data or the pipeline: `outputs/**`, `data/**`, `src/**`,
   `deploy/**`, `.github/**`, `requirements.txt` and `tests/**` are off limits. This pass changes how
   the page looks, never what it says.
3. **Never invent data.** Every number on screen comes from `outputs/summary.csv` (736 DUIDs × 38
   columns) and is traceable to the source named in the card foot. No mock arrays, no sample series,
   no "for now" values. A missing value stays stated — that is an answer, not a gap to fill.
4. **Verify in a browser, not by grep.** Done = a real browser renders it with real data: element
   present, height > 0, text non-empty. An HTTP 200 on the HTML proves nothing about a visual change.
5. **Keep the two gates green.** They are committed and they are the handback evidence:

   | command | today (measured 2026-09-30, before the pass) | at handback |
   |---|---|---|
   | `/opt/anaconda3/bin/python3 tests/validate_outputs.py` | exit 0 — "All validations passed" | exit 0 (the data gate; not yours to edit) |
   | `/opt/anaconda3/bin/python3 scripts/verify-interactions.py` | exit 0 — 28 checks, "all interactions intact" | exit 0, unchanged |
   | `/opt/anaconda3/bin/python3 scripts/verify-design.py` | **exit 1** — 18 of 25 checks fail (listed in `BRIEF.md`) | **exit 0** — this is the pass's own gate |

## Facts

| | |
|---|---|
| What it is | One static page: `index.html` (572 lines — one inline `<style>`, one inline `<script>`) |
| Served by | **GitHub Pages from the repository root** (branch `main`, folder `/`; `build_type: legacy`, no deploy workflow). Public, no server, no login, no build step at deploy. |
| **Everything committed here is public** | the deployed site *is* the whole tracked tree. Never commit a secret, a private snapshot, or a screenshot you would not publish. Evidence lives in `design/` **on the branch** — it is not part of the site only for as long as it never reaches `main`. |
| Stack | hand-written HTML/CSS + vanilla JS · PapaParse 5.4.1 (CSV) and SheetJS 0.18.5 (XLSX, client-side export) from CDN · the compiled Tailwind stylesheet `assets/css/app.css`, built by the standalone CLI (no Node, no npm). No chart library — the heat is HTML cells. |
| Data | `outputs/summary.csv` (736 rows × 38 columns; 12 financial years FY15-16 … FY26-27, plus 7 per-FY `Import` columns, all battery-only) + 5 regional `.xlsx`, fetched relative to the page — served because they are committed |
| Design language | `assets/css/tailwind.src.css` (the only file with literal colours) → `./scripts/build-css.sh` → `assets/css/app.css` (committed). Rules: `design/design-tokens.md`. Proof page: `design/tokens.html` |
| Data lane | `deploy/run-update.sh` on the NAS (QNAP `ai-wif-runner`): fetch → refresh → validate → commit `outputs/` as `aemo-nas-bot` → push. Annual cadence (final MLFs in April, draft/indicative in October); it never writes `index.html`. |
| Upstream | AEMO `DUDETAILSUMMARY` via the MMSDM archive (complete MLF history in one file), with generator metadata resolved from three further AEMO sources — see the repo README |
| Why this copy exists | The live checkout is `~/Documents/Zalen/AI Wif Brain Projects/AEMO MLF Tracker`. The lane hard-resets it to `origin/main`, and macOS TCC blocks GUI agents from running git under `~/Documents`. This clone carries its own `.git` and cannot be clobbered. |

## Local preview

```bash
cd ~/Design/"AEMO MLF Tracker" && /opt/anaconda3/bin/python3 -m http.server 9380 --bind 127.0.0.1
# http://127.0.0.1:9380/index.html      9380 is this pass's port — do not take another
```

Cloud browsers cannot reach `127.0.0.1`; screenshots and checks go through Playwright:

```bash
/opt/anaconda3/bin/python3 scripts/verify-design.py --screens   # + design/screens/after-*.png
```

## DOM contract — the hooks the gates and the next agent depend on

| Hook | Meaning |
|---|---|
| `<html data-theme="dark\|light">` + `#themeToggle` | theme; `?theme=light` / `?theme=dark` must force one |
| `#stats` → `.kpi-value` + `.kpi-label` | the KPI strip (≥ 4 valued, labelled tiles) |
| `#tabs` → `.tab` / `.seg-item` (active state) | All + 5 region switches |
| `#regionFilter` (select, All mode), `#typeFilter`, `#fuelFilter`, `#search`, `#showRetired` | the filters — keep their ids, they are the wiring |
| `#genSection` / `#genTable` / `#genThead` / `#genTbody` | the generator table (576 live rows today) |
| `#battSection` / `#battTable` / `#battThead` / `#battTbody` | the battery table (62 live rows today), the only table with `Import` columns |
| rows carry `data-duid` | selection and the gates key off it |
| MLF cells: `td.mlf-cell` carrying `seq-0` … `seq-7` (or `seq-none`) | the ramp — no inline `rgb()` |
| `[data-heat-legend]` naming the stops **and** the direction ("more lost") | the stated scale |
| unpublished MLF values stated (`N/A`), never a bare `-` | absence is explained, never blanked |
| `#exportSelected`, `#clearSelection`, `#selCount`, select-all `[data-selectall]` | selection + the SheetJS export (`MLF_<n>_assets.xlsx`) |
| `a[download]` × 6 | 5 regional workbooks + the CSV |
| `.state` > `.state-title` + `.state-body` | loading / missing-file / no-matches states |
| `#footer` (or `.card-foot`) containing `Source:` and what the count covers | provenance |

Renaming or dropping any of these breaks a committed gate. If a name must change, change the gate in
the same commit and say why.

## Pitfalls

- **`mlfColor()` + `textColor()` write a diverging red→yellow→green as an inline
  `style="background:rgb(...); color:#1a1a1a"` on every cell** (0.80 → 0.95 → 1.05, with a contrast
  guess). Two consequences: the page cannot follow a theme flush, and "red" on this page means *two*
  different things — a low MLF *and* the family's `.pill-bad`. Moving it onto the ramp means the JS
  picks a **step**, never a colour, and the meaning is stated on screen.
- **The page's own inline `:root` palette wins over the token layer.** `--bg:#0f1117`, `--surface:#1a1d27`,
  `--accent:#4472C4` … are declared *after* the `app.css` link, at equal specificity, so the body
  stays `rgb(15,17,23)` and a `data-theme` flip changes nothing until that block is deleted. Preflight
  is ON in `tailwind.config.js` and becomes the page's only reset the moment the inline
  `* { margin:0; padding:0 }` and body rules go (BRIEF step 1).
- **Blank values inflate what the page says.** 4 rows carry an empty `REGIONID` and 57 an empty
  `FUEL_CATEGORY` (43 of them live), so the stat row renders an "UNKNOWN 43" card and "OTHER 5" beside
  the real fuels, and the footer says "736 generators tracked" when the file holds 736 *DUIDs* of
  mixed types (723 Generator, 12 Unknown, 1 Network Load), 638 of them live. Unknown values must never
  become a filter option or inflate a count — decide and state what they do.
- **A hidden section keeps its rows.** `renderOneTable()` sets `section.style.display='none'` and
  returns without clearing the tbody, so after `fuel=Wind` the battery table still holds 62 stale rows
  in the DOM (invisible to the reader, but it is how a checker reports 169 rows for a 107-row filter,
  and it means "empty" is never actually stated anywhere). An empty result must be *stated*, and stale
  rows must not persist.
- **`.draft-header` / `.import-header` are literal hexes** (`#5a4a8a` / `#4a6a3a` plus their hover
  variants) and the `Import` columns carry a dashed `#7b68ae` left border with nothing on the page
  explaining what "Import" means. Today's CSV has **no `Draft` columns** — the code path exists for
  them; keep it working, and use a token for whatever distinguishes the column groups.
- **`th`/`td` are centred** (`th` on the accent fill, uppercase-ish, sort shown only as a ▲/▼ appended
  to the sorted header). The family right-aligns numeric cells in tabular figures — MLF at 4 dp must
  line up down the column, and the sort affordance (`aria-sort` + a visible arrow) should not depend on
  guessing which header is active.
- **Tailwind purges what its scanner cannot see.** `index.html` is a content source (so class strings
  built in the inline script are found), but a class added only to the CSS does nothing until
  `./scripts/build-css.sh` runs and `assets/css/app.css` is committed. `.seq-*` sit outside `@layer`,
  so purging never removes them.
- **The XLSX export is generated client-side** from `allData` (not from the visible selection), and its
  filename is `MLF_<DUID>.xlsx` / `MLF_<n>_assets.xlsx`. Keep it working, keep the columns it writes,
  and re-verify it after the restyle — the gates pin the filename shape and the download.
- The table's own scroll box (`max-height: 75vh`) is deliberate — 638 rows across 21–30 columns. Keep a
  contained scroll box, not a 5,000-pixel page.
- Nothing in this repo feeds the Brain dashboard or any lane. Do not add a side channel.

## Finishing (the handback)

- [ ] Working tree clean; everything committed **on `design/2026-10`**.
- [ ] Branch pushed: `git push -u origin design/2026-10` — a branch that exists only in this folder
      dies with the folder. (Do not push `main`, do not merge, do not open a PR.)
- [ ] All three gates run and their exact results stated.
- [ ] After-screenshots in `design/screens/` (desktop + phone, same scroll positions as `before-*.png`).
- [ ] `./scripts/build-css.sh` run after the last class change; `assets/css/app.css` committed.
- [ ] `git diff --stat main` shows no `outputs/**`, `data/**`, `src/**`, `deploy/**`, `tests/**`,
      `.github/**`.
- [ ] `docs/design-pass-2026-09-30.md` — what changed (files + visual summary), how it was verified,
      what you deliberately left alone, anything you suspect you bent. This is how the next agent
      reconstructs intent without the transcript.
- [ ] A short report: surfaces changed of the ones that exist; what is half-done; decisions the brief
      did not cover.

If time runs out mid-change: commit what works, leave the branch pushed, and say plainly what is
half-done. Never leave a half-finished change uncommitted — an end-session sweep would commit it as
one opaque blob.
