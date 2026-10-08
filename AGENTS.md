# Repo contract: AEMO MLF Tracker (`cutout-z/aemo-mlf-tracker`)

Read this before touching anything, for a design pass or a logic/integrity pass. It says how work
here is allowed to happen; a task brief says what to do. The family conventions and shared AEMO facts
below are generated from the `agent-contracts` repo; where this file's repo-specific rules are
stricter, they win.

<!-- BEGIN agent-contracts:family -->
<!-- source: family/AGENTS.family.md sha256:f43f0fc253a7 — edit in cutout-z/agent-contracts, not here -->
## Family conventions (every repo, every agent)

*Generated from `agent-contracts/family/AGENTS.family.md`. Edit it there, never here: a drift check
reports any local edit.* These apply to every agent (Hermes, Claude Code, Codex or any other),
whatever app drives it. Where this repo's own rules (above or below this block) are stricter, they
win. Machine-specific conventions (which checkout is which, the lane runtime, where memory lives)
are in the owner's private contract, which each harness loads separately.

### Branches, concurrency, cleanup

- Work in a working clone, on a branch, never in a live or serving checkout. Merge to `main` only if
  this repo's contract says the agent may; otherwise push the branch and hand back for review.
- Other agents may be working in this repo right now. Fetch before you act. If a branch moved
  unexpectedly, or files you didn't touch changed, stop and report rather than reconcile.
- Use a worktree for parallel work, not a second clone. At session end, remove the worktrees you
  created and leave each checkout on the branch it was on when you arrived.
- Push every branch you want kept. An unpushed branch is one disk failure from gone.

### What needs the owner's yes

An explicit instruction from the owner in the current session covers that action only, not similar
later ones. Without one, declare these and wait for a yes:

- **Publishing**: anything that changes what other people can see (`main` on a published repo,
  Pages, public data files).
- **Data and ETL**: data files, pipeline code, data contracts (columns, keys, paths, schemas).
- **The instruments**: `check.sh`, guard tests, audit and verify scripts. Never weaken one to make
  something pass. If one is wrong, say so and leave it.
- **Infra**: ports, scheduled jobs, servers, publish pipelines.
- **Another agent's state**: another agent's memory, config or notes.

Never read, quote or commit secrets: `.env`, auth files, keys, tokens.

### Verification standard

- A change is done when you have seen the evidence yourself: tests run (exact counts, failures
  named, pre-existing failures shown to exist on `main`), and for UI, a real browser render.
- For a fix, show its test fails with the fix reverted and passes with it applied.
- Don't relay another agent's or subagent's numbers. Re-run or read the evidence yourself.

### Attribution and handback

- **Every commit names its agent** in a trailer: `Co-Authored-By: <Agent> <model> <email>`, e.g.
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`, `Co-Authored-By: Codex …`,
  `Co-Authored-By: Hermes …`. Audits match on the `Co-Authored-By: <Agent>` prefix.
- **End every piece of work with a handback note on the branch**, using the repo's own path
  convention, or `docs/handback-<YYYY-MM-DD>-<topic>.md` if it has none. On a repo whose `main` is
  published, keep notes and evidence on the branch; they don't merge. Start the note with:

  ```yaml
  ---
  project: <project name as on the owner's board>
  agent: claude | codex | hermes
  branch: <branch>
  merged: false
  status: <one line>
  outstanding:
    - "to-do [mac-local] <item>"   # [mac-local] = only actionable on the owner's Mac
  ---
  ```

  Then cover what changed, the evidence, what's left, and any decision the brief didn't cover.
<!-- END agent-contracts:family -->

<!-- BEGIN agent-contracts:aemo-facts -->
<!-- source: facts/AEMO-FACTS.md sha256:c6c3cdf689bd — edit in cutout-z/agent-contracts, not here -->
## AEMO shared facts — every agent, every repo

One canonical home for cross-repo AEMO domain facts, so a fact established in one repo is never
invisible to an agent working in another (the battery-MLF lesson, 2026-10-05: the TLF orientation
was established inside one repo's rollout doc while other agents recomputed with the wrong factor).

**Who reads this:** any agent (Hermes, Claude Code, Codex) working in an AEMO repo. It is stamped
into each AEMO repo's `AGENTS.md` between `agent-contracts:aemo-facts` markers, and the `aemo-audit`
and `aemo-logic-pass` skills point here instead of duplicating facts. A fact here overrides anything
remembered or re-derived.

**Maintenance:** append with date + source; supersede in place (`~~SUPERSEDED~~ <date> <reason>`).
Location: `agent-contracts/facts/AEMO-FACTS.md` (git, `cutout-z/agent-contracts`). Edit here only; `scripts/stamp.py` copies it into each AEMO repo's AGENTS.md.

| Fact | Detail | Verified | Source |
|---|---|---|---|
| TLF orientation | `TRANSMISSIONLOSSFACTOR` = **Import** MLF; `SECONDARY_TLF` = **Export** MLF for BIDIRECTIONAL (battery) units. A battery's export MLF comes from `SECONDARY_TLF`, never `TRANSMISSIONLOSSFACTOR`. Confirmed 51/51 differing batteries against AEMO's 2026-27 workbook. The MLF Tracker used the import factor for FY24-25/FY25-26 battery export values until fix `dbaf6f7` (2026-10-05); downstream battery revenue was restated (~−$13.1M across 26 batteries' months) after the fix. | 2026-10-05 | AEMO 2026-27 MLF workbook; `aemo-credit-design/audits/Logic Pass Rollout 2026-10-05.md` |
| FCAS regime | FCAS causer-pays contribution factors are DEAD — replaced by the Frequency Performance Payment (FPP) on 8 June 2025 (5-minute contribution factors on NEMWEB). Never recommend or resurrect causer-pays factor tracking; FPP cost-allocation factors per DUID are the future extension. | 2026-09-01 | AEMO/NEMWEB; `aemo-audit` skill; credit repo `docs/FUTURE_DATA_SOURCES.md` |
<!-- END agent-contracts:aemo-facts -->

## Hard rules

1. **Everything tracked on `main` is public.** GitHub Pages serves the whole tree from `main`, `/`
   (legacy build, public repo). Never commit a secret, a private snapshot or design evidence
   (`design/`, screenshots, briefs) on a path that can reach `main`.
2. **`outputs/**` belongs to the pipeline.** It is written only by `python -m src.main` and committed
   by the NAS lane as `aemo-nas-bot`. Never hand-edit it. Do not run `src.main`, the lane or the
   fallback workflow unless the task says so: they fetch from AEMO and their output publishes.
3. **Data contracts change only with the owner's yes** (table below). The credit dashboard reads the
   published `summary.csv`, so a rename here breaks a repo you are not working in.
4. **Never invent a number.** Every MLF traces to DUDETAILSUMMARY or an AEMO MLF workbook. A missing
   value stays missing (the page states `N/A`); it is not filled from a neighbouring year.
5. **Keep the gates green**: `pytest` and `tests/validate_outputs.py` (commands below). If a change
   breaks a test, the change or the test is wrong; say which.

## Facts

| | |
|---|---|
| What it is | Python pipeline (`src/`) builds per-DUID MLF history FY15-16 to FY26-27; one static page `index.html` (881 lines) reads it (README.md) |
| Served by | GitHub Pages, branch `main`, path `/`, `build_type: legacy`, public, no deploy workflow (`gh api repos/cutout-z/aemo-mlf-tracker/pages`, 2026-10-08) |
| Live URL | https://cutout-z.github.io/aemo-mlf-tracker/ (README.md) |
| Stack | Python 3.11 in CI, pandas, openpyxl, requests, pyarrow (`requirements.txt`, `.github/workflows/annual-update.yml`). Page: vanilla JS, PapaParse 5.4.1 and SheetJS 0.18.5 from jsDelivr, compiled Tailwind `assets/css/app.css` (`index.html` lines 7-9) |
| Production lane | NAS runner (QNAP `ai-wif-runner`), `nas-job aemo-mlf-tracker` runs `deploy/run-update.sh` with `--full-refresh`: pipeline, validator, `git add outputs/`, commit as `aemo-nas-bot`, push `main`, then `python -m src.post_publish_check` (deploy/README.md, deploy/run-update.sh) |
| Lane schedule | Not in this repo (README.md). Private ops notes list `40 9 15 4 *`, annual 15 April; unverified against the NAS config |
| Fallback | `.github/workflows/annual-update.yml`, `workflow_dispatch` only; it also commits `outputs/` and pushes |
| Gitignored | `data/` (caches, workbooks), `tools/` (Tailwind binary), venvs (`.gitignore`) |
| Downstream | `aemo-generator-credit-dashboard` `src/fetch_mlf.py` fetches `…/aemo-mlf-tracker/outputs/summary.csv` and reads `DUID`, `CONNECTIONPOINTID`, `FYyy-yy`, `FYyy-yy (Draft)` |

### Data contracts (no change without the owner's yes)

| Contract | Defined in | Read by |
|---|---|---|
| `outputs/summary.csv`: 38 columns today (`DUID … DUID_TYPE, STATUS`), 722 rows (2026-10-07 data) | `src/analyse.py` | page, validator, credit dashboard |
| Column patterns `FYyy-yy`, `FYyy-yy Import`, `FYyy-yy (Draft)`, `FYyy-yy (Draft) Import`; `PREVIOUS_DUIDS`, `LATEST_MLF`, `YOY_CHANGE`, `IMPORT_YOY_*` | `src/analyse.py` 164-312 | page (`isDft`, import columns), `src/fetch_mlf.py` draft regex |
| `outputs/{NSW,QLD,VIC,SA,TAS}_mlf.xlsx` | `src/excel_output.py` 53 | page `EXCEL_FILES` |
| `outputs/run_status.json` keys (`run_date`, `mmsdm_month`, `final_workbook`, `draft_workbook.state` …) | `src/main.py`, `src/config.py` | page footer, validator input-age checks, `src/post_publish_check.py` |
| `DUID_TYPE` labels = README "Asset type labels" table | `src/generators.py` | `tests/test_repo.py` pins them equal |
| Validator bounds: 400+ rows, MLF in [0.5, 1.5], 5 regions, YoY within 0.001 | `tests/validate_outputs.py` | lane and fallback gate before commit |

## Local preview and verify

```bash
cd <repo> && /opt/anaconda3/bin/python3 -m http.server 9380 --bind 127.0.0.1
# http://127.0.0.1:9380/   serves the repo root, as Pages does; the page fetches outputs/ relatively
```
9380 is this repo's port (private design-pass notes; the
`design/2026-10` branch's own contract says the same). Cloud browsers cannot reach `127.0.0.1`; use
Playwright. Done means a real browser shows the tables filled from `summary.csv`, the footer filled
from `run_status.json`, and no console errors.

```bash
cd <repo> && /opt/anaconda3/bin/python3 -m pytest -q tests      # baseline 2026-10-08: 91 passed
cd <repo> && /opt/anaconda3/bin/python3 tests/validate_outputs.py  # exit 0 = "All validations passed"
```
- `tests/test_page.py` runs `index.html` functions under `node` and **skips** without it.
- The validator's workbook cross-check needs `data/final_mlf_<fy>.xlsx` (gitignored; only a pipeline
  run creates it). On a fresh clone it prints "skipped workbook cross-check"; that is environmental.
- The validator's input-age checks compare `run_status.json` to today (archive month 75 days, list
  60 days), so a checkout whose `outputs/` is old fails locally without any regression.
- The design gates `scripts/verify-design.py` and `scripts/verify-interactions.py` and
  `scripts/build-css.sh` exist only on branch `design/2026-10`, not on `main`.

## Pitfalls that have bitten

- **Battery MLF orientation** (fixed `dbaf6f7`, 2026-10-05). The pipeline read the import factor as a
  battery's export MLF: every DUDETAILSUMMARY battery year was swapped while the workbook year was not,
  so YoY mixed orientations (CAPBES1 -0.0606 instead of AEMO's +0.0009) and FY25-26 matched AEMO's
  Export MLF for 14/65 batteries (64/65 after). The credit dashboard consumed the wrong values
  (commit message; `aemo-credit-design/audits/Logic Pass Rollout 2026-10-05.md`). Orientation is set
  per record from `DISPATCHTYPE` in `src/analyse.py` 16-26. Guards: `tests/test_analyse.py`
  (`test_bidirectional_export_is_secondary_tlf`, `test_battery_yoy_matches_final_workbook_orientation`),
  `tests/test_validate_outputs.py::test_workbook_cross_check_catches_swapped_batteries`, and the
  validator's `check_against_final_workbook` (≥ 90% battery match, only when the workbook is cached).
- **A re-registered battery is one asset.** AEMO re-registered batteries as bidirectional units under
  new DUIDs in 2024-25 (HPRG1 → HPR1); the new row carries the old history and `PREVIOUS_DUIDS`
  (`e835396`, README). Do not split them back into two rows.
- **AEMO's missing file is a 403, not a 404.** A workbook that does not exist redirects to `/404`, a
  Cloudflare page returning 403. The redirect chain means "not published"; any other 403 or challenge is
  "blocked" (`dc752a9`). A blocked draft in March turns the lane RED after publishing (`ebb93db`).
- **Staging an ignored path kills the commit step.** The fallback ran `git add outputs/ data/*.feather`;
  git exits 1 on an ignored path, so it could never publish (`72b992e`). `tests/test_repo.py` checks
  every `git add` in the runners against `.gitignore`.
- **Docs are tested.** `tests/test_repo.py` fails if README.md or deploy/README.md dates the draft to
  October (AEMO publishes it early in March) or if the README type table drifts from the labels.
- **Merging a design branch by path.** Design work reaches `main` by carrying named files only. A carry
  list once matched `index.html` but not `assets/css/app.css`, so the live page ran against the old
  stylesheet (private design-pass notes, MLF `dc35712`). Print the carry list and check `app.css` is in it.
- **The lane resets `main`.** If `git pull --ff-only` fails it runs `git reset --hard origin/main`
  (`deploy/run-update.sh`), so whatever is on `origin/main` is what the next run builds and publishes.
- **`FY_END` follows the clock** (`src/config.py`): from April the new FY is expected. A test or
  check that hard-codes the current FY will break each April.

## Working alongside other agents

- The NAS lane (`aemo-nas-bot`) is the only routine writer of `outputs/` and pushes straight to `main`.
  Regenerated data from a logic pass has gone in as a owner-authored commit (`d78af62`, `3b7f102`).
- The credit dashboard depends on this repo's published CSV. A logic fix here publishes first, then the
  credit refresh runs (Logic Pass Rollout, owner step 2). Say in the handback if its inputs changed.
- Branch `design/2026-10` carries an older design-pass-only `AGENTS.md`/`CLAUDE.md` (736 rows, "draft
  in October"). This file replaces them; do not merge those two files from that branch.
- If you need data that does not exist (e.g. a cached workbook), stop and say so. Do not synthesise it.

## Handback checklist

- [ ] `pytest -q tests` result stated as counts (91 passed today); any skip named (node missing?).
- [ ] `tests/validate_outputs.py` exit code stated, and whether the workbook cross-check ran or skipped.
- [ ] `git diff --stat origin/main` shows no `outputs/**` unless the task was a regeneration the owner asked for.
- [ ] Any data-contract change named, with the owner's yes and its effect on `src/fetch_mlf.py` downstream.
- [ ] `src/analyse.py` touched: battery tests pass, and a battery spot check (old and new DUID) against
      AEMO's workbook is reported.
- [ ] Page changed: checked in a real browser at 127.0.0.1:9380 (desktop and phone width), console clean;
      `assets/css/app.css` rebuilt and committed if classes changed.
- [ ] README updated if behaviour, sources or type labels changed (the tests read it).
- [ ] Short report: what changed, what was verified and how, what is unverified or half-done.
