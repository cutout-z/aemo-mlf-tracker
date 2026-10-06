# Annual Updates — NAS runner (production)

The production model:

- The **NAS runner** (QNAP `ai-wif-runner` container) runs the annual
  final/draft MLF refreshes.
- GitHub stores code and publishable `outputs/`.
- GitHub Pages deploys after the NAS lane pushes updated outputs.
- GitHub Actions remains available for manual verification, but is not the
  primary scheduled data runner.

The source data footprint is tiny, so the lane intentionally runs
`--full-refresh` rather than preserving long-lived source caches.

## Lane

QNAP scheduled tasks invoke `nas-job aemo-mlf-tracker`, which runs this repo's
`deploy/run-update.sh` (renamed from the retired VPS-era `run-vps-update.sh`
in the 2026-09 cleanup) with the lane's `PIPELINE_ARGS`:

| Lane | `PIPELINE_ARGS` | Purpose |
| --- | --- | --- |
| Annual MLF refresh | `--full-refresh` | Refresh after AEMO's final MLFs (by 1 April) and the new year's DUDETAILSUMMARY load (July). AEMO's draft for the next FY is published early in March (2 March 2026 for 2026-27), so only a run in March shows a draft column; an October run finds no draft. |

The lane registry, cadence windows and report paths live in
`tools/nas-runner/configs/brain-ops.nas.toml` (the NAS runner tooling).
`deploy/run-update.sh` runs the full test suite and commits/pushes only when
`outputs/` changed, and the script self-heals a rewritten `main`: if
`git pull --ff-only` is impossible it resets onto the fetched remote instead
of exiting 128.

## Env

`deploy/env.example` documents the settings the lane injects (`APP_DIR`,
`PIPELINE_ARGS`, test/push toggles).
