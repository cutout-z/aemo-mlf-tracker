# Updates — NAS runner (production)

The production model:

- The **NAS runner** (QNAP `ai-wif-runner` container) runs the MLF
  refreshes.
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
| MLF refresh | `--full-refresh` | Re-fetch every source and republish. AEMO's draft for the next FY is published early in March (2 March 2026 for 2026-27) and replaced by the final by 1 April, so only a run in March shows a draft column; at other times the draft is recorded as not yet published. |

The lane registry, cadence windows and report paths live in
`tools/nas-runner/configs/brain-ops.nas.toml` (the NAS runner tooling).
`deploy/run-update.sh` runs the output validator (`tests/validate_outputs.py`,
not the pytest suite) and commits/pushes only when `outputs/` changed, and the script self-heals a rewritten `main`: if
`git pull --ff-only` is impossible it resets onto the fetched remote instead
of exiting 128.

After the push (or the "no changes" exit) it runs `python -m src.post_publish_check`,
which reads `outputs/run_status.json` and fails the lane when the run is in March and the
draft workbook was blocked: the page has already gone out without the draft column (its
footer says the draft is out but could not be downloaded), and the lane goes RED so the
fetch gets looked at. A blocked draft outside March, or a draft not yet published, passes.

## Env

`deploy/env.example` documents the settings the lane injects (`APP_DIR`,
`PIPELINE_ARGS`, test/push toggles).
