# AEMO MLF Tracker

**[Live Dashboard](https://cutout-z.github.io/aemo-mlf-tracker/)**

Automated tracker for Marginal Loss Factors (MLFs) across all generator assets in Australia's National Electricity Market (NEM). MLFs directly impact generator revenue — a 0.01 change can mean millions for large assets.

## What it does

- Downloads AEMO's DUDETAILSUMMARY table from the MMSDM archive (complete MLF history in a single ~380 KB zip)
- Reads AEMO's final MLF workbook for the current FY (published by 1 April, before DUDETAILSUMMARY carries the year) and, when available, the draft workbook for the next FY
- Resolves generator metadata from three AEMO sources (see [DUID identification](#duid-identification) below)
- Extracts per-generator MLFs across 12 financial years (FY15-16 to FY26-27)
- Computes year-on-year changes and flags degradation
- Outputs summary CSV, per-region Excel workbooks with heatmaps, and an interactive GitHub Pages dashboard

## Coverage

| | |
|---|---|
| **DUIDs tracked** | ~720 across all 5 NEM regions, ~650 of them live (2026-08 data; the page footer gives the current count) |
| **Regions** | NSW, QLD, VIC, SA, TAS |
| **Asset types** | Generator (batteries included), Scheduled Load, Network Load, Dummy Generator, Interconnector — see [Asset type labels](#asset-type-labels) |
| **Fuel types** | Solar, Wind, Hydro, Fossil, Battery, Other Renewable |
| **History** | FY15-16 to FY26-27 (12 years) |
| **Update frequency** | NAS lane refresh. AEMO publishes the final MLFs for the next FY by 1 April (effective 1 July) and the draft early in March (2 March 2026 for 2026-27) |

## Dashboard features

- **All Regions** tab with region dropdown filter, plus individual state tabs
- **Type filter** — one option per asset type present in the data
- **Stat tiles** — the Average MLF, Deepest loss and Worse-than tiles read generating units only (DUID type Generator, batteries included); loads, load points, dummy generators and Basslink stay in the tables
- Search by DUID or station name (a battery's DUID from before its bidirectional re-registration finds its current row)
- Sort by any column (click headers)
- Filter by fuel type
- Heatmap colouring (red = low MLF, green = high)
- Asset type badges — colour-coded labels on every row
- Select assets and export to Excel

## DUID identification

AEMO's MLF data (DUDETAILSUMMARY) covers ~750 DUIDs spanning over a decade, but no single AEMO reference file identifies all of them. The pipeline resolves metadata from three sources in priority order:

### Tier 1 — NEM Registration and Exemption List (current participants)
The primary source for currently registered assets. Provides station name, fuel type, technology, registered capacity and Dispatch Type for ~580 units (generating, bidirectional and scheduled-load units; Dispatch Type "Load" is labelled **Scheduled Load**).

Two additional sheets in the same file extend coverage:
- **Ancillary Services** — DUIDs registered for FCAS markets
- **Wholesale Demand Response Units** — demand response DUIDs

### Tier 2 — MMSDM PARTICIPANTREGISTRATION tables (historical participants)
Many DUIDs in the historical MLF record belong to assets that have since been **deregistered** and no longer appear in the current Registration List. The MMSDM archive (the same source used for MLF data) publishes two participant registration tables that cover all historical registrations:

- **STATION** — maps `STATIONID` → full station name (e.g. `CALLIDE` → `Callide Power Station`)
- **GENUNITS** — one row per *generating set*: fuel type (`CO2E_ENERGY_SOURCE`), registered capacity and set type (`GENSETTYPE`: GENERATOR, BIDIRECTIONAL or LOAD)
- **DUALLOC** — allocates generating sets to DUIDs. A set ID is not a DUID: QUERIVE1 is the sets QUERIVE1 + QUERIVE2, so its capacity is their sum (48 MW). Each DUID's latest allocation is used.

This tier resolves the DUIDs not in Tier 1 (~150 in the 2026-08 data); none are left Unknown.

### Tier 3 — Fallback pattern matching
For the small remainder:
For DUIDs the registration list omits, AEMO's naming conventions set the type (they win over GENUNITS, which lists these as GENERATOR):
- An `NL` suffix (e.g. `CALLNL4`, `MURAYNL1`) → **Network Load**: a load point at a power station used as a reference in MLF calculations, not a generator.
- A `DG_` prefix (`DG_NSW1` …) → **Dummy Generator**: AEMO's regional market-system units.
- A `BLNK` prefix (`BLNKTAS`, `BLNKVIC`) → **Interconnector**: Basslink.
- Any DUID not matched by Tiers 1–2 falls back to its abbreviated `STATIONID` as the station name and is labelled **Unknown**.

### Asset type labels
Every DUID in the dashboard carries a type badge:

| Badge | Meaning |
|---|---|
| Generator | Registered generating or bidirectional (battery) unit, current or historical |
| Scheduled Load | A load with its own MLF: pumps (SHPUMP, SNOWYP, PUMP1/2), the load side of dual-MLF units (KIDSPHL1/2), auxiliary loads (GPWFEL1/GPWFWL1), a battery's separate load DUID (KEPBL1) |
| Network Load | Load point at a power station used as a reference in MLF calculations |
| Dummy Generator | AEMO's `DG_<region>` market-system unit |
| Interconnector | Basslink (`BLNKTAS`, `BLNKVIC`) |
| Ancillary Service | FCAS-registered asset (from the registration list's Ancillary Services sheet) |
| Demand Response | Wholesale demand response unit |
| Unknown | In MLF data but not identifiable in any AEMO reference file |

### Retired and superseded DUIDs
A DUID with an MLF in an earlier year but none for the current FY (nor in the next FY's draft) is **Retired**. Batteries that AEMO re-registered as bidirectional units in 2024-25 under a new DUID (HPRG1 → HPR1, CAPBES1G → CAPBES1, …) are merged: the new DUID's row carries the old DUID's earlier MLFs and names it in `PREVIOUS_DUIDS`.

## Run locally

```bash
pip install -r requirements.txt
python -m src.main              # incremental (uses cache)
python -m src.main --full-refresh  # re-download everything
```

## Automation

Production updates run on the **NAS runner** (QNAP `ai-wif-runner` container) via the `nas-job aemo-mlf-tracker` lane; see [`deploy/README.md`](deploy/README.md) for details. A QNAP scheduled task fires the lane around AEMO's publications: the final MLFs (by 1 April) and the DUDETAILSUMMARY load of the new year (from July). The draft for the next FY appears early in March and is replaced by the final in April, so only a run between the two shows a draft column. The lane intentionally uses `--full-refresh` because the source footprint is small and AEMO publishes final/draft MLF data on an annual cadence.

The lane commits as `aemo-nas-bot` and pushes its updated outputs. GitHub Actions is kept as a manual verification/fallback runner. GitHub Pages deploys on those pushes.

*Historical:* this lane ran on a Hetzner VPS under the `aemo-mlf-tracker.timer` systemd timer before the 2026-09 NAS migration. That setup is retired and its unit files were deleted in the same cleanup.

## Output Validation

After the pipeline runs and before committing, an automated validation step (`tests/validate_outputs.py`) checks:

- `summary.csv` exists and has 400+ generators
- No null DUIDs
- All 5 NEM regions are present
- All FY-column MLF values in [0.5, 1.5]
- LATEST_MLF values in [0.5, 1.5]
- YOY_CHANGE is consistent with LATEST_MLF - PREV_MLF (within 0.001 tolerance)
- All 5 regional Excel workbooks exist
- The current FY is filled in and not last year's values copied forward, and the previous FY matches AEMO's final workbook
- At least 90% of live generating units have a fuel category and a capacity (a run without the registration list fails)
- Input age, from `outputs/run_status.json`: the MMSDM archive month ended no more than 75 days before the run, the latest final MLF year is the current FY or the next one, and the registration list in use was fetched no more than 60 days before the run

If any check fails, the NAS lane or manual fallback workflow exits before committing — preventing bad data from reaching the dashboard.

### Run status

Each run writes `outputs/run_status.json`: the run date, the MMSDM archive month used, the final and draft workbooks (URL, AEMO's Last-Modified, SHA-1, and for the draft whether it was found, not yet published or blocked), the registration list (downloaded, cached, or a failed refresh with the date of the copy kept) and whether the MMSDM unit tables were downloaded or cached. The page footer shows it. AEMO answers a workbook that doesn't exist by redirecting to `/404`, whose Cloudflare page returns 403; the pipeline reads that redirect as "not published", and any other 403 or challenge page as "blocked".

## Data sources

| Source | Used for |
|---|---|
| [DUDETAILSUMMARY](https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/) | MLF values and date ranges for all DUIDs |
| [NEM Registration List](https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls) | Station name, fuel type, capacity (current participants) |
| [MMSDM STATION table](https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/) | Full station names for historical/deregistered assets |
| [MMSDM GENUNITS table](https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/) | Fuel type, capacity and set type for historical/deregistered assets |
| [MMSDM DUALLOC table](https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/) | Which generating sets make up each DUID |
| AEMO final / draft MLF workbooks (aemo.com.au, Loss factors and regional boundaries; URL patterns in `src/indicative.py`) | Current-FY MLFs from 1 April; next-FY draft MLFs (March) |
