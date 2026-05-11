# AEMO MLF Tracker

**[Live Dashboard](https://cutout-z.github.io/aemo-mlf-tracker/)**

Automated tracker for Marginal Loss Factors (MLFs) across all generator assets in Australia's National Electricity Market (NEM). MLFs directly impact generator revenue — a 0.01 change can mean millions for large assets.

## What it does

- Downloads AEMO's DUDETAILSUMMARY table from the MMSDM archive (complete MLF history in a single ~125KB file)
- Resolves generator metadata from three AEMO sources (see [DUID identification](#duid-identification) below)
- Extracts per-generator MLFs across 12 financial years (FY15-16 to FY26-27)
- Computes year-on-year changes and flags degradation
- Outputs summary CSV, per-region Excel workbooks with heatmaps, and an interactive GitHub Pages dashboard

## Coverage

| | |
|---|---|
| **DUIDs tracked** | 667 across all 5 NEM regions |
| **Regions** | NSW, QLD, VIC, SA, TAS |
| **Asset types** | Generator, Network Load, Ancillary Service, Demand Response |
| **Fuel types** | Solar, Wind, Hydro, Fossil, Battery, Other Renewable |
| **History** | FY15-16 to FY26-27 (12 years) |
| **Update frequency** | Annual/draft-annual Hetzner VPS refresh (final MLFs in April, draft/indicative MLFs in October) |

## Dashboard features

- **All Regions** tab with region dropdown filter, plus individual state tabs
- **Type filter** — filter by Generator, Network Load, Ancillary Service, Demand Response
- Search by DUID or station name
- Sort by any column (click headers)
- Filter by fuel type
- Heatmap colouring (red = low MLF, green = high)
- Asset type badges — colour-coded labels on every row
- Select assets and export to Excel

## DUID identification

AEMO's MLF data (DUDETAILSUMMARY) covers 667 DUIDs spanning over a decade, but no single AEMO reference file identifies all of them. The pipeline resolves metadata from three sources in priority order:

### Tier 1 — NEM Registration and Exemption List (current participants)
The primary source for currently registered assets. Provides station name, fuel type, technology, and registered capacity for ~564 generators.

Two additional sheets in the same file extend coverage:
- **Ancillary Services** — DUIDs registered for FCAS markets
- **Wholesale Demand Response Units** — demand response DUIDs

### Tier 2 — MMSDM PARTICIPANTREGISTRATION tables (historical participants)
Many DUIDs in the historical MLF record belong to assets that have since been **deregistered** and no longer appear in the current Registration List. The MMSDM archive (the same source used for MLF data) publishes two participant registration tables that cover all historical registrations:

- **STATION** — maps `STATIONID` → full station name (e.g. `CALLIDE` → `Callide Power Station`)
- **GENUNITS** — maps DUID → fuel type (`CO2E_ENERGY_SOURCE`), registered capacity, and dispatch type

This tier resolves ~445 additional DUIDs not found in Tier 1, reducing unknowns from ~140 to ~12.

### Tier 3 — Fallback pattern matching
For the small remainder:
- DUIDs with an `NL` suffix (e.g. `CALLNL4`, `MURAYNL1`) are labelled **Network Load** — these are large industrial loads at power stations used as reference points in MLF calculations, not generators.
- Any DUID not matched by Tiers 1–2 falls back to its abbreviated `STATIONID` as the station name and is labelled **Unknown**.

### Asset type labels
Every DUID in the dashboard carries a type badge:

| Badge | Meaning |
|---|---|
| Generator | Registered generating unit (current or historical) |
| Network Load | Industrial load reference node used in MLF calculations |
| Ancillary Service | FCAS-registered asset |
| Demand Response | Wholesale demand response unit |
| Unknown | In MLF data but not identifiable in any AEMO reference file |

## Run locally

```bash
pip install -r requirements.txt
python -m src.main              # incremental (uses cache)
python -m src.main --full-refresh  # re-download everything
```

## Automation

Production updates run on the Hetzner VPS via `aemo-mlf-tracker.timer`; see [`deploy/README.md`](deploy/README.md) for setup details. The VPS lane intentionally uses `--full-refresh` because the source footprint is small and AEMO publishes final/draft MLF data on an annual cadence.

GitHub Actions is kept as a manual verification/fallback runner. GitHub Pages deploys after the VPS pushes updated outputs.

## Output Validation

After the pipeline runs and before committing, an automated validation step (`tests/validate_outputs.py`) checks:

- `summary.csv` exists and has 400+ generators
- No null DUIDs
- All 5 NEM regions are present
- All FY-column MLF values in [0.5, 1.5]
- LATEST_MLF values in [0.5, 1.5]
- YOY_CHANGE is consistent with LATEST_MLF - PREV_MLF (within 0.001 tolerance)
- All 5 regional Excel workbooks exist

If any check fails, the VPS runner or manual fallback workflow exits before committing — preventing bad data from reaching the dashboard.

## Data sources

| Source | Used for |
|---|---|
| [DUDETAILSUMMARY](https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/) | MLF values and date ranges for all DUIDs |
| [NEM Registration List](https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls) | Station name, fuel type, capacity (current participants) |
| [MMSDM STATION table](https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/) | Full station names for historical/deregistered assets |
| [MMSDM GENUNITS table](https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/) | Fuel type and capacity for historical/deregistered assets |
