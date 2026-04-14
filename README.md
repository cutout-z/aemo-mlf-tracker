# AEMO MLF Tracker

**[Live Dashboard](https://cutout-z.github.io/aemo-mlf-tracker/)**

Automated tracker for Marginal Loss Factors (MLFs) across all generator assets in Australia's National Electricity Market (NEM). MLFs directly impact generator revenue — a 0.01 change can mean millions for large assets.

## What it does

- Downloads AEMO's DUDETAILSUMMARY table from the MMSDM archive (complete MLF history in a single ~125KB file)
- Cross-references with AEMO's NEM Registration List for fuel type, technology, and capacity metadata
- Extracts per-generator MLFs across 11 financial years (FY15-16 to FY25-26)
- Computes year-on-year changes and flags degradation
- Outputs summary CSV, per-region Excel workbooks with heatmaps, and an interactive GitHub Pages dashboard

## Coverage

| | |
|---|---|
| **Generators** | 667 across all 5 NEM regions |
| **Regions** | NSW, QLD, VIC, SA, TAS |
| **Fuel types** | Solar, Wind, Hydro, Fossil, Battery, Other Renewable |
| **History** | FY15-16 to FY25-26 (11 years) |
| **Update frequency** | Annually (MLFs take effect July 1) |

## Dashboard features

- **All Regions** tab with region dropdown filter, plus individual state tabs
- Search by DUID or station name
- Sort by any column (click headers)
- Filter by fuel type
- Heatmap colouring (red = low MLF, green = high)
- Select assets and export to Excel

## Run locally

```bash
pip install -r requirements.txt
python -m src.main              # incremental (uses cache)
python -m src.main --full-refresh  # re-download everything
```

## Output Validation

After the pipeline runs and before committing, an automated validation step (`tests/validate_outputs.py`) checks:

- `summary.csv` exists and has 400+ generators
- No null DUIDs
- All 5 NEM regions are present
- All FY-column MLF values in [0.5, 1.5]
- LATEST_MLF values in [0.5, 1.5]
- YOY_CHANGE is consistent with LATEST_MLF - PREV_MLF (within 0.001 tolerance)
- All 5 regional Excel workbooks exist

If any check fails, the workflow exits before committing — preventing bad data from reaching the dashboard.

## Data sources

- **DUDETAILSUMMARY** — [AEMO MMSDM Archive](https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/)
- **NEM Registration List** — [AEMO Participant Information](https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls)
