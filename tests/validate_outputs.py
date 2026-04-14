"""Post-pipeline validation for AEMO MLF Tracker.

Checks summary.csv and regional Excel workbooks for data integrity
before committing to the repository. Exits non-zero on any failure.
"""

import sys
from pathlib import Path

import pandas as pd

OUTPUTS_DIR = Path(__file__).parent.parent / "outputs"
REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
REGION_NAMES = {"NSW1": "NSW", "QLD1": "QLD", "VIC1": "VIC", "SA1": "SA", "TAS1": "TAS"}

errors = []


def check(condition, msg):
    if not condition:
        errors.append(msg)
        print(f"  FAIL: {msg}")
    return condition


def validate():
    summary_path = OUTPUTS_DIR / "summary.csv"
    check(summary_path.exists(), "summary.csv does not exist")
    if not summary_path.exists():
        return

    df = pd.read_csv(summary_path)
    print(f"summary.csv: {len(df)} rows, {len(df.columns)} columns")

    # --- Structure ---
    check(len(df) >= 400, f"Unexpectedly few generators: {len(df)} (expected 400+)")
    required_cols = ["DUID", "REGIONID"]
    for col in required_cols:
        check(col in df.columns, f"Missing column: {col}")

    # --- No null DUIDs ---
    if "DUID" in df.columns:
        nulls = df["DUID"].isna().sum()
        check(nulls == 0, f"DUID has {nulls} null values")

    # --- All 5 regions present ---
    if "REGIONID" in df.columns:
        regions_present = set(df["REGIONID"].unique())
        for r in REGIONS:
            check(r in regions_present, f"Region {r} missing from summary.csv")

    # --- MLF values in [0.5, 1.5] ---
    fy_cols = [c for c in df.columns if c.startswith("FY")]
    for col in fy_cols:
        vals = pd.to_numeric(df[col], errors="coerce").dropna()
        if len(vals) > 0:
            check(vals.min() >= 0.5, f"{col} has value below 0.5 (min={vals.min():.4f})")
            check(vals.max() <= 1.5, f"{col} has value above 1.5 (max={vals.max():.4f})")

    # --- LATEST_MLF in [0.5, 1.5] where present ---
    if "LATEST_MLF" in df.columns:
        vals = df["LATEST_MLF"].dropna()
        if len(vals) > 0:
            check(vals.min() >= 0.5, f"LATEST_MLF below 0.5 (min={vals.min():.4f})")
            check(vals.max() <= 1.5, f"LATEST_MLF above 1.5 (max={vals.max():.4f})")

    # --- YOY_CHANGE consistency (spot check) ---
    if all(c in df.columns for c in ["LATEST_MLF", "PREV_MLF", "YOY_CHANGE"]):
        sample = df.dropna(subset=["LATEST_MLF", "PREV_MLF", "YOY_CHANGE"])
        if len(sample) > 0:
            computed = (sample["LATEST_MLF"] - sample["PREV_MLF"]).round(4)
            mismatch = (computed - sample["YOY_CHANGE"]).abs() > 0.001
            check(
                mismatch.sum() == 0,
                f"{mismatch.sum()} rows have inconsistent YOY_CHANGE",
            )

    # --- Regional Excel workbooks exist ---
    for region_id, name in REGION_NAMES.items():
        xlsx_path = OUTPUTS_DIR / f"{name}_mlf.xlsx"
        check(xlsx_path.exists(), f"{xlsx_path.name} does not exist")


if __name__ == "__main__":
    print("Validating AEMO MLF Tracker outputs...")
    validate()
    if errors:
        print(f"\n{len(errors)} validation error(s) found — aborting.")
        sys.exit(1)
    else:
        print("\nAll validations passed.")
