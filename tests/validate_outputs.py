"""Post-pipeline validation for AEMO MLF Tracker.

Checks summary.csv and regional Excel workbooks for data integrity
before committing to the repository. Exits non-zero on any failure.
"""

import calendar
import json
import re
import sys
import warnings
from datetime import date
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
DATA_DIR = PROJECT_ROOT / "data"
REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
REGION_NAMES = {"NSW1": "NSW", "QLD1": "QLD", "VIC1": "VIC", "SA1": "SA", "TAS1": "TAS"}
RUN_STATUS = OUTPUTS_DIR / "run_status.json"

# Input-age limits, counted back from the run date in run_status.json.
# MMSDM month M lands on nemweb about 4 weeks after M ends (2026_08 on 28 Sep 2026), so
# the newest archive is normally 30-60 days past its month end; 75 allows a late month.
MAX_ARCHIVE_AGE_DAYS = 75
# The registration list is re-fetched on every --full-refresh; a copy this old means the
# refresh has been failing (the cached copy is kept and recorded as refresh_failed).
MAX_REGISTRATION_AGE_DAYS = 60

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

    check_current_fy(df)
    check_against_final_workbook(df)
    check_metadata_coverage(df)
    status = load_run_status()
    if status is not None:
        check_input_freshness(df, status)

    # --- Regional Excel workbooks exist ---
    for region_id, name in REGION_NAMES.items():
        xlsx_path = OUTPUTS_DIR / f"{name}_mlf.xlsx"
        check(xlsx_path.exists(), f"{xlsx_path.name} does not exist")


def _final_fy_cols(df):
    """Final (non-draft, export) FY columns, oldest first."""
    return sorted(c for c in df.columns if re.fullmatch(r"FY\d{2}-\d{2}", c))


def check_current_fy(df, max_flat_share=0.25, min_coverage=0.5):
    """The current FY must be filled in, and not be last year's values copied forward.

    AEMO's MLFs move every year (about 1-7% of DUIDs are genuinely unchanged), so a
    current FY that mostly equals the previous one, or is mostly blank, means the
    final workbook was not applied.
    """
    fy_cols = _final_fy_cols(df)
    if len(fy_cols) < 2:
        return
    cur, prev = (pd.to_numeric(df[c], errors="coerce") for c in fy_cols[-2:][::-1])
    n_prev = int(prev.notna().sum())
    if n_prev == 0:
        return
    coverage = cur.notna().sum() / n_prev
    check(
        coverage >= min_coverage,
        f"{fy_cols[-1]} has values for {coverage:.0%} as many DUIDs as {fy_cols[-2]} "
        f"(expected >= {min_coverage:.0%}) — final MLF workbook missing?",
    )
    both = cur.notna() & prev.notna()
    if both.sum() > 0:
        flat = ((cur - prev).abs() < 1e-9)[both].mean()
        check(
            flat <= max_flat_share,
            f"{fy_cols[-1]} equals {fy_cols[-2]} for {flat:.0%} of DUIDs "
            f"(expected <= {max_flat_share:.0%}) — previous FY copied forward?",
        )


def load_run_status(path=None):
    """outputs/run_status.json as written by src/main.py, or None (a failure) if absent."""
    path = path or RUN_STATUS
    if not check(path.exists(), f"{path.name} does not exist — run src.main to record what the outputs were built from"):
        return None
    try:
        return json.loads(path.read_text())
    except ValueError as e:
        check(False, f"{path.name} is not valid JSON: {e}")
        return None


def check_input_freshness(df, status, max_archive_age=MAX_ARCHIVE_AGE_DAYS,
                          max_registration_age=MAX_REGISTRATION_AGE_DAYS):
    """The inputs must be recent relative to the run date, not just well-formed.

    - The MMSDM archive month used ended no more than `max_archive_age` days before the run.
    - The latest final FY column is the current FY or the next one (from 1 April).
    - The registration list in use was fetched no more than `max_registration_age` days ago.
    """
    try:
        run_date = date.fromisoformat(str(status.get("run_date")))
    except ValueError:
        check(False, f"run_status.json has no valid run_date ({status.get('run_date')!r})")
        return

    month = status.get("mmsdm_month")
    if check(bool(month), "run_status.json does not say which MMSDM archive month was used "
                          "(cache from before the month was recorded? run with --full-refresh)"):
        year, mon = (int(x) for x in str(month).split("-"))
        month_end = date(year, mon, calendar.monthrange(year, mon)[1])
        age = (run_date - month_end).days
        check(age <= max_archive_age,
              f"MMSDM archive {month} ended {age} days before the run on {run_date} "
              f"(expected <= {max_archive_age}) — newer archive months not picked up?")

    fy_cols = _final_fy_cols(df)
    if check(bool(fy_cols), "summary.csv has no final FY columns"):
        latest = 2000 + int(fy_cols[-1][2:4])
        current = run_date.year if run_date.month >= 7 else run_date.year - 1
        check(latest in (current, current + 1),
              f"Latest final MLF year is {fy_cols[-1]} but the run on {run_date} is in "
              f"FY{current % 100:02d}-{(current + 1) % 100:02d} — final MLFs out of date?")

    reg = status.get("registration_list") or {}
    if check(bool(reg.get("file_date")), "run_status.json does not record the registration list in use"):
        age = (run_date - date.fromisoformat(reg["file_date"])).days
        check(age <= max_registration_age,
              f"The registration list in use was fetched on {reg['file_date']}, {age} days before the "
              f"run (state: {reg.get('state')}; expected <= {max_registration_age}) — refresh failing?")


def check_metadata_coverage(df, min_share=0.9):
    """Live generating units must carry fuel and capacity from the registration list.

    A run without generator metadata used to publish anyway: every unit typed Unknown,
    no fuel or capacity. About 95% of live generating units have a fuel category.
    """
    missing = [c for c in ("DUID_TYPE", "FUEL_CATEGORY", "CAPACITY_MW") if c not in df.columns]
    if not check(not missing, f"summary.csv has no {', '.join(missing)} — generator metadata missing?"):
        return
    live = df if "STATUS" not in df.columns else df[df["STATUS"] != "Retired"]
    gens = live[live["DUID_TYPE"] == "Generator"]
    if not check(len(gens) > 0, "No live DUIDs typed Generator — generator metadata missing?"):
        return
    fuel = (gens["FUEL_CATEGORY"].fillna("").astype(str).str.strip() != "").mean()
    check(fuel >= min_share,
          f"Only {fuel:.0%} of {len(gens)} live generating units have a fuel category "
          f"(expected >= {min_share:.0%}) — registration list not read?")
    capacity = pd.to_numeric(gens["CAPACITY_MW"], errors="coerce").notna().mean()
    check(capacity >= min_share,
          f"Only {capacity:.0%} of {len(gens)} live generating units have a capacity "
          f"(expected >= {min_share:.0%}) — registration list not read?")


def check_against_final_workbook(df, min_gen_match=0.95, min_bdu_match=0.9):
    """The previous FY must agree with the comparison columns of AEMO's final workbook.

    The cached final workbook for the current FY (data/final_mlf_<fy>.xlsx) also lists
    last year's MLFs; batteries have separate Export and Import columns. A swapped
    battery orientation or a wrong value-selection rule shows up here. AEMO's column
    carries the latest revision, so a few mid-year changes legitimately differ.
    Skipped when the workbook isn't cached.
    """
    fy_cols = _final_fy_cols(df)
    if len(fy_cols) < 2:
        return
    cur_label = f"20{fy_cols[-1][2:]}"
    prev_label = f"20{fy_cols[-2][2:]}"
    xlsx = DATA_DIR / f"final_mlf_{cur_label}.xlsx"
    if not xlsx.exists():
        print(f"  (skipped workbook cross-check: {xlsx.name} not cached)")
        return

    sys.path.insert(0, str(PROJECT_ROOT))
    from src.indicative import _parse_mlf_excel

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        aemo = _parse_mlf_excel(xlsx, prev_label, "PREV_MLF")
    if aemo is None or aemo.empty:
        check(False, f"Could not parse {prev_label} columns from {xlsx.name}")
        return
    aemo = aemo.set_index("DUID")
    summary = df.set_index("DUID")
    common = aemo.index.intersection(summary.index)
    ours = pd.to_numeric(summary.loc[common, fy_cols[-2]], errors="coerce")
    theirs = aemo.loc[common, "PREV_MLF"]
    is_bdu = (
        aemo.loc[common, "PREV_IMPORT_MLF"].notna()
        if "PREV_IMPORT_MLF" in aemo.columns
        else pd.Series(False, index=common)
    )
    compared = ours.notna() & theirs.notna()
    match = (ours - theirs).abs() < 5e-5

    gen = compared & ~is_bdu
    if gen.sum() > 0:
        share = match[gen].mean()
        check(
            share >= min_gen_match,
            f"{fy_cols[-2]} matches AEMO's {prev_label} MLF for only {share:.0%} of "
            f"{gen.sum()} generators (expected >= {min_gen_match:.0%})",
        )

    bdu = compared & is_bdu
    import_col = f"{fy_cols[-2]} Import"
    if bdu.sum() > 0 and import_col in summary.columns:
        ours_imp = pd.to_numeric(summary.loc[common, import_col], errors="coerce")
        imp_match = (ours_imp - aemo.loc[common, "PREV_IMPORT_MLF"]).abs() < 5e-5
        share = (match & imp_match)[bdu].mean()
        check(
            share >= min_bdu_match,
            f"{fy_cols[-2]} battery Export/Import MLFs match AEMO's {prev_label} columns for "
            f"only {share:.0%} of {bdu.sum()} batteries (expected >= {min_bdu_match:.0%}) "
            f"— export/import swapped?",
        )


if __name__ == "__main__":
    print("Validating AEMO MLF Tracker outputs...")
    validate()
    if errors:
        print(f"\n{len(errors)} validation error(s) found — aborting.")
        sys.exit(1)
    else:
        print("\nAll validations passed.")
