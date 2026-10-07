"""CLI orchestrator for AEMO MLF Tracker."""

import argparse
import datetime as _dt
import json
import logging
import sys
from pathlib import Path

import pandas as pd

from . import config
from .download import download_dudetailsummary, get_latest_available_month
from .generators import fetch_generator_metadata
from .indicative import fetch_mlf_workbooks, get_indicative_fy
from .analyse import extract_fy_mlfs, build_summary, find_superseded
from .excel_output import generate_all_workbooks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _file_date(path: Path) -> str | None:
    return _dt.date.fromtimestamp(path.stat().st_mtime).isoformat() if path.exists() else None


def write_run_status(status: dict, path: Path) -> None:
    """outputs/run_status.json: what the published outputs were built from.

    The regional workbooks already change on every run (openpyxl stamps their save
    time), so the run date here adds no commit that wouldn't happen anyway.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2) + "\n")
    logger.info(f"Saved {path.name}")


def run(full_refresh: bool = False):
    """Main execution flow."""
    cache_dir = str(PROJECT_ROOT / config.DATA_DIR)
    output_dir = str(PROJECT_ROOT / config.OUTPUT_DIR)
    cache_file = PROJECT_ROOT / config.CACHE_FILE
    month_file = PROJECT_ROOT / config.CACHE_MONTH_FILE
    summary_path = PROJECT_ROOT / config.SUMMARY_CSV
    status = {
        "run_date": _dt.date.today().isoformat(),
        "final_fy": f"{config.FY_END}-{(config.FY_END + 1) % 100:02d}",
    }

    # Step 1: Probe AEMO for latest available month
    latest = get_latest_available_month()
    if latest is None:
        logger.error("Cannot determine latest available month. Exiting.")
        sys.exit(1)

    latest_year, latest_month = latest
    status["mmsdm_month_available"] = f"{latest_year}-{latest_month:02d}"

    # Step 2: Download DUDETAILSUMMARY (contains all historical MLF data)
    if not full_refresh and cache_file.exists():
        logger.info("Loading cached DUDETAILSUMMARY...")
        detail_df = pd.read_feather(cache_file)
        # A cache from before the month was recorded leaves it unknown (the validator fails it)
        status["mmsdm_month"] = month_file.read_text().strip() if month_file.exists() else None
        status["dudetailsummary_source"] = "cache"
    else:
        detail_df = download_dudetailsummary(latest_year, latest_month, cache_dir)
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        detail_df.to_feather(cache_file)
        month_file.write_text(status["mmsdm_month_available"] + "\n")
        logger.info(f"Cached DUDETAILSUMMARY to {cache_file}")
        status["mmsdm_month"] = status["mmsdm_month_available"]
        status["dudetailsummary_source"] = "download"

    # Step 3: Fetch generator metadata (fuel type, capacity) + MMSDM station lookup
    gen_cache = PROJECT_ROOT / config.GENERATOR_CACHE
    station_names = pd.Series(dtype=str)
    if not full_refresh and gen_cache.exists():
        logger.info("Loading cached generator metadata...")
        generators = pd.read_feather(gen_cache)
        # Still need station_names for enrichment — load from MMSDM cache if present
        from .generators import fetch_mmsdm_participant_metadata
        station_cache = PROJECT_ROOT / "data" / "mmsdm_station.feather"
        if station_cache.exists():
            _sdf = pd.read_feather(station_cache)
            station_names = _sdf.set_index("STATIONID")["STATIONNAME"]
        xls = PROJECT_ROOT / config.DATA_DIR / "NEM-Registration-and-Exemption-List.xls"
        status["registration_list"] = {"state": "cached",
                                       "file_date": _file_date(xls) or _file_date(gen_cache)}
        status["mmsdm_tables"] = {"metadata": "cached"}
    else:
        try:
            generators, station_names = fetch_generator_metadata(
                cache_dir,
                mmsdm_year=latest_year,
                mmsdm_month=latest_month,
                refresh=full_refresh,
                status=status,
            )
            gen_cache.parent.mkdir(parents=True, exist_ok=True)
            generators.to_feather(gen_cache)
            logger.info(f"Cached generator metadata to {gen_cache}")
        except Exception as e:
            # Publishing without fuel, capacity or unit type would empty the page's fuel
            # filters, battery table and type split while the run still looked green.
            logger.error(f"Could not build generator metadata: {e}")
            logger.error("Not publishing without fuel type / capacity data. Exiting.")
            sys.exit(1)

    # Step 4: Extract FY-level MLFs
    fy_mlfs = extract_fy_mlfs(detail_df)
    if fy_mlfs.empty:
        logger.error("No FY-level MLF data extracted. Exiting.")
        sys.exit(1)

    # Steps 5-6: final MLF Excel for the current FY (published April; required only until
    # DUDETAILSUMMARY carries the year) and the indicative/draft MLFs for the next FY
    final_excel, indicative = fetch_mlf_workbooks(cache_dir, detail_df, full_refresh=full_refresh,
                                                  status=status)

    # Step 7: Build summary (wide format with metadata)
    summary = build_summary(fy_mlfs, generators, indicative, final_excel, station_names,
                            successors=find_superseded(detail_df))

    # Step 8: Save outputs
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_path, index=False)
    logger.info(f"Saved summary.csv ({len(summary)} rows)")

    # Step 9: Generate Excel workbooks
    generate_all_workbooks(summary, output_dir)

    # Step 10: Record what this run used (page footer, input-age checks)
    write_run_status(status, PROJECT_ROOT / config.RUN_STATUS_JSON)

    logger.info("Done.")


def main():
    parser = argparse.ArgumentParser(description="AEMO MLF Tracker")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Re-download all data (default: use cached if available)",
    )
    args = parser.parse_args()
    run(full_refresh=args.full_refresh)


if __name__ == "__main__":
    main()
