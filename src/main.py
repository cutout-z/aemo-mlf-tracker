"""CLI orchestrator for AEMO MLF Tracker."""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from . import config
from .download import download_dudetailsummary, get_latest_available_month
from .generators import fetch_generator_metadata
from .indicative import download_draft_mlfs, get_indicative_fy
from .analyse import extract_fy_mlfs, build_summary
from .excel_output import generate_all_workbooks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run(full_refresh: bool = False):
    """Main execution flow."""
    cache_dir = str(PROJECT_ROOT / config.DATA_DIR)
    output_dir = str(PROJECT_ROOT / config.OUTPUT_DIR)
    cache_file = PROJECT_ROOT / config.CACHE_FILE
    summary_path = PROJECT_ROOT / config.SUMMARY_CSV

    # Step 1: Probe AEMO for latest available month
    latest = get_latest_available_month()
    if latest is None:
        logger.error("Cannot determine latest available month. Exiting.")
        sys.exit(1)

    latest_year, latest_month = latest

    # Step 2: Download DUDETAILSUMMARY (contains all historical MLF data)
    if not full_refresh and cache_file.exists():
        logger.info("Loading cached DUDETAILSUMMARY...")
        detail_df = pd.read_feather(cache_file)
    else:
        detail_df = download_dudetailsummary(latest_year, latest_month, cache_dir)
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        detail_df.to_feather(cache_file)
        logger.info(f"Cached DUDETAILSUMMARY to {cache_file}")

    # Step 3: Fetch generator metadata (fuel type, capacity)
    gen_cache = PROJECT_ROOT / config.GENERATOR_CACHE
    if not full_refresh and gen_cache.exists():
        logger.info("Loading cached generator metadata...")
        generators = pd.read_feather(gen_cache)
    else:
        try:
            generators = fetch_generator_metadata(cache_dir)
            gen_cache.parent.mkdir(parents=True, exist_ok=True)
            generators.to_feather(gen_cache)
            logger.info(f"Cached generator metadata to {gen_cache}")
        except Exception as e:
            logger.warning(f"Could not fetch generator metadata: {e}")
            logger.warning("Proceeding without fuel type / capacity data")
            generators = None

    # Step 4: Extract FY-level MLFs
    fy_mlfs = extract_fy_mlfs(detail_df)
    if fy_mlfs.empty:
        logger.error("No FY-level MLF data extracted. Exiting.")
        sys.exit(1)

    # Step 5: Fetch indicative/draft MLFs for upcoming FY
    indicative = download_draft_mlfs(cache_dir)

    # Step 6: Build summary (wide format with metadata)
    summary = build_summary(fy_mlfs, generators, indicative)

    # Step 7: Save outputs
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_path, index=False)
    logger.info(f"Saved summary.csv ({len(summary)} rows)")

    # Step 8: Generate Excel workbooks
    generate_all_workbooks(summary, output_dir)

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
