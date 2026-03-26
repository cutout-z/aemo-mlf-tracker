"""Download and parse AEMO's draft/indicative MLFs for the upcoming financial year."""

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

# AEMO publishes draft MLFs here. URL pattern may change each year.
DRAFT_MLF_URL = (
    "https://aemo.com.au/-/media/files/electricity/nem/security_and_reliability/"
    "loss_factors_and_regional_boundaries/{fy_folder}/"
    "draft-marginal-loss-factors-for-the-{fy_label}-financial-year-xls.xlsx"
)

# Sheet name → NEM region mapping (Gen sheets only)
SHEET_REGION_MAP = {
    "QLD Gen": "QLD1",
    "NSW Gen": "NSW1",
    "ACT Gen": "NSW1",
    "VIC Gen": "VIC1",
    "SA Gen": "SA1",
    "TAS Gen": "TAS1",
}


def get_indicative_fy() -> tuple[int, str, str]:
    """Determine which FY the next indicative/draft MLFs are for.

    Returns (start_year, fy_label, fy_folder) e.g. (2026, '2026-27', '2026-27')
    """
    # The upcoming FY is the one after the current FY_END in config
    next_fy = config.FY_END + 1
    fy_label = f"{next_fy}-{(next_fy + 1) % 100:02d}"
    fy_folder = fy_label
    return next_fy, fy_label, fy_folder


def download_draft_mlfs(cache_dir: str) -> pd.DataFrame | None:
    """Download and parse AEMO's draft MLF Excel file.

    Returns DataFrame with columns [DUID, REGIONID, INDICATIVE_MLF] or None if unavailable.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    next_fy, fy_label, fy_folder = get_indicative_fy()
    url = DRAFT_MLF_URL.format(fy_folder=fy_folder, fy_label=fy_label)
    xlsx_path = cache_path / f"draft_mlf_{fy_label}.xlsx"

    # Download if not cached
    if not xlsx_path.exists():
        logger.info(f"Downloading draft MLFs for FY{fy_label}...")
        for attempt in range(config.MAX_RETRIES):
            try:
                resp = requests.get(
                    url, timeout=30,
                    headers={"User-Agent": "Mozilla/5.0 AEMO-MLF-Tracker"},
                )
                if resp.status_code == 404:
                    logger.info(f"Draft MLFs for FY{fy_label} not yet published (404)")
                    return None
                resp.raise_for_status()
                xlsx_path.write_bytes(resp.content)
                logger.info(f"Downloaded draft MLFs ({len(resp.content) / 1024:.0f} KB)")
                break
            except requests.RequestException as e:
                if attempt < config.MAX_RETRIES - 1:
                    wait = config.RETRY_BACKOFF * (attempt + 1)
                    logger.warning(f"Download failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    logger.warning(f"Could not download draft MLFs: {e}")
                    return None

    return parse_draft_mlf_excel(xlsx_path, fy_label)


def parse_draft_mlf_excel(xlsx_path: Path, fy_label: str) -> pd.DataFrame | None:
    """Parse AEMO's draft MLF Excel file into a clean DataFrame."""
    logger.info(f"Parsing draft MLF Excel for FY{fy_label}...")

    try:
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    except Exception as e:
        logger.error(f"Failed to open draft MLF Excel: {e}")
        return None

    all_rows = []
    for sheet_name, region in SHEET_REGION_MAP.items():
        if sheet_name not in xls.sheet_names:
            continue

        df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

        # Find header row containing "DUID"
        header_idx = None
        for i in range(min(20, len(df))):
            row_vals = [str(v).strip() for v in df.iloc[i].tolist()]
            if "DUID" in row_vals:
                header_idx = i
                break

        if header_idx is None:
            logger.warning(f"No DUID header found in sheet '{sheet_name}'")
            continue

        headers = [str(v).strip() for v in df.iloc[header_idx].tolist()]
        data = df.iloc[header_idx + 1:].copy()
        data.columns = headers
        data = data.dropna(subset=["DUID"])

        # Find the MLF column for the target FY
        fy_short = fy_label  # e.g. "2026-27"
        mlf_col = [c for c in headers if fy_short in c and "MLF" in c]
        if not mlf_col:
            logger.warning(f"No {fy_short} MLF column in sheet '{sheet_name}'")
            continue

        for _, row in data.iterrows():
            duid = str(row["DUID"]).strip()
            mlf = pd.to_numeric(row[mlf_col[0]], errors="coerce")
            if pd.notna(mlf) and duid:
                all_rows.append({"DUID": duid, "REGIONID": region, "INDICATIVE_MLF": mlf})

    if not all_rows:
        logger.warning("No indicative MLF data parsed")
        return None

    result = pd.DataFrame(all_rows)
    # Deduplicate (ACT Gen may overlap with NSW Gen)
    result = result.drop_duplicates(subset="DUID", keep="first")

    logger.info(f"Parsed {len(result)} indicative MLFs for FY{fy_label}")
    return result
