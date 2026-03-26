"""Generator metadata from AEMO NEM Registration and Exemption List."""

import logging
import time
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

REGISTRATION_URL = (
    "https://www.aemo.com.au/-/media/Files/Electricity/NEM/"
    "Participant_Information/NEM-Registration-and-Exemption-List.xls"
)
SHEET_NAME = "PU and Scheduled Loads"


def fetch_generator_metadata(cache_dir: str) -> pd.DataFrame:
    """Download AEMO NEM Registration List and extract generator metadata.

    Returns DataFrame with DUID, STATION_NAME, FUEL_SOURCE, FUEL_CATEGORY,
    TECHNOLOGY, CAPACITY_MW, REGION.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    xls_path = cache_path / "NEM-Registration-and-Exemption-List.xls"

    # Download if not cached
    if not xls_path.exists():
        logger.info("Downloading NEM Registration List from AEMO...")
        for attempt in range(config.MAX_RETRIES):
            try:
                resp = requests.get(
                    REGISTRATION_URL,
                    timeout=30,
                    headers={"User-Agent": "Mozilla/5.0 AEMO-MLF-Tracker"},
                )
                resp.raise_for_status()
                xls_path.write_bytes(resp.content)
                logger.info(f"Downloaded {len(resp.content) / 1024:.0f} KB")
                break
            except requests.RequestException as e:
                if attempt < config.MAX_RETRIES - 1:
                    wait = config.RETRY_BACKOFF * (attempt + 1)
                    logger.warning(f"Download failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise RuntimeError(f"Failed to download registration list: {e}")

    logger.info("Parsing generator metadata...")
    df = pd.read_excel(xls_path, engine="openpyxl", sheet_name=SHEET_NAME)

    # Rename columns
    col_map = {
        "DUID": "DUID",
        "Station Name": "STATION_NAME",
        "Fuel Source - Descriptor": "FUEL_SOURCE",
        "Fuel Source - Primary": "FUEL_PRIMARY",
        "Technology Type - Descriptor": "TECHNOLOGY",
        "Reg Cap generation (MW)": "CAPACITY_MW",
        "Region": "REGION",
        "Dispatch Type": "DISPATCH_TYPE",
        "Classification": "CLASSIFICATION",
    }

    available = {k: v for k, v in col_map.items() if k in df.columns}
    df = df[list(available.keys())].rename(columns=available)

    # Drop rows without DUID
    df = df.dropna(subset=["DUID"])

    # Map fuel source to simplified category
    if "FUEL_PRIMARY" in df.columns:
        df["FUEL_CATEGORY"] = df["FUEL_PRIMARY"].map(config.FUEL_TYPE_MAP).fillna("Other")
    elif "FUEL_SOURCE" in df.columns:
        df["FUEL_CATEGORY"] = df["FUEL_SOURCE"].map(config.FUEL_TYPE_MAP).fillna("Other")
    else:
        df["FUEL_CATEGORY"] = "Unknown"

    # Convert capacity to numeric
    if "CAPACITY_MW" in df.columns:
        df["CAPACITY_MW"] = pd.to_numeric(df["CAPACITY_MW"], errors="coerce")

    # Deduplicate by DUID (keep first occurrence)
    df = df.drop_duplicates(subset="DUID", keep="first")

    logger.info(f"Loaded metadata for {len(df)} generators")
    return df
