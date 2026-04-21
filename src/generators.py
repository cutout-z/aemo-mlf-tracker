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
PRIMARY_SHEET = "PU and Scheduled Loads"

# Additional sheets present in the NEM Registration and Exemption List.
# Each entry is (sheet_name, DUID_TYPE label, duid_col_override, name_col_override).
# Use None for overrides to fall back to candidate-list scanning.
SECONDARY_SHEETS = [
    ("Ancillary Services",            "Ancillary Service", "DUID",      "Facility"),
    ("Wholesale Demand Response Units","Demand Response",  "WDRU DUID", "Facility Name (WDRU Name)"),
]

# Candidate column names to look for a DUID/connection-point identifier (fallback)
DUID_CANDIDATES = [
    "DUID", "WDRU DUID", "Connection Point ID", "Participant ID",
    "TNI", "Connection Point Identifier",
]
# Candidate column names for a human-readable station/participant name (fallback)
NAME_CANDIDATES = [
    "Facility", "Facility Name (WDRU Name)", "Station Name",
    "Company Name", "Participant Name", "Name", "Asset Name",
]
REGION_CANDIDATES = ["Region", "REGIONID", "NMI Jurisdiction Code", "Jurisdiction"]


def _download_xls(cache_dir: str) -> Path:
    """Download NEM Registration List to cache_dir if not already there."""
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    xls_path = cache_path / "NEM-Registration-and-Exemption-List.xls"

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
                    logger.warning(
                        f"Download failed (attempt {attempt + 1}): {e}. "
                        f"Retrying in {wait}s..."
                    )
                    time.sleep(wait)
                else:
                    raise RuntimeError(f"Failed to download registration list: {e}")
    return xls_path


def _first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first candidate column name that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _parse_secondary_sheet(
    xls_path: Path, sheet_name: str, duid_type: str,
    duid_col_override: str | None = None,
    name_col_override: str | None = None,
) -> pd.DataFrame | None:
    """Try to parse one secondary sheet; return slim DataFrame or None."""
    try:
        df = pd.read_excel(xls_path, engine="openpyxl", sheet_name=sheet_name)
    except Exception:
        return None

    duid_col = duid_col_override if (duid_col_override and duid_col_override in df.columns) \
        else _first_col(df, DUID_CANDIDATES)
    if duid_col is None:
        logger.debug(f"Sheet '{sheet_name}': no DUID-like column found, skipping")
        return None

    name_col = name_col_override if (name_col_override and name_col_override in df.columns) \
        else _first_col(df, NAME_CANDIDATES)
    region_col = _first_col(df, REGION_CANDIDATES)

    rows = pd.DataFrame()
    rows["DUID"] = df[duid_col].astype(str).str.strip()
    rows["STATION_NAME"] = df[name_col].astype(str).str.strip() if name_col else None
    rows["REGION"] = df[region_col].astype(str).str.strip() if region_col else None
    rows["DUID_TYPE"] = duid_type
    rows = rows[rows["DUID"].notna() & (rows["DUID"] != "") & (rows["DUID"] != "nan")]
    rows = rows.drop_duplicates(subset="DUID", keep="first").reset_index(drop=True)

    logger.info(
        f"Sheet '{sheet_name}': found {len(rows)} {duid_type} DUIDs "
        f"(duid_col='{duid_col}', name_col='{name_col}')"
    )
    return rows


def fetch_generator_metadata(cache_dir: str) -> pd.DataFrame:
    """Download AEMO NEM Registration List and extract all participant metadata.

    Returns DataFrame with DUID, STATION_NAME, FUEL_SOURCE, FUEL_CATEGORY,
    TECHNOLOGY, CAPACITY_MW, REGION, DUID_TYPE.

    Primary sheet ('PU and Scheduled Loads') → DUID_TYPE='Generator'.
    Secondary sheets (NSPs, MNSPs, etc.)     → DUID_TYPE='Network Load' etc.
    """
    xls_path = _download_xls(cache_dir)

    # --- Primary sheet: generators & scheduled loads ---
    logger.info("Parsing primary sheet (PU and Scheduled Loads)...")
    df = pd.read_excel(xls_path, engine="openpyxl", sheet_name=PRIMARY_SHEET)

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
    gen = df[list(available.keys())].rename(columns=available)
    gen = gen.dropna(subset=["DUID"])

    if "FUEL_PRIMARY" in gen.columns:
        gen["FUEL_CATEGORY"] = gen["FUEL_PRIMARY"].map(config.FUEL_TYPE_MAP).fillna("Other")
    elif "FUEL_SOURCE" in gen.columns:
        gen["FUEL_CATEGORY"] = gen["FUEL_SOURCE"].map(config.FUEL_TYPE_MAP).fillna("Other")
    else:
        gen["FUEL_CATEGORY"] = "Unknown"

    if "CAPACITY_MW" in gen.columns:
        gen["CAPACITY_MW"] = pd.to_numeric(gen["CAPACITY_MW"], errors="coerce")

    gen = gen.drop_duplicates(subset="DUID", keep="first")
    gen["DUID_TYPE"] = "Generator"
    logger.info(f"Primary sheet: {len(gen)} generators")

    # --- Secondary sheets ---
    secondary_frames = []
    for sheet_name, duid_type, duid_override, name_override in SECONDARY_SHEETS:
        result = _parse_secondary_sheet(
            xls_path, sheet_name, duid_type, duid_override, name_override
        )
        if result is not None and not result.empty:
            secondary_frames.append(result)

    if secondary_frames:
        secondary = pd.concat(secondary_frames, ignore_index=True)
        # Only keep DUIDs not already covered by the primary sheet
        secondary = secondary[~secondary["DUID"].isin(gen["DUID"])]
        secondary = secondary.drop_duplicates(subset="DUID", keep="first")
        logger.info(
            f"Secondary sheets: {len(secondary)} additional DUIDs "
            f"({secondary['DUID_TYPE'].value_counts().to_dict()})"
        )
        combined = pd.concat([gen, secondary], ignore_index=True)
    else:
        combined = gen

    logger.info(f"Total metadata: {len(combined)} DUIDs")
    return combined
