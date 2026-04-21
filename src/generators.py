"""Generator metadata from AEMO NEM Registration and Exemption List + MMSDM tables."""

import csv
import io
import logging
import time
import zipfile
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
SECONDARY_SHEETS = [
    ("Ancillary Services",             "Ancillary Service", "DUID",      "Facility"),
    ("Wholesale Demand Response Units","Demand Response",   "WDRU DUID", "Facility Name (WDRU Name)"),
]

DUID_CANDIDATES = [
    "DUID", "WDRU DUID", "Connection Point ID", "Participant ID",
    "TNI", "Connection Point Identifier",
]
NAME_CANDIDATES = [
    "Facility", "Facility Name (WDRU Name)", "Station Name",
    "Company Name", "Participant Name", "Name", "Asset Name",
]
REGION_CANDIDATES = ["Region", "REGIONID", "NMI Jurisdiction Code", "Jurisdiction"]

# MMSDM PARTICIPANTREGISTRATION tables for historical DUID coverage
MMSDM_PR_URL_TEMPLATE = (
    "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
    "{year:04d}/MMSDM_{year:04d}_{month:02d}/"
    "MMSDM_Historical_Data_SQLLoader/DATA/"
    "PUBLIC_ARCHIVE%23{table}%23FILE01%23{year:04d}{month:02d}010000.zip"
)
STATION_COLS = [
    "STATIONID", "STATIONNAME", "ADDRESS1", "ADDRESS2", "ADDRESS3",
    "ADDRESS4", "CITY", "STATE", "POSTCODE", "LASTCHANGED", "CONNECTIONPOINTID",
]
GENUNITS_COLS = [
    "GENSETID", "STATIONID", "SETLOSSFACTOR", "CDINDICATOR", "AGCFLAG",
    "SPINNINGFLAG", "VOLTLEVEL", "REGISTEREDCAPACITY", "DISPATCHTYPE", "STARTTYPE",
    "MKTGENERATORIND", "NORMALSTATUS", "MAXCAPACITY", "GENSETTYPE", "GENSETNAME",
    "LASTCHANGED", "CO2E_EMISSIONS_FACTOR", "CO2E_ENERGY_SOURCE", "CO2E_DATA_SOURCE",
    "MINCAPACITY", "REGISTEREDMINCAPACITY", "MAXSTORAGECAPACITY",
]

# Maps GENUNITS CO2E_ENERGY_SOURCE → FUEL_CATEGORY used in the dashboard
CO2E_TO_FUEL_MAP = {
    "Solar": "Solar",
    "Wind": "Wind",
    "Hydro": "Hydro",
    "Battery Storage": "Battery",
    "Black coal": "Fossil",
    "Brown coal": "Fossil",
    "Natural Gas (Pipeline)": "Fossil",
    "Natural Gas (LNG)": "Fossil",
    "Diesel oil": "Fossil",
    "Kerosene - non aviation": "Fossil",
    "Coal seam methane": "Fossil",
    "Coal mine waste gas": "Fossil",
    "Landfill biogas methane": "Other Renewable",
    "Biomass and industrial materials": "Other Renewable",
    "Bagasse": "Other Renewable",
    "Biogas": "Other Renewable",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
                    REGISTRATION_URL, timeout=30,
                    headers={"User-Agent": "Mozilla/5.0 AEMO-MLF-Tracker"},
                )
                resp.raise_for_status()
                xls_path.write_bytes(resp.content)
                logger.info(f"Downloaded {len(resp.content) / 1024:.0f} KB")
                break
            except requests.RequestException as e:
                if attempt < config.MAX_RETRIES - 1:
                    wait = config.RETRY_BACKOFF * (attempt + 1)
                    logger.warning(f"Download failed (attempt {attempt+1}): {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise RuntimeError(f"Failed to download registration list: {e}")
    return xls_path


def _first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _parse_secondary_sheet(
    xls_path: Path, sheet_name: str, duid_type: str,
    duid_col_override: str | None = None,
    name_col_override: str | None = None,
) -> pd.DataFrame | None:
    try:
        df = pd.read_excel(xls_path, engine="openpyxl", sheet_name=sheet_name)
    except Exception:
        return None

    duid_col = (duid_col_override if duid_col_override and duid_col_override in df.columns
                else _first_col(df, DUID_CANDIDATES))
    if duid_col is None:
        return None

    name_col = (name_col_override if name_col_override and name_col_override in df.columns
                else _first_col(df, NAME_CANDIDATES))
    region_col = _first_col(df, REGION_CANDIDATES)

    rows = pd.DataFrame()
    rows["DUID"] = df[duid_col].astype(str).str.strip()
    rows["STATION_NAME"] = df[name_col].astype(str).str.strip() if name_col else None
    rows["REGION"] = df[region_col].astype(str).str.strip() if region_col else None
    rows["DUID_TYPE"] = duid_type
    rows = rows[rows["DUID"].notna() & (rows["DUID"] != "") & (rows["DUID"] != "nan")]
    rows = rows.drop_duplicates(subset="DUID", keep="first").reset_index(drop=True)

    logger.info(f"Sheet '{sheet_name}': {len(rows)} {duid_type} DUIDs")
    return rows


def _parse_aemo_csv(content: bytes, col_names: list[str]) -> pd.DataFrame:
    """Parse an AEMO MMSDM CSV (rows prefixed D, table_group, table, version)."""
    data_lines = [l for l in content.decode("utf-8").splitlines() if l.startswith("D,")]
    rows = []
    for fields in csv.reader(io.StringIO("\n".join(data_lines))):
        values = fields[4:]  # skip D, group, table, version
        if len(values) >= len(col_names):
            rows.append(dict(zip(col_names, values[:len(col_names)])))
    return pd.DataFrame(rows)


def _download_mmsdm_zip(url: str) -> bytes | None:
    """Download a MMSDM zip, return raw bytes or None on failure."""
    headers = {"User-Agent": "Mozilla/5.0 AEMO-MLF-Tracker"}
    try:
        resp = requests.get(url, timeout=30, headers=headers)
        resp.raise_for_status()
        return resp.content
    except requests.RequestException as e:
        logger.warning(f"Could not download {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# MMSDM participant registration lookup
# ---------------------------------------------------------------------------

def fetch_mmsdm_participant_metadata(
    cache_dir: str, year: int, month: int
) -> tuple[pd.Series, pd.DataFrame]:
    """Download STATION and GENUNITS tables from the MMSDM archive.

    Returns:
        station_names  — pd.Series indexed by STATIONID, values = STATIONNAME
        genunits_df    — DataFrame with columns:
                         GENSETID, STATIONID, REGISTEREDCAPACITY,
                         CO2E_ENERGY_SOURCE, FUEL_CATEGORY, DISPATCHTYPE
    """
    cache_path = Path(cache_dir)
    station_cache = cache_path / "mmsdm_station.feather"
    genunits_cache = cache_path / "mmsdm_genunits.feather"

    # --- STATION table ---
    if station_cache.exists():
        station_df = pd.read_feather(station_cache)
        logger.info(f"Loaded STATION cache ({len(station_df)} rows)")
    else:
        url = MMSDM_PR_URL_TEMPLATE.format(year=year, month=month, table="STATION")
        raw = _download_mmsdm_zip(url)
        if raw is None:
            station_df = pd.DataFrame(columns=STATION_COLS)
        else:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                content = zf.read(zf.namelist()[0])
            station_df = _parse_aemo_csv(content, STATION_COLS)
            # keep only STATIONID + STATIONNAME; clean empties
            station_df = station_df[["STATIONID", "STATIONNAME"]].copy()
            station_df = station_df[
                station_df["STATIONID"].notna() & (station_df["STATIONID"] != "")
            ]
            station_df = station_df.drop_duplicates("STATIONID", keep="first")
            station_df.reset_index(drop=True).to_feather(station_cache)
        logger.info(f"STATION table: {len(station_df)} stations")

    station_names = station_df.set_index("STATIONID")["STATIONNAME"]

    # --- GENUNITS table ---
    if genunits_cache.exists():
        genunits_df = pd.read_feather(genunits_cache)
        logger.info(f"Loaded GENUNITS cache ({len(genunits_df)} rows)")
    else:
        url = MMSDM_PR_URL_TEMPLATE.format(year=year, month=month, table="GENUNITS")
        raw = _download_mmsdm_zip(url)
        if raw is None:
            genunits_df = pd.DataFrame(columns=GENUNITS_COLS)
        else:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                content = zf.read(zf.namelist()[0])
            genunits_df = _parse_aemo_csv(content, GENUNITS_COLS)
            keep = ["GENSETID", "STATIONID", "REGISTEREDCAPACITY",
                    "CO2E_ENERGY_SOURCE", "DISPATCHTYPE"]
            genunits_df = genunits_df[[c for c in keep if c in genunits_df.columns]].copy()
            genunits_df["REGISTEREDCAPACITY"] = pd.to_numeric(
                genunits_df["REGISTEREDCAPACITY"], errors="coerce"
            )
            genunits_df = genunits_df[
                genunits_df["GENSETID"].notna() & (genunits_df["GENSETID"] != "")
            ]
            genunits_df = genunits_df.drop_duplicates("GENSETID", keep="last")
            genunits_df.reset_index(drop=True).to_feather(genunits_cache)
        logger.info(f"GENUNITS table: {len(genunits_df)} units")

    # Add FUEL_CATEGORY from CO2E mapping
    if "CO2E_ENERGY_SOURCE" in genunits_df.columns:
        genunits_df["FUEL_CATEGORY"] = (
            genunits_df["CO2E_ENERGY_SOURCE"].map(CO2E_TO_FUEL_MAP).fillna("")
        )

    return station_names, genunits_df


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fetch_generator_metadata(
    cache_dir: str, mmsdm_year: int | None = None, mmsdm_month: int | None = None
) -> tuple[pd.DataFrame, pd.Series]:
    """Fetch all DUID metadata from NEM Registration List + MMSDM tables.

    Returns:
        combined   — DataFrame with DUID, STATION_NAME, FUEL_CATEGORY,
                     CAPACITY_MW, REGION, DUID_TYPE (and optional FUEL_SOURCE,
                     TECHNOLOGY columns where available)
        station_names — pd.Series(STATIONID → STATIONNAME) for name enrichment
                        in build_summary
    """
    xls_path = _download_xls(cache_dir)

    # --- Primary sheet: currently registered generators ---
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

    # --- Secondary registration sheets ---
    secondary_frames = []
    for sheet_name, duid_type, duid_override, name_override in SECONDARY_SHEETS:
        result = _parse_secondary_sheet(xls_path, sheet_name, duid_type, duid_override, name_override)
        if result is not None and not result.empty:
            secondary_frames.append(result)

    if secondary_frames:
        secondary = pd.concat(secondary_frames, ignore_index=True)
        secondary = secondary[~secondary["DUID"].isin(gen["DUID"])]
        secondary = secondary.drop_duplicates(subset="DUID", keep="first")
        logger.info(
            f"Secondary sheets: {len(secondary)} additional DUIDs "
            f"({secondary['DUID_TYPE'].value_counts().to_dict()})"
        )
        combined = pd.concat([gen, secondary], ignore_index=True)
    else:
        combined = gen

    registered_duids = set(combined["DUID"])

    # --- MMSDM GENUNITS tier: historical/deregistered DUIDs ---
    station_names = pd.Series(dtype=str)
    if mmsdm_year is not None and mmsdm_month is not None:
        try:
            station_names, genunits_df = fetch_mmsdm_participant_metadata(
                cache_dir, mmsdm_year, mmsdm_month
            )
            # Build rows for DUIDs in GENUNITS not already in registration list
            new_rows = genunits_df[~genunits_df["GENSETID"].isin(registered_duids)].copy()
            new_rows = new_rows.rename(columns={
                "GENSETID": "DUID",
                "REGISTEREDCAPACITY": "CAPACITY_MW",
            })
            # Resolve station name via GENUNITS.STATIONID → STATION table
            if "STATIONID" in new_rows.columns:
                new_rows["STATION_NAME"] = new_rows["STATIONID"].map(station_names)
            new_rows["DUID_TYPE"] = "Generator"
            new_rows = new_rows.drop_duplicates(subset="DUID", keep="first")
            logger.info(f"MMSDM GENUNITS tier: {len(new_rows)} historical DUIDs added")
            combined = pd.concat([combined, new_rows], ignore_index=True)
        except Exception as e:
            logger.warning(f"MMSDM participant metadata unavailable: {e}")

    logger.info(f"Total metadata: {len(combined)} DUIDs")
    return combined, station_names
