"""Generator metadata from AEMO NEM Registration and Exemption List + MMSDM tables."""

import csv
import datetime as _dt
import io
import logging
import re
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
# DUID_TYPE labels. Only "Generator" units sell energy at their MLF; the rest carry an MLF
# because AEMO publishes one for the connection point (see README → Asset type labels).
GENERATOR = "Generator"
SCHEDULED_LOAD = "Scheduled Load"      # pumps, auxiliary loads, the load side of dual-MLF units
NETWORK_LOAD = "Network Load"          # power-station load points (…NL1)
DUMMY_GENERATOR = "Dummy Generator"    # AEMO's DG_<region> market-system units
INTERCONNECTOR = "Interconnector"      # Basslink's BLNKTAS / BLNKVIC

# Registration list "Dispatch Type" → DUID_TYPE (a bidirectional unit is a generator here; its
# battery fuel puts it in the battery table)
DISPATCH_TYPE_MAP = {"Generating Unit": GENERATOR, "Bidirectional Unit": GENERATOR, "Load": SCHEDULED_LOAD}

_NAME_PATTERNS = [
    (re.compile(r"NL\d*$", re.IGNORECASE), NETWORK_LOAD),
    (re.compile(r"^DG_", re.IGNORECASE), DUMMY_GENERATOR),
    (re.compile(r"^BLNK", re.IGNORECASE), INTERCONNECTOR),
]


def type_from_name(duid) -> str | None:
    """DUID_TYPE implied by an AEMO naming convention, for units the registration list omits."""
    for pattern, label in _NAME_PATTERNS:
        if pattern.search(str(duid)):
            return label
    return None


# DUALLOC allocates generating sets (GENUNITS rows) to DUIDs. A GENSETID is not a DUID:
# QUERIVE1 is one DUID over the gensets QUERIVE1 and QUERIVE2 (24 MW each).
DUALLOC_COLS = ["EFFECTIVEDATE", "VERSIONNO", "DUID", "GENSETID", "LASTCHANGED"]

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

def _download_xls(cache_dir: str, refresh: bool = False, status: dict | None = None) -> Path:
    """Download the NEM Registration List to cache_dir.

    Without ``refresh`` a cached copy is reused. With it (every --full-refresh) the list is
    fetched again, because units registered since the cached copy otherwise get no fuel,
    capacity or type (the NAS lane's copy dated from 11 May 2026). A failed refresh keeps the
    cached copy and says so; with no cached copy it raises.

    The body must be a workbook (an xlsx zip, starting "PK"): AEMO's Cloudflare can answer
    with a challenge page and HTTP 200, which used to overwrite the good cache and leave the
    run with no fuel or capacity data. Such a body counts as a failed refresh.

    `status`, when given, records state (downloaded, cached or refresh_failed), AEMO's
    Last-Modified, the date the file in use was fetched (file_date) and any error.
    """
    rec = status if status is not None else {}
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    xls_path = cache_path / "NEM-Registration-and-Exemption-List.xls"

    if refresh or not xls_path.exists():
        logger.info("Downloading NEM Registration List from AEMO...")
        for attempt in range(config.MAX_RETRIES):
            try:
                resp = requests.get(
                    REGISTRATION_URL, timeout=30,
                    headers={"User-Agent": "Mozilla/5.0 AEMO-MLF-Tracker"},
                )
                resp.raise_for_status()
                if not resp.content.startswith(b"PK"):
                    raise ValueError("HTTP 200 but the body is not a workbook (challenge page?)")
                xls_path.write_bytes(resp.content)
                rec.update(state="downloaded", error=None,
                           last_modified=(getattr(resp, "headers", None) or {}).get("Last-Modified"))
                logger.info(f"Downloaded {len(resp.content) / 1024:.0f} KB")
                break
            except (requests.RequestException, ValueError) as e:
                if attempt < config.MAX_RETRIES - 1:
                    wait = config.RETRY_BACKOFF * (attempt + 1)
                    logger.warning(f"Download failed (attempt {attempt+1}): {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                elif xls_path.exists():
                    logger.warning(f"Registration list refresh failed ({e}); keeping the cached copy")
                    rec.update(state="refresh_failed", error=str(e), last_modified=None)
                else:
                    raise RuntimeError(f"Failed to download registration list: {e}")
    else:
        rec.update(state="cached", error=None, last_modified=None)
    rec["file_date"] = _dt.date.fromtimestamp(xls_path.stat().st_mtime).isoformat()
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

def _table_state(raw, cache: Path) -> str:
    """downloaded / cached / missing: where an MMSDM table in use came from."""
    if raw is not None:
        return "downloaded"
    return "cached" if cache.exists() else "missing"


def fetch_mmsdm_participant_metadata(
    cache_dir: str, year: int, month: int, refresh: bool = False, status: dict | None = None
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
    # With refresh (every --full-refresh) the tables are re-fetched for the newest month; a failed
    # fetch falls back to the cached table rather than to an empty one.
    raw = None
    if refresh or not station_cache.exists():
        url = MMSDM_PR_URL_TEMPLATE.format(year=year, month=month, table="STATION")
        raw = _download_mmsdm_zip(url)
    if raw is None and station_cache.exists():
        if refresh:
            logger.warning("STATION table refresh failed; using the cached table")
        station_df = pd.read_feather(station_cache)
        logger.info(f"Loaded STATION cache ({len(station_df)} rows)")
    else:
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

    if status is not None:
        status["STATION"] = _table_state(raw, station_cache)
    station_names = station_df.set_index("STATIONID")["STATIONNAME"]

    # --- GENUNITS table ---
    raw = None
    if refresh or not genunits_cache.exists():
        url = MMSDM_PR_URL_TEMPLATE.format(year=year, month=month, table="GENUNITS")
        raw = _download_mmsdm_zip(url)
    if raw is None and genunits_cache.exists():
        if refresh:
            logger.warning("GENUNITS table refresh failed; using the cached table")
        genunits_df = pd.read_feather(genunits_cache)
        logger.info(f"Loaded GENUNITS cache ({len(genunits_df)} rows)")
    else:
        if raw is None:
            genunits_df = pd.DataFrame(columns=GENUNITS_COLS)
        else:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                content = zf.read(zf.namelist()[0])
            genunits_df = _parse_aemo_csv(content, GENUNITS_COLS)
            keep = ["GENSETID", "STATIONID", "REGISTEREDCAPACITY",
                    "CO2E_ENERGY_SOURCE", "DISPATCHTYPE", "GENSETTYPE"]
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
    if status is not None:
        status["GENUNITS"] = _table_state(raw, genunits_cache)

    # Add FUEL_CATEGORY from CO2E mapping
    if "CO2E_ENERGY_SOURCE" in genunits_df.columns:
        genunits_df["FUEL_CATEGORY"] = (
            genunits_df["CO2E_ENERGY_SOURCE"].map(CO2E_TO_FUEL_MAP).fillna("")
        )

    return station_names, genunits_df


def fetch_mmsdm_dualloc(
    cache_dir: str, year: int, month: int, refresh: bool = False, status: dict | None = None
) -> pd.DataFrame:
    """The current DUID → GENSETID allocation from the MMSDM DUALLOC table.

    DUALLOC keeps every allocation since 1998; a DUID's current gensets are the rows of its
    latest EFFECTIVEDATE (highest VERSIONNO on that date). Cached and refreshed like the
    STATION and GENUNITS tables; an empty frame when neither a download nor a cache exists.
    """
    cache = Path(cache_dir) / "mmsdm_dualloc.feather"
    raw = None
    if refresh or not cache.exists():
        raw = _download_mmsdm_zip(MMSDM_PR_URL_TEMPLATE.format(year=year, month=month, table="DUALLOC"))
    if status is not None:
        status["DUALLOC"] = _table_state(raw, cache)
    if raw is None and cache.exists():
        if refresh:
            logger.warning("DUALLOC table refresh failed; using the cached table")
        return pd.read_feather(cache)
    if raw is None:
        return pd.DataFrame(columns=["DUID", "GENSETID"])
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        df = _parse_aemo_csv(zf.read(zf.namelist()[0]), DUALLOC_COLS)
    df["EFFECTIVEDATE"] = pd.to_datetime(df["EFFECTIVEDATE"], errors="coerce")
    df["VERSIONNO"] = pd.to_numeric(df["VERSIONNO"], errors="coerce")
    latest = (
        df.sort_values(["EFFECTIVEDATE", "VERSIONNO"])
        .groupby("DUID")[["EFFECTIVEDATE", "VERSIONNO"]].last()
    )
    df = df.join(latest, on="DUID", rsuffix="_LATEST")
    current = df[
        (df["EFFECTIVEDATE"] == df["EFFECTIVEDATE_LATEST"]) & (df["VERSIONNO"] == df["VERSIONNO_LATEST"])
    ][["DUID", "GENSETID"]].drop_duplicates().reset_index(drop=True)
    current.to_feather(cache)
    logger.info(f"DUALLOC: {current['DUID'].nunique()} DUIDs over {len(current)} gensets")
    return current


def units_by_duid(genunits_df: pd.DataFrame, dualloc_df: pd.DataFrame) -> pd.DataFrame:
    """Roll GENUNITS (one row per generating set) up to one row per DUID via DUALLOC.

    Capacity is the sum over the DUID's current gensets; fuel and type come from the first
    genset that states them. A genset DUALLOC has never allocated keeps its own ID as the
    DUID (the old behaviour), so a missing DUALLOC table degrades to that rather than to nothing.
    """
    units = genunits_df.copy()
    if dualloc_df is None or dualloc_df.empty:
        logger.warning("DUALLOC unavailable: using GENUNITS set IDs as DUIDs")
        return units.rename(columns={"GENSETID": "DUID"})
    alloc = dualloc_df[["DUID", "GENSETID"]].merge(units, on="GENSETID", how="inner")
    first_stated = lambda s: next((v for v in s if pd.notna(v) and v != ""), None)
    agg = {c: first_stated for c in units.columns if c not in ("GENSETID", "REGISTEREDCAPACITY")}
    agg["REGISTEREDCAPACITY"] = lambda s: s.sum(min_count=1)
    rolled = alloc.groupby("DUID", sort=False).agg(agg).reset_index()
    loose = units[~units["GENSETID"].isin(dualloc_df["GENSETID"])].rename(columns={"GENSETID": "DUID"})
    loose = loose[~loose["DUID"].isin(rolled["DUID"])]
    return pd.concat([rolled, loose], ignore_index=True)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def fetch_generator_metadata(
    cache_dir: str, mmsdm_year: int | None = None, mmsdm_month: int | None = None,
    refresh: bool = False, status: dict | None = None,
) -> tuple[pd.DataFrame, pd.Series]:
    """Fetch all DUID metadata from NEM Registration List + MMSDM tables.

    Returns:
        combined   — DataFrame with DUID, STATION_NAME, FUEL_CATEGORY,
                     CAPACITY_MW, REGION, DUID_TYPE (and optional FUEL_SOURCE,
                     TECHNOLOGY columns where available)
        station_names — pd.Series(STATIONID → STATIONNAME) for name enrichment
                        in build_summary

    `status`, when given, gets a "registration_list" record (see _download_xls) and an
    "mmsdm_tables" record (each table downloaded, cached or missing, plus any error).
    """
    status = status if status is not None else {}
    xls_path = _download_xls(cache_dir, refresh=refresh, status=status.setdefault("registration_list", {}))
    tables = status.setdefault("mmsdm_tables", {})

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
    # The sheet is "PU and Scheduled Loads": pumps (SHPUMP, SNOWYP, PUMP1/2) and a battery's
    # separate load DUID (KEPBL1) are registered with Dispatch Type "Load", not as generators.
    if "DISPATCH_TYPE" in gen.columns:
        gen["DUID_TYPE"] = gen["DISPATCH_TYPE"].map(DISPATCH_TYPE_MAP).fillna(GENERATOR)
    else:
        gen["DUID_TYPE"] = GENERATOR
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
                cache_dir, mmsdm_year, mmsdm_month, refresh=refresh, status=tables
            )
            dualloc_df = fetch_mmsdm_dualloc(cache_dir, mmsdm_year, mmsdm_month, refresh=refresh, status=tables)
            by_duid = units_by_duid(genunits_df, dualloc_df)
            # Build rows for DUIDs in GENUNITS not already in registration list
            new_rows = by_duid[~by_duid["DUID"].isin(registered_duids)].copy()
            new_rows = new_rows.rename(columns={"REGISTEREDCAPACITY": "CAPACITY_MW"})
            # Resolve station name via GENUNITS.STATIONID → STATION table
            if "STATIONID" in new_rows.columns:
                new_rows["STATION_NAME"] = new_rows["STATIONID"].map(station_names)
            # GENSETTYPE says LOAD for pumps and auxiliary loads (KIDSPHL1/2, GPWFEL1); the
            # naming conventions catch the station load points, dummy generators and Basslink,
            # which GENUNITS lists as GENERATOR.
            settype = new_rows["GENSETTYPE"] if "GENSETTYPE" in new_rows.columns else pd.Series("", index=new_rows.index)
            by_name = new_rows["DUID"].map(type_from_name)
            new_rows["DUID_TYPE"] = by_name.where(
                by_name.notna(), settype.map(lambda t: SCHEDULED_LOAD if t == "LOAD" else GENERATOR)
            )
            new_rows = new_rows.drop_duplicates(subset="DUID", keep="first")
            logger.info(f"MMSDM GENUNITS tier: {len(new_rows)} historical DUIDs added")
            combined = pd.concat([combined, new_rows], ignore_index=True)
        except Exception as e:
            logger.warning(f"MMSDM participant metadata unavailable: {e}")
            tables["error"] = str(e)

    logger.info(f"Total metadata: {len(combined)} DUIDs")
    return combined, station_names
