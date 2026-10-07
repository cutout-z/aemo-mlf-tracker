"""Data acquisition from AEMO MMSDM archive."""

import csv
import io
import logging
import time
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

# Column mapping for DUDETAILSUMMARY CSV (AEMO's MMSDM format)
DUDETAILSUMMARY_COLUMNS = [
    "DUID", "START_DATE", "END_DATE", "DISPATCHTYPE", "CONNECTIONPOINTID",
    "REGIONID", "STATIONID", "PARTICIPANTID", "LASTCHANGED",
    "TRANSMISSIONLOSSFACTOR", "STARTTYPE", "DISTRIBUTIONLOSSFACTOR",
    "MINIMUM_ENERGY_PRICE", "MAXIMUM_ENERGY_PRICE", "SCHEDULE_TYPE",
    "MIN_RAMP_RATE_UP", "MIN_RAMP_RATE_DOWN", "MAX_RAMP_RATE_UP",
    "MAX_RAMP_RATE_DOWN", "IS_AGGREGATED", "DISPATCHSUBTYPE", "ADG_ID",
    "LOAD_MINIMUM_ENERGY_PRICE", "LOAD_MAXIMUM_ENERGY_PRICE",
    "LOAD_MIN_RAMP_RATE_UP", "LOAD_MIN_RAMP_RATE_DOWN",
    "LOAD_MAX_RAMP_RATE_UP", "LOAD_MAX_RAMP_RATE_DOWN", "SECONDARY_TLF",
]


def months_to_probe(today: date, count: int = 4) -> list[tuple[int, int]]:
    """This calendar month and the `count - 1` before it, newest first.

    Steps by calendar month, not by 30 days: on 31 March, 30-day steps land on 1 March
    and then 30 January, so February was never probed and a month-older archive was used.
    """
    index = today.year * 12 + (today.month - 1)
    return [((index - k) // 12, (index - k) % 12 + 1) for k in range(count)]


def get_latest_available_month(today: date | None = None) -> tuple[int, int] | None:
    """Probe AEMO directory listing to find the newest published month."""
    for year, month in months_to_probe(today or date.today()):
        url = f"{config.MMSDM_BASE_URL}{year:04d}/MMSDM_{year:04d}_{month:02d}/"
        for attempt in range(config.MAX_RETRIES):
            try:
                resp = requests.head(url, timeout=15, allow_redirects=True)
                if resp.status_code == 200:
                    logger.info(f"Latest available month: {year}-{month:02d}")
                    return (year, month)
                elif resp.status_code == 404:
                    break
                else:
                    logger.warning(f"Unexpected status {resp.status_code} for {url}")
                    break
            except requests.RequestException as e:
                if attempt < config.MAX_RETRIES - 1:
                    time.sleep(config.RETRY_BACKOFF * (attempt + 1))
                else:
                    logger.error(f"Failed to probe {url}: {e}")
    logger.error("Could not determine latest available month from AEMO")
    return None


def download_dudetailsummary(year: int, month: int, cache_dir: str) -> pd.DataFrame:
    """Download DUDETAILSUMMARY from MMSDM archive and parse into DataFrame.

    This single file contains the complete historical record of all DUIDs,
    their connection points, regions, and transmission loss factors (MLFs)
    with effective date ranges.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    url = config.DUDETAILSUMMARY_URL_TEMPLATE.format(year=year, month=month)
    logger.info(f"Downloading DUDETAILSUMMARY for {year}-{month:02d}...")

    for attempt in range(config.MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt < config.MAX_RETRIES - 1:
                wait = config.RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"Download failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"Failed to download DUDETAILSUMMARY after {config.MAX_RETRIES} attempts: {e}"
                )

    # Extract CSV from ZIP
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".CSV") or n.endswith(".csv")]
        if not csv_names:
            raise RuntimeError("No CSV file found in DUDETAILSUMMARY ZIP")
        csv_content = zf.read(csv_names[0]).decode("utf-8")

    # Parse AEMO MMSDM CSV format: data rows start with "D,"
    data_lines = [line for line in csv_content.splitlines() if line.startswith("D,")]
    logger.info(f"Parsing {len(data_lines)} data rows...")

    rows = []
    reader = csv.reader(io.StringIO("\n".join(data_lines)))
    for fields in reader:
        # Skip prefix fields: D, table_group, table_name, version
        values = fields[4:]
        if len(values) >= len(DUDETAILSUMMARY_COLUMNS):
            row = dict(zip(DUDETAILSUMMARY_COLUMNS, values[:len(DUDETAILSUMMARY_COLUMNS)]))
            rows.append(row)

    df = pd.DataFrame(rows)

    # Type conversions
    # AEMO uses 2999/12/31 as open-ended sentinel — clamp to stay within pandas timestamp range
    df["START_DATE"] = pd.to_datetime(df["START_DATE"])
    df["END_DATE"] = pd.to_datetime(
        df["END_DATE"].str.replace("2999/12/31", "2099/12/31", regex=False)
    )
    df["TRANSMISSIONLOSSFACTOR"] = pd.to_numeric(df["TRANSMISSIONLOSSFACTOR"], errors="coerce")
    df["DISTRIBUTIONLOSSFACTOR"] = pd.to_numeric(df["DISTRIBUTIONLOSSFACTOR"], errors="coerce")
    df["SECONDARY_TLF"] = pd.to_numeric(df["SECONDARY_TLF"], errors="coerce")

    # Keep GENERATOR and BIDIRECTIONAL dispatch types.
    # AEMO migrated batteries from separate GENERATOR/LOAD G-suffix DUIDs
    # (e.g. HPRG1, VBBG1) to single BIDIRECTIONAL registration DUIDs
    # (e.g. HPR1, VBB1) during 2024. Excluding BIDIRECTIONAL drops all post-
    # migration battery MLF history.
    df = df[df["DISPATCHTYPE"].isin(["GENERATOR", "BIDIRECTIONAL"])].copy()

    logger.info(f"Parsed {len(df)} generator/bidirectional records for {df['DUID'].nunique()} unique DUIDs")
    return df
