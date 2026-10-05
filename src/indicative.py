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

# AEMO publishes final MLFs in April (same folder, no "draft-" prefix).
FINAL_MLF_URL = (
    "https://aemo.com.au/-/media/files/electricity/nem/security_and_reliability/"
    "loss_factors_and_regional_boundaries/{fy_folder}/"
    "marginal-loss-factors-for-the-{fy_label}-financial-year-xls.xlsx"
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


def _download_mlf_excel(
    url: str, cache_path: Path, fy_label: str, col_name: str, required: bool = False
) -> pd.DataFrame | None:
    """Shared downloader for draft and final MLF Excel files.

    A 404 means AEMO hasn't published the workbook yet and returns None — unless
    `required`, when it is an error. Any other non-200 response (AEMO's Cloudflare
    answers scripted fetches with 403), a network failure, or a body that isn't an
    xlsx raises, so a blocked fetch can never silently drop a column.
    """
    if not cache_path.exists():
        logger.info(f"Downloading MLF Excel from {url} ...")
        for attempt in range(config.MAX_RETRIES):
            last_attempt = attempt == config.MAX_RETRIES - 1
            try:
                resp = requests.get(
                    url, timeout=30,
                    headers={"User-Agent": "Mozilla/5.0 AEMO-MLF-Tracker"},
                )
            except requests.RequestException as e:
                if last_attempt:
                    raise RuntimeError(f"Could not download MLF Excel {url}: {e}") from e
                wait = config.RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"Download failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 404 and not required:
                logger.info(f"MLF Excel not yet published (404): {url}")
                return None
            if resp.status_code >= 500 and not last_attempt:
                wait = config.RETRY_BACKOFF * (attempt + 1)
                logger.warning(f"HTTP {resp.status_code} (attempt {attempt + 1}). Retrying in {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"MLF Excel download returned HTTP {resp.status_code}: {url}")
            if not resp.content.startswith(b"PK"):
                raise RuntimeError(f"MLF Excel download is not an xlsx (challenge page?): {url}")
            cache_path.write_bytes(resp.content)
            logger.info(f"Downloaded ({len(resp.content) / 1024:.0f} KB) → {cache_path.name}")
            break

    result = _parse_mlf_excel(cache_path, fy_label, col_name)
    if result is None and required:
        raise RuntimeError(f"No MLFs parsed from {cache_path} (delete it to re-download)")
    return result


def _parse_mlf_excel(xlsx_path: Path, fy_label: str, col_name: str) -> pd.DataFrame | None:
    """Parse an AEMO MLF Excel file (draft or final) into a clean DataFrame.

    Each Gen sheet has two sections:
    - Regular generators: single MLF column (e.g. "2026-27 MLF")
    - BDU section: separate Import/Export MLF columns (e.g. "2026-27 Import MLF", "2026-27 Export MLF")

    Returns a DataFrame with col_name (export MLF) and optionally an import column.
    """
    logger.info(f"Parsing MLF Excel for FY{fy_label} ({xlsx_path.name})...")
    import_col_name = col_name.replace("MLF", "IMPORT_MLF")

    try:
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    except Exception as e:
        logger.error(f"Failed to open MLF Excel: {e}")
        return None

    all_rows = []
    for sheet_name, region in SHEET_REGION_MAP.items():
        if sheet_name not in xls.sheet_names:
            continue

        df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

        # Find ALL header rows containing "DUID" — there may be two:
        # one for regular generators and one for the BDU section
        header_indices = []
        for i in range(len(df)):
            row_vals = [str(v).strip() for v in df.iloc[i].tolist()]
            if "DUID" in row_vals:
                header_indices.append(i)

        if not header_indices:
            logger.warning(f"No DUID header found in sheet '{sheet_name}'")
            continue

        for sec_idx, header_idx in enumerate(header_indices):
            headers = [str(v).strip() for v in df.iloc[header_idx].tolist()]

            # Determine where this section ends (next header row or end of sheet)
            next_header = header_indices[sec_idx + 1] if sec_idx + 1 < len(header_indices) else len(df)
            data = df.iloc[header_idx + 1:next_header].copy()
            data.columns = headers
            data = data.dropna(subset=["DUID"])

            # Check if this is a BDU section with Import/Export MLF columns
            import_mlf_col = [c for c in headers if fy_label in c and "Import MLF" in c]
            export_mlf_col = [c for c in headers if fy_label in c and "Export MLF" in c]

            if import_mlf_col and export_mlf_col:
                # BDU section — separate import and export MLFs
                for _, row in data.iterrows():
                    duid = str(row["DUID"]).strip()
                    export_mlf = pd.to_numeric(row[export_mlf_col[0]], errors="coerce")
                    import_mlf = pd.to_numeric(row[import_mlf_col[0]], errors="coerce")
                    if duid and (pd.notna(export_mlf) or pd.notna(import_mlf)):
                        entry = {"DUID": duid, "REGIONID": region}
                        if pd.notna(export_mlf):
                            entry[col_name] = export_mlf
                        if pd.notna(import_mlf):
                            entry[import_col_name] = import_mlf
                        all_rows.append(entry)
            else:
                # Regular section — single MLF column
                mlf_col = [c for c in headers if fy_label in c and "MLF" in c]
                if not mlf_col:
                    logger.warning(f"No {fy_label} MLF column in sheet '{sheet_name}' section {sec_idx}")
                    continue
                for _, row in data.iterrows():
                    duid = str(row["DUID"]).strip()
                    mlf = pd.to_numeric(row[mlf_col[0]], errors="coerce")
                    if pd.notna(mlf) and duid:
                        all_rows.append({"DUID": duid, "REGIONID": region, col_name: mlf})

    if not all_rows:
        logger.warning("No MLF data parsed")
        return None

    result = pd.DataFrame(all_rows)
    result = result.drop_duplicates(subset="DUID", keep="first")

    bdu_count = result[import_col_name].notna().sum() if import_col_name in result.columns else 0
    logger.info(f"Parsed {len(result)} MLFs for FY{fy_label} (col: {col_name}, {bdu_count} with import MLF)")
    return result


def download_draft_mlfs(cache_dir: str) -> pd.DataFrame | None:
    """Download and parse AEMO's draft MLF Excel for the next FY.

    Returns DataFrame with columns [DUID, REGIONID, INDICATIVE_MLF], or None if not
    yet published (404); raises on any other download failure.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    next_fy, fy_label, fy_folder = get_indicative_fy()
    url = DRAFT_MLF_URL.format(fy_folder=fy_folder, fy_label=fy_label)
    xlsx_path = cache_path / f"draft_mlf_{fy_label}.xlsx"
    return _download_mlf_excel(url, xlsx_path, fy_label, "INDICATIVE_MLF")


def download_final_mlfs(cache_dir: str, full_refresh: bool = False) -> pd.DataFrame | None:
    """Download and parse AEMO's final MLF Excel for the current FY (published each April).

    AEMO loads final MLFs into DUDETAILSUMMARY only on July 1. This function reads
    the published Excel directly so final values are available from April onwards.

    Returns DataFrame with columns [DUID, REGIONID, FINAL_MLF]; raises if the
    workbook can't be fetched or parsed.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    # Final MLFs are for the current FY (FY_END → FY_END+1)
    fy_start = config.FY_END
    fy_label = f"{fy_start}-{(fy_start + 1) % 100:02d}"
    fy_folder = fy_label
    url = FINAL_MLF_URL.format(fy_folder=fy_folder, fy_label=fy_label)
    xlsx_path = cache_path / f"final_mlf_{fy_label}.xlsx"

    if full_refresh and xlsx_path.exists():
        xlsx_path.unlink()
        logger.info(f"Cleared cached final MLF Excel for FY{fy_label}")

    # FY_END only rolls over in April, once the final workbook is due, so a missing
    # workbook is an error rather than a reason to show last year's MLFs.
    return _download_mlf_excel(url, xlsx_path, fy_label, "FINAL_MLF", required=True)

