"""MLF analysis: extract FY-level loss factors and compute trends."""

import logging

import pandas as pd

from . import config

logger = logging.getLogger(__name__)


def extract_fy_mlfs(detail_df: pd.DataFrame) -> pd.DataFrame:
    """Extract one MLF value per DUID per financial year.

    Financial years run July 1 to June 30. We look for records where the
    START_DATE falls within a FY period and take the MLF that was in effect
    for the majority of that FY.

    Returns: DataFrame with columns [DUID, REGIONID, CONNECTIONPOINTID,
             STATIONID, FY, MLF]
    """
    rows = []
    for fy_start_year in range(config.FY_START, config.FY_END + 1):
        fy_begin = pd.Timestamp(f"{fy_start_year}-07-01")
        fy_end = pd.Timestamp(f"{fy_start_year + 1}-07-01")
        fy_label = f"FY{fy_start_year % 100:02d}-{(fy_start_year + 1) % 100:02d}"

        # Find records that overlap with this FY
        # A record overlaps if: START_DATE < fy_end AND END_DATE > fy_begin
        mask = (detail_df["START_DATE"] < fy_end) & (detail_df["END_DATE"] > fy_begin)
        fy_data = detail_df[mask].copy()

        if fy_data.empty:
            continue

        # For each DUID, pick the record that covers the most of this FY
        # (typically the one starting on July 1 of this FY)
        for duid, group in fy_data.groupby("DUID"):
            # Prefer the record whose START_DATE is closest to (and <= ) fy_begin
            # This is the MLF that was set for this FY
            fy_start_records = group[group["START_DATE"] <= fy_begin]
            if not fy_start_records.empty:
                # Take the most recent start before or on July 1
                best = fy_start_records.sort_values("START_DATE").iloc[-1]
            else:
                # Fallback: first record that starts during this FY
                best = group.sort_values("START_DATE").iloc[0]

            rows.append({
                "DUID": duid,
                "REGIONID": best["REGIONID"],
                "CONNECTIONPOINTID": best["CONNECTIONPOINTID"],
                "STATIONID": best["STATIONID"],
                "FY": fy_label,
                "FY_START_YEAR": fy_start_year,
                "MLF": best["TRANSMISSIONLOSSFACTOR"],
            })

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values(["DUID", "FY_START_YEAR"]).reset_index(drop=True)

    logger.info(f"Extracted {len(result)} DUID×FY records across "
                f"{result['DUID'].nunique()} DUIDs and {result['FY'].nunique()} FYs")
    return result


def compute_yoy_changes(fy_mlfs: pd.DataFrame) -> pd.DataFrame:
    """Compute year-on-year MLF changes for each DUID.

    Adds columns: PREV_MLF, YOY_CHANGE, YOY_PCT_CHANGE
    """
    df = fy_mlfs.sort_values(["DUID", "FY_START_YEAR"]).copy()
    df["PREV_MLF"] = df.groupby("DUID")["MLF"].shift(1)
    df["YOY_CHANGE"] = df["MLF"] - df["PREV_MLF"]
    df["YOY_PCT_CHANGE"] = (df["YOY_CHANGE"] / df["PREV_MLF"] * 100).round(2)
    return df


def build_summary(fy_mlfs: pd.DataFrame, generators: pd.DataFrame | None = None,
                  indicative: pd.DataFrame | None = None,
                  final_excel: pd.DataFrame | None = None) -> pd.DataFrame:
    """Build the master summary: pivot FYs to columns, merge generator metadata.

    Returns a wide-format DataFrame: one row per DUID with FY columns.

    - final_excel: DataFrame [DUID, FINAL_MLF] from AEMO's published final Excel.
      When provided, overrides the current FY column (which would otherwise carry
      forward FY25-26 values until DUDETAILSUMMARY is updated on July 1).
    - indicative: DataFrame [DUID, INDICATIVE_MLF] for the *next* FY draft column.
    """
    df = compute_yoy_changes(fy_mlfs)

    # Pivot to wide format: DUID as rows, FY as columns
    pivot = df.pivot_table(index="DUID", columns="FY", values="MLF", aggfunc="first")
    pivot.columns = [str(c) for c in pivot.columns]

    # Add metadata from the latest FY record per DUID
    latest = df.sort_values("FY_START_YEAR").drop_duplicates("DUID", keep="last")
    meta = latest[["DUID", "REGIONID", "CONNECTIONPOINTID", "STATIONID"]].set_index("DUID")
    result = meta.join(pivot)

    # Apply final Excel overrides for the current FY column.
    # AEMO publishes the final Excel in April; DUDETAILSUMMARY isn't updated until July.
    # This ensures the current FY column reflects genuine final values, not FY-1 fallbacks.
    if final_excel is not None and not final_excel.empty:
        from . import config as _cfg
        current_fy_col = f"FY{_cfg.FY_END % 100:02d}-{(_cfg.FY_END + 1) % 100:02d}"
        if current_fy_col in result.columns:
            final_map = final_excel.set_index("DUID")["FINAL_MLF"]
            overridden = result.index.map(final_map)
            result[current_fy_col] = overridden.where(overridden.notna(), result[current_fy_col])
            logger.info(
                f"Applied final Excel overrides to '{current_fy_col}': "
                f"{overridden.notna().sum()} DUIDs updated"
            )

    # Compute latest YoY change (final FYs only)
    fy_cols = sorted([c for c in pivot.columns if c.startswith("FY")])
    if len(fy_cols) >= 2:
        current_fy = fy_cols[-1]
        prev_fy = fy_cols[-2]
        result["LATEST_MLF"] = result[current_fy]
        result["PREV_MLF"] = result[prev_fy]
        result["YOY_CHANGE"] = (result["LATEST_MLF"] - result["PREV_MLF"]).round(4)
        result["YOY_PCT_CHANGE"] = (
            result["YOY_CHANGE"] / result["PREV_MLF"] * 100
        ).round(2)

    # Merge indicative/draft MLFs if available
    draft_col = None
    if indicative is not None and not indicative.empty:
        from .indicative import get_indicative_fy
        next_fy_start, fy_label, _ = get_indicative_fy()
        draft_col = f"FY{next_fy_start % 100:02d}-{(next_fy_start + 1) % 100:02d} (Draft)"
        ind = indicative.set_index("DUID")[["INDICATIVE_MLF"]].rename(
            columns={"INDICATIVE_MLF": draft_col}
        )
        result = result.join(ind, how="left")
        logger.info(f"Added indicative column '{draft_col}' ({indicative['DUID'].nunique()} DUIDs)")

    # Merge generator metadata if available
    if generators is not None and not generators.empty:
        gen_meta = generators.set_index("DUID")[
            [c for c in ["STATION_NAME", "FUEL_SOURCE", "FUEL_CATEGORY",
                         "TECHNOLOGY", "CAPACITY_MW"] if c in generators.columns]
        ]
        result = result.join(gen_meta, how="left")

    result = result.reset_index()

    # Sort by region then latest MLF (ascending = worst MLFs first)
    sort_cols = ["REGIONID"]
    if "LATEST_MLF" in result.columns:
        sort_cols.append("LATEST_MLF")
    result = result.sort_values(sort_cols).reset_index(drop=True)

    total_cols = len(fy_cols) + (1 if draft_col else 0)
    logger.info(f"Built summary: {len(result)} DUIDs × {total_cols} FY columns")
    return result
