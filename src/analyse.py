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

            import_mlf = best.get("SECONDARY_TLF")
            rows.append({
                "DUID": duid,
                "REGIONID": best["REGIONID"],
                "CONNECTIONPOINTID": best["CONNECTIONPOINTID"],
                "STATIONID": best["STATIONID"],
                "FY": fy_label,
                "FY_START_YEAR": fy_start_year,
                "MLF": best["TRANSMISSIONLOSSFACTOR"],
                "IMPORT_MLF": import_mlf if pd.notna(import_mlf) else None,
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
                  final_excel: pd.DataFrame | None = None,
                  station_names: "pd.Series | None" = None) -> pd.DataFrame:
    """Build the master summary: pivot FYs to columns, merge generator metadata.

    Returns a wide-format DataFrame: one row per DUID with FY columns.

    - final_excel: DataFrame [DUID, FINAL_MLF] from AEMO's published final Excel.
      When provided, overrides the current FY column (which would otherwise carry
      forward FY25-26 values until DUDETAILSUMMARY is updated on July 1).
    - indicative: DataFrame [DUID, INDICATIVE_MLF] for the *next* FY draft column.
    """
    df = compute_yoy_changes(fy_mlfs)

    # Pivot to wide format: DUID as rows, FY as columns (export MLF)
    pivot = df.pivot_table(index="DUID", columns="FY", values="MLF", aggfunc="first")
    pivot.columns = [str(c) for c in pivot.columns]

    # Pivot import MLFs where available (BIDIRECTIONAL DUIDs, FY24-25 onwards)
    has_import = df["IMPORT_MLF"].notna().any()
    import_pivot = None
    if has_import:
        import_pivot = df.pivot_table(index="DUID", columns="FY", values="IMPORT_MLF", aggfunc="first")
        import_pivot.columns = [f"{c} Import" for c in import_pivot.columns]

    # Add metadata from the latest FY record per DUID
    latest = df.sort_values("FY_START_YEAR").drop_duplicates("DUID", keep="last")
    meta = latest[["DUID", "REGIONID", "CONNECTIONPOINTID", "STATIONID"]].set_index("DUID")
    result = meta.join(pivot)
    if import_pivot is not None:
        result = result.join(import_pivot)

    # Add stub rows for DUIDs in the final Excel that have no DUDETAILSUMMARY history.
    # These are either newly commissioned assets (first MLF = current FY) or DUIDs
    # where AEMO changed the identifier between the registration list and DUDETAILSUMMARY
    # (e.g. HPR1 vs HPRG1). The final Excel override block below will populate their
    # current-FY MLF value.
    if final_excel is not None and not final_excel.empty:
        new_duids = final_excel.loc[
            ~final_excel["DUID"].isin(result.index), "DUID"
        ].tolist()
        if new_duids:
            gen_region: dict = {}
            if generators is not None and "REGION" in generators.columns:
                gen_region = (
                    generators.dropna(subset=["DUID"])
                    .set_index("DUID")["REGION"]
                    .to_dict()
                )
            stubs = pd.DataFrame(
                {"REGIONID": pd.Series(new_duids).map(gen_region).values},
                index=pd.Index(new_duids, name="DUID"),
            )
            result = pd.concat([result, stubs])
            logger.info(
                f"Added {len(new_duids)} stub rows for final-Excel-only DUIDs "
                f"(newly commissioned or DUID-aliased assets)"
            )

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

        # Apply import MLF overrides from the final Excel (BDU Import MLF column)
        current_fy_import_col = f"{current_fy_col} Import"
        if "FINAL_IMPORT_MLF" in final_excel.columns:
            import_map = final_excel.set_index("DUID")["FINAL_IMPORT_MLF"]
            import_overridden = result.index.map(import_map)
            result[current_fy_import_col] = import_overridden.where(
                import_overridden.notna(),
                result.get(current_fy_import_col),
            )
            logger.info(
                f"Applied final Excel import MLF overrides to '{current_fy_import_col}': "
                f"{import_overridden.notna().sum()} DUIDs updated"
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

        # Import MLF YoY (only meaningful for BIDIRECTIONAL DUIDs)
        current_import = f"{current_fy} Import"
        prev_import = f"{prev_fy} Import"
        if current_import in result.columns and prev_import in result.columns:
            result["LATEST_IMPORT_MLF"] = result[current_import]
            result["PREV_IMPORT_MLF"] = result[prev_import]
            result["IMPORT_YOY_CHANGE"] = (
                result["LATEST_IMPORT_MLF"] - result["PREV_IMPORT_MLF"]
            ).round(4)
            result["IMPORT_YOY_PCT_CHANGE"] = (
                result["IMPORT_YOY_CHANGE"] / result["PREV_IMPORT_MLF"] * 100
            ).round(2)

    # Merge indicative/draft MLFs if available
    draft_col = None
    if indicative is not None and not indicative.empty:
        from .indicative import get_indicative_fy
        next_fy_start, fy_label, _ = get_indicative_fy()
        draft_col = f"FY{next_fy_start % 100:02d}-{(next_fy_start + 1) % 100:02d} (Draft)"
        join_cols = {"INDICATIVE_MLF": draft_col}
        if "INDICATIVE_IMPORT_MLF" in indicative.columns:
            draft_import_col = f"{draft_col} Import"
            join_cols["INDICATIVE_IMPORT_MLF"] = draft_import_col
        ind = indicative.set_index("DUID")[list(join_cols.keys())].rename(columns=join_cols)
        result = result.join(ind, how="left")
        logger.info(f"Added indicative column '{draft_col}' ({indicative['DUID'].nunique()} DUIDs)")

    # Merge generator/participant metadata if available
    if generators is not None and not generators.empty:
        gen_meta = generators.set_index("DUID")[
            [c for c in ["STATION_NAME", "FUEL_SOURCE", "FUEL_CATEGORY",
                         "TECHNOLOGY", "CAPACITY_MW", "DUID_TYPE"]
             if c in generators.columns]
        ]
        result = result.join(gen_meta, how="left")

    result = result.reset_index()

    # --- Fallback labelling for DUIDs not in any registration sheet ---
    import re as _re
    _nl_pattern = _re.compile(r"NL\d*$", _re.IGNORECASE)

    # Enrich STATION_NAME using the MMSDM STATION table (proper full names)
    # Apply to ALL rows so even registered generators that have abbreviated
    # station IDs as a fallback get the full name.
    if station_names is not None and not station_names.empty and "STATIONID" in result.columns:
        if "STATION_NAME" not in result.columns:
            result["STATION_NAME"] = None
        # Only replace where STATION_NAME is missing or still equals the raw STATIONID
        # (i.e. hasn't been set by the registration list)
        proper = result["STATIONID"].map(station_names)
        mask_use_proper = (
            result["STATION_NAME"].isna()
            | (result["STATION_NAME"] == "")
            | (result["STATION_NAME"] == result["STATIONID"])
        ) & proper.notna()
        result.loc[mask_use_proper, "STATION_NAME"] = proper[mask_use_proper]
        logger.info(
            f"Station name enrichment: {mask_use_proper.sum()} rows updated "
            f"with proper names from MMSDM STATION table"
        )

    # Use STATIONID from DUDETAILSUMMARY as fallback station name (abbreviated)
    if "STATIONID" in result.columns and "STATION_NAME" in result.columns:
        mask_no_name = result["STATION_NAME"].isna() | (result["STATION_NAME"] == "")
        result.loc[mask_no_name, "STATION_NAME"] = result.loc[mask_no_name, "STATIONID"]

    # Infer DUID_TYPE from DUID suffix if still missing
    if "DUID_TYPE" in result.columns:
        mask_no_type = result["DUID_TYPE"].isna() | (result["DUID_TYPE"] == "")
        if mask_no_type.any():
            def _infer_type(duid):
                d = str(duid)
                if _nl_pattern.search(d):
                    return "Network Load"
                return "Unknown"
            result.loc[mask_no_type, "DUID_TYPE"] = result.loc[mask_no_type, "DUID"].map(_infer_type)
    else:
        result["DUID_TYPE"] = result["DUID"].map(
            lambda d: "Network Load" if _nl_pattern.search(str(d)) else "Unknown"
        )

    # Flag retired DUIDs: have historical data but nothing in the two most recent FYs.
    # These are typically old G-suffix battery dispatch DUIDs that AEMO replaced with
    # BIDIRECTIONAL registration DUIDs during 2024.
    if len(fy_cols) >= 2:
        recent_cols = fy_cols[-2:]  # FY25-26 and FY26-27
        historic_cols = fy_cols[:-2]
        has_recent = result[recent_cols].notna().any(axis=1)
        has_historic = result[historic_cols].notna().any(axis=1) if historic_cols else pd.Series(False, index=result.index)
        result["STATUS"] = "Active"
        result.loc[has_historic & ~has_recent, "STATUS"] = "Retired"
    else:
        result["STATUS"] = "Active"

    # Sort by region then latest MLF (ascending = worst MLFs first)
    sort_cols = ["REGIONID"]
    if "LATEST_MLF" in result.columns:
        sort_cols.append("LATEST_MLF")
    result = result.sort_values(sort_cols).reset_index(drop=True)

    total_cols = len(fy_cols) + (1 if draft_col else 0)
    logger.info(f"Built summary: {len(result)} DUIDs × {total_cols} FY columns")
    return result
