"""
Shared logic for ISONE incremental updates: expected grid building, date grouping, DST handling,
gap-filling with defaults. Used by automated_isone_data_update and reusable for other tables.
"""
import pandas as pd
from datetime import date, timedelta
from typing import List


def safe_round_datetime(series: pd.Series, tz: str) -> pd.Series:
    """
    Safely round datetime series to second precision, handling ambiguous times.
    Converts to UTC first to avoid DST ambiguity, then converts back.
    """
    result = series.dt.tz_convert("UTC")
    result = result.dt.floor("s")
    result = result.dt.tz_convert(tz)
    return result


def build_expected_grid(
    start_date,
    end_date,
    tz: str,
    unique_combos: pd.DataFrame,
    key_columns: List[str],
) -> pd.DataFrame:
    """
    Build expected (datetime_he, ...keys) grid for a date range with hourly frequency.
    Handles DST spring-forward (hour after gap stored as XX:59:59). Normalizes to second precision.
    key_columns must be ['datetime_he'] + key names that exist as columns in unique_combos.
    """
    keys = [c for c in key_columns if c != "datetime_he"]
    start_dt = pd.Timestamp(start_date, tz=tz) + timedelta(hours=1)
    end_dt = pd.Timestamp(end_date + timedelta(days=1), tz=tz)

    expected_datetimes = pd.date_range(
        start=start_dt,
        end=end_dt,
        freq="h",
        inclusive="right",
    )

    # Spring forward: first hour after gap stored as XX:59:59 in DB
    adjusted_datetimes = []
    for i, dt in enumerate(expected_datetimes):
        if i > 0:
            prev_dt = expected_datetimes[i - 1]
            prev_hour = prev_dt.hour
            curr_hour = dt.hour
            same_date = prev_dt.date() == dt.date()
            if same_date and (curr_hour - prev_hour) > 1:
                adjusted_datetimes.append(dt.replace(minute=59, second=59, microsecond=0))
                continue
        adjusted_datetimes.append(dt)

    expected_records = []
    for _, combo in unique_combos.iterrows():
        for dt in adjusted_datetimes:
            record = {"datetime_he": dt}
            for k in keys:
                record[k] = combo[k]
            expected_records.append(record)

    expected_df = pd.DataFrame(expected_records)
    expected_df["datetime_he"] = safe_round_datetime(expected_df["datetime_he"], tz)
    expected_df = expected_df.sort_values(by=key_columns).reset_index(drop=True)
    return expected_df


def group_contiguous_dates(dates: List) -> List[List]:
    """Group sorted list of dates into contiguous ranges (list of lists)."""
    if not dates:
        return []
    groups = []
    current_group = [dates[0]]
    for i in range(1, len(dates)):
        if dates[i] == dates[i - 1] + timedelta(days=1):
            current_group.append(dates[i])
        else:
            groups.append(current_group)
            current_group = [dates[i]]
    groups.append(current_group)
    return groups


def segment_into_monthly_chunks(date_group: List, max_days: int = 30) -> List[List]:
    """Split a contiguous date group into chunks of at most max_days (e.g. for API limits)."""
    if not date_group:
        return []
    chunks = []
    current_chunk = [date_group[0]]
    for i in range(1, len(date_group)):
        current_date = date_group[i]
        chunk_start = current_chunk[0]
        days_diff = (current_date - chunk_start).days
        if days_diff >= max_days:
            chunks.append(current_chunk)
            current_chunk = [current_date]
        else:
            current_chunk.append(current_date)
    if current_chunk:
        chunks.append(current_chunk)
    return chunks


def chunk_date_groups(date_groups: List[List], *, max_days: int) -> List[List]:
    """
    Apply segment_into_monthly_chunks to every date group and flatten the result.

    Used to keep API requests small (e.g. avoid timeouts).
    """
    segmented: List[List] = []
    for group in date_groups:
        segmented.extend(segment_into_monthly_chunks(group, max_days=max_days))
    return segmented


def debug_spring_forward_missing(
    missing_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    expected_df: pd.DataFrame,
    spring_forward_date: date,
) -> pd.DataFrame:
    """
    If there are missing records on the given spring-forward DST date, print debug info
    (missing vs existing vs expected datetime_he for that date). Returns missing_df
    with no extra columns (drops temporary 'date' if added).
    """
    if len(missing_df) == 0:
        return missing_df

    missing = missing_df.copy()
    missing["date"] = missing["datetime_he"].dt.date
    spring_forward_missing = missing[missing["date"] == spring_forward_date]
    if len(spring_forward_missing) == 0:
        return missing_df

    print(f"\nDEBUG: Found {len(spring_forward_missing)} missing records on spring forward date {spring_forward_date}")
    print("DEBUG: Unique missing datetime_he values on this date:")
    for dt in sorted(spring_forward_missing["datetime_he"].unique()):
        print(f"  {dt} (hour: {dt.hour})")

    if len(existing_df) > 0 and "datetime_he" in existing_df.columns:
        existing = existing_df.copy()
        existing["date"] = existing["datetime_he"].dt.date
        spring_forward_existing = existing[existing["date"] == spring_forward_date]
        if len(spring_forward_existing) > 0:
            print(f"DEBUG: Found {len(spring_forward_existing)} existing records in DB for {spring_forward_date}")
            print("DEBUG: Unique datetime_he values in DB for this date:")
            for dt in sorted(spring_forward_existing["datetime_he"].unique()):
                print(f"  {dt} (hour: {dt.hour})")
        else:
            print(f"DEBUG: No existing records in DB for {spring_forward_date}")

    if "datetime_he" in expected_df.columns:
        expected = expected_df.copy()
        expected["date"] = expected["datetime_he"].dt.date
        spring_forward_expected = expected[expected["date"] == spring_forward_date]
        if len(spring_forward_expected) > 0:
            print(f"DEBUG: Expected {len(spring_forward_expected)} records for {spring_forward_date}")
            print("DEBUG: Unique expected datetime_he values:")
            for dt in sorted(spring_forward_expected["datetime_he"].unique()):
                print(f"  {dt} (hour: {dt.hour})")

    return missing_df


def fill_missing_with_defaults(
    df_final: pd.DataFrame,
    expected_df: pd.DataFrame,
    composite_cols: List[str],
    table_name: str,
    mis_report: str,
    mapping_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Find gaps in df_final within its datetime range (vs expected_df), fill those gaps with
    table-specific default values (0 volumes, unit, interval_width_s, asset via mapping_df), and
    return df_final with the filled rows appended.

    mapping_df must have columns 'name' and 'asset' (e.g. from ISONE location mapping).
    """
    print("\nChecking for any remaining missing data to fill with zeros...")

    if "datetime_he" not in df_final.columns or len(df_final) == 0:
        return df_final
    min_api_datetime = df_final["datetime_he"].min()
    max_api_datetime = df_final["datetime_he"].max()
    print(f"Data range: {min_api_datetime} to {max_api_datetime}")

    expected_df_filtered = expected_df[
        (expected_df["datetime_he"] >= min_api_datetime)
        & (expected_df["datetime_he"] <= max_api_datetime)
    ].copy()
    if len(expected_df_filtered) == 0:
        print("No expected records in data range to check for missing data.")
        return df_final

    df_final_subset = df_final[composite_cols].copy()
    df_final_subset["exists"] = True
    df_final_subset = df_final_subset.sort_values(by=composite_cols)
    merged_updated = expected_df_filtered.merge(
        df_final_subset, on=composite_cols, how="left"
    )
    still_missing_df = merged_updated[merged_updated["exists"].isna()].copy()
    still_missing_df = still_missing_df[composite_cols].copy()

    if len(still_missing_df) == 0:
        print("No missing records found between data range.")
        return df_final

    still_missing_to_fill = still_missing_df[composite_cols].copy()
    print(
        f"\nFilling {len(still_missing_to_fill)} missing combinations with default values (0) "
        f"for gaps between {min_api_datetime} and {max_api_datetime}..."
    )
    initial_fill_count = len(still_missing_to_fill)

    if table_name == "ops.isone_hourly_ancillary":
        if mis_report == "SD_DAASCLEARED":
            still_missing_to_fill["da_volume"] = 0
            still_missing_to_fill["rt_volume"] = pd.NA
            still_missing_to_fill["unit"] = "MW"
            still_missing_to_fill["interval_width_s"] = 3600
            still_missing_to_fill = still_missing_to_fill.merge(mapping_df, how="left", on="name")
        elif mis_report == "OI_UNITRTRSV":
            still_missing_to_fill["da_volume"] = pd.NA
            still_missing_to_fill["rt_volume"] = 0
            still_missing_to_fill["unit"] = "MW"
            still_missing_to_fill["interval_width_s"] = 3600
            still_missing_to_fill = still_missing_to_fill.merge(mapping_df, how="left", on="name")
        else:
            raise ValueError(
                f"For table 'ops.isone_hourly_ancillary', expected MIS reports are "
                f"'SD_DAASCLEARED' or 'OI_UNITRTRSV' but instead was {mis_report}."
            )
    elif table_name == "ops.isone_hourly_energy":
        still_missing_to_fill["da_volume"] = 0
        still_missing_to_fill["rt_volume"] = 0
        still_missing_to_fill["unit"] = "MWh"
        still_missing_to_fill["interval_width_s"] = 3600
        still_missing_to_fill = still_missing_to_fill.merge(mapping_df, how="left", on="name")
    else:
        raise ValueError(f"Table name '{table_name}' not supported for fill_missing_with_defaults.")

    if len(still_missing_to_fill) > initial_fill_count:
        print(
            f"WARNING: Merge with mapping created duplicates! "
            f"Before merge: {initial_fill_count}, After merge: {len(still_missing_to_fill)}"
        )
        still_missing_to_fill = still_missing_to_fill.drop_duplicates(subset=composite_cols, keep="first")
        if len(still_missing_to_fill) != initial_fill_count:
            print(
                f"After deduplication: {len(still_missing_to_fill)} records (expected {initial_fill_count})"
            )
    still_missing_to_fill = still_missing_to_fill.drop_duplicates(subset=composite_cols, keep="first")
    if len(still_missing_to_fill) < initial_fill_count:
        print(
            f"WARNING: Removed {initial_fill_count - len(still_missing_to_fill)} duplicate records from filled missing combos"
        )

    df_final = pd.concat([df_final, still_missing_to_fill], ignore_index=True)
    print(f"Added {len(still_missing_to_fill)} records with default values.")
    return df_final
