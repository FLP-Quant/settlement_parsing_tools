import pandas as pd
import numpy as np
import os
import sys
import warnings
from datetime import timedelta

# Add the src directory to the Python path so we can import the parsers module
project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.parsers import retrieve_isone_location_map

## Parse SD_DAASCLEARED data

def process_daas_cleared_data(dfs, mapping_path):
    """
    Process ISO-NE Day Ahead Ancillary Services (DAAS) cleared data.
    
    Parameters
    ----------
    df : list of pd.DataFrames
        Raw dataframe from API (equivalent to pd.read_csv with skiprows=4)
    mapping_path : str
        Path to the ISONE Location Mapping CSV file
        
    Returns
    -------
    pd.DataFrame
        Processed dataframe ready for database upload
    """
    
    dfs_clean = []
    # Expected column structure for SD_DAASCLEARED:
    expected_columns = ['A', 'B', 'Date', 'Version', 'Data_vs_Header_Code', 'Hour_Ending', 'G', 
                        'Asset_Name', 'Asset_Type', 'DA_TMSR_Obligation', 'DA_TMNSR_Obligation',
                        'DA_TMOR_Obligation', 'DA_EIR_Obligation']
    
    # Loop through first to ensure that column names match so that concat works
    for i, df in enumerate(dfs):
        # # Get the headers from first row
        # headers = df.iloc[0]
        
        # Drop the first two rows (headers and units) from the main dataframe
        df = df.iloc[2:].reset_index(drop=True)
        
        # Check column count and provide helpful error message
        actual_col_count = len(df.columns)
        expected_col_count = len(expected_columns)
        
        if actual_col_count != expected_col_count:
            # Print debugging information
            print(f"WARNING: DataFrame {i+1} has {actual_col_count} columns, expected {expected_col_count}")
            print(f"First few actual column names: {list(df.columns[:10])}")
            if actual_col_count > 0:
                print(f"First row sample: {df.iloc[0].head(10).to_dict()}")
            
            # If we have more columns than expected, use only the first N columns
            if actual_col_count > expected_col_count:
                print(f"Using only the first {expected_col_count} columns and ignoring the rest.")
                df = df.iloc[:, :expected_col_count]
            else:
                raise ValueError(
                    f"DataFrame {i+1} has {actual_col_count} columns but expected {expected_col_count}. "
                    f"This suggests the API response format has changed. "
                    f"Actual columns: {list(df.columns)}"
                )
        
        # Rename columns to match the structure
        df.columns = expected_columns
        dfs_clean.append(df)
    
    # Combine the dfs
    df = pd.concat(dfs_clean, ignore_index=True)
    # Filter for code 'D' in the Data vs. Header Code column
    df = df[df['Data_vs_Header_Code'] == 'D']
    
    # Filter out duplicates, keeping latest version
    def filter_duplicate_reports(df):
        # Count rows before deduplication
        initial_row_count = len(df)
        
        # Sort by Version descending, so the latest version comes first
        df['Version'] = pd.to_datetime(df['Version'])
        df_sorted = df.sort_values(by='Version', ascending=False)
        
        # Identify duplicates
        dup_cols = ['Date', 'Hour_Ending', 'Asset_Name', 'Asset_Type']
        duplicates_mask = df_sorted.duplicated(subset=dup_cols, keep=False)
        
        # Find rows that are true duplicates in key columns *and* have same Version
        duplicate_groups = df_sorted[duplicates_mask].groupby(dup_cols)
        same_version_conflicts = []
        
        for keys, group in duplicate_groups:
            if group['Version'].nunique() == 1:
                same_version_conflicts.append(keys)
        
        # Warn about same-Version duplicates
        if same_version_conflicts:
            warnings.warn(
                f"{len(same_version_conflicts)} duplicated groups had identical Version values. "
                f"These will still be deduplicated arbitrarily.\n"
                f"Examples: {same_version_conflicts[:3]}"
            )
        
        # Drop duplicates, keeping latest Version
        df_deduped = df_sorted.drop_duplicates(subset=dup_cols, keep='first')
        
        # Print how many rows were removed
        deleted_rows = initial_row_count - len(df_deduped)
        print(f"{deleted_rows} duplicate rows removed based on most recent Version.")
        return df_deduped
    
    df = filter_duplicate_reports(df)
    
    # Keep only the specified columns
    columns_to_keep = ['Date', 'Hour_Ending', 'Asset_Name', 'Asset_Type',
                      'DA_TMSR_Obligation', 'DA_TMNSR_Obligation',
                      'DA_TMOR_Obligation', 'DA_EIR_Obligation']
    df = df[columns_to_keep]
    
    # Create DST flag before replacing "02X" - True means it's the second 2AM (EST, after fall back)
    df["is_dst_fallback"] = df["Hour_Ending"] == "02X"
    df["Hour_Ending"] = df["Hour_Ending"].replace({"02X": "02"}) # Handle DST transition
    df["Hour_Ending"] = pd.to_numeric(df["Hour_Ending"], errors="raise")
    df.sort_values(by=["Date", "Hour_Ending", "Asset_Name"], inplace=True)
    
    # # DEBUG: Helper function to extract ambiguous HE2 hours
    # def debug_he2_rows(df_debug, stage_name):
    #     """Extract and print HE2 rows around DST fallback date (Nov 2, 2025)"""
    #     if len(df_debug) == 0:
    #         print(f"\n[{stage_name}] DataFrame is empty")
    #         return None
            
    #     # Filter for date around DST fallback (Nov 1-2, 2025)
    #     date_mask = pd.Series([True] * len(df_debug), index=df_debug.index)
        
    #     if 'Date' in df_debug.columns:
    #         if df_debug['Date'].dtype == 'object':
    #             date_mask = df_debug['Date'].astype(str).str.contains('2025-11-02', na=False)
    #         else:
    #             date_vals = pd.to_datetime(df_debug['Date']).dt.date
    #             target_dates = [pd.Timestamp('2025-11-01').date(), pd.Timestamp('2025-11-02').date()]
    #             date_mask = date_vals.isin(target_dates)
    #     elif 'datetime_he' in df_debug.columns:
    #         # Extract date from datetime_he
    #         try:
    #             dt_series = pd.to_datetime(df_debug['datetime_he'])
    #             date_vals = dt_series.dt.date if hasattr(dt_series.dt, 'date') else dt_series.dt.normalize().dt.date
    #             target_dates = [pd.Timestamp('2025-11-01').date(), pd.Timestamp('2025-11-02').date()]
    #             date_mask = date_vals.isin(target_dates)
    #         except:
    #             date_mask = df_debug['datetime_he'].astype(str).str.contains('2025-11-02', na=False)
    #     else:
    #         print(f"\n[{stage_name}] Cannot find date column. Available columns: {list(df_debug.columns)}")
    #         return None
        
    #     # Filter for HE=2
    #     if 'Hour_Ending' in df_debug.columns:
    #         he_mask = df_debug['Hour_Ending'] == 2
    #         debug_rows = df_debug[date_mask & he_mask].copy()
    #     elif 'datetime_he' in df_debug.columns:
    #         # HE2 means hour ending at 2 AM, which is hour 1 (1:00-2:00 AM)
    #         try:
    #             dt_series = pd.to_datetime(df_debug['datetime_he'])
    #             if hasattr(dt_series.dt, 'hour'):
    #                 he_mask = dt_series.dt.hour == 1
    #                 debug_rows = df_debug[date_mask & he_mask].copy()
    #             else:
    #                 # Fallback: just use date mask
    #                 debug_rows = df_debug[date_mask].copy()
    #         except:
    #             debug_rows = df_debug[date_mask].copy()
    #     else:
    #         debug_rows = df_debug[date_mask].copy()
        
    #     if len(debug_rows) > 0:
    #         print(f"\n{'='*80}")
    #         print(f"[{stage_name}] Found {len(debug_rows)} HE2 rows around DST fallback:")
    #         print(f"{'='*80}")
    #         # Show key columns
    #         cols_to_show = [col for col in ['Date', 'Hour_Ending', 'is_dst_fallback', 'datetime_he', 
    #                                        'Asset_Name', 'asset', 'name', 'ops_type', 'service'] if col in debug_rows.columns]
    #         if len(cols_to_show) > 0:
    #             print(debug_rows[cols_to_show].head(10).to_string())
    #         else:
    #             print(debug_rows.head(10).to_string())
            
    #         if 'datetime_he' in debug_rows.columns:
    #             print(f"\nDatetime_he values (first 10 unique):")
    #             unique_dts = debug_rows['datetime_he'].unique()[:10]
    #             for dt in unique_dts:
    #                 print(f"  {dt} (type: {type(dt)})")
    #                 if hasattr(dt, 'tz') and dt.tz is not None:
    #                     print(f"    Timezone: {dt.tz}, UTC offset: {dt.utcoffset()}")
            
    #         # Check for duplicates in primary key columns if they exist
    #         if all(col in debug_rows.columns for col in ['datetime_he', 'asset', 'name', 'ops_type', 'service']):
    #             pk_cols = ['datetime_he', 'asset', 'name', 'ops_type', 'service']
    #             dup_mask = debug_rows.duplicated(subset=pk_cols, keep=False)
    #             if dup_mask.any():
    #                 print(f"\n⚠️  DUPLICATES FOUND in HE2 rows at this stage!")
    #                 print(f"Number of duplicate rows: {dup_mask.sum()}")
    #                 print("Duplicate rows:")
    #                 print(debug_rows[dup_mask][pk_cols].to_string())
            
    #         return debug_rows
    #     else:
    #         print(f"\n[{stage_name}] No HE2 rows found around DST fallback date")
    #         return None
    
    # # DEBUG: After creating is_dst_fallback
    # debug_he2_rows(df, "After creating is_dst_fallback")

    ## Create datetime column, localize, and handle timezone localization with DST disambiguation
    # Convert hour-ending integer to time delta
    def compute_datetime_he(date, hour_ending):
        return pd.to_datetime(date) + pd.to_timedelta(hour_ending, unit='h')

    # Create datetime column
    df["datetime_he"] = df.apply(
        lambda row: compute_datetime_he(row["Date"], row["Hour_Ending"]), axis=1
    )
    
    # # DEBUG: After creating datetime_he but before localization
    # debug_he2_rows(df, "After creating datetime_he (before localization)")

    # Localize datetime column
    tz = 'America/New_York'
    ambiguous_array = ~df['is_dst_fallback']  # True for first 2AM (EDT), False for second 2AM (EST)
    df['datetime_he'] = df['datetime_he']-timedelta(hours=1); # Temporarily convert to hour beginning for localization
    df['datetime_he'] = df['datetime_he'].dt.tz_localize(
        tz, ambiguous=ambiguous_array.values, nonexistent='shift_backward'
    )
    df['datetime_he'] = df['datetime_he']+timedelta(hours=1); # Convert back to hour ending
    
    # # DEBUG: After localizing datetime_he
    # debug_he2_rows(df, "After localizing datetime_he")
    
    # Mapping for asset names
    mapping = retrieve_isone_location_map(mapping_path)
    mapping = mapping[["ISO-NE Name", "FLP Asset Name", "Operation Type"]]
    
    
    # Melt (unpivot) the obligation columns
    value_vars = ["DA_TMSR_Obligation", "DA_TMNSR_Obligation", 
                  "DA_TMOR_Obligation", "DA_EIR_Obligation"]
    df_melted = df.melt(id_vars=["datetime_he", "Asset_Name", "Asset_Type"],
                        value_vars=value_vars,
                        var_name="service_raw",
                        value_name="da_volume")
    
    # # DEBUG: After melting
    # debug_he2_rows(df_melted, "After melting")
    
    # Clean up and transform columns
    df_melted["service"] = df_melted["service_raw"].str.replace("DA_", "").str.replace("_Obligation", "")
    df_melted["rt_volume"] = ""  # leave blank
    df_melted["unit"] = "MW"
    df_melted["interval_width_s"] = 3600
    
    # Add values from mapping & rename
    df_melted = df_melted.merge(
        mapping, how="left", left_on="Asset_Name", right_on="ISO-NE Name"
    )
    df_melted.rename(
        columns={"Asset_Name":"name", "FLP Asset Name":"asset", "Operation Type":"ops_type"},
        inplace=True
    )
    
    # # DEBUG: After merging with mapping
    # debug_he2_rows(df_melted, "After merging with mapping")
    
    # Final column order
    final_columns = ["datetime_he", "asset", "name", "ops_type", "service",
                           "da_volume", "rt_volume", "unit", "interval_width_s"]
    df_final = df_melted[final_columns].copy()

    # Replace NaN with 0 in da_volume column
    df_final['da_volume'] = df_final['da_volume'].fillna(0)

    # Sort the result for readability
    df_final = df_final.sort_values(by=["datetime_he", "service"])

    # # DEBUG: Before duplicates check
    # debug_he2_rows(df_final, "Before duplicates check (df_final)")
    
    # Add before: return df_final
    primary_key_columns = ['datetime_he', 'asset', 'name', 'ops_type', 'service']
    dup_check = df_final.duplicated(subset=primary_key_columns, keep=False)
    if dup_check.any():
        print(f"WARNING: Duplicate primary keys detected in df_final!")
        print(f"Number of duplicate rows: {dup_check.sum()}")
        print("Sample duplicates:")
        print(df_final[dup_check].sort_values(by=primary_key_columns).head(20))
        raise ValueError("Duplicate primary keys detected in df_final!")

    return df_final


## Parse OI_UNITRTRSV data

def process_rt_reserve_data(dfs, mapping_path):
    """
    Process ISO-NE Real-Time Unit Reserve (OI_UNITRTRSV) data.
    
    Parameters
    ----------
    dfs : list of pd.DataFrames
        Raw dataframe from API (equivalent to pd.read_csv with skiprows=4)
    mapping_path : str
        Path to the ISONE Location Mapping CSV file
        
    Returns
    -------
    pd.DataFrame
        Processed dataframe ready for database upload
    """
    
    # Expected column structure:
    # First 5 columns: A, B, Date, Version, Data_vs_Header_Code
    # Then: Asset ID, Timestamp, TMSR Designation, TMNSR Designation, TMOR Designation
    # Total: 10 columns
    expected_columns = ['A', 'B', 'Date', 'Version', 'Data_vs_Header_Code', 
                       'Asset_ID', 'Timestamp', 'TMSR_Designation', 'TMNSR_Designation', 'TMOR_Designation']
    
    dfs_clean = []
    # Loop through first to ensure that column names match so that concat works
    for df in dfs:
        # Drop the first two rows (headers and units) from the main dataframe
        df = df.iloc[2:].reset_index(drop=True)
        
        # Validate column structure - throw error if it doesn't match
        if len(df.columns) != len(expected_columns):
            raise ValueError(
                f"Expected {len(expected_columns)} columns but found {len(df.columns)}. "
                f"Expected columns: {expected_columns}"
            )
        
        # Rename columns to match the expected structure
        df.columns = expected_columns
        dfs_clean.append(df)
    
    # Combine the dfs
    df = pd.concat(dfs_clean, ignore_index=True)
    
    # Filter for code 'D' in the Data vs. Header Code column
    df = df[df['Data_vs_Header_Code'] == 'D']
    
    # Filter out duplicates, keeping latest version (same logic as DAAS)
    def filter_duplicate_reports(df):
        initial_row_count = len(df)
        
        # Sort by Version descending, so the latest version comes first
        df['Version'] = pd.to_datetime(df['Version'], errors='coerce')
        df_sorted = df.sort_values(by='Version', ascending=False, na_position='last')
        
        # Identify duplicates based on Asset_ID, Timestamp, and service columns
        # Note: We'll handle service columns after melting, so for now use Asset_ID and Timestamp
        dup_cols = ['Asset_ID', 'Timestamp']
        duplicates_mask = df_sorted.duplicated(subset=dup_cols, keep=False)
        
        # Find rows that are true duplicates in key columns *and* have same Version
        duplicate_groups = df_sorted[duplicates_mask].groupby(dup_cols)
        same_version_conflicts = []
        
        for keys, group in duplicate_groups:
            if group['Version'].nunique() == 1:
                same_version_conflicts.append(keys)
        
        # Warn about same-Version duplicates
        if same_version_conflicts:
            warnings.warn(
                f"{len(same_version_conflicts)} duplicated groups had identical Version values. "
                f"These will still be deduplicated arbitrarily.\n"
                f"Examples: {same_version_conflicts[:3]}"
            )
        
        # Drop duplicates, keeping latest Version
        df_deduped = df_sorted.drop_duplicates(subset=dup_cols, keep='first')
        
        # Print how many rows were removed
        deleted_rows = initial_row_count - len(df_deduped)
        if deleted_rows > 0:
            print(f"{deleted_rows} duplicate rows removed based on most recent Version.")
        return df_deduped
    
    df = filter_duplicate_reports(df)
    
    # Handle DST before parsing - check if Timestamp contains "X" (similar to DA function's "02X")
    # Create DST flag before parsing timestamps - True means it's the second 2AM (EST, after fall back)
    df['is_dst_fallback'] = df['Timestamp'].astype(str).str.contains('X', na=False)
    
    # Remove "X" from Timestamp strings before parsing (similar to replacing "02X" with "02" in DA function)
    df['Timestamp'] = df['Timestamp'].astype(str).str.replace('X', '', regex=False)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    
    # Convert designation columns to numeric
    designation_cols = ['TMSR_Designation', 'TMNSR_Designation', 'TMOR_Designation']
    for col in designation_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert 5-minute intervals to hourly intervals by averaging
    # Compute hour-ending from timestamp (similar to DA function's Hour_Ending)
    # Hour-ending is the hour that the interval ends in (1-24, where 24 = midnight)
    # For a timestamp at hour H, the hour-ending is H+1
    df['Hour_Ending'] = df['Timestamp'].dt.hour + 1
    
    # Get Date from Timestamp
    df['Date'] = df['Timestamp'].dt.date
    
    # Group by Asset_ID, Date, Hour_Ending to aggregate 5-minute intervals to hourly
    # Also preserve is_dst_fallback (take any True value, or False if all False)
    groupby_cols = ['Asset_ID', 'Date', 'Hour_Ending']
    agg_dict = {col: 'mean' for col in designation_cols}
    agg_dict['is_dst_fallback'] = lambda x: x.any()  # True if any interval in the hour is fallback
    agg_dict['Version'] = 'first'
    
    df_hourly = df.groupby(groupby_cols, as_index=False).agg(agg_dict)

    ## Create datetime column, localize, and handle timezone localization with DST disambiguation
    # Convert hour-ending integer to time delta
    def compute_datetime_he(date, hour_ending):
        return pd.to_datetime(date) + pd.to_timedelta(hour_ending, unit='h')

    # Create datetime column
    df_hourly["datetime_he"] = df_hourly.apply(
        lambda row: compute_datetime_he(row["Date"], row["Hour_Ending"]), axis=1
    )

    # Localize datetime column
    tz = 'America/New_York'
    ambiguous_array = ~df_hourly['is_dst_fallback']  # True for first 2AM (EDT), False for second 2AM (EST)
    df_hourly['datetime_he'] = df_hourly['datetime_he']-timedelta(hours=1); # Temporarily convert to hour beginning for localization
    df_hourly['datetime_he'] = df_hourly['datetime_he'].dt.tz_localize(
        tz, ambiguous=ambiguous_array.values, nonexistent='shift_backward'
    )
    df_hourly['datetime_he'] = df_hourly['datetime_he']+timedelta(hours=1); # Convert back to hour ending
    
    # Mapping for asset names using PNode ID (do this before melting, more efficient)
    mapping = retrieve_isone_location_map(mapping_path)
    
    # Check if PNode ID column exists in mapping
    if 'PNode ID' not in mapping.columns:
        raise ValueError("'PNode ID' column not found in mapping file. Cannot map Asset IDs to asset names.")
    
    mapping_subset = mapping[["PNode ID", "ISO-NE Name", "FLP Asset Name", "Operation Type"]].copy()
    
    # Convert Asset_ID to string for matching (in case one is numeric and one is string)
    df_hourly['Asset_ID'] = df_hourly['Asset_ID'].astype(str)
    mapping_subset['PNode ID'] = mapping_subset['PNode ID'].astype(str)
    
    # Merge with mapping using PNode ID
    df_hourly = df_hourly.merge(
        mapping_subset,
        how="left",
        left_on="Asset_ID",
        right_on="PNode ID"
    )
    
    # Rename columns
    df_hourly.rename(
        columns={
            "ISO-NE Name": "name",
            "FLP Asset Name": "asset",
            "Operation Type": "ops_type"
        },
        inplace=True
    )
    
    # Keep only the specified columns (similar to DA function)
    columns_to_keep = ['datetime_he', 'asset', 'name', 'ops_type'] + designation_cols
    df_hourly = df_hourly[columns_to_keep]
    
    # Sort by Date, Hour_Ending, asset (similar to DA function)
    df_hourly.sort_values(by=["datetime_he", "asset"], inplace=True)
    
    # Melt (unpivot) the designation columns (same as DA function structure)
    value_vars = ['TMSR_Designation', 'TMNSR_Designation', 'TMOR_Designation']
    df_melted = df_hourly.melt(
        id_vars=["datetime_he", "asset", "name", "ops_type"],
        value_vars=value_vars,
        var_name="service_raw",
        value_name="rt_volume"
    )
    
    # Clean up service names (remove "_Designation" suffix)
    df_melted["service"] = df_melted["service_raw"].str.replace("_Designation", "")
    
    # Add required columns (same as DA function, but map to rt_volume instead of da_volume)
    df_melted["da_volume"] = ""  # leave blank for RT data
    df_melted["unit"] = "MW"
    df_melted["interval_width_s"] = 3600
    
    # Final column order
    final_columns = ["datetime_he", "asset", "name", "ops_type", "service",
                     "da_volume", "rt_volume", "unit", "interval_width_s"]
    df_final = df_melted[final_columns].copy()

    # Replace NaN with 0 in rt_volume column
    df_final['rt_volume'] = df_final['rt_volume'].fillna(0)

    # Sort the result for readability
    df_final = df_final.sort_values(by=["datetime_he", "service"])
    
    return df_final