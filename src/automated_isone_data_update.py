import pandas as pd
import os
import sys
import warnings
from datetime import datetime, timedelta, date
from itertools import product

from pandas._libs.tslibs import tz_compare

# Add the src directory to the Python path
project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.process_as_positions import process_daas_cleared_data, process_rt_reserve_data
from src.pharos_ams_query import query_ams_with_basic_auth
from src.parsers import RealTimeOps, prep_rtlocsum_for_quant_db, retrieve_isone_location_map

# FLP database connection tools path
flp_db_tools_path = r"C:\Users\cbrooks\OneDrive - FIRSTLIGHTPOWER.COM\Documents\Python\flp_database_connection_tools"
database_helpers = os.path.join(flp_db_tools_path,"Helpers")
if database_helpers not in sys.path:
    sys.path.append(database_helpers)
from flp_database_connector import flp_database_connector

def automated_isone_data_update(username, token, table_name, tz, mis_report, start_date=None, fill_with_zeros=False):

    keys = ['name', 'ops_type', 'service']
    supported_tables = ['ops.isone_hourly_ancillary', 'ops.isone_hourly_energy']
    if table_name not in supported_tables:
        raise ValueError(f"Table name '{table_name}' not yet supported. supported values are: {supported_tables}")
    
    # Composite columns used for identifying unique records
    composite_cols = ['datetime_he'] + keys
    
    # End date: end of day 2 days prior to today (common for both cases)
    end_date = (datetime.now().date() - timedelta(days=2))
    
    # Determine start_date first (before querying) - either from input or default
    if start_date is None:
        # Use default date range based on table
        if table_name == 'ops.isone_hourly_ancillary':
            start_date = datetime(2025,3,1).date()
        elif table_name == 'ops.isone_hourly_energy':
            start_date = datetime(2016,5,11).date()
        print(f"Using default start_date: {start_date}")
    else:
        # User provided start_date, use it
        # Ensure start_date is a date object if it's a datetime
        if isinstance(start_date, datetime):
            start_date = start_date.date()
        elif not isinstance(start_date, date):
            # Try to convert if it's a string or other type
            start_date = pd.to_datetime(start_date).date()
        print(f"Using user-specified start_date: {start_date}")
    
    print(f"Checking for missing data from {start_date} to {end_date}")
    
    # Query existing data in database with date filter
    # Use the "date" column for filtering (more efficient than datetime_he)
    db_conn = flp_database_connector(username)
    
    # Format start_date as string for SQL query
    start_date_str = start_date.strftime('%Y-%m-%d')
    
    # Check if table exists before querying
    if not db_conn.table_exists(table_name, server="DataQuant01"):
        print(f"Table {table_name} does not exist in database. Treating as empty table.")
        # Create DataFrame with correct columns but 0 rows to prevent KeyError later
        # Columns needed: composite_cols + volume columns
        existing_data = pd.DataFrame(columns=composite_cols + ['da_volume', 'rt_volume'])
    else:
        # Query with date filter to only get data >= start_date
        sql_query = f"""
            SELECT *
            FROM {table_name}
            WHERE date >= '{start_date_str}'
        """
        existing_data = db_conn.read_from_db("DataQuant01", "", sql_query)
    
    # Process existing_data datetime - datetime_he is stored in 'America/New_York'
    # When pulled from database, it comes out as UTC + offset, so we need to handle timezone conversion
    if len(existing_data) > 0:
        # Parse datetime_he - it comes from DB as UTC+offset, but represents America/New_York time
        existing_data['datetime_he'] = pd.to_datetime(
            existing_data['datetime_he'], 
            utc=True
        ).dt.tz_convert(tz)
        # Normalize to second precision to handle microsecond differences between databases
        # Round to nearest second to match both 3:59:59.0 and 3:59:59.999... formats
        existing_data['datetime_he'] = existing_data['datetime_he'].dt.round('S')
    
    # Build unique_combos (same logic for both existing and new tables)
    if table_name == 'ops.isone_hourly_ancillary':
        asset_names = ['NORTHFIELD MOUNTAIN 1', 'NORTHFIELD MOUNTAIN 2', 'NORTHFIELD MOUNTAIN 3', 'NORTHFIELD MOUNTAIN 4',
            'NORTHFIELD MOUNTAIN PUMP 1', 'NORTHFIELD MOUNTAIN PUMP 2', 'NORTHFIELD MOUNTAIN PUMP 3', 'NORTHFIELD MOUNTAIN PUMP 4',
            'CABOT', 'ROCKY RIVER', 'ROCKY RIVER PUMP 1-2']
        ops_types = ['Pumping', 'Generation']
        if mis_report == 'SD_DAASCLEARED':
            services = ['TMNSR', 'TMSR', 'TMOR', 'EIR']
        elif mis_report == 'OI_UNITRTRSV':
            services = ['TMNSR', 'TMSR', 'TMOR']
        # Build dataframe
        unique_combos = pd.DataFrame(
            [
                {
                    'name': asset,
                    'ops_type': 'Pumping' if 'PUMP' in asset else 'Generation',
                    'service': service,
                }
                for asset, service in product(asset_names, services)
            ]
        )
    elif table_name == 'ops.isone_hourly_energy':
        asset_names = ['NORTHFIELD MOUNTAIN PUMP 1','NORTHFIELD MOUNTAIN PUMP 2','NORTHFIELD MOUNTAIN PUMP 3','NORTHFIELD MOUNTAIN PUMP 4',
                         'ROCKY RIVER PUMP 1-2','BULLS BRIDGE','FALLS VILLAGE','CABOT','TURNERSFALLS','NORTHFIELD MOUNTAIN 1','NORTHFIELD MOUNTAIN 2',
                         'NORTHFIELD MOUNTAIN 3','NORTHFIELD MOUNTAIN 4','ROCKY RIVER','SHEPAUG','STEVENSON','TUNNEL 10','NORTHFIELD SOLAR',
                         'ROBERTSVILLE','SCOTLAND_TAFTVILLE']
        unique_combos = pd.DataFrame(
            [
                {
                    'name': asset,
                    'ops_type': 'Pumping' if 'PUMP' in asset else 'Generation',
                    'service': 'Energy',
                }
                for asset in asset_names
            ]
        )
    
    # Create expected datetime range (hourly intervals) - same for both cases
    # Make start and end timezone-aware to properly handle DST transitions
    # Start at 1:00 AM on start_date (hour-ending 1)
    start_dt = pd.Timestamp(start_date, tz=tz) + timedelta(hours=1)
    # End at end of end_date (midnight of the next day, which is exclusive)
    end_dt = pd.Timestamp(end_date + timedelta(days=1), tz=tz)
    
    expected_datetimes = pd.date_range(
        start=start_dt,
        end=end_dt,
        freq='h',
        inclusive='right'
    )
    
    # Handle spring forward DST transition: convert hour-ending from XX:00:00 to XX:59:59
    # On spring forward day, the hour from 2 AM to 3 AM doesn't exist, so the database
    # stores the hour ending as 03:59:59 instead of 03:00:00
    adjusted_datetimes = []
    for i, dt in enumerate(expected_datetimes):
        # Check if this is the first hour after a gap (spring forward)
        if i > 0:
            prev_dt = expected_datetimes[i-1]
            
            # Check if the hour jumps by more than 1 (indicating spring forward gap)
            # For example: hour 1 -> hour 3 means hour 2 was skipped
            prev_hour = prev_dt.hour
            curr_hour = dt.hour
            
            # Also check if we're on the same date (spring forward happens within a day)
            same_date = prev_dt.date() == dt.date()
            
            # If hour jumps by more than 1 on the same date, it's spring forward
            if same_date and (curr_hour - prev_hour) > 1:
                # This is the hour after spring forward - convert to XX:59:59 format
                # Replace the time component: keep date and timezone, change time to XX:59:59
                adjusted_dt = dt.replace(minute=59, second=59, microsecond=0)
                adjusted_datetimes.append(adjusted_dt)
                continue
        adjusted_datetimes.append(dt)
    
    # Create complete expected dataset - same for both cases
    expected_records = []
    for _, combo in unique_combos.iterrows():
        for dt in adjusted_datetimes:
            record = {'datetime_he': dt}
            for k in keys:
                record[k] = combo[k]
            expected_records.append(record)
    
    expected_df = pd.DataFrame(expected_records)
    # Normalize to second precision to handle microsecond differences between databases
    # Round to nearest second to match both 3:59:59.0 and 3:59:59.999... formats
    expected_df['datetime_he'] = expected_df['datetime_he'].dt.round('S')
    expected_df = expected_df.sort_values(by=composite_cols)
    # print(f"Expected data:\n{expected_df.head()}")
    
    # Merge to find missing records - same logic for both cases
    # For 'ops.isone_hourly_ancillary', also check if the relevant column is blank
    if table_name == 'ops.isone_hourly_ancillary':
        # Determine which column to check based on mis_report
        if mis_report == 'SD_DAASCLEARED':
            volume_col = 'da_volume'
        elif mis_report == 'OI_UNITRTRSV':
            volume_col = 'rt_volume'
        else:
            raise ValueError(f"For table 'ops.isone_hourly_ancillary', expected MIS reports are 'SD_DAASCLEARED' or 'OI_UNITRTRSV' but instead was {mis_report}.")
        
        # Merge existing data with expected data, including the volume column
        # If existing_data is empty, existing_subset will be empty and merge will result in all NaN
        if len(existing_data) > 0:
            existing_subset = existing_data[composite_cols + [volume_col]].copy()
            existing_subset = existing_subset.sort_values(by=composite_cols)
        else:
            existing_subset = pd.DataFrame(columns=composite_cols + [volume_col])
        
        merged = expected_df.merge(
            existing_subset,
            on=composite_cols,
            how='left'
        )
        
        # A record is missing if:
        # 1. The record doesn't exist at all (volume_col is NaN), OR
        # 2. The record exists but the volume column is blank/null/empty string
        # NOTE: We do NOT treat 0 as blank here - legitimate zeros should not trigger API queries
        # If we get nonzero data from API that conflicts with existing 0, we'll handle that in deduplication
        def is_blank(val):
            if pd.isna(val):
                return True
            if isinstance(val, str):
                return val.strip() == ''
            # Do NOT treat numeric 0 as blank - legitimate zeros should not be re-queried
            return False
        
        merged['is_missing'] = merged[volume_col].apply(is_blank)
        
        missing_df = merged[merged['is_missing']].copy()
        missing_df = missing_df[composite_cols].copy()  # Keep only composite columns
        missing_df = missing_df.sort_values(by=composite_cols)
    else:
        # For other tables, use the original logic
        # If existing_data is empty, existing_subset will be empty and merge will result in all NaN
        if len(existing_data) > 0:
            existing_subset = existing_data[composite_cols].copy()
            existing_subset['exists'] = True
            existing_subset = existing_subset.sort_values(by=composite_cols)
        else:
            existing_subset = pd.DataFrame(columns=composite_cols + ['exists'])
        
        merged = expected_df.merge(
            existing_subset,
            on=composite_cols,
            how='left'
        )
        
        merged = merged.sort_values(by=composite_cols)
        missing_df = merged[merged['exists'].isna()].copy()
        missing_df = missing_df[composite_cols].copy()
        missing_df = missing_df.sort_values(by=composite_cols)
        
        # Debug: Check for spring forward date (3/9/25) issues
        spring_forward_date = date(2025, 3, 9)
        if len(missing_df) > 0:
            missing_df['date'] = missing_df['datetime_he'].dt.date
            spring_forward_missing = missing_df[missing_df['date'] == spring_forward_date]
            if len(spring_forward_missing) > 0:
                print(f"\nDEBUG: Found {len(spring_forward_missing)} missing records on spring forward date {spring_forward_date}")
                print(f"DEBUG: Unique missing datetime_he values on this date:")
                unique_dts = spring_forward_missing['datetime_he'].unique()
                for dt in sorted(unique_dts):
                    print(f"  {dt} (hour: {dt.hour})")
                # Check what exists in database for this date
                if len(existing_data) > 0:
                    existing_data['date'] = existing_data['datetime_he'].dt.date
                    spring_forward_existing = existing_data[existing_data['date'] == spring_forward_date]
                    if len(spring_forward_existing) > 0:
                        print(f"DEBUG: Found {len(spring_forward_existing)} existing records in DB for {spring_forward_date}")
                        print(f"DEBUG: Unique datetime_he values in DB for this date:")
                        existing_dts = spring_forward_existing['datetime_he'].unique()
                        for dt in sorted(existing_dts):
                            print(f"  {dt} (hour: {dt.hour})")
                    else:
                        print(f"DEBUG: No existing records in DB for {spring_forward_date}")
                # Check what's expected for this date
                expected_df['date'] = expected_df['datetime_he'].dt.date
                spring_forward_expected = expected_df[expected_df['date'] == spring_forward_date]
                if len(spring_forward_expected) > 0:
                    print(f"DEBUG: Expected {len(spring_forward_expected)} records for {spring_forward_date}")
                    print(f"DEBUG: Unique expected datetime_he values:")
                    expected_dts = spring_forward_expected['datetime_he'].unique()
                    for dt in sorted(expected_dts):
                        print(f"  {dt} (hour: {dt.hour})")
                missing_df = missing_df.drop(columns=['date'])
    
    # Check if there are any missing records to query (common for both table types)
    if len(missing_df) > 0:
        # Get unique missing dates
        missing_df['date'] = missing_df['datetime_he'].dt.date
        missing_dates = sorted(missing_df['date'].unique())
        
        print(f"Found {len(missing_df)} missing records across {len(missing_dates)} dates")
        print(f"Missing dates: {missing_dates[:10]}...")  # Show first 10
        
        # Group contiguous dates for efficient API queries
        def group_contiguous_dates(dates):
            """Group contiguous dates into ranges."""
            if not dates:
                return []
            
            groups = []
            current_group = [dates[0]]
            
            for i in range(1, len(dates)):
                if dates[i] == dates[i-1] + timedelta(days=1):
                    current_group.append(dates[i])
                else:
                    groups.append(current_group)
                    current_group = [dates[i]]
            
            groups.append(current_group)
            return groups
        
        date_groups = group_contiguous_dates(missing_dates)
        
        # For OI_UNITRTRSV, segment date groups into monthly chunks to avoid API timeouts
        # (this report has hourly data instead of daily, so it's much larger)
        if mis_report == 'OI_UNITRTRSV':
            def segment_into_monthly_chunks(date_group):
                """Split a date group into chunks that are at most 1 month long."""
                if not date_group:
                    return []
                
                chunks = []
                current_chunk = [date_group[0]]
                
                for i in range(1, len(date_group)):
                    current_date = date_group[i]
                    chunk_start = current_chunk[0]
                    
                    # Check if adding this date would exceed 1 month
                    # Calculate days difference
                    days_diff = (current_date - chunk_start).days
                    
                    # If adding this date would exceed ~30 days, start a new chunk
                    # Using 30 days as a safe limit (slightly less than 1 month to be safe)
                    if days_diff >= 30:
                        chunks.append(current_chunk)
                        current_chunk = [current_date]
                    else:
                        current_chunk.append(current_date)
                
                # Add the last chunk
                if current_chunk:
                    chunks.append(current_chunk)
                
                return chunks
            
            # Segment all date groups into monthly chunks
            segmented_groups = []
            for group in date_groups:
                monthly_chunks = segment_into_monthly_chunks(group)
                segmented_groups.extend(monthly_chunks)
            
            date_groups = segmented_groups
            print(f"Segmented into {len(date_groups)} monthly chunks (max 30 days each) for OI_UNITRTRSV")
        
        print(f"Grouped into {len(date_groups)} contiguous date ranges")
        
        # Query API for each date range
        all_raw_data = []
        
        for i, date_group in enumerate(date_groups):
            group_start = date_group[0].strftime('%Y-%m-%d')
            group_end = (date_group[-1] + timedelta(days=1)).strftime('%Y-%m-%d')  # API expects exclusive end
            if mis_report == 'OI_UNITRTRSV':
                most_recent_version = 'false' # For OI_UNITRTRSV, reports are stored hourly andthis flag appears to treat each hour as a duplicate, so it only returns HE 24
            else:
                most_recent_version = 'true'
            
            print(f"Querying API {i+1}/{len(date_groups)}: {group_start} to {group_end}")
            
            url = f"https://ams.pharos-ei.com/api/v2/isone/mis/downloads.csv?organization_key=ho-fl&settle_since={group_start}&settle_before={group_end}&most_recent_version={most_recent_version}&report_name={mis_report}"
            
            try:
                df_raw = query_ams_with_basic_auth(url, token)
                
                if len(df_raw) > 0:
                    all_raw_data.append(df_raw)
                    print(f"  Retrieved {len(df_raw)} rows")
                else:
                    print(f"  No data returned for this date range")
            
            except Exception as e:
                print(f"  Error querying API: {e}")
                continue
        
        # Combine all raw data
        if all_raw_data:
            print(f"\nCombining data from {len(all_raw_data)} API responses...")
            # df_raw_combined = pd.concat(all_raw_data, ignore_index=True)
            
            # Process the combined data
            print("Processing combined data...")
            # Load relevant paths
            project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
            data_folder = os.path.join(project_root, "data")
            mapping_path = os.path.join(data_folder,"maps","ISONE Location Mapping.csv")
            # Parse raw MIS data
            if table_name == 'ops.isone_hourly_ancillary':
                if mis_report == 'SD_DAASCLEARED':
                    df_final = process_daas_cleared_data(all_raw_data, mapping_path)
                elif mis_report == 'OI_UNITRTRSV':
                    df_final = process_rt_reserve_data(all_raw_data, mapping_path)
                else:
                    raise ValueError(f"For table 'ops.isone_hourly_ancillary', expected MIS reports are 'SD_DAASCLEARED' or 'OI_UNITRTRSV' but instead was {mis_report}.")
            elif table_name == 'ops.isone_hourly_energy':
                df_final = RealTimeOps(
                    all_raw_data,
                    summarize=True,
                    mapping_file=str(mapping_path)
                ).data
                df_final = prep_rtlocsum_for_quant_db(df_final)
            else:
                raise ValueError("This should have caused an error already on the first line of the program that checks table names.")
                
                # # Example: Filter data for specific assets
                # target_assets = ['Northfield Mountain',
                #                 'Rocky River',
                #                 'Bulls Bridge',
                #                 'Falls Village',
                #                 'Cabot',
                #                 'Turners Falls',
                #                 'Shepaug',
                #                 'Stevenson',
                #                 'Tunnel Hydro',
                #                 'Northfield Solar',
                #                 'Robertsville',
                #                 'Scotland-Taftville']
                # df_final = df_final[df_final['Asset'].isin(target_assets)]
                
                # # Example: Print summary statistics
                # print("\nSummary of filtered data:")
                # print(f"Number of records: {len(filtered_df)}")
                # print(f"Date range: {filtered_df['Flow Date'].min()} to {filtered_df['Flow Date'].max()}")
                # print("\nAssets included:")
                # print(filtered_df['Asset'].unique())
            
            print(f"Processed {len(df_final)} rows for upload.")

            # Deduplicate: Remove records that already exist in the database
            print("\nDeduplicating against existing database records...")
            
            # Ensure datetime_he is in the same timezone for comparison
            if 'datetime_he' in df_final.columns:
                df_final['datetime_he'] = pd.to_datetime(df_final['datetime_he']).dt.tz_convert(tz)
                # Normalize to second precision to handle microsecond differences between databases
                df_final['datetime_he'] = df_final['datetime_he'].dt.round('S')
            
            # Create a subset of df_final with composite columns for merging
            df_final_subset_for_dedup = df_final[composite_cols].copy()
            df_final_subset_for_dedup = df_final_subset_for_dedup.sort_values(by=composite_cols)
            
            if table_name == 'ops.isone_hourly_ancillary':
                # For ancillary table, check if record exists AND volume column is populated
                # IMPORTANT: We want to UPDATE records that exist with 0/blank values, not skip them
                if mis_report == 'SD_DAASCLEARED':
                    volume_col = 'da_volume'
                elif mis_report == 'OI_UNITRTRSV':
                    volume_col = 'rt_volume'
                else:
                    raise ValueError(f"For table 'ops.isone_hourly_ancillary', expected MIS reports are 'SD_DAASCLEARED' or 'OI_UNITRTRSV' but instead was {mis_report}.")
                
                # Helper function to check if value is blank
                # IMPORTANT: In deduplication, treat 0 as blank so we can overwrite existing 0s with new API data
                # This allows us to fix cases where a 0 was incorrectly stored but API now has nonzero data
                def is_blank(val):
                    if pd.isna(val):
                        return True
                    if isinstance(val, str):
                        return val.strip() == ''
                    # For numeric values, treat 0 as blank so we can update records that have 0
                    try:
                        if isinstance(val, (int, float)) and val == 0:
                            return True
                    except (TypeError, ValueError):
                        pass
                    return False
                
                # Get existing records with non-blank/non-zero volume values
                # Only skip records that already have non-zero values (to avoid overwriting good data)
                # Records with blank/0 values should be updated, so we don't include them here
                # Check if required columns exist and if DataFrame has data
                required_cols = composite_cols + [volume_col]
                if len(existing_data) == 0 or not all(col in existing_data.columns for col in required_cols):
                    # Empty or missing columns - treat as empty
                    existing_subset_dedup = pd.DataFrame(columns=required_cols)
                    existing_with_zero = pd.DataFrame(columns=composite_cols)
                else:
                    existing_subset_dedup = existing_data[required_cols].copy()
                    existing_subset_dedup = existing_subset_dedup.sort_values(by=composite_cols)
                    
                    # Identify records that exist with 0 values (for warning purposes)
                    if len(existing_subset_dedup) > 0:
                        existing_with_zero = existing_subset_dedup[
                            existing_subset_dedup[volume_col].apply(is_blank)
                        ][composite_cols].copy()
                    else:
                        existing_with_zero = pd.DataFrame(columns=composite_cols)
                    
                    # Filter to only records with non-blank/non-zero volume values
                    # This means records with 0 or blank values will be updated with new API data
                    if len(existing_subset_dedup) > 0:
                        existing_subset_dedup = existing_subset_dedup[
                            ~existing_subset_dedup[volume_col].apply(is_blank)
                        ]
                    existing_subset_dedup = existing_subset_dedup[composite_cols].copy()
                
                existing_subset_dedup['exists_in_db'] = True
                
                # Merge to find duplicates
                merged_dedup = df_final_subset_for_dedup.merge(
                    existing_subset_dedup,
                    on=composite_cols,
                    how='left'
                )
                
                # Keep only records that don't exist in DB with non-blank values
                # This means: keep records that are new OR that exist but have blank/0 values (so we can update them)
                df_final = df_final.merge(
                    merged_dedup[composite_cols + ['exists_in_db']],
                    on=composite_cols,
                    how='left'
                )
                df_final = df_final[df_final['exists_in_db'].isna()].copy()
                df_final = df_final.drop(columns=['exists_in_db'])
                
                # Check if we're overwriting any records that had 0 values with nonzero data
                if len(existing_with_zero) > 0 and len(df_final) > 0:
                    # Check which records in df_final correspond to existing records with 0
                    df_final_subset_check = df_final[composite_cols + [volume_col]].copy()
                    overwrite_check = df_final_subset_check.merge(
                        existing_with_zero,
                        on=composite_cols,
                        how='inner'
                    )
                    
                    # Check if any of these have nonzero values (overwriting 0)
                    def is_nonzero(val):
                        if pd.isna(val):
                            return False
                        if isinstance(val, str):
                            return val.strip() != ''
                        try:
                            if isinstance(val, (int, float)):
                                return val != 0
                        except (TypeError, ValueError):
                            pass
                        return False
                    
                    overwriting_zero = overwrite_check[
                        overwrite_check[volume_col].apply(is_nonzero)
                    ]
                    
                    if len(overwriting_zero) > 0:
                        warnings.warn(
                            f"WARNING: Found {len(overwriting_zero)} records where existing database value was 0, "
                            f"but API returned nonzero data. These will be updated.\n"
                            f"Sample records being overwritten:\n{overwriting_zero[composite_cols + [volume_col]].head(10).to_string()}",
                            UserWarning
                        )
                
            else:
                # For other tables, just check if record exists
                existing_subset_dedup = existing_data[composite_cols].copy()
                existing_subset_dedup['exists_in_db'] = True
                existing_subset_dedup = existing_subset_dedup.sort_values(by=composite_cols)
                
                # Merge to find duplicates
                merged_dedup = df_final_subset_for_dedup.merge(
                    existing_subset_dedup,
                    on=composite_cols,
                    how='left'
                )
                
                # Keep only records that don't exist in DB
                df_final = df_final.merge(
                    merged_dedup[composite_cols + ['exists_in_db']],
                    on=composite_cols,
                    how='left'
                )
                df_final = df_final[df_final['exists_in_db'].isna()].copy()
                df_final = df_final.drop(columns=['exists_in_db'])
            
            print(f"After deduplication: {len(df_final)} rows remaining for upload.")
            
            # Skip upload if no data to upload
            if len(df_final) == 0:
                print("No data to upload after deduplication. Skipping upload.")
            else:
                # Check for any remaining missing data and fill with 0s if fill_with_zeros is True
                if fill_with_zeros:
                    print("\nChecking for any remaining missing data to fill with zeros...")
                    
                    # Get the datetime range from API data (not just dates, to handle partial days)
                    if 'datetime_he' in df_final.columns:
                        # Find the minimum and maximum datetime_he with actual data
                        min_api_datetime = df_final['datetime_he'].min() if len(df_final) > 0 else None
                        max_api_datetime = df_final['datetime_he'].max() if len(df_final) > 0 else None
                        
                        if min_api_datetime is not None and max_api_datetime is not None:
                            print(f"Data range: {min_api_datetime} to {max_api_datetime}")
                            
                            # Filter expected_df to only include records between min and max datetimes (inclusive)
                            # This ensures we only fill gaps between legitimate data, not before or after
                            # This also handles partial days - if data starts at hour 10, we won't fill hours 1-9
                            expected_df_filtered = expected_df[
                                (expected_df['datetime_he'] >= min_api_datetime) & 
                                (expected_df['datetime_he'] <= max_api_datetime)
                            ].copy()
                        else:
                            expected_df_filtered = pd.DataFrame(columns=expected_df.columns)
                    else:
                        # Can't extract datetimes, so skip filling
                        expected_df_filtered = pd.DataFrame(columns=expected_df.columns)
                        min_api_datetime = None
                        max_api_datetime = None
                    
                    if len(expected_df_filtered) > 0:
                        df_final_subset = df_final[composite_cols].copy()
                        df_final_subset['exists'] = True
                        df_final_subset = df_final_subset.sort_values(by=composite_cols)
                        
                        merged_updated = expected_df_filtered.merge(
                            df_final_subset,
                            on=composite_cols,
                            how='left'
                        )
                        
                        still_missing_df = merged_updated[merged_updated['exists'].isna()].copy()
                        still_missing_df = still_missing_df[composite_cols].copy()
                        
                        if len(still_missing_df) > 0:
                            # Only fill gaps between min and max datetimes (already filtered above)
                            if 'datetime_he' in still_missing_df.columns:
                                still_missing_to_fill = still_missing_df[composite_cols].copy()
                                
                                if len(still_missing_to_fill) > 0:
                                    print(f"\nFilling {len(still_missing_to_fill)} missing combinations with default values (0) "
                                          f"for gaps between {min_api_datetime} and {max_api_datetime}...")
                                    
                                    # Fill in default values
                                    initial_fill_count = len(still_missing_to_fill)
                                    if table_name == 'ops.isone_hourly_ancillary':
                                        if mis_report == 'SD_DAASCLEARED':
                                            still_missing_to_fill['da_volume'] = 0
                                            still_missing_to_fill['rt_volume'] = ""
                                            still_missing_to_fill['unit'] = "MW"
                                            still_missing_to_fill['interval_width_s'] = 3600
                                            mapping = retrieve_isone_location_map(mapping_path)
                                            mapping = mapping[["ISO-NE Name", "FLP Asset Name"]]
                                            mapping.rename(columns={"ISO-NE Name":"name", "FLP Asset Name":"asset"}, inplace=True)
                                            # Remove duplicates from mapping to prevent merge from creating duplicates
                                            mapping = mapping.drop_duplicates(subset=['name'], keep='first')
                                            still_missing_to_fill = still_missing_to_fill.merge(mapping, how="left", on="name")
                                        elif mis_report == 'OI_UNITRTRSV':
                                            still_missing_to_fill['da_volume'] = ""
                                            still_missing_to_fill['rt_volume'] = 0
                                            still_missing_to_fill['unit'] = "MW"
                                            still_missing_to_fill['interval_width_s'] = 3600
                                            mapping = retrieve_isone_location_map(mapping_path)
                                            mapping = mapping[["ISO-NE Name", "FLP Asset Name"]]
                                            mapping.rename(columns={"ISO-NE Name":"name", "FLP Asset Name":"asset"}, inplace=True)
                                            # Remove duplicates from mapping to prevent merge from creating duplicates
                                            mapping = mapping.drop_duplicates(subset=['name'], keep='first')
                                            still_missing_to_fill = still_missing_to_fill.merge(mapping, how="left", on="name")
                                    elif table_name == 'ops.isone_hourly_energy':
                                        still_missing_to_fill['da_volume'] = 0
                                        still_missing_to_fill['rt_volume'] = 0
                                        still_missing_to_fill['unit'] = "MWh"
                                        still_missing_to_fill['interval_width_s'] = 3600
                                        mapping = retrieve_isone_location_map(mapping_path)
                                        mapping = mapping[["ISO-NE Name", "FLP Asset Name"]]
                                        mapping.rename(columns={"ISO-NE Name":"name", "FLP Asset Name":"asset"}, inplace=True)
                                        # Remove duplicates from mapping to prevent merge from creating duplicates
                                        mapping = mapping.drop_duplicates(subset=['name'], keep='first')
                                        still_missing_to_fill = still_missing_to_fill.merge(mapping, how="left", on="name")
                                    else:
                                        raise ValueError("This should have caused a table name error already in 2 other places.")
                                    
                                    # Check if merge created duplicates
                                    if len(still_missing_to_fill) > initial_fill_count:
                                        print(f"WARNING: Merge with mapping created duplicates! "
                                              f"Before merge: {initial_fill_count}, After merge: {len(still_missing_to_fill)}")
                                        # Deduplicate after merge
                                        still_missing_to_fill = still_missing_to_fill.drop_duplicates(subset=composite_cols, keep='first')
                                        if len(still_missing_to_fill) != initial_fill_count:
                                            print(f"After deduplication: {len(still_missing_to_fill)} records "
                                                  f"(expected {initial_fill_count})")
                                    
                                    # Check for duplicates in still_missing_to_fill before appending
                                    initial_fill_count = len(still_missing_to_fill)
                                    still_missing_to_fill = still_missing_to_fill.drop_duplicates(subset=composite_cols, keep='first')
                                    if len(still_missing_to_fill) < initial_fill_count:
                                        print(f"WARNING: Removed {initial_fill_count - len(still_missing_to_fill)} duplicate records from filled missing combos")
                                    
                                    # Append the default-filled records to df_final
                                    df_final = pd.concat([df_final, still_missing_to_fill], ignore_index=True)
                                    print(f"Added {len(still_missing_to_fill)} records with default values.")
                                else:
                                    print("No missing combinations found to fill.")
                            else:
                                # Can't extract datetimes, so warn but don't fill
                                warnings.warn(
                                    f"WARNING: Found {len(still_missing_df)} records that were expected but not returned by API.\n"
                                    f"Cannot determine datetime range, so these will NOT be filled.",
                                    UserWarning
                                )
                        else:
                            print("No missing records found between data range.")
                    else:
                        print("No expected records in data range to check for missing data.")
                else:
                    print("\nfill_with_zeros is False - skipping zero-filling step.")
            #     print(f"WARNING: Found {len(still_missing_df)} records that were expected but not returned by API.")
            #     print("These records will be filled with default values (0).")
            #     print("If these records should have non-zero values, this may indicate:")
            #     print("  - API data not yet available for these dates")
            #     print("  - API error or data quality issue")
            #     print("  - Records genuinely don't exist for this period")
            #     print("Filling with default values...")
                
            #     # Fill in default values
            #     if table_name == 'ops.isone_hourly_ancillary':
            #         if mis_report == 'SD_DAASCLEARED':
            #             still_missing_df['da_volume'] = 0
            #             still_missing_df['rt_volume'] = ""
            #             still_missing_df['unit'] = "MW"
            #             still_missing_df['interval_width_s'] = 3600
            #             mapping = retrieve_isone_location_map(mapping_path)
            #             mapping = mapping[["ISO-NE Name", "FLP Asset Name"]]
            #             mapping.rename(columns={"ISO-NE Name":"name", "FLP Asset Name":"asset"}, inplace=True)
            #             still_missing_df = still_missing_df.merge(mapping, how="left", on="name")
            #         elif mis_report == 'OI_UNITRTRSV':
            #             still_missing_df['da_volume'] = ""
            #             still_missing_df['rt_volume'] = 0
            #             still_missing_df['unit'] = "MW"
            #             still_missing_df['interval_width_s'] = 3600
            #             mapping = retrieve_isone_location_map(mapping_path)
            #             mapping = mapping[["ISO-NE Name", "FLP Asset Name"]]
            #             mapping.rename(columns={"ISO-NE Name":"name", "FLP Asset Name":"asset"}, inplace=True)
            #             still_missing_df = still_missing_df.merge(mapping, how="left", on="name")
            #     elif table_name == 'ops.isone_hourly_energy':
            #         still_missing_df['da_volume'] = 0
            #         still_missing_df['rt_volume'] = 0
            #         still_missing_df['unit'] = "MWh"
            #         still_missing_df['interval_width_s'] = 3600
            #         mapping = retrieve_isone_location_map(mapping_path)
            #         mapping = mapping[["ISO-NE Name", "FLP Asset Name"]]
            #         mapping.rename(columns={"ISO-NE Name":"name", "FLP Asset Name":"asset"}, inplace=True)
            #         still_missing_df = still_missing_df.merge(mapping, how="left", on="name")
            #     else:
            #         raise ValueError("This should have caused a table name error already in 2 other places.")

            #     # Append the default-filled records to df_final
            #     df_final = pd.concat([df_final, still_missing_df], ignore_index=True)

                # Remove temporary 'date' column if it was added
                if 'date' in df_final.columns:
                    df_final = df_final.drop(columns=['date'])

                # Final deduplication check before upload to prevent MERGE errors
                print("\nPerforming final deduplication check before upload...")
                initial_upload_count = len(df_final)
                df_final = df_final.drop_duplicates(subset=composite_cols, keep='first')
                if len(df_final) < initial_upload_count:
                    removed = initial_upload_count - len(df_final)
                    # Debug: Show which records were duplicates
                    print(f"DEBUG: Found {removed} duplicate records before upload.")
                    print(f"DEBUG: Original count: {initial_upload_count}, Final count: {len(df_final)}")
                    print(f"DEBUG: Checking for duplicate groups...")
                    dup_mask_before = df_final.duplicated(subset=composite_cols, keep=False)
                    if dup_mask_before.any():
                        print(f"DEBUG: Still found {dup_mask_before.sum()} duplicate rows after drop_duplicates!")
                        print(f"DEBUG: Sample duplicates:\n{df_final[dup_mask_before][composite_cols].head(10)}")
                    else:
                        print(f"DEBUG: No remaining duplicates found in df_final after drop_duplicates")
                    
                    # Raise error instead of warning - duplicates indicate a problem in processing
                    raise ValueError(
                        f"ERROR: Found and removed {removed} duplicate records before upload. "
                        f"This indicates duplicates were created during processing, which suggests a bug. "
                        f"Upload aborted to prevent incorrect data. "
                        f"Original count: {initial_upload_count}, Final count: {len(df_final)}. "
                        f"Please review the debug output above to identify the source of duplicates."
                    )
                else:
                    print(f"No duplicates found in final data (count: {len(df_final)})")

                # Upload to database
                print("Uploading to database...")
                if table_name == 'ops.isone_hourly_energy':
                    db_conn.upload_data_to_quant_db(
                        table_name=table_name,
                        df=df_final,
                        tz=tz,
                        mode="update",
                        update_columns=['da_volume','rt_volume'],
                        skip_prompt=True
                    )
                elif table_name == 'ops.isone_hourly_ancillary':
                    # For ancillary table, we need to update existing records or insert new ones
                    # Determine which column we're updating based on mis_report
                    if mis_report == 'SD_DAASCLEARED':
                        volume_col = 'da_volume'
                    elif mis_report == 'OI_UNITRTRSV':
                        volume_col = 'rt_volume'
                    else:
                        raise ValueError(f"For table 'ops.isone_hourly_ancillary', expected MIS reports are 'SD_DAASCLEARED' or 'OI_UNITRTRSV' but instead was {mis_report}.")
                    
                    # Use the update mode to update the volume column and insert new records
                    db_conn.upload_data_to_quant_db(
                        table_name=table_name,
                        df=df_final,
                        tz=tz,
                        mode="update",
                        update_columns=[volume_col],
                        skip_prompt=True
                    )
                else:
                    # For other tables, use original append method
                    db_conn.upload_data_to_quant_db(
                        table_name=table_name,
                        df=df_final,
                        tz=tz,
                        mode="append",
                        skip_prompt=True
                    )
                
                print("Upload complete!")
        else:
            print("No data retrieved from API")
    else:
        print("No missing data found!")