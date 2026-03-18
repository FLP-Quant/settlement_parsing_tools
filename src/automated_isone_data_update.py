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
from src.pharos_ams_query import query_ams_with_basic_auth, query_schedule_offers_historic, process_schedule_offers_historic
from src.parsers import RealTimeOps, prep_rtlocsum_for_quant_db, retrieve_isone_location_map

# FLP database connection tools path
flp_db_tools_path = r"C:\Users\cbrooks\OneDrive - FIRSTLIGHTPOWER.COM\Documents\Python\flp_database_connection_tools"
database_helpers = os.path.join(flp_db_tools_path,"Helpers")
if database_helpers not in sys.path:
    sys.path.append(database_helpers)
from flp_database_connector import (
    flp_database_connector,
    find_missing_timeseries_records,
    filter_rows_for_upsert,
)
from src.isone_update_core import (
    build_expected_grid,
    debug_spring_forward_missing,
    fill_missing_with_defaults,
    group_contiguous_dates,
    chunk_date_groups,
    segment_into_monthly_chunks,
    safe_round_datetime,
)


def automated_isone_data_update(
    username,
    token,
    table_name,
    tz,
    mis_report=None,
    market=None,
    offers_ops_type_mode: str = "both",
    start_date=None,
    fill_with_zeros=False,
):
    """
    mis_report : used for ops.isone_hourly_ancillary (e.g. 'SD_DAASCLEARED', 'OI_UNITRTRSV').
    market : used for offers.flp_isone_energy: 'DA' (day-ahead) or 'RT' (real-time). Default 'DA'.
    """
    supported_tables = ['ops.isone_hourly_ancillary', 'ops.isone_hourly_energy', 'offers.flp_isone_energy']
    if table_name not in supported_tables:
        raise ValueError(f"Table name '{table_name}' not yet supported. supported values are: {supported_tables}")

    if table_name == 'offers.flp_isone_energy':
        if market is None:
            market = 'DA'
        if market not in ('DA', 'RT'):
            raise ValueError(f"market must be one of 'DA', 'RT', got {market!r}")
        if offers_ops_type_mode not in ("both", "Generation", "Pumping"):
            raise ValueError(
                f"offers_ops_type_mode must be one of 'both', 'Generation', 'Pumping', got {offers_ops_type_mode!r}"
            )
        keys = ['name', 'market_type', 'ops_type', 'service']
    else:
        if mis_report is None:
            raise ValueError(f"mis_report is required for table {table_name}")
        keys = ['name', 'ops_type', 'service']

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
        elif table_name == 'offers.flp_isone_energy':
            start_date = datetime(2025, 1, 1).date()
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
    table_exists = db_conn.table_exists(table_name, server="DataQuant01")
    if not table_exists:
        print(f"Table {table_name} does not exist in database. Treating as empty table.")
        existing_data = pd.DataFrame(columns=composite_cols + (['da_volume', 'rt_volume'] if table_name != 'offers.flp_isone_energy' else []))
    else:
        # Query with date filter to only get data >= start_date
        sql_query = f"""
            SELECT *
            FROM {table_name}
            WHERE date >= '{start_date_str}'
        """
        existing_data = db_conn.read_from_db("DataQuant01", "", sql_query)
    
    # Process existing_data datetime (all tables have datetime_he and datetime_hb)
    if len(existing_data) > 0:
        if 'datetime_he' in existing_data.columns:
            existing_data['datetime_he'] = pd.to_datetime(existing_data['datetime_he'], utc=True).dt.tz_convert(tz)
            existing_data['datetime_he'] = safe_round_datetime(existing_data['datetime_he'], tz)
        if 'datetime_hb' in existing_data.columns:
            existing_data['datetime_hb'] = pd.to_datetime(existing_data['datetime_hb'], utc=True).dt.tz_convert(tz)
            existing_data['datetime_hb'] = safe_round_datetime(existing_data['datetime_hb'], tz)

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
    elif table_name == 'offers.flp_isone_energy':
        # One placeholder row so expected grid = one row per hour; API returns all units (ops_type/service fixed in process)
        if offers_ops_type_mode == "both":
            ops_types = ["Generation", "Pumping"]
        else:
            ops_types = [offers_ops_type_mode]
        unique_combos = pd.DataFrame(
            [
                {"name": "*", "market_type": market, "ops_type": ot, "service": "energy"}
                for ot in ops_types
            ]
        )

    # Build expected (datetime_he, keys) grid; DST spring-forward handled in isone_update_core
    expected_df = build_expected_grid(
        start_date, end_date, tz, unique_combos, composite_cols
    )

    # Identify missing records (gap detection via database helpers)
    volume_col = None
    if table_name == 'ops.isone_hourly_ancillary':
        if mis_report == 'SD_DAASCLEARED':
            volume_col = 'da_volume'
        elif mis_report == 'OI_UNITRTRSV':
            volume_col = 'rt_volume'
        else:
            raise ValueError(f"For table 'ops.isone_hourly_ancillary', expected MIS reports are 'SD_DAASCLEARED' or 'OI_UNITRTRSV' but instead was {mis_report}.")

    missing_df = find_missing_timeseries_records(
        expected_df,
        existing_data,
        composite_cols,
        value_column=volume_col,
        treat_value_as_missing=None,  # default: NaN/empty = missing, 0 = not missing
        table_exists=table_exists,
        skip_high_missing_pct_check=(mis_report == 'OI_UNITRTRSV' or table_name == 'offers.flp_isone_energy'),
    )

    # Debug: optionally print spring-forward date missing vs existing vs expected
    spring_forward_date = date(2026, 3, 8)
    missing_df = debug_spring_forward_missing(missing_df, existing_data, expected_df, spring_forward_date)

    # Check if there are any missing records to query (common for both table types)
    if len(missing_df) > 0:
        # Get unique missing dates
        missing_df['date'] = missing_df['datetime_he'].dt.date
        missing_dates = sorted(missing_df['date'].unique())
        
        print(f"Found {len(missing_df)} missing records across {len(missing_dates)} dates")
        print(f"Missing dates: {missing_dates[:10]}...")  # Show first 10
        
        # Group contiguous dates for efficient API queries (from isone_update_core)
        date_groups = group_contiguous_dates(missing_dates)

        # For OI_UNITRTRSV, segment date groups into monthly chunks to avoid API timeouts
        if mis_report == 'OI_UNITRTRSV':
            date_groups = chunk_date_groups(date_groups, max_days=30)
            print(f"Segmented into {len(date_groups)} monthly chunks (max 30 days each) for OI_UNITRTRSV")
        # Schedule offers API: use smaller chunks to avoid timeouts
        elif table_name == 'offers.flp_isone_energy':
            date_groups = chunk_date_groups(date_groups, max_days=30)
            print(f"Segmented into {len(date_groups)} monthly chunks (max 30 days each) for schedule offers API")

        print(f"Grouped into {len(date_groups)} contiguous date ranges")
        
        # Query API for each date range
        all_raw_data = []
        
        for i, date_group in enumerate(date_groups):
            group_start = date_group[0].strftime('%Y-%m-%d')
            group_end = (date_group[-1] + timedelta(days=1)).strftime('%Y-%m-%d')  # MIS API expects exclusive end
            print(f"Querying API {i+1}/{len(date_groups)}: {group_start} to {group_end}")
            try:
                if table_name == 'offers.flp_isone_energy':
                    group_end_inclusive = date_group[-1].strftime('%Y-%m-%d')
                    ops_types_to_run = (
                        ["Generation", "Pumping"]
                        if offers_ops_type_mode == "both"
                        else [offers_ops_type_mode]
                    )
                    # API expects day_ahead/real_time/reoffer; we use DA/RT/reoffer in app
                    api_market = {"DA": "day_ahead", "RT": "real_time"}.get(market, market)
                    offer_parts = []
                    for ot in ops_types_to_run:
                        df_part = query_schedule_offers_historic(
                            token,
                            organization_key="ho-fl",
                            start_date=group_start,
                            market=api_market,
                            end_date=group_end_inclusive,
                            ops_type=ot,
                            timeout=120,
                        )
                        if len(df_part) > 0:
                            offer_parts.append(df_part)
                    df_raw = pd.concat(offer_parts, ignore_index=True) if offer_parts else pd.DataFrame()
                else:
                    if mis_report == 'OI_UNITRTRSV':
                        most_recent_version = 'false'
                    else:
                        most_recent_version = 'true'
                    url = f"https://ams.pharos-ei.com/api/v2/isone/mis/downloads.csv?organization_key=ho-fl&settle_since={group_start}&settle_before={group_end}&most_recent_version={most_recent_version}&report_name={mis_report}"
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
            print(f"\nProcessing combined data from {len(all_raw_data)} API responses...")
            
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
            elif table_name == 'offers.flp_isone_energy':
                df_raw = pd.concat(all_raw_data, ignore_index=True)
                df_final = process_schedule_offers_historic(df_raw, mapping_path, tz=tz)
                df_final['datetime_he'] = df_final['datetime_hb'] + pd.Timedelta(hours=1)
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

            # Deduplicate: keep only rows to insert/update (via filter_rows_for_upsert in flp_database_connector)
            print("\nDeduplicating against existing database records...")
            if 'datetime_he' in df_final.columns:
                df_final['datetime_he'] = pd.to_datetime(df_final['datetime_he']).dt.tz_convert(tz)
                df_final['datetime_he'] = safe_round_datetime(df_final['datetime_he'], tz)

            # Skip only when existing value is "good" (non-blank, non-zero); keep for update when blank/0
            def _treat_existing_as_skip(val):
                if pd.isna(val):
                    return False
                if isinstance(val, str) and val.strip() == '':
                    return False
                if isinstance(val, (int, float)) and val == 0:
                    return False
                return True

            if table_name == 'ops.isone_hourly_ancillary':
                _vol = 'da_volume' if mis_report == 'SD_DAASCLEARED' else 'rt_volume'
                df_to_upsert, existing_with_blank = filter_rows_for_upsert(
                    df_final, existing_data, composite_cols,
                    value_column=_vol, treat_existing_as_skip=_treat_existing_as_skip,
                )
            elif table_name == 'ops.isone_hourly_energy':
                _vol = 'rt_volume'  # value column used for overwrite logic (energy table has da_volume + rt_volume)
                df_to_upsert, existing_with_blank = filter_rows_for_upsert(
                    df_final, existing_data, composite_cols,
                    value_column=_vol, treat_existing_as_skip=_treat_existing_as_skip,
                )
            else:
                df_to_upsert, existing_with_blank = filter_rows_for_upsert(
                    df_final, existing_data, composite_cols,
                )
            df_final = df_to_upsert

            # Warn when overwriting existing 0/blank with nonzero data
            if table_name in ('ops.isone_hourly_ancillary', 'ops.isone_hourly_energy') and len(existing_with_blank) > 0 and len(df_final) > 0:
                if table_name == 'ops.isone_hourly_ancillary':
                    _vol = 'da_volume' if mis_report == 'SD_DAASCLEARED' else 'rt_volume'
                else:
                    _vol = 'rt_volume'
                overwrite_check = df_final[composite_cols + [_vol]].merge(
                    existing_with_blank, on=composite_cols, how='inner'
                )
                def _is_nonzero(val):
                    if pd.isna(val):
                        return False
                    if isinstance(val, str):
                        return val.strip() != ''
                    try:
                        return isinstance(val, (int, float)) and val != 0
                    except (TypeError, ValueError):
                        return False
                overwriting_zero = overwrite_check[overwrite_check[_vol].apply(_is_nonzero)]
                if len(overwriting_zero) > 0:
                    warnings.warn(
                        f"WARNING: Found {len(overwriting_zero)} records where existing database value was 0, "
                        f"but API returned nonzero data. These will be updated.\n"
                        f"Sample records being overwritten:\n{overwriting_zero[composite_cols + [_vol]].head(10).to_string()}",
                        UserWarning
                    )

            print(f"After deduplication: {len(df_final)} rows remaining for upload.")
            
            # Skip upload if no data to upload
            if len(df_final) == 0:
                print("No data to upload after deduplication. Skipping upload.")
            else:
                # # Debug: for offers table, compare new vs existing columns before upload
                # if table_name == 'offers.flp_isone_energy' and table_exists:
                #     new_cols = set(df_final.columns)
                #     existing_cols = set(existing_data.columns)
                #     only_in_new = sorted(new_cols - existing_cols)
                #     only_in_existing = sorted(existing_cols - new_cols)
                #     print(f"\nDEBUG (offers.flp_isone_energy):")
                #     print(f"  Columns only in new data: {only_in_new}")
                #     print(f"  Columns only in existing table: {only_in_existing}")

                if fill_with_zeros and table_name != 'offers.flp_isone_energy':
                    mapping = retrieve_isone_location_map(mapping_path)
                    mapping_df = mapping[["ISO-NE Name", "FLP Asset Name"]].rename(
                        columns={"ISO-NE Name": "name", "FLP Asset Name": "asset"}
                    ).drop_duplicates(subset=["name"], keep="first")
                    df_final = fill_missing_with_defaults(
                        df_final, expected_df, composite_cols, table_name, mis_report, mapping_df
                    )
                elif not fill_with_zeros:
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

                # Ensure da_volume and rt_volume are numeric before upload
                # Convert empty strings and other non-numeric values to NaN, then coerce to numeric type
                print("Converting volume columns to numeric types...")
                if 'da_volume' in df_final.columns:
                    # Replace empty strings and None with NaN, then convert to numeric
                    df_final['da_volume'] = df_final['da_volume'].replace(['', None], pd.NA)
                    df_final['da_volume'] = pd.to_numeric(df_final['da_volume'], errors='coerce')
                    # Ensure the dtype is numeric (float64 to handle NaN values)
                    df_final['da_volume'] = df_final['da_volume'].astype('float64')
                if 'rt_volume' in df_final.columns:
                    # Replace empty strings and None with NaN, then convert to numeric
                    df_final['rt_volume'] = df_final['rt_volume'].replace(['', None], pd.NA)
                    df_final['rt_volume'] = pd.to_numeric(df_final['rt_volume'], errors='coerce')
                    # Ensure the dtype is numeric (float64 to handle NaN values)
                    df_final['rt_volume'] = df_final['rt_volume'].astype('float64')

                # Upload to database (create table if it doesn't exist, else update/append)
                print("Uploading to database...")
                if not table_exists:
                    db_conn.upload_data_to_quant_db(
                        table_name=table_name,
                        df=df_final,
                        tz=tz,
                        mode="create",
                        skip_prompt=True,
                    )
                elif table_name == 'ops.isone_hourly_energy':
                    db_conn.upload_data_to_quant_db(
                        table_name=table_name,
                        df=df_final,
                        tz=tz,
                        mode="update",
                        update_columns=['da_volume','rt_volume'],
                        skip_prompt=True
                    )
                elif table_name == 'ops.isone_hourly_ancillary':
                    if mis_report == 'SD_DAASCLEARED':
                        volume_col = 'da_volume'
                    elif mis_report == 'OI_UNITRTRSV':
                        volume_col = 'rt_volume'
                    else:
                        raise ValueError(f"For table 'ops.isone_hourly_ancillary', expected MIS reports are 'SD_DAASCLEARED' or 'OI_UNITRTRSV' but instead was {mis_report}.")
                    db_conn.upload_data_to_quant_db(
                        table_name=table_name,
                        df=df_final,
                        tz=tz,
                        mode="update",
                        update_columns=[volume_col],
                        skip_prompt=True
                    )
                else:
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