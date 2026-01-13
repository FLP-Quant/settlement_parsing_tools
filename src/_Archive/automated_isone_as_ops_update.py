import pandas as pd
import os
import sys
from datetime import datetime, timedelta

from pandas._libs.tslibs import tz_compare

# Add the src directory to the Python path
project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.process_daas_positions import process_daas_cleared_data
from src.pharos_ams_query import query_ams_with_basic_auth

# FLP database connection tools path
flp_db_tools_path = r"C:\Users\cbrooks\OneDrive - FIRSTLIGHTPOWER.COM\Documents\Python\flp_database_connection_tools"
database_helpers = os.path.join(flp_db_tools_path,"Helpers")
if database_helpers not in sys.path:
    sys.path.append(database_helpers)
from flp_database_connector import flp_database_connector

def automated_isone_as_ops_update(token):

    # Set parameters for database query & upload
    USERNAME = r"firstlightpower\cbrooks"
    db_table_name = "ops.isone_hourly_ancillary"
    tz = 'America/New_York'

    # Query existing data in database
    db_conn = flp_database_connector(USERNAME)
    sql_query = f"""
        SELECT *
        FROM {db_table_name}
    """
    existing_data = db_conn.read_from_db("DataQuant01", "", sql_query)

    # Determine date range to check
    if len(existing_data) > 0:

        existing_data['datetime_he'] = pd.to_datetime(
            existing_data['datetime_he'], 
            utc=True
        ).dt.tz_convert(tz)
        
        # Start date: earliest date in existing data
        start_date = existing_data['datetime_he'].min().date()
        
        # End date: end of day 2 days prior to today
        end_date = (datetime.now().date() - timedelta(days=2))
        
        print(f"Checking for missing data from {start_date} to {end_date}")
        
        # Create expected datetime range (hourly intervals)
        expected_datetimes = pd.date_range(
            start=start_date + timedelta(hours=1),
            end=end_date + timedelta(days=1),  # Include full last day
            freq='h',
            tz=tz,
            inclusive='right'
        )
        # print(f"Expected datetimes:\n{expected_datetimes}")
        
        # Get unique combinations of name, ops_type, and service from existing data
        unique_combos = existing_data[['name', 'ops_type', 'service']].drop_duplicates()
        
        # Create complete expected dataset
        expected_records = []
        for _, combo in unique_combos.iterrows():
            for dt in expected_datetimes:
                expected_records.append({
                    'datetime_he': dt,
                    'name': combo['name'],
                    'ops_type': combo['ops_type'],
                    'service': combo['service']
                })
        
        expected_df = pd.DataFrame(expected_records)
        expected_df = expected_df.sort_values(by=['datetime_he', 'name', 'ops_type', 'service'])
        # print(f"Expected data:\n{expected_df.head()}")
        
        # Merge to find missing records
        existing_subset = existing_data[['datetime_he', 'name', 'ops_type', 'service']].copy()
        existing_subset['exists'] = True
        existing_subset = existing_subset.sort_values(by=['datetime_he', 'name', 'ops_type', 'service'])
        # print(f"Existing data:\n{existing_subset.head()}")
        
        merged = expected_df.merge(
            existing_subset,
            on=['datetime_he', 'name', 'ops_type', 'service'],
            how='left'
        )

        # print(f"Merged data:\n{merged.head()}")
        merged = merged.sort_values(by=['datetime_he', 'name', 'ops_type', 'service'])
        missing_df = merged[merged['exists'].isna()].copy()
        missing_df = missing_df.sort_values(by=['datetime_he', 'name', 'ops_type', 'service'])
        # print(f"Missing data:\n{missing_df.head()}")
        
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
            
            print(f"Grouped into {len(date_groups)} contiguous date ranges")
            
            # Query API for each date range
            all_raw_data = []
            
            for i, date_group in enumerate(date_groups):
                group_start = date_group[0].strftime('%Y-%m-%d')
                group_end = (date_group[-1] + timedelta(days=1)).strftime('%Y-%m-%d')  # API expects exclusive end
                
                print(f"Querying API {i+1}/{len(date_groups)}: {group_start} to {group_end}")
                report = "SD_DAASCLEARED"
                url = f"https://ams.pharos-ei.com/api/v2/isone/mis/downloads.csv?organization_key=ho-fl&settle_since={group_start}&settle_before={group_end}&most_recent_version=TRUE&report_name={report}"
                
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
                df_raw_combined = pd.concat(all_raw_data, ignore_index=True)
                
                # Process the combined data
                print("Processing combined data...")
                # Load relevant paths
                project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
                data_folder = os.path.join(project_root, "data")
                mapping_path = os.path.join(data_folder,"maps","ISONE Location Mapping.csv")
                # For parsing raw MIS data
                df_final = process_daas_cleared_data(df_raw_combined, mapping_path)
                
                print(f"Processed {len(df_final)} rows for upload")

                # Check for any remaining missing data and fill with defaults
                print("\nChecking for any remaining missing data...")
                
                # Compare df_final against expected_df to find still-missing records
                df_final_subset = df_final[['datetime_he', 'name', 'ops_type', 'service']].copy()
                df_final_subset['exists'] = True
                df_final_subset = df_final_subset.sort_values(by=['datetime_he', 'name', 'ops_type', 'service'])
                
                merged_updated = expected_df.merge(
                    df_final_subset,
                    on=['datetime_he', 'name', 'ops_type', 'service'],
                    how='left'
                )
                
                still_missing_df = merged_updated[merged_updated['exists'].isna()].copy()
                still_missing_df = still_missing_df[['datetime_he', 'name', 'ops_type', 'service']]
                
                if len(still_missing_df) > 0:
                    print(f"Found {len(still_missing_df)} records still missing. Filling with default values...")
                    
                    # Fill in default values
                    still_missing_df['da_volume'] = 0
                    still_missing_df['rt_volume'] = ""
                    still_missing_df['unit'] = "MW"
                    still_missing_df['interval_width_s'] = 3600

                    # Append the default-filled records to df_final
                    df_final = pd.concat([df_final, still_missing_df], ignore_index=True)

                # Upload to database
                print("Uploading to database...")
                db_conn.upload_data_to_quant_db(
                    table_name=db_table_name,
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

    else:
        print("No existing data in database. Please load initial data first.")