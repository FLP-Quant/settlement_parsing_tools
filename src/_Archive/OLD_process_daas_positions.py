import pandas as pd
import numpy as np
import os
import sys
import warnings

# FLP database connection tools path
flp_db_tools_path = r"C:\Users\cbrooks\OneDrive - FIRSTLIGHTPOWER.COM\Documents\Python\flp_database_connection_tools"
database_helpers = os.path.join(flp_db_tools_path,"Helpers")
if database_helpers not in sys.path:
    sys.path.append(database_helpers)
from flp_database_connector import flp_database_connector

# Add the src directory to the Python path so we can import the parsers module
project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.parsers import retrieve_isone_location_map

## Inputs
USERNAME = r"firstlightpower\cbrooks"
db_table_name = "ops.isone_hourly_ancillary"
# Get Excel file from MIS "SD DAASCLEARED" files downloaded from here & merged into single CSV:
#      https://ams.pharos-ei.com/org/ho-fl/isone/ftp_downloads

## Parse downloaded file
# Load relevant paths
data_folder = os.path.join(project_root, "data")
output_folder = os.path.join(project_root, "outputs")
mapping_path = os.path.join(data_folder,"maps","ISONE Location Mapping.csv")

# Read the Excel file, skipping first 4 rows
df = pd.read_csv(os.path.join(data_folder, 'SD_DAASCLEARED.csv'), skiprows=4)

# Get the headers and units from rows 5-6
headers = df.iloc[0]  # Row 5 (0-based index)
# units = df.iloc[1]    # Row 6 (0-based index)

# # Convert spaces to underscores in units for obligation columns only
# obligation_cols = ['DA TMSR Obligation', 'DA TMNSR Obligation', 'DA TMOR Obligation', 'DA EIR Obligation']
# for col in obligation_cols:
#     if pd.notna(units[col]):  # Check if the unit exists
#         units[col] = str(units[col]).replace(' ', '_')

# Drop the first two rows (headers and units) from the main dataframe
df = df.iloc[2:].reset_index(drop=True)

# Rename columns to match the Excel structure
df.columns = ['A', 'B', 'Date', 'Version', 'Data_vs_Header_Code', 'Hour_Ending', 'G', 
              'Asset_Name', 'Asset_Type', 'DA_TMSR_Obligation', 'DA_TMNSR_Obligation',
              'DA_TMOR_Obligation', 'DA_EIR_Obligation']

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
    # Keep the first (i.e., latest Version due to sorting), but track duplicates where Version values are equal
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
    print(f"{deleted_rows} duplicate rows removed based on most recent Version within Date, Hour_Ending, Asset_Name, and Asset_Type.")
    return df_deduped

df = filter_duplicate_reports(df)

# Keep only the specified columns
columns_to_keep = ['Date', 'Hour_Ending', 'Asset_Name', 'Asset_Type',
                  'DA_TMSR_Obligation', 'DA_TMNSR_Obligation',
                  'DA_TMOR_Obligation', 'DA_EIR_Obligation']
df = df[columns_to_keep]
df["Hour_Ending"] = pd.to_numeric(df["Hour_Ending"], errors="raise")
df.sort_values(by=["Date", "Hour_Ending", "Asset_Name"], inplace=True)

# # Store the units for reference
# obligation_units = {
#     'DA_TMSR_Obligation': units['DA_TMSR_Obligation'],
#     'DA_TMNSR_Obligation': units['DA_TMNSR_Obligation'],
#     'DA_TMOR_Obligation': units['DA_TMOR_Obligation'],
#     'DA_EIR_Obligation': units['DA_EIR_Obligation']
# }

# Save the processed data to a new Excel file
output_file = 'processed_daas_positions.xlsx'
df.to_excel(os.path.join(output_folder,output_file), index=False)
print(f"\nProcessed data saved to {output_file}")

## Write to datab

# First need to reshape the data to the proper format

# Mapping for asset names
mapping = retrieve_isone_location_map(mapping_path)
mapping = mapping[["ISO-NE Name", "FLP Asset Name", "Operation Type"]]

# Convert hour-ending integer to time delta
def compute_datetime_he(date, hour_ending):
    return pd.to_datetime(date) + pd.to_timedelta(hour_ending, unit='h')

# Melt (unpivot) the obligation columns
value_vars = ["DA_TMSR_Obligation", "DA_TMNSR_Obligation", "DA_TMOR_Obligation", "DA_EIR_Obligation"]
df_melted = df.melt(id_vars=["Date", "Hour_Ending", "Asset_Name", "Asset_Type"],
                    value_vars=value_vars,
                    var_name="service_raw",
                    value_name="da_volume")

# Clean up and transform columns
df_melted["service"] = df_melted["service_raw"].str.replace("DA_", "").str.replace("_Obligation", "")
df_melted["datetime_he"] = df_melted.apply(lambda row: compute_datetime_he(row["Date"], row["Hour_Ending"]), axis=1)
df_melted["rt_volume"] = ""  # leave blank
df_melted["unit"] = "MW"
df_melted["interval_width_s"] = 3600

# Add values from mapping & rename
df_melted = df_melted.merge(
                            mapping, how="left", left_on="Asset_Name", right_on="ISO-NE Name"
                           )
df_melted.rename(columns={"Asset_Name":"name","FLP Asset Name":"asset","Operation Type":"ops_type"},inplace=True)

# Final column order
final_columns = ["datetime_he", "asset", "name", "ops_type", "service",
                 "da_volume", "rt_volume", "unit", "interval_width_s"]
# print(df_melted.columns)
df_final = df_melted[final_columns]


# Sort the result for readability
df_final.sort_values(by=["datetime_he", "service"], inplace=True)
df_final.to_excel("test.xlsx")

# Write result to database
db_conn = flp_database_connector(USERNAME)
db_conn.upload_data_to_quant_db(
        table_name=db_table_name,
        df=df_final,
        tz='America/New_York',
        mode="append",
        skip_prompt=True
    )