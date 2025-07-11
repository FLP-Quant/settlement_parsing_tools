import pandas as pd
import numpy as np
import os

# Get Excel file from MIS "SD DAASCLEARED" files downloaded from here & merged into single CSV, then save as XLSX:
#      https://ams.pharos-ei.com/org/ho-fl/isone/ftp_downloads

# Read the Excel file, skipping first 4 rows
folder = r'C:\Users\cbrooks\OneDrive - FIRSTLIGHTPOWER.COM\Documents\Python\settlement_parsing_tools'
df = pd.read_excel(os.path.join(folder, '20250701_DAAS_Positions_raw_export.xlsx'), skiprows=4)

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
df.columns = ['A', 'B', 'Date', 'D', 'Data_vs_Header_Code', 'Hour_Ending', 'G', 
              'Asset_Name', 'Asset_Type', 'DA_TMSR_Obligation', 'DA_TMNSR_Obligation',
              'DA_TMOR_Obligation', 'DA_EIR_Obligation']

# Filter for code 'D' in the Data vs. Header Code column
df = df[df['Data_vs_Header_Code'] == 'D']

# Keep only the specified columns
columns_to_keep = ['Date', 'Hour_Ending', 'Asset_Name', 'Asset_Type',
                  'DA_TMSR_Obligation', 'DA_TMNSR_Obligation',
                  'DA_TMOR_Obligation', 'DA_EIR_Obligation']
df = df[columns_to_keep]

# # Store the units for reference
# obligation_units = {
#     'DA_TMSR_Obligation': units['DA_TMSR_Obligation'],
#     'DA_TMNSR_Obligation': units['DA_TMNSR_Obligation'],
#     'DA_TMOR_Obligation': units['DA_TMOR_Obligation'],
#     'DA_EIR_Obligation': units['DA_EIR_Obligation']
# }

# Print some basic information about the processed data
print(f"Processed data shape: {df.shape}")
# print("\nUnits for obligation columns:")
# for col, unit in obligation_units.items():
#     print(f"{col}: {unit}")

# Save the processed data to a new Excel file
output_file = 'processed_daas_positions.xlsx'
df.to_excel(os.path.join(folder,output_file), index=False)
print(f"\nProcessed data saved to {output_file}")
