"""
Example script demonstrating how to use the RealTimeOps class to process and analyze RTLOCSUM data.
"""

import sys
from pathlib import Path
import pandas as pd
from os.path import join
import sys

# Add the src directory to the Python path so we can import the parsers module
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))
flp_db_tools_path = r"C:\Users\cbrooks\OneDrive - FIRSTLIGHTPOWER.COM\Documents\Python\flp_database_connection_tools"
database_helpers = join(flp_db_tools_path,"Helpers")
if database_helpers not in sys.path:
    sys.path.append(database_helpers)
from flp_database_connector import flp_database_connector

from src.parsers import RealTimeOps, prep_rtlocsum_for_quant_db

# Build the path to the CSV mapping file
mapping_path = project_root / "data" / "maps" / "ISONE Location Mapping.csv"

def main():

    # Example usage of RealTimeOps class
    # Replace 'path_to_your_rtlocsum_file.csv' with your actual file path
    rt_ops = RealTimeOps(
        dfs=[pd.read_csv(r"C:\Users\cbrooks\OneDrive - FIRSTLIGHTPOWER.COM\Documents\Python\settlement_parsing_tools\data\SR_RTLOCSUM.csv")],
        summarize=True,
        mapping_file=str(mapping_path)
    ).data
    
    # Example: Filter data for specific assets
    target_assets = ['Northfield Mountain',
                     'Rocky River',
                     'Bulls Bridge',
                     'Falls Village',
                     'Cabot',
                     'Turners Falls',
                     'Shepaug',
                     'Stevenson',
                     'Tunnel Hydro',
                     'Northfield Solar',
                     'Robertsville',
                     'Scotland-Taftville']
    filtered_df = rt_ops[rt_ops['Asset'].isin(target_assets)]
    
    # Example: Print summary statistics
    print("\nSummary of filtered data:")
    print(f"Number of records: {len(filtered_df)}")
    print(f"Date range: {filtered_df['Flow Date'].min()} to {filtered_df['Flow Date'].max()}")
    print("\nAssets included:")
    print(filtered_df['Asset'].unique())

    # Write data to database
    final_df = prep_rtlocsum_for_quant_db(filtered_df)

    # Write result to database
    db_conn = flp_database_connector(r"firstlightpower\cbrooks")
    db_conn.upload_data_to_quant_db(
            table_name="ops.isone_hourly_energy",
            df=final_df,
            tz="America/New_York",
            mode="append",
            skip_prompt=True
        )

if __name__ == "__main__":
    main() 