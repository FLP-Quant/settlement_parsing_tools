"""
Example script demonstrating how to use the RealTimeOps class to process and analyze RTLOCSUM data.
"""

import sys
from pathlib import Path

# Add the src directory to the Python path so we can import the parsers module
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.parsers import RealTimeOps

# Build the path to the CSV mapping file
mapping_path = project_root / "data" / "maps" / "ISONE Location Mapping.csv"

def main():
    # Example usage of RealTimeOps class
    # Replace 'path_to_your_rtlocsum_file.csv' with your actual file path
    rt_ops = RealTimeOps(
        r"C:\Users\cbrooks\OneDrive - FIRSTLIGHTPOWER.COM\Documents\Python\rqmd_utilities\data\SR_RTLOCSUM.csv",
        summarize=True,
        mapping_file=str(mapping_path)
    ).data
    
    # Example: Filter data for specific assets
    target_assets = ['Cabot', 'Turners Falls']
    filtered_df = rt_ops[rt_ops['Asset'].isin(target_assets)]
    
    # Example: Export filtered data to clipboard for Excel
    filtered_df.to_clipboard(excel=True, index=False)
    
    # Example: Print summary statistics
    print("\nSummary of filtered data:")
    print(f"Number of records: {len(filtered_df)}")
    print(f"Date range: {filtered_df['Flow Date'].min()} to {filtered_df['Flow Date'].max()}")
    print("\nAssets included:")
    print(filtered_df['Asset'].unique())

if __name__ == "__main__":
    main() 