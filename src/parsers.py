import pandas as pd
import warnings
from os.path import isfile

# Constants
ISONE_LOCATION_MAPPING_PATH = "../data/maps/ISONE Location Mapping.csv"

def retrieve_isone_location_map(mapping_file: str) -> pd.DataFrame:
    """
    Retrieve data for ISONE location mapping to asset name from CSV
    :return: DataFrame of mapping data
    :rtype: pd.DataFrame
    """
    if not isfile(mapping_file):
        print(f"Mapping file not found at {mapping_file}")
        raise FileNotFoundError("Mapping file not found.")
    return pd.read_csv(mapping_file, encoding = "ISO-8859-1")

def filter_duplicate_rows(df):
    # Count rows before deduplication
    initial_row_count = len(df)

    # Identify duplicates
    dup_cols = ['datetime_he', 'asset', 'name', 'ops_type', 'service']
    duplicates_mask = df.duplicated(subset=dup_cols, keep=False)

    # Check if there are any duplicates
    if duplicates_mask.any():
        # Get all duplicate rows
        duplicate_rows = df[duplicates_mask]
        n_duplicate_rows = len(duplicate_rows)
        
        # Count actual duplicate groups
        n_groups = len(df[duplicates_mask].groupby(dup_cols))
        
        # Get examples for warning (limit to 20 rows)
        n_examples = min(20, n_duplicate_rows)
        example_rows = []
        
        for i in range(n_examples):
            row_dict = duplicate_rows.iloc[i].to_dict()
            example_rows.append(row_dict)
        
        warnings.warn(
            f"{n_groups} duplicate groups found ({n_duplicate_rows} total duplicate rows). "
            f"The first occurrence will be kept.\n"
            f"Example duplicate rows:\n{example_rows}"
        )
    
        # Actually remove duplicates (keep first occurrence)
        df_filtered = df.drop_duplicates(subset=dup_cols, keep='first')
        
        print(f"Removed {initial_row_count - len(df_filtered)} duplicate rows")
    else:
        df_filtered = df
        print("No duplicate rows found.")
    
    return df_filtered

def prep_rtlocsum_for_quant_db(df: pd.DataFrame):
    
    # Convert the new data to the proper format
    # Convert hour-ending integer to time delta
    def compute_datetime_he(df):
        # Step 1: Build Hour Beginning (HB) as naive datetimes
        df = df.copy()
        df['hb_naive'] = pd.to_datetime(df['Flow Date']) + pd.to_timedelta(df['HE'] - 1, unit="h")

        # Step 2: Identify ambiguous (fall back) duplicates per unique asset-timestep combo
        # 'duplicated' returns True for everything *after* the first occurrence
        # We want a boolean mask that says:
        #   True  → first occurrence (EDT)
        #   False → second occurrence (EST)
        ambiguous_mask = ~df.duplicated(
            subset=['hb_naive', 'asset', 'name', 'ops_type', 'service'],
            keep="first"
        )

        # Step 3: Localize HB to Eastern Time
        df['hb'] = df['hb_naive'].dt.tz_localize(
            "America/New_York",
            ambiguous=ambiguous_mask,
            nonexistent="shift_backward"  # handles spring-forward gaps
        )

        # Step 4: Convert to Hour Ending (HE = HB + 1 hour)
        df['datetime_he'] = df['hb'] + pd.Timedelta(hours=1)

        return df
    
    df.rename(columns={"Asset":"asset","Name":"name","Ops Type":"ops_type","DA Dispatch":"da_volume","RT Dispatch":"rt_volume"},inplace=True)

    # Clean up and transform columns
    df["service"] = "Energy"
    df = compute_datetime_he(df)
    df["unit"] = "MWh"
    df["interval_width_s"] = 3600

    # Final column order
    final_columns = ["datetime_he", "asset", "name", "ops_type", "service",
                    "da_volume", "rt_volume", "unit", "interval_width_s"]

    df = df[final_columns]

    # # Add date and hour ending columns
    # df['date'] = (
    #     df['datetime_he']
    #     .dt.tz_localize(None)       # drop timezone info
    #     - pd.Timedelta(hours=1)     # convert HE to HB
    # ).dt.date                       # keep only the date
    # df['he'] = df['datetime_he'].dt.tz_localize(None).dt.hour.replace(0, 24)

    # Sort the result for readability
    df.sort_values(by=["datetime_he", "service", "asset"], inplace=True)

    # Check for duplicate rows
    df = filter_duplicate_rows(df)

    return df

class RealTimeOps:
    """
    Representation of RTLOCSUM data from Pharos
    """

    def __init__(self, dfs, summarize=False, mapping_file: str = ISONE_LOCATION_MAPPING_PATH):
        """
        Parse RTLOCSUM data from ISONE MIS
        :param dfs: list of DataFrames
        :param summarize: bool, whether to summarize the data
        :param mapping_file: str, path to the mapping file
        :return: None
        """

        # TODO: Add data validation for union of pathlike and string
        self.mapping_file = mapping_file
        self.dfs = dfs
        self.data = self._parse_rtlocsum_data()
        if summarize:
            self.data = self._summarize_ops()

    def _parse_rtlocsum_data(self):
        """
        Parse real-time locational summary data saved as a CSV exported from Pharos front-end

        :return: DataFrame of parsed MIS data
        :rtype: pd.DataFrame
        """

        dfs_clean = []
        for df in self.dfs:
            df.columns = range(40)
            dfs_clean.append(df)
        real_time = pd.concat(dfs_clean,ignore_index=True)
        # # Import data from CSV. Use a named range to create space for jagged lines
        # real_time = pd.read_csv(self.report_path, engine="python", names=range(40))

        # Create column names
        real_time.loc[4, 0] = "Org Name"
        real_time.loc[4, 1] = "Report Name"
        real_time.loc[4, 2] = "Flow Date"
        real_time.loc[4, 3] = "Report Date"
        real_time.loc[4, 4] = "Data Type"
        real_time.columns = real_time.iloc[4]
        # print(real_time.columns)

        # Filter to only retain data
        real_time = real_time[real_time["Data Type"] == "D"]

        # Drop superfluous columns that are all null from jagged column parsing
        real_time.dropna(axis="columns", how="all", inplace=True)

        # Drop columns that are superfluous due to lack of use
        real_time.drop(
            columns=["Org Name", "Report Date", "Report Name", "Location Type", "Data Type", None],
            errors="ignore",
            inplace=True,
        )

        # Correct data types
        real_time["Flow Date"] = pd.to_datetime(real_time["Flow Date"])
        real_time["Trading Interval"] = real_time["Trading Interval"].replace(
            "02X", "02"
        )  # For daylight savings
        real_time["Trading Interval"] = real_time["Trading Interval"].astype(int)
        real_time["Location ID"] = real_time["Location ID"].astype(int)
        for col in [
            col
            for col in list(real_time)
            if col
            not in ["Flow Date", "Trading Interval", "Location ID", "Location Name"]
        ]:
            real_time[col] = pd.to_numeric(real_time[col])
        # Add ISONE location to operational mapping
        mapping = retrieve_isone_location_map(self.mapping_file)
        real_time = real_time.merge(
            mapping, how="left", left_on="Location Name", right_on="Location"
        )
        real_time.drop(columns=["Location"], inplace=True)

        return real_time

    def _summarize_ops(self):
        """
        Summarize operations as positions of DA, RT, and RT LMP
        :return: DataFrame of summarized operations data
        :rtype: pd.DataFrame
        """

        # Remove columns that aren't relevant to summarized operations to simplify end-user interactions
        rt_ops = self.data.copy()
        retain = {
            "FLP Asset Name": "Asset",
            "ISO-NE Name": "Name",
            "Operation Type": "Ops Type",
            "Flow Date": "Flow Date",
            "Trading Interval": "HE",
            "Location ID": "Location ID",
            "Location Name": "Location Name",
            "Real Time Adjusted Net Interchange": "RT Dispatch",
            "Adjusted Net Interchange Deviation": "Dispatch Deviation",
            "Real Time Energy Component": "RT Energy",
            "Real Time Congestion Component": "RT Congestion",
            "Real Time Marginal Loss Component": "RT Loss",
        }
        rt_ops = rt_ops[retain.keys()]
        rt_ops.rename(columns=retain, inplace=True)

        # Calculate LMPs and DA dispatch from individual components
        rt_ops["RT LMP"] = (
            rt_ops["RT Energy"] + rt_ops["RT Congestion"] + rt_ops["RT Loss"]
        )
        rt_ops["DA Dispatch"] = rt_ops["RT Dispatch"] - rt_ops["Dispatch Deviation"]
        rt_ops.drop(
            columns=[
                "RT Energy",
                "RT Congestion",
                "RT Loss",
                "Dispatch Deviation",
                "Location Name",
            ],
            inplace=True,
        )

        # Drop data for non-asset locations reported in settlement file and summarize data
        rt_ops.dropna(subset=["Asset"], inplace=True)

        return rt_ops


if __name__ == "__main__":
    pass