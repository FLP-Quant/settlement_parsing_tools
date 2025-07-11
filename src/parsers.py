import pandas as pd
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
    return pd.read_csv(mapping_file)


class RealTimeOps:
    """
    Representation of RTLOCSUM data from Pharos
    """

    def __init__(self, report_path: str, summarize=False, mapping_file: str = ISONE_LOCATION_MAPPING_PATH):
        """
        Parse RTLOCSUM data from ISONE MIS
        :param report_path:
        """

        # TODO: Add data validation for union of pathlike and string
        self.report_path = report_path
        self.mapping_file = mapping_file
        self.data = self._parse_rtlocsum_data()
        if summarize:
            self.data = self._summarize_ops()

    def _parse_rtlocsum_data(self):
        """
        Parse real-time locational summary data saved as a CSV exported from Pharos front-end

        :return: DataFrame of parsed MIS data
        :rtype: pd.DataFrame
        """

        # Import data from CSV. Use a named range to create space for jagged lines
        real_time = pd.read_csv(self.report_path, engine="python", names=range(40))

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