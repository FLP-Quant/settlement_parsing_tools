import pandas as pd
import pyodbc
from os.path import isfile, join
import sys
flp_db_tools_path = r"C:\Users\cbrooks\OneDrive - FIRSTLIGHTPOWER.COM\Documents\Python\flp_database_connection_tools"
database_helpers = join(flp_db_tools_path,"Helpers")
if database_helpers not in sys.path:
    sys.path.append(database_helpers)
from flp_database_connector import flp_database_connector

# --- Step 1: Create tz-aware test DataFrame ---
tz = "America/New_York"

# First: naive times for the ambiguous hour
naive_times = [
    pd.Timestamp("2021-11-07 01:00:00"),  # First 1 AM (EDT)
    pd.Timestamp("2021-11-07 01:00:00"),  # Second 1 AM (EST)
]

# Localize them explicitly — ambiguous=[True, False] picks the DST instance
times = [
    pd.DatetimeIndex([naive_times[0]]).tz_localize(tz, ambiguous=True)[0],   # EDT (UTC-4)
    pd.DatetimeIndex([naive_times[1]]).tz_localize(tz, ambiguous=False)[0],  # EST (UTC-5)
]

df = pd.DataFrame({
    "he_datetime": times,
    "value": [100, 200]
})

df = pd.DataFrame({
    "datetime_he": times,
    "asset": ["dummy", "dummy"],
    "name": ["dummy", "dummy"],
    "ops_type": ["Generation", "Generation"],
    "service": ["Energy", "Energy"],
    "da_volume": [100, 200],
    "rt_volume": [0, 0],
    "unit": ["MWh", "MWh"],
    "interval_width_s": [3600, 3600]
    })

print(df)
print(df.dtypes)
print(df.datetime_he.map(lambda x: x.utcoffset()))

# --- Step 2: Create dummy table in ops schema with DATETIMEOFFSET PK ---

# Write result to database
db_conn = flp_database_connector(r"firstlightpower\cbrooks")
db_conn.upload_data_to_quant_db(
        table_name="ops.test_hourly_energy",
        df=df,
        mode="create",
        skip_prompt=True
    )