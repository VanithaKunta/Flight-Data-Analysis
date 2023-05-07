from glob import glob
import pandas as pd

# Get a list of parquet files
parquet_files = glob("../data_clean/preprocessed_data.parquet/*.parquet")

# Define the subset of columns to read from the parquet files
column_subset = [
    "FlightDate",
    "Airline",
    "Flight_Number_Marketing_Airline",
    "Origin",
    "Dest",
    "Cancelled",
    "Diverted",
    "CRSDepTime",
    "DepTime",
    "DepDelayMinutes",
    "OriginAirportID",
    "OriginCityName",
    "OriginStateName",
    "DestAirportID",
    "DestCityName",
    "DestStateName",
    "TaxiOut",
    "TaxiIn",
    "CRSArrTime",
    "ArrTime",
    "ArrDelayMinutes",
]

# Read the parquet files and store them in a list of DataFrames
dfs = []
for f in parquet_files:
    dfs.append(pd.read_parquet(f, columns=column_subset))

# Concatenate the DataFrames into a single DataFrame
sdf = pd.concat(dfs).reset_index(drop=True)

# Add a new column "DelayGroup" based on the "DepDelayMinutes" values
sdf["DelayGroup"] = None
sdf.loc[sdf["DepDelayMinutes"] == 0, "DelayGroup"] = "OnTime_Early"
sdf.loc[
    (sdf["DepDelayMinutes"] > 0) & (sdf["DepDelayMinutes"] <= 15), "DelayGroup"
] = "Small_Delay"
sdf.loc[
    (sdf["DepDelayMinutes"] > 15) & (sdf["DepDelayMinutes"] <= 45), "DelayGroup"
] = "Medium_Delay"
sdf.loc[sdf["DepDelayMinutes"] > 45, "DelayGroup"] = "Large_Delay"
sdf.loc[sdf["Cancelled"], "DelayGroup"] = "Cancelled"

# Convert the "FlightDate" column to date format
sdf['FlightDate'] = pd.to_datetime(sdf['FlightDate'])

# Extract year, month, and day of the week from the "FlightDate" column
sdf['Year'] = sdf['FlightDate'].dt.year
sdf['Month'] = sdf['FlightDate'].dt.month
sdf['DayOfWeek'] = sdf['FlightDate'].dt.strftime('%a')

# Convert categorical columns to the category data type
cat_cols = ["Airline", "Origin", "Dest", "OriginStateName", "DestStateName"]
for c in cat_cols:
    sdf[c] = sdf[c].astype("category")

# Save the preprocessed data to a parquet file
sdf.to_parquet("../data_clean/preprocessed_data_with_delay_groups.parquet")