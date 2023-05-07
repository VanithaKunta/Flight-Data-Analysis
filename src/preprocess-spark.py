from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create SparkSession object
spark = SparkSession.builder \
    .appName("AirlineDataAnalysis") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

# Use glob to read all the parquet files
df = spark.read.format("parquet").load("../data_raw/Combined_Flights_*.parquet")

# Drop the columns that are not required for analysis
df = df.drop(*["DivAirportLandings"])

# Write the preprocessed data to a new parquet file
df.write.format("parquet").mode("overwrite").save("../data_clean/preprocessed_data.parquet")
