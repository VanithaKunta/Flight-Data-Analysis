from pyspark.sql import SparkSession

# Create a SparkSession and Configure memory for driver and executor
spark = SparkSession.builder.appName("AirlineDataAnalysis").config("spark.driver.memory", "4g").config("spark.executor.memory", "8g").getOrCreate()

# Read the parquet files and merge them into one DataFrame
df1 = spark.read.parquet("../data_raw/Combined_Flights_2018.parquet")
df2 = spark.read.parquet("../data_raw/Combined_Flights_2019.parquet")
df3 = spark.read.parquet("../data_raw/Combined_Flights_2020.parquet")
df4 = spark.read.parquet("../data_raw/Combined_Flights_2021.parquet")
df5 = spark.read.parquet("../data_raw/Combined_Flights_2022.parquet")
df = df1.union(df2).union(df3).union(df4).union(df5)

# Drop unnecessary columns
df = df.drop("Tail_Number", "Taxi_Out", "Taxi_In", "Wheels_On", "Wheels_Off")

# Drop rows with null values
df = df.dropna()

# Write the preprocessed data to a new parquet file
df.write.parquet("../data_clean/Preprocessed_Airline_Data.parquet")
