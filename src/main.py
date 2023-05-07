# Import all the necessary libraries
from flask import Flask, render_template, request
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import seaborn as sns
plt.style.use("seaborn-colorblind")
pal = sns.color_palette()

app = Flask(__name__)

# Create a SparkSession
# set the `spark.sql.legacy.timeParserPolicy` to "LEGACY" to use the pre-3.0 behavior
spark = SparkSession.builder\
        .appName("AirlineDataAnalysis")\
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
        .getOrCreate()

# Read the preprocessed data
df = spark.read.parquet("../data_clean/Preprocessed_Airline_Data.parquet")

# Read the preprocessed data with delay groups from the parquet file
sdf = pd.read_parquet("../data_clean/preprocessed_data_with_delay_groups.parquet")

def get_airlines():
    # Get the unique airlines with their airline codes
    airlines_data = df.select("Airline", "Operating_Airline").distinct().collect()
    return airlines_data


@app.route('/')
def airlines():
    airlines_data = get_airlines()
    return render_template('airlines.html', airlines=airlines_data)


@app.route('/average_delays')
def average_delays():

    # Calculate the average flight delay by day of week with day names
    avg_delay_by_dayofweek = df.groupBy(date_format("FlightDate", "EEEE").alias("DayOfWeek")) \
        .agg(avg("DepDelay").alias("AvgDelay")) \
        .orderBy(when(col("DayOfWeek") == "Monday", 1)
                 .when(col("DayOfWeek") == "Tuesday", 2)
                 .when(col("DayOfWeek") == "Wednesday", 3)
                 .when(col("DayOfWeek") == "Thursday", 4)
                 .when(col("DayOfWeek") == "Friday", 5)
                 .when(col("DayOfWeek") == "Saturday", 6)
                 .when(col("DayOfWeek") == "Sunday", 7))

    # Calculate the average flight delay by month with month names and sort by month order
    avg_delay_by_month = df.groupBy(date_format("FlightDate", "MMMM").alias("Month")) \
        .agg(avg("DepDelay").alias("AvgDelay")) \
        .orderBy(when(col("Month") == "January", 1)
                 .when(col("Month") == "February", 2)
                 .when(col("Month") == "March", 3)
                 .when(col("Month") == "April", 4)
                 .when(col("Month") == "May", 5)
                 .when(col("Month") == "June", 6)
                 .when(col("Month") == "July", 7)
                 .when(col("Month") == "August", 8)
                 .when(col("Month") == "September", 9)
                 .when(col("Month") == "October", 10)
                 .when(col("Month") == "November", 11)
                 .when(col("Month") == "December", 12))

    # Calculate the average flight delay by year
    avg_delay_by_year = df.groupBy(date_format("FlightDate", "YYYY").alias("Year")) \
        .agg(avg("DepDelay").alias("AvgDelay")) \
        .orderBy("Year")

    # Calculate the average flight delay by airline
    avg_delay_by_airline = df.groupBy("Airline") \
        .agg(avg("DepDelay").alias("AvgDelay")) \
        .withColumn("AvgDelay", when(col("AvgDelay") < 0, 1).otherwise(col("AvgDelay")))

    # Convert the DataFrames to Pandas and send them to Flask
    data = {
        "avg_delay_by_dayofweek": avg_delay_by_dayofweek.toPandas().to_dict(),
        "avg_delay_by_month": avg_delay_by_month.toPandas().to_dict(),
        "avg_delay_by_year": avg_delay_by_year.toPandas().to_dict(),
        "avg_delay_by_airline": avg_delay_by_airline.toPandas().to_dict(),
    }

    # Create subplots for each chart
    fig, axs = plt.subplots(nrows=2, ncols=2, figsize=(14, 10))

    # Plot the average delay by day of week
    dayofweek_df = pd.DataFrame(data["avg_delay_by_dayofweek"])
    axs[0, 0].bar(dayofweek_df["DayOfWeek"], dayofweek_df["AvgDelay"])
    axs[0, 0].set_title("Average Flight Delay by Day of Week")
    axs[0, 0].set_xlabel("Day of Week")
    axs[0, 0].set_ylabel("Average Delay (minutes)")
    axs[0, 0].tick_params(axis="x", labelrotation=90)

    # Plot the average delay by month
    month_df = pd.DataFrame(data["avg_delay_by_month"])
    axs[0, 1].plot(month_df["Month"], month_df["AvgDelay"])
    axs[0, 1].set_title("Average Flight Delay by Month")
    axs[0, 1].set_xlabel("Month")
    axs[0, 1].set_ylabel("Average Delay (minutes)")
    axs[0, 1].tick_params(axis="x", labelrotation=90)

    # Plot the average delay by year
    year_df = pd.DataFrame(data["avg_delay_by_year"])
    axs[1, 0].bar(year_df["Year"], year_df["AvgDelay"], width=0.5)
    axs[1, 0].set_title("Average Flight Delay by Year")
    axs[1, 0].set_xlabel("Year")
    axs[1, 0].set_ylabel("Average Delay (minutes)")

    # Plot the average delay by airline
    airline_df = pd.DataFrame(data["avg_delay_by_airline"])
    airline_df = airline_df.sort_values("AvgDelay", ascending=False)
    axs[1, 1].bar(airline_df["Airline"], airline_df["AvgDelay"])
    axs[1, 1].set_title("Average Flight Delay by Airline")
    axs[1, 1].set_xlabel("Airline")
    axs[1, 1].set_ylabel("Average Delay (minutes)")
    axs[1, 1].tick_params(axis="x", labelrotation=90)

    # Adjust the layout to prevent overlap
    plt.tight_layout()

    # Save the figure as a PNG file
    plt.savefig("../results/flight_delays.png")
    plt.close()

    # % of Flight Results by Year
    sdf_agg = sdf.groupby("Year")["DelayGroup"].value_counts(normalize=True).unstack() * 100
    col_order = ["OnTime_Early", "Small_Delay", "Medium_Delay", "Large_Delay", "Cancelled"]
    styled_sdf = sdf_agg[col_order].style.background_gradient(cmap="Greens")

    # Save the styled dataframe as an image
    fig = plt.figure(figsize=(8, 4))
    ax = fig.add_subplot(111)
    ax.axis("off")
    ax.axis("tight")

    # Save the styled dataframe as an HTML file
    flights_year_results_html = styled_sdf.to_html()
    with open('../results/styled_table', 'w') as f:
        f.write(flights_year_results_html)

    # % of Flight Results by Month
    sdf["Month"] = sdf["FlightDate"].dt.strftime('%B').sort_values(key=lambda x: pd.to_datetime(x, format='%B'))
    sdf_agg = sdf.groupby("Month")["DelayGroup"].value_counts(normalize=True).unstack() * 100
    col_order = ["OnTime_Early", "Small_Delay", "Medium_Delay", "Large_Delay", "Cancelled"]
    styled_sdf = sdf_agg[col_order].style.background_gradient(cmap="Blues")
    # Save the styled dataframe as an HTML file
    flights_month_results_html = styled_sdf.to_html()
    with open('../results/styled_table_by_month.html', 'w') as f:
        f.write(flights_month_results_html)

    # Average Departure deplay by Origin State Name
    plt.figure(figsize=(10, 8))
    ax = sdf.groupby('OriginStateName')['DepDelayMinutes'].mean().sort_values(ascending=False).plot(kind='barh', color=sns.color_palette("Set3", 8))
    ax.set_xlabel('Average Departure Delay (minutes)')
    ax.set_ylabel('Origin State Name')
    ax.set_title('Average Departure Delay by Origin State Name')
    ax.invert_yaxis()  # invert y-axis to show longer bars on top
    plt.subplots_adjust(left=0.3, right=0.98, top=0.95, bottom=0.1)  # adjust the margins
    plt.savefig('../results/avg_delay_by_origin_state.png')
    plt.close()

    # Average departure delay by origin airport
    df_delay_by_origin = sdf.groupby('Origin')['DepDelayMinutes'].mean().reset_index()
    df_delay_by_origin = df_delay_by_origin.sort_values(by='DepDelayMinutes', ascending=False).head(35)

    # Create a color palette with the desired number of colors
    num_colors = len(df_delay_by_origin)
    color_palette = sns.color_palette("Set3", num_colors)

    # Create the bar plot and specify the color palette
    plt.figure(figsize=(12, 6))
    plt.bar(df_delay_by_origin['Origin'], df_delay_by_origin['DepDelayMinutes'], color=color_palette)
    plt.title('Average Departure Delay by Origin Airport')
    plt.xlabel('Airport')
    plt.ylabel('Average Delay (minutes)')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('../results/avg_delay_by_origin_airport.png')
    plt.close()

    # Distribution of Flight Delays
    plt.figure(figsize=(8, 6))
    ax1 = sdf.query("DepDelayMinutes > 1 and DepDelayMinutes < 61")["DepDelayMinutes"].plot(
        kind="hist", bins=30, title="Distribution of Flight Delays"
    )
    ax1.set_xlabel("Delay Time (Minutes)")
    ax1.set_ylabel("Frequency")
    plt.savefig('../results/distribution_of_flight_delays.png')
    plt.close()

    return render_template("average_delays.html", data=data, flights_year_results_html=flights_year_results_html, flights_month_results_html=flights_month_results_html, image_file="../results/flight_delays.png", airport_file="../results/avg_delay_by_origin_airport.png", airport_state="../results/avg_delay_by_origin_state.png", distribution_of_flight_delays="../results/distribution_of_flight_delays.png")


@app.route('/scheduled_flights')
def scheduled_flights():

    # Scheduled Flights by Airline
    fig, ax = plt.subplots(figsize=(10, 10))
    airlines_ordered = (sdf["Airline"].value_counts(ascending=True) / 100_000).plot(
        kind="barh", ax=ax, color=pal[2], width=1, edgecolor="black"
    )
    ax.set_title("Number of Flights in Dataset")
    ax.set_xlabel("Flights (100k)")
    plt.tight_layout()
    plt.savefig('../results/scheduled_flights_by_airline.png')
    plt.close()
    scheduled_flights_by_airline = '../results/scheduled_flights_by_airline.png'

    # Scheduled Flights by Year for all airlines combined

    # Extract the year from the FlightDate column
    sdf["Year"] = sdf["FlightDate"].dt.year

    # Calculate the number of flights by year
    year_counts = sdf["Year"].value_counts().sort_index()

    # Create a pie chart
    plt.figure(figsize=(10, 6))
    plt.pie(year_counts.values, labels=year_counts.index, autopct="%1.1f%%")
    plt.title("Scheduled Flights by Year")

    # Save the chart
    plt.savefig("../results/flights_by_year.png")
    plt.close()
    flights_by_year = '../results/flights_by_year.png'

    # year_counts = sdf["Year"].value_counts().reset_index().rename(columns={"index": "Year", "Year": "Count"})

    # Scheduled Flights by year for each airline
    # Group the data by airline, year, and count the number of flights
    grouped_df = sdf.groupby(['Airline', 'Year']).size().reset_index(name='count')

    # Convert the data to pandas dataframe for plotting
    pandas_df = grouped_df.pivot(index="Airline", columns="Year", values="count")

    # Plot the data as a stacked bar chart
    pandas_df.plot(kind="bar", stacked=True)

    # Set the title and axis labels
    plt.title("Scheduled Flights by Year for Each Airline")
    plt.xlabel("Year")
    plt.ylabel("Count")
    plt.savefig('../results/scheduled_flights_by_year_each_airline.png', bbox_inches='tight')
    scheduled_flights_by_year_each_airline = '../results/scheduled_flights_by_year_each_airline.png'

    # Line chart for scheduled flights by year for each airline
    # Define the 5 years of interest
    years = [2018, 2019, 2020, 2021, 2022]

    # Filter the data to only include the 5 years of interest
    df_years = sdf.loc[sdf["Year"].isin(years)]

    # Group the data by airline, year, and count the number of flights
    grouped_df = df_years.groupby(['Airline', 'Year']).size().reset_index(name='count')

    # Convert the data to pandas dataframe for plotting
    pandas_df = grouped_df.pivot(index="Airline", columns="Year", values="count")

    # Set the figure size
    fig1, ax1 = plt.subplots(figsize=(10, 6))

    # Plot each airline's flight count over time
    for airline in pandas_df.index:
        ax1.plot(years, pandas_df.loc[airline], label=airline)

    # Set the title, x and y labels
    ax1.set_title("Scheduled Flights by Year for Each Airline", fontsize=16)
    ax1.set_xlabel("Year", fontsize=12)
    ax1.set_ylabel("Flight Count", fontsize=12)

    # Set the tick values for the x-axis
    plt.xticks(years)

    # Add a legend to the plot
    ax1.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    # Save the plot to an image file and show it
    plt.savefig('../results/timeseries_scheduled_flights_by_year.png', bbox_inches='tight')
    timeseries_scheduled_flights_by_year = '../results/timeseries_scheduled_flights_by_year.png'

    return render_template('scheduled_flights.html', scheduled_flights_by_airline=scheduled_flights_by_airline, flights_by_year=flights_by_year, scheduled_flights_by_year_each_airline=scheduled_flights_by_year_each_airline, timeseries_scheduled_flights_by_year=timeseries_scheduled_flights_by_year)


@app.route('/total_distance', methods=['GET', 'POST'])
def total_distance():

    # select the FlightDate column and extract the year
    year_df = df.select(date_format("FlightDate", "YYYY").alias("Year"))

    # get the distinct years
    distinct_years = year_df.distinct().orderBy("Year")

    # collect the years as a list of strings
    year_list = [str(row["Year"]) for row in distinct_years.collect()]

    selected_year = 0  # initialize to 0
    airline_distance_list = None  # initialize to None
    img_path = None  # initialize to None

    if request.method == 'POST':
        selected_year = request.form['yearSelect']

        # filter the dataframe by year
        year_df = df.filter(date_format("FlightDate", "YYYY") == selected_year)

        # group by airline and sum the distance
        airline_distance_list = year_df.groupBy("Airline").agg(sum("Distance").alias("Total_Distance"))

        # convert the airline and distance data to lists
        airline_list = [row['Airline'] for row in airline_distance_list.collect()]
        distance_list = [row['Total_Distance'] for row in airline_distance_list.collect()]

        # create a line plot
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(airline_list, distance_list)

        # set plot title and axis labels
        ax.set_title('Total Distance Flown by Airlines in {}'.format(selected_year))
        ax.set_xlabel('Airlines')
        ax.set_ylabel('Total Distance (miles)')

        # rotate the x-axis labels for better readability
        plt.xticks(rotation=90)
        plt.subplots_adjust(bottom=0.65)

        plt.savefig('../results/total_distance.png')  # save figure to file
        plt.close()
        img_path = '../results/total_distance.png'

    return render_template('total_distance.html',
                           year_list=year_list,
                           selected_year=selected_year,
                           airline_distance_list=airline_distance_list,
                           img_path=img_path)


@app.route('/airports')
def airports():
    # On-time flights by origin airport

    # Filter flights that were not delayed
    df_on_time = sdf[sdf['DepDelayMinutes'] == 0]

    # Group by origin and count the number of on-time flights from each origin
    df_on_time_by_origin = df_on_time.groupby('Origin')['DepDelayMinutes'].count().reset_index()

    # Sort the origins in descending order by the count of on-time flights
    df_on_time_by_origin = df_on_time_by_origin.sort_values(by='DepDelayMinutes', ascending=False).head(35)

    # Create a bar plot
    plt.figure(figsize=(12,6))
    plt.bar(df_on_time_by_origin['Origin'], df_on_time_by_origin['DepDelayMinutes'],  color=sns.color_palette())
    plt.title('Number of On-Time Flights by Origin Airport')
    plt.xlabel('Airport')
    plt.ylabel('Number of On-Time Flights')
    plt.xticks(rotation=90)
    plt.savefig('../results/on_time_flights_by_origin_airport.png')
    plt.close()

    # Number of cancellations by airline
    # Group the data by airline and count the number of cancellations
    airline_cancellations = sdf[sdf['Cancelled'] == True].groupby('Airline')['Cancelled'].count().reset_index()

    # Sort the data by the number of cancellations
    airline_cancellations = airline_cancellations.sort_values(by="Cancelled", ascending=False)

    # Plot the data using Seaborn
    plt.figure(figsize=(12, 6))
    sns.barplot(x="Airline", y="Cancelled", data=airline_cancellations)
    sns.set_palette(sns.color_palette())
    plt.xticks(rotation=90)
    plt.title("Number of Cancellations by Airline")
    plt.xlabel("Airline")
    plt.ylabel("Number of Cancellations")
    plt.tight_layout()
    plt.savefig('../results/airline_cancellations.png')
    plt.close()

    return render_template("airports.html",
                           on_time_flights_by_origin_airport="../results/on_time_flights_by_origin_airport.png",
                           airline_cancellations="../results/airline_cancellations.png")


@app.route('/reasons')
def reasons():
    # Create a new DataFrame with the average delay for each airline and delay type
    avg_delays = sdf.groupby(['Airline']).agg(
        {'CarrierDelay': 'mean', 'WeatherDelay': 'mean', 'NASDelay': 'mean', 'SecurityDelay': 'mean',
         'LateAircraftDelay': 'mean'})

    # Reset the index to make the airline column a regular column
    avg_delays = avg_delays.reset_index()

    # Melt the DataFrame to make it "tidy"
    melted_delays = pd.melt(avg_delays, id_vars=['Airline'],
                            value_vars=['CarrierDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay',
                                        'LateAircraftDelay'], var_name='DelayType', value_name='AvgDelay')

    # Create a barplot using seaborn
    plt.figure(figsize=(12, 6))  # Increase the figure size
    sns.set(style='whitegrid')
    sns.barplot(x='Airline', y='AvgDelay', hue='DelayType', data=melted_delays)
    plt.xticks(rotation=45, ha='right')
    plt.title("Average Delay by Airline and Delay Type")
    plt.tight_layout()
    plt.savefig('../results/avg_delay_by_airline_and_delay_type.png')
    plt.close()

    return render_template("reasons.html", avg_delay_by_airline_and_delay_type="../results/avg_delay_by_airline_and_delay_type.png")


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5002, threaded=True)
