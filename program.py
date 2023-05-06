import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns
from glob import glob
import numpy as np
import random

pd.set_option("display.max_columns", 500)
plt.style.use("seaborn-colorblind")
pal = sns.color_palette()


# Read the preprocessed data with delay groups from the parquet file
sdf = pd.read_parquet("static/data_clean/preprocessed_data_with_delay_groups.parquet")

# Distribution of flight delays
ax = sdf.query("DepDelayMinutes > 1 and DepDelayMinutes < 61")["DepDelayMinutes"].plot(
    kind="hist", bins=30, title="Distribution of Flight Delays"
)
ax.set_xlabel("Delay Time (Minutes)")
ax.set_ylabel("Frequency")
ax.figure.savefig('static/images/distribution_of_flight_delays.png')
plt.show()


# Create a bar chart of the delay groups
# Group the data by delay groups and calculate the percentage of flights in each group
delay_groups = sdf['DelayGroup'].value_counts(normalize=True).sort_index() * 100

plt.bar(delay_groups.index, delay_groups.values)
plt.title('Percentage of Delay Groups for Flights')
plt.xlabel('Delay Group')
plt.ylabel('Percentage of Flights')
plt.savefig('static/images/percentage_delays_of_flights.png')
plt.show()
plt.close()


# Average Departure deplay by Origin State Name
plt.figure(figsize=(10, 6))
ax = sdf.groupby('OriginStateName')['DepDelayMinutes'].mean().sort_values().plot(kind='barh', color='green')
ax.set_xlabel('Average Departure Delay (minutes)')
ax.set_ylabel('Origin State Name')
ax.set_title('Average Departure Delay by Origin State Name')
plt.savefig('static/images/avg_delay_by_origin_state.png')
plt.show()
plt.close()


# Average delay by month for airline
# Group the data by airline and month and calculate the average delay
avg_delay_by_month = sdf.groupby(['Airline', 'Month']).agg({'DepDelayMinutes': 'mean'})

# Reset the index to make the airline and month columns regular columns
avg_delay_by_month = avg_delay_by_month.reset_index()

# Set the figure size
fig, ax = plt.subplots(figsize=(12,8))

# Loop through each airline and plot its average delay by month
for airline in avg_delay_by_month['Airline'].unique():
    airline_data = avg_delay_by_month[avg_delay_by_month['Airline'] == airline]
    ax.plot(airline_data['Month'], airline_data['DepDelayMinutes'], label=airline)

# Set the title, x and y labels
ax.set_title('Average Delay by Month for Each Airline')
ax.set_xlabel('Month')
ax.set_ylabel('Average Delay (Minutes)')

months = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
# Set the x-axis tick labels to show only the month names
plt.xticks(range(len(months)), months)

# Add a legend to the plot
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

plt.savefig('static/images/avg_delay_by_month_airline.png', bbox_inches='tight')
plt.show()


# Average delay by year for airline
# Group the data by airline and year and calculate the average delay
avg_delay_by_year = sdf.groupby(['Airline', 'Year']).agg({'DepDelayMinutes': 'mean'})

# Reset the index to make the airline and year columns regular columns
avg_delay_by_year = avg_delay_by_year.reset_index()

# Set the figure size
fig, ax = plt.subplots(figsize=(12,8))

# Loop through each airline and plot its average delay by year
for airline in avg_delay_by_year['Airline'].unique():
    airline_data = avg_delay_by_year[avg_delay_by_year['Airline'] == airline]
    ax.plot(airline_data['Year'], airline_data['DepDelayMinutes'], label=airline)

# Set the title, x and y labels
ax.set_title('Average Delay by Year for Each Airline')
ax.set_xlabel('Year')
ax.set_ylabel('Average Delay (Minutes)')

# Set the x-axis tick labels to show only the year values
plt.xticks(avg_delay_by_year['Year'].unique())

# Add a legend to the plot
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

plt.savefig('static/images/avg_delay_by_year_airline.png', bbox_inches='tight')
plt.show()


# Average delay by day of week for airline
# Group the data by airline and day of the week and calculate the average delay
avg_delay_by_dayofweek = sdf.groupby(['Airline', 'DayOfWeek']).agg({'DepDelayMinutes': 'mean'})

# Reset the index to make the airline and day of week columns regular columns
avg_delay_by_dayofweek = avg_delay_by_dayofweek.reset_index()

# Set the figure size
fig, ax = plt.subplots(figsize=(12,8))

# Loop through each airline and plot its average delay by day of the week
for airline in avg_delay_by_dayofweek['Airline'].unique():
    airline_data = avg_delay_by_dayofweek[avg_delay_by_dayofweek['Airline'] == airline]
    ax.plot(airline_data['DayOfWeek'], airline_data['DepDelayMinutes'], label=airline)

# Set the title, x and y labels
ax.set_title('Average Delay by Day of the Week for Each Airline')
ax.set_xlabel('Day of the Week')
ax.set_ylabel('Average Delay (Minutes)')

days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
# Set the x-axis tick labels to show only the day of the week names
plt.xticks(range(len(days_of_week)), days_of_week)

# Add a legend to the plot
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

plt.savefig('static/images/avg_delay_by_dayofweek_airline.png', bbox_inches='tight')
plt.show()


# Percentage of delayed flights in each delay group
# Group the data by airline and count the number of flights
flight_counts = sdf.groupby('Airline').size().reset_index(name='FlightCounts')

# Group the data by airline and calculate the percentage of delayed flights
delay_percentages = sdf[sdf['DelayGroup'] != None].groupby('Airline').size() / flight_counts['FlightCounts'] * 100
delay_percentages = delay_percentages.reset_index(name='DelayPercentages')

# Merge the flight counts and delay percentages dataframes on the airline column
merged_data = pd.merge(flight_counts, delay_percentages, on='Airline')

# Create a horizontal bar plot of the delay percentages
fig, ax = plt.subplots(figsize=(10, 8))
ax.barh(merged_data['Airline'], merged_data['DelayPercentages'])
ax.set_xlabel('Percentage of Delayed Flights')
ax.set_ylabel('Airline')
ax.set_title('Percentage of Delayed Flights by Airline')
plt.savefig('static/images/percent_delayed_flights1.png', bbox_inches='tight')
plt.show()


# Percentage of flights in each delay group
# Group the data by airline and calculate the percentage of delayed flights
delayed_flight_percents = sdf[sdf['DelayGroup'] != 'None'].groupby('Airline')['DelayGroup'].value_counts(normalize=True).mul(100).reset_index(name='DelayedFlightPercents')
delayed_flight_percents = delayed_flight_percents[delayed_flight_percents['DelayGroup'] != 'None']

# Plot the data as a bar chart
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(x='Airline', y='DelayedFlightPercents', data=delayed_flight_percents, hue='DelayGroup', ax=ax)
ax.set_title('Percentage of Delay Groups by Airline')
ax.set_xlabel('Airline')
ax.set_ylabel('Percentage of Delay Groups')
plt.xticks(rotation=90)
plt.savefig('static/images/percent_delayed_flights2.png', bbox_inches='tight')
plt.show()


# Total vs delayed flights for each airline
# Group the data by airline and count the number of flights and delayed flights
flight_counts = sdf.groupby('Airline').size().reset_index(name='TotalFlights')
# Group the data by airline and count the number of delayed flights
delayed_flight_counts = sdf[(sdf['DepDelayMinutes'] > 0) & (sdf['DepDelayMinutes'].notna())].groupby('Airline').size().reset_index(name='DelayedFlights')

# Merge the flight counts and delayed flight counts into one dataframe
flight_data = flight_counts.merge(delayed_flight_counts, on='Airline')

# Plot the data as a bar chart
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(x='Airline', y='TotalFlights', data=flight_data, color='b', ax=ax)
sns.barplot(x='Airline', y='DelayedFlights', data=flight_data, color='r', ax=ax)
ax.legend(handles=ax.containers, labels=['Total Flights', 'Delayed Flights'])
ax.set_title('Total vs Delayed Flights by Airline')
ax.set_xlabel('Airline')
ax.set_ylabel('Number of Flights')
plt.xticks(rotation=90)
plt.savefig('static/images/total_vs_delayed_flights.png', bbox_inches='tight')
plt.show()


# Average delay by day of week for top 5 airlines
# Group the data by airline and day of the week and calculate the average delay
avg_delay_by_dayofweek = sdf.groupby(['Airline', 'DayOfWeek']).agg({'DepDelayMinutes': 'mean'})

# Reset the index to make the airline and day of week columns regular columns
avg_delay_by_dayofweek = avg_delay_by_dayofweek.reset_index()

# Calculate the average delay for each airline
avg_delay_by_airline = avg_delay_by_dayofweek.groupby('Airline').agg({'DepDelayMinutes': 'mean'}).reset_index()

# Sort the airlines by their average delay in descending order
sorted_airlines = avg_delay_by_airline.sort_values(by='DepDelayMinutes', ascending=False)

# Select the top 10 airlines based on their average delay
top_5_airlines = sorted_airlines.head(5)['Airline'].tolist()

# Filter the data to only include the top 10 airlines
top_5_data = avg_delay_by_dayofweek[avg_delay_by_dayofweek['Airline'].isin(top_5_airlines)]

# Set the figure size
fig, ax = plt.subplots(figsize=(12,8))

# Loop through each airline and plot its average delay by day of the week
for airline in top_5_airlines:
    airline_data = top_5_data[top_5_data['Airline'] == airline]
    ax.plot(airline_data['DayOfWeek'], airline_data['DepDelayMinutes'], label=airline)

# Set the title, x and y labels
ax.set_title('Average Delay by Day of the Week for Top 5 Airlines')
ax.set_xlabel('Day of the Week')
ax.set_ylabel('Average Delay (Minutes)')

days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
# Set the x-axis tick labels to show only the day of the week names
plt.xticks(range(len(days_of_week)), days_of_week)

# Add a legend to the plot
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

plt.savefig('static/images/avg_delay_by_dayofweek_top5_airlines.png', bbox_inches='tight')
plt.show()


# Average delay by year for top 5 airlines
# Group the data by airline and year and calculate the average delay
avg_delay_by_year = sdf.groupby(['Airline', 'Year']).agg({'DepDelayMinutes': 'mean'})

# Reset the index to make the airline and year columns regular columns
avg_delay_by_year = avg_delay_by_year.reset_index()

# Calculate the average delay for each airline
avg_delay_by_airline = avg_delay_by_year.groupby('Airline').agg({'DepDelayMinutes': 'mean'}).reset_index()

# Sort the airlines by their average delay in descending order
sorted_airlines = avg_delay_by_airline.sort_values(by='DepDelayMinutes', ascending=False)

# Select the top 10 airlines based on their average delay
top_5_airlines = sorted_airlines.head(5)['Airline'].tolist()

# Filter the data to only include the top 10 airlines
top_5_data = avg_delay_by_year[avg_delay_by_year['Airline'].isin(top_5_airlines)]

# Set the figure size
fig, ax = plt.subplots(figsize=(12,8))

# Loop through each airline and plot its average delay by year
for airline in top_5_airlines:
    airline_data = top_5_data[top_5_data['Airline'] == airline]
    ax.plot(airline_data['Year'], airline_data['DepDelayMinutes'], label=airline)

# Set the title, x and y labels
ax.set_title('Average Delay by Year for Top 5 Airlines')
ax.set_xlabel('Year')
ax.set_ylabel('Average Delay (Minutes)')

# Set the x-axis tick labels to show only the year values
plt.xticks(avg_delay_by_year['Year'].unique())

# Add a legend to the plot
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

plt.savefig('static/images/avg_delay_by_year_top5_airline.png', bbox_inches='tight')
plt.show()


# Average delay by month for top 5 airlines
# Group the data by airline and month and calculate the average delay
avg_delay_by_month = sdf.groupby(['Airline', 'Month']).agg({'DepDelayMinutes': 'mean'})

# Reset the index to make the airline and month columns regular columns
avg_delay_by_month = avg_delay_by_month.reset_index()

# Calculate the average delay for each airline
avg_delay_by_airline = avg_delay_by_month.groupby('Airline').agg({'DepDelayMinutes': 'mean'}).reset_index()

# Sort the airlines by their average delay in descending order
sorted_airlines = avg_delay_by_airline.sort_values(by='DepDelayMinutes', ascending=False)

# Select the top 10 airlines based on their average delay
top_5_airlines = sorted_airlines.head(5)['Airline'].tolist()

# Filter the data to only include the top 10 airlines
top_5_data = avg_delay_by_month[avg_delay_by_month['Airline'].isin(top_5_airlines)]

# Set the figure size
fig, ax = plt.subplots(figsize=(12,8))

# Loop through each airline and plot its average delay by month
for airline in top_5_airlines:
    airline_data = top_5_data[top_5_data['Airline'] == airline]
    ax.plot(airline_data['Month'], airline_data['DepDelayMinutes'], label=airline)

# Set the title, x and y labels
ax.set_title('Average Delay by Month for Top 5 Airlines')
ax.set_xlabel('Month')
ax.set_ylabel('Average Delay (Minutes)')

months = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
# Set the x-axis tick labels to show only the month names
plt.xticks(range(len(months)), months)

# Add a legend to the plot
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

plt.savefig('static/images/avg_delay_by_month_top5_airline.png', bbox_inches='tight')
plt.show()


# Number of on-time flights by each airline
# Filter the data to include only on-time flights
on_time_df = sdf[sdf["DelayGroup"] == "OnTime_Early"]

# Group the data by airline and count the number of on-time flights
airline_counts = on_time_df.groupby("Airline")["DelayGroup"].count().reset_index()

# Sort the airlines by the number of on-time flights
airline_counts = airline_counts.sort_values(by="DelayGroup", ascending=False)

# Create the barplot
plt.figure(figsize=(12, 6))
plt.xticks(rotation=90)
sns.set_palette(sns.color_palette())
sns.barplot(data=airline_counts, x="Airline", y="DelayGroup")
plt.title("Number of On-time Flights by Airline (2018-2022)")
plt.xlabel("Airline")
plt.ylabel("Number of Flights")
plt.savefig('static/images/on_time_flights_by_airline.png', bbox_inches='tight')
plt.show()


# Calculate number of cancellations by airport
cancel_count = sdf[sdf["Cancelled"] == True].groupby("Origin")["Cancelled"].count().reset_index(name='count')

# Sort by count and take top 35 airports
cancel_count = cancel_count.sort_values(by="count", ascending=False).head(35)

# Create bar plot
fig, ax = plt.subplots(figsize=(12,8))
ax.bar(cancel_count["Origin"], cancel_count["count"], color=sns.color_palette())
ax.set_title("Number of Cancellations by Airport (Top 35)")
ax.set_xlabel("Airport")
ax.set_ylabel("Number of Cancellations")
ax.tick_params(axis='x', labelrotation=90)
plt.savefig('static/images/cancellations_by_airport.png', bbox_inches='tight')
plt.show()
