# Flight-Data-Analysis

#### ABSTRACT
The goal of this data analysis project is to better serve passengers and airlines by examining and understanding the patterns and trends in flight delays and cancellations. 
Information such as airline, departure and arrival airports, planned and real departure and arrival timings, and delay and cancellations are included in the data. 
The findings of this study may be utilized to shape initiatives aimed at enhancing punctuality and decreasing delays, which would eventually benefit passengers.

#### 1. Introduction
Flight Delay Analysis is an essential component of airline operations and research due to the significant impact that flight delays have on airlines, passengers, and the economy. 
To determine the root causes or trends of flight delays, the analysis of flight delay data is the main goal of this research work. 
This research aims to provide insights into the causes of aircraft delays and propose methods for minimizing delays and enhancing airline performance via the analysis of flight delay data. 
The association between flight delays and the many contributing variables will be investigated using a variety of statistical and machine-learning approaches. 
Airlines, airports, and air traffic control authorities will benefit from the study's results as they create methods for reducing flight delays and enhancing customer satisfaction.

#### 2. Architecture 
1.	**Data Collection:** Kaggle datasets for years 2018 – 2022. 
2.	**Data Preprocessing:** Data Integration, Cleaning, Transformation. 
3.	**Data Analysis:**  Using Spark and Python.
4.	**Data Visualization** and User Interface using Flask framework

#### 3. Dataset
The Flight Status Prediction dataset, which was obtained from Kaggle, was utilized for our study. The dataset encompasses comprehensive flight information, encompassing instances of cancellations and delays by airlines, spanning the time period from January 2018 to October 2022. 
The dataset comprises several million rows and 61 columns. The comprehensive dataset encompassing a period of five years is estimated to be approximately 10 gigabytes in size. 
Over a period of five years, we amassed datasets in the parquet format, owing to its ability to facilitate efficient data storage and processing. 
The dataset commonly comprises attributes such as flight numbers, scheduled departure and arrival times, factual departure and arrival times, flight durations, airline particulars, and flight status.

####  Dataset Summary
| Dataset                  | Flight Status Prediction Dataset     |
|--------------------------|--------------------------------------|
| Source                   | Kaggle                               |
| Dataset link             | [Flight Delay Dataset (2018-2022)](https://www.kaggle.com/datasets/flight-delay-dataset) |
| Date Range               | January 2018 – October 2022          |
| Size                     | 10 GB (CSV), 1 GB (Parquet)          |
| Number of rows           | Around 29 million rows               |
| Number of Columns        | 61                                   |

#### 4. Data Preprocessing
Preprocessed the data by integrating all 5 years of datasets, data cleaning, and by applying transformations.
1. The dataset has 61 columns of which few columns are not necessary for analysis. Removed all the unnecessary columns.
2. Extracted the column **DelayGroup** with 5 values by categorizing the column **DepDelayMinutes** and with column **Cancelled** which is either True or False.

          DepDelayMinutes <= 0: OnTime_Early 
          DepDelayMinutes <= 15: Small_Delay 
          DepDelayMinutes <= 60: Medium_Delay 
          DepDelayMinutes > 60: Large_Delay 
          Canceled == True: Cancelled
3. The **FlightDate** (yyyy-MM-dd) column is used to extract new columns.
          
          Year
          Day of week
          Month

#### 5. Tools Used
 - **Frameworks**: Flask, PySpark 
 - **IDE**: PyCharm 
 - **Programming Language**: Python 
 - **Front-end**: HTML, CSS, Bootstrap, jQuery

#### 6. Implementation
The suggested approach employs Flask to provide the user interface while PySpark and Python are used for data processing. PySpark is a data processing framework that is quick and scalable and can deal with plenty of data. It is appropriate for evaluating flight data since it can analyze data in parallel. An easy-to-use web application framework for creating user interfaces is Flask. It is great for creating data visualizations since it offers flexibility and customization choices.

The analysis of the airline data is done in an organized manner by the Python Flask web application. The preprocessed airline data is read into a Spark Session once it has been created. To obtain insights and effectively explain the results, the data is then processed and displayed. To find patterns, trends, and correlations in the data, a variety of data visualization methods are used, including bar charts, line charts, stacked bar charts, and histograms.
The Flask framework was used to create the user interface for displaying the graphs. To conduct the analysis and provide illuminating visuals, the system integrates several libraries, including PySpark, Flask, Matplotlib, Pandas, Seaborn, and Glob.
JQuery is used to manage user events like button clicks and page transitions to improve user engagement. The web pages are made to be visually beautiful and responsive to multiple screen widths by using CSS as well. 
The implemented system was used to conduct the following significant analyzes: 
1.	**_Overall distance flown by airlines each year_**: This report gives a broad summary of the travel distances made by each airline throughout the last five years. It aids in locating any notable variances in travel lengths and gauges the general expansion or contraction of airlines.
2.	**_Distribution of flight delays in the dataset_**: We can determine the frequency and severity of delays by examining the distribution of flight delays. Understanding the effects of delays on aircraft operations and passenger experiences requires knowledge of this information.
3.	**_The number of planned flights_**: It offers information on the number of flights that are scheduled annually, by airline, and annually for each airline. This research aids in identifying scheduling patterns and evaluating the effectiveness of airlines in terms of flight frequency.
4.	**_Average flight delay_**: It determines the average flight delay by airline, month, day of the week, and year. This report offers useful details on the usual delays encountered by various airlines throughout a range of time periods. It aids in the discovery of trends and patterns in flight delays..
5.	**_Average flight delay for top 5 airlines_**: The average flight delay for each of the top 5 airlines is calculated and broken out by day of the week, month, and year. This investigation sheds light on the performance of the most well-known airlines and aids in the discovery of any notable discrepancies in their typical delay times.
6.	**_Percentage of flights in each delay group_**: Based on the calendar year and month, it determines the percentage of flights in each delay group. This study aids in determining how many flights fall into the on-time, small delay, medium delay, large delay, and cancellation categories.
7.	**_Percentage of flights in each category of delays by airline_**: The system also examines the proportion of flights in each group of delays by airline. This study aids in assessing each airline's performance in terms of delays and contrasting the distribution of delays among them.
8.	**_Flight cancellations and on-time arrivals by airline_**: The system offers information on the percentage of on-time arrivals and cancellations for each airline. In terms of on-time arrivals and the frequency of aircraft cancellations, this research aids in evaluating the punctuality and dependability of airlines.
9.	In addition to this time-based analysis, we also carried out location-based ones. For example, we calculated the average departure delay by origin airport and airport state. 

To perform the previous research, the system makes use of several Python modules, including PySpark, Flask, Matplotlib, Pandas, Seaborn, and Glob. These libraries have been chosen with care after careful consideration to provide the essential capability for handling and displaying massive volumes of data effectively and efficiently. Flask facilitates the development of a nice online interface, while PySpark offers effective data processing capabilities. For data visualization, Matplotlib and Seaborn are utilized, offering a variety of tools and methods for creating meaningful graphs and charts. Pandas is used to manipulate and analyze data. 

In conclusion, the Flask-based user interface offers a simple environment for viewing and analyzing the created graphs. The underlying implementation makes use of the features provided by the PySpark, Flask, Matplotlib, Pandas, Seaborn, and Glob libraries to efficiently analyze data and create perceptive visuals.

#### 7. Conclusion 
The purpose of this study was to calculate each airline's on-time flight % and analyze flight delays and cancellations. In particular, the research analyzed the average flight delay and the flights in different delay groups. Airline executives may use this data to make better decisions that benefit passengers.The study also recommends using real-time data in future studies to improve the reliability and usability of the results. More recent and dynamic analysis might then be conducted, leading to more effective interventions and solutions for the problem of aircraft delays and cancellations. More research on the causes of airline delays and cancellations is needed to improve the safety and efficiency of air travel.

#### 8. Team Member Contributions
**Vanitha Kunta:** The lead developer responsible for developing and implementing the technical strategy for the project including selecting the right tools and effectively implementing them, and for preparing and delivering the presentation to the class.

**Akhil Yalla:** Responsible for collecting the data and preprocessing the dataset, making changes like adding new columns, extract new columns from existing, removing unnecessary columns. Responsible for conducting research related to the project's topic. Responsible for testing the Distance feature, airline location and delay reasons. Reviewed the code, conducted research related to the project, and developed a User Interface for the project.



