### Dataset description:
The dataset that we collected is the Flight Status Prediction dataset from Kaggle. It contains flight information which includes fields like cancellations, delay reason, time of delay, etc., for various airlines from January 2018 to October 2022. 
- This dataset has a total of 61 columns and a few million rows. 
- The original dataset was in CSV format, with a size of approximately 10GB for the complete dataset spanning five years. 
- To address this, we opted to use the Parquet storage format, which offers efficient data storage and processing capabilities.
- By converting the dataset to the Parquet dataset, we managed to reduce the size for the complete five-year period data to 1GB from the original size of 10 GB.

| Dataset                  | Flight Status Prediction Dataset     |
|--------------------------|--------------------------------------|
| Source                   | Kaggle                               |
| Dataset link             | [Flight Delay Dataset (2018-2022)](https://www.kaggle.com/datasets/flight-delay-dataset) |
| Date Range               | January 2018 – October 2022          |
| Size                     | 10 GB (CSV), 1 GB (Parquet)           |
| Number of rows           | Around 29 million rows               |
| Number of Columns        | 61                                   |
