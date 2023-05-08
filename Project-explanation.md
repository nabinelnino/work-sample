

## Project Overview

This project aims to predict stock volume using machine learning techniques. To accomplish this project, I utilized Airflow as a DAG to sequentially execute tasks. Due to the large volume of data, which contains over 20 million records, I employed the PySpark framework to simplify the data processing task. 

## Data Processing

The CSV files in both the stock and ETF repositories were converted into Parquet files using PySpark. Conversion was done in batches with the option to define the number of files to be processed, and the resulting Parquet files were stored in the data folder.

## Problem 2

I utilized PySpark to compute the vol_moving_avg and adj_close_rolling_med of both stocks and etfs

## Problem 3

In order to streamline the process for Problem 3 and to 
To improve the accuracy of predicting `volume` based on `adj_close_rolling_med` and `vol_moving_avg`, I have calculated these metrics separately for both stocks and ETFs. 
This decision was made to gain a better understanding of the overall market trends and to consider the unique characteristics of each type of security. 
I have also trained the model separately for both stocks and ETFs because they can have different underlying holdings, 
track different indices, and exhibit different levels of volatility. By processing the data separately, I was able to account for these differences and train more accurate models.

And, to reduce the computational processing, I used randomly select 50% subset of the DataFrame. 
But In actual production env we can take all data by allocating powerful resources.


## How to Use
To predict volume using the machine learning model, cd to the src and pipeline directory and run `uvicorn app:app --reload`.

### Also for model building I  have used XGBOOST instead of RandomForest:
- XGBoost is highly scalable and capable of handling large datasets with millions of rows and columns.
- XGBoost outperforms other algorithms in terms of accuracy and speed when dealing with complex, structured data problems.
- The gradient boosting approach used by XGBoost iteratively corrects errors made by the previous model, which reduces the likelihood of overfitting and underfitting. In contrast, random forests build independent trees that are more prone to these issues.
- XGBoost is faster than random forests for a large number of data points.
- XGBoost provides a better way to measure feature importance by computing it based on the frequency of feature usage in decision trees, which is useful for feature selection and dimensionality reduction. Random forests measure feature importance based on the reduction in impurity achieved by the feature.

### In order to complete this project I utilized following sites:
- https://airflow.apache.org/docs/docker-stack/index.html
- https://github.com/apache/airflow-client-python/blob/main/airflow_client/README.md
- https://spark.apache.org/docs/latest/api/python/
- https://spark.apache.org/docs/latest/sql-programming-guide.html
