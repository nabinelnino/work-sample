# Stock ETF Pipeline

This project consists of an ETL pipeline to process stocks and ETFs (Exchange Traded Funds) data. The pipeline performs raw data processing and calculates the Moving Average and Rolling Mean of the data.
Note: Since available dataset contains large volume of data, I've decided to use PySpark framework to process data in distributed manner and entire process is dockerized.
## Requirements

- Python 3.6+
- PySpark
- Pandas
- os
- datetime
- sys
- glob
- src.exception.CustomException
- src.logger.logging

## Usage

The pipeline is executed by running the `main.py` script. Before running the script, the following configuration parameters need to be set in the `config_dict` dictionary in the script:

- `stocks_input`: The path to the directory containing the input stocks data in CSV format.
- `etfs_input`: The path to the directory containing the input ETFs data in CSV format.
- `nos_file_to_process`: The number of files to process per iteration.
- `stocks_output`: The path to the directory where the processed stocks data will be stored in Parquet format.
- `etfs_output`: The path to the directory where the processed ETFs data will be stored in Parquet format.
- `stocks_parquet`: The path and prefix of the Parquet files containing the processed stocks data.
- `etfs_parquet`: The path and prefix of the Parquet files containing the processed ETFs data.
- `stocks_etfs_combined`: The path to the directory where the combined stocks and ETFs data will be stored in Parquet format.
- `stocks_calculate`: The path to the directory where the calculated Moving Average and Rolling Mean for the stocks data will be stored in Parquet format.
- `etfs_calculate`: The path to the directory where the calculated Moving Average and Rolling Mean for the ETFs data will be stored in Parquet format.
- `stocks_parquet_location`: The path to the Parquet file containing the processed stocks data.
- `etfs_parquet_location`: The path to the Parquet file containing the processed ETFs data.
- `symbols_meta_file`: The path to the CSV file containing the metadata for the stocks and ETFs.

## Functionality

### Raw Data Processing

The `TaskOne` class is responsible for the raw data processing of both stocks and ETFs data. The `stocks_csv_to_parquet_file` function reads CSV files from the `stocks_input` directory in batches and converts them to Parquet files. The function takes a `symbols_meta_df` dataframe as input, which contains metadata for the stocks and ETFs.

The `convert_data_types` function is used to convert the CSV files to the appropriate data format. The function takes a dataframe as input and returns a processed dataframe.

### Moving Average and Rolling Mean Calculation

The `TaskTwo` class is responsible for calculating the Moving Average and Rolling Mean of both stocks and ETFs data. The `calculate_moving_average` function takes a dataframe as input and returns a new dataframe containing the calculated Moving Average.

The `calculate_rolling_mean` function takes a dataframe as input and returns a new dataframe containing the calculated Rolling Mean.

### Combined Data

The `TaskThree` class is responsible for combining the processed stocks and ETFs data into a single Parquet file. The `combine_stocks_etfs` function reads the processed stocks and ETFs data from the `stocks_parquet_location` and `etfs_parquet_location` files, respectively, and combines them into a single Parquet file.

## Error Handling

The `CustomException` class, defined in `src.exception.py`, is used for error handling. The class extends the built-in `Exception` class and takes a message and a module as input.

The `logging` module, defined in `src.logger.py`, is used for logging. The module logs messages


Combining data from ETFs and stocks together to calculate vol_moving_avg and adj_close_rolling_med can provide a more comprehensive view of the overall market trends, as ETFs can provide exposure to a basket of stocks and can provide a more diversified view of the market.

However, when predicting volume based on adj_close_rolling_med and vol_moving_avg, it is important to consider the individual characteristics of the stocks and ETFs in question. ETFs can have different underlying holdings and can track different indices, which can affect their volatility and the relationship between their price movements and trading volumes. Similarly, individual stocks can have unique characteristics such as their sector, size, and growth prospects, which can also affect their price and volume dynamics.

Therefore, it may be useful to segment the data into different groups based on their characteristics and analyze each group separately to better understand the relationships between the variables and make more accurate predictions. Additionally, it is important to use appropriate statistical methods and machine learning algorithms to analyze the data and make predictions, and to validate the results using appropriate metrics and testing procedures.