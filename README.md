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

The pipeline is executed by running the `docker-compose up` script. Before running the script, the following configuration parameters need to be set in the `config_dict` dictionary in the script:

- `data`: Path to the input data directory..
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
- `combined_parquet_location`:  Path to the directory where combined stock and ETF data Parquet files will be saved..
- `KAGGLE_API`:  Path to the Kaggle API JSON file.

## Functionality

### Download File

### Raw Data Processing

The `ConvertToStructured` class is responsible for the raw data processing of both stocks and ETFs data. The `stocks_csv_to_parquet_file` function reads CSV files from the `stocks_input` directory in batches and converts them to Parquet files. The function takes a `symbols_meta_df` dataframe as input, which contains metadata for the stocks and ETFs.

The `convert_data_types` function is used to convert the CSV files to the appropriate data format. The function takes a dataframe as input and returns a processed dataframe.

### Moving Average and Rolling Mean Calculation

The `FeatureEngineering` class is responsible for calculating the Moving Average and Rolling Mean of both stocks and ETFs data. The `calculate_moving_average` function takes a dataframe as input and returns a new dataframe containing the calculated Moving Average.

The `calculate_rolling_mean` function takes a dataframe as input and returns a new dataframe containing the calculated Rolling Mean.

## Error Handling

The `CustomException` class, defined in `src.exception.py`, is used for error handling. The class extends the built-in `Exception` class and takes a message and a module as input.

The `logging` module, defined in `src.logger.py`, is used for logging. The module logs messages


## Usage

To run the pipeline, execute the `docker-compose up` file. The pipeline consists of the following steps:

1. Download data from Kaggle.
2. Convert stock and ETF CSV files to Parquet files.
3. Calculate moving averages and rolling means for stock and ETF data.
4. Combine stock and ETF data.
5. Calculate moving averages and rolling means for combined stock and ETF data.
6. Train model for stocks and etfs and stored trained model data into specified location 


### To predict the volume using the machine learning model, navigate to the `src` and `pipeline` directories and execute the command `uvicorn app:app --reload`.

               +----------------------------------+
               |                                  |
               |              Kaggle              |
               |                                  |
               +-----------------+----------------+
                                 |
                                 | download
                                 |
               +-----------------v----------------+
               |                                  |
               |            Raw Data              |
               |           Processing             |
               |                                  |
               +-----------------+----------------+
                                 |
                                 | transform
                                 |
               +-----------------v----------------+
               |                                  |
               |       Moving Average & Rolling   |
               |              Calculation         |
               |                                  |
               +-----------------+----------------+
                                 |
                                 | load
                                 |
               +-----------------v----------------+
               |                                  |
               |            STRUCTURED DATA        |
               |                                  |
               +-----------------+----------------+
                                 |
                                 | pyspark dataframe
                                 |
               +-----------------v----------------+
               |    calculate vol_moving_avg and  |
               |      adj_close_rolling_med       |
               |                                  |
               +-----------------+----------------+    
                                 |
                                 |
                                 | train
                                 |
               +-----------------v----------------+
               |                                  |
               |            Machine Learning       |
               |                                  |
               +-----------------+----------------+
                                 |
                                 | predict
                                 |
               +-----------------v----------------+
               |                                  |
               |            Application            |
               |                                  |
               +-----------------+----------------+
                                 |
                                 | schedule tasks
                                 |
               +-----------------v----------------+
               |                                  |
               |              Airflow             |
               |              Pipeline            |
               |                                  |
               +----------------------------------+ 



