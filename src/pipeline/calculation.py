from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import input_file_name
import pandas as pd
import os
import kaggle
import zipfile
from pyspark.sql.functions import regexp_replace, date_format, when, lit, col, avg, percentile_approx
import sys
import glob
from exception import CustomException
from logger import logging

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName('Stock ETF Pipeline').getOrCreate()

data_dir = os.path.abspath(os.path.join(os.getcwd(), 'data'))

config_dict = {
    "data": "/data/input_data/",
    "stocks_input": "/input_data/stocks/",
    "etfs_input": "/input_data/etfs/",
    "nos_file_to_process": 1000,
    "stocks_output": "/processed_data/stocks/",
    "etfs_output": "/processed_data/etfs/",

    "stocks_parquet": "/processed_data/stocks/stocks",
    "etfs_parquet": "/processed_data/etfs/",
    "stocks_etfs_combined": "/processed_data/task2/combined_file/",
    "combined_parquet_location": "/processed_data/combined_parquet/combined.parquet",

    "stocks_calculate": "processed_data/task2/stocks_MA_RM/",
    "etfs_calculate": "/processed_data/task2/etfs_MA_RM/",
    "combined_calculated": "/processed_data/task2/combined/",

    "stocks_parquet_location": "/processed_data/stocks/stocks.parquet",
    "etfs_parquet_location": "/processed_data/etfs/etfs.parquet",

    "symbols_meta_file": "/symbols_valid_meta.csv",
    "KAGGLE_API": "/KAGGLE.json",
}


def download_data():
    os.environ['KAGGLE_CONFIG_DIR'] = f"{data_dir}/{config_dict['KAGGLE_API']}"
    kaggle.api.dataset_download_files("https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset")
    with zipfile.ZipFile('stock-market-dataset.zip', 'r') as zip_ref:
        zip_ref.extractall(f"{data_dir}/{config_dict['data']}")


# Problem 1: Raw Data Processing
# For problem 1 I have decided to process both stocks and etfs data separately and convert them into individual parquet
# files and stored inside processed_data

class ConvertToStructured:
    def __init__(self):
        self.config_dict = config_dict
        # symbols_meta_file = config_dict['symbols_meta_file']
        symbols_meta_file = f"{data_dir}/{config_dict['symbols_meta_file']}"
        print(data_dir)
        symbols_meta_df = spark.read.option("header", True).csv(symbols_meta_file)
        # return symbols_meta_df
        print(symbols_meta_df.show(10))
        # self.combined_etfs_stocks()
        # self.stocks_csv_to_parquet_file(symbols_meta_df)
        # self.etf_csv_to_parquet_file(symbols_meta_df)

    def convert_data_types(self, df):
        """
        Convert the csv files to appropriate data formnat
        :param df: dataframe to be processed
        :return: dataframe of processed data
        """
        try:

            df = df.withColumn('Symbol',
                               when(col('Symbol').cast('string').isNotNull(), col('Symbol').cast('string')).otherwise(
                                   None))
            df = df.withColumn("Security Name", col("Security Name").cast("string"))
            df = df.withColumn("Date", date_format("Date", "yyyy-MM-dd"))
            df = df.withColumn('Open',
                               when(col('Open').cast('float').isNotNull(), col('Open').cast('float')).otherwise(None))
            df = df.withColumn('High',
                               when(col('High').cast('float').isNotNull(), col('High').cast('float')).otherwise(None))
            df = df.withColumn('Low',
                               when(col('Low').cast('float').isNotNull(), col('Low').cast('float')).otherwise(None))
            df = df.withColumn('Close',
                               when(col('Close').cast('float').isNotNull(), col('Close').cast('float')).otherwise(None))
            df = df.withColumn('Adj Close',
                               when(col('Adj Close').cast('float').isNotNull(),
                                    col('Adj Close').cast('float')).otherwise(
                                   None))
            df = df.withColumn('Volume',
                               when(col('Volume').cast('int').isNotNull(), col('Volume').cast('int')).otherwise(None))
            return df
        except Exception as e:
            raise CustomException(e, sys)

    def stocks_csv_to_parquet_file(self, symbols_meta) -> None:
        """
        Read csv files from stocks folder in a batch and convert into single parquet file

        """
        try:
            stocks_input = f"{data_dir}/{config_dict['stocks_input']}"
            stocks_output = f"{data_dir}/{config_dict['stocks_output']}"
            symbols_meta_df = symbols_meta
            csv_files = glob.glob(stocks_input + "/*.csv")

            if not os.path.exists(stocks_output):
                os.makedirs(stocks_output)

            parquet_file_name = f"{stocks_output}stocks.parquet"
            batch_size = config_dict['nos_file_to_process']
            logging.info(
                f"Changing stocks csv files to parquet files at a rate of {config_dict['nos_file_to_process']}"
                f"files per iteration")

            for i in range(0, len(csv_files), batch_size):
                batch_files = csv_files[i:i + batch_size]
                df = spark.read.option("header", True).csv(batch_files)
                df = df.withColumn("Symbol", lit(regexp_replace(input_file_name(), r'.*/|\..*', '')))
                joined_df = df.join(symbols_meta_df.select("Symbol", "Security Name"), "Symbol", "left")
                df = joined_df.select("*")
                df = self.convert_data_types(df)
                if i == 0:
                    df.write.mode("overwrite").parquet(parquet_file_name)
                    df_one_here = pd.read_parquet(parquet_file_name)
                    print(parquet_file_name)
                    print("total form parquet", df_one_here.shape[0], df.count())
                else:
                    df.coalesce(1).write.mode("append").parquet(parquet_file_name)
                    df_one_here = pd.read_parquet(parquet_file_name)
            df_one_here = pd.read_parquet(parquet_file_name)
            logging.info(
                f" all csv files are converted successfully into parquet files... total records are {df_one_here.shape[0]}"
                f"")

        except Exception as e:
            raise CustomException(e, sys)

    def etf_csv_to_parquet_file(self, symbols_meta_df) -> None:
        """
        Read csv files from etfs folder in a batch and convert into single parquet file

        """
        try:

            etfs_input = f"{data_dir}/{config_dict['etfs_input']}"
            etfs_output = f"{data_dir}/{config_dict['etfs_output']}"
            symbols_meta_df = symbols_meta_df
            csv_files = glob.glob(etfs_input + "/*.csv")
            if not os.path.exists(etfs_output):
                os.makedirs(etfs_output)

            parquet_file_name = f"{etfs_output}etfs.parquet"
            batch_size = config_dict['nos_file_to_process']
            logging.info(
                f"Changing  etfs csv files to parquet files at a rate of {batch_size}"
                f"files per iteration")

            for i in range(0, len(csv_files), batch_size):
                batch_files = csv_files[i:i + batch_size]
                df = spark.read.option("header", True).csv(batch_files)
                df = df.withColumn("Symbol", lit(regexp_replace(input_file_name(), r'.*/|\..*', '')))
                joined_df = df.join(symbols_meta_df.select("Symbol", "Security Name"), "Symbol", "left")
                df = joined_df.select("*")
                df = self.convert_data_types(df)
                if i == 0:
                    df.write.mode("overwrite").parquet(parquet_file_name)
                    df_one_here = pd.read_parquet(parquet_file_name)
                    print(parquet_file_name)
                    print("total form parquet", df_one_here.shape[0], df.count())
                else:
                    df.write.mode("append").parquet(parquet_file_name)
                    df_one_there = pd.read_parquet(parquet_file_name)

            df_one = pd.read_parquet(parquet_file_name)
            logging.info(
                f" all csv files are converted successfully into parquet files... total records are {df_one.shape[0]}"
                f"")
        except Exception as e:
            raise CustomException(e, sys)

    def combined_etfs_stocks(self):
        stocks_output = f"{data_dir}/{config_dict['stocks_parquet_location']}"
        etfs_output = f"{data_dir}/{config_dict['etfs_parquet_location']}"
        combined_parquet_location = f"{data_dir}/{config_dict['combined_parquet_location']}"
        if not os.path.exists(combined_parquet_location):
            os.makedirs(combined_parquet_location)
        stocks_df = spark.read.parquet(stocks_output)

        etfs_df = spark.read.parquet(etfs_output)

        # Combine the two DataFrames
        combined_df = stocks_df.union(etfs_df)
        combined_df.write.mode("append").parquet(combined_parquet_location)
        # Count the number of rows in the combined DataFrame
        row_count = combined_df.count()
        df = spark.read.parquet(combined_parquet_location)

        # Count the number of rows in the DataFrame
        row_count_after_combined = df.count()
        # Print the row count
        print("Row count:", row_count)
        print("Row count:", row_count_after_combined)
        # 23820922


class FeatureEngineering:
    def __init__(self):
        self.config_dict = config_dict
        # Calculating moving average and rolling median for `stocks` and store result as a parquet file
        stocks_parquet_location = f"{data_dir}/{config_dict['stocks_parquet_location']}"
        etfs_parquet_location = f"{data_dir}/{config_dict['etfs_parquet_location']}"
        combined_parquet_location = f"{data_dir}/{config_dict['combined_parquet_location']}"
        self.calculate_moving_avg_and_rolling_median(combined_parquet_location)
        self.calculate_nos()
        location_file = [stocks_parquet_location, etfs_parquet_location]
        for file in location_file:
            self.calculate_moving_avg_and_rolling_median(file)

    def calculate_moving_avg_and_rolling_median(self, input_file):
        """
        calculate moving average and rolling median of provided parquet files and adds
        vol_moving_avg and Adj_close_rolling_med in the existing data
        :param file_directory: parquet file location
        :return: None
        """
        try:
            path = input_file
            print(path)
            calculated_stocks_location = f"{data_dir}/{config_dict['stocks_calculate']}"
            calculated_etfs_location = f"{data_dir}/{config_dict['etfs_calculate']}"
            calculated_combined_location = f"{data_dir}/{config_dict['combined_calculated']}"
            if os.path.basename(path) == "stocks.parquet":
                if not os.path.exists(calculated_stocks_location):
                    os.makedirs(calculated_stocks_location)
                output_folder = calculated_stocks_location
            elif os.path.basename(path) == "etfs.parquet":
                if not os.path.exists(calculated_etfs_location):
                    os.makedirs(calculated_etfs_location)
                output_folder = calculated_etfs_location
            else:
                if not os.path.exists(calculated_combined_location):
                    os.makedirs(calculated_combined_location)
                output_folder = calculated_combined_location

            # print(output_folder)

            # Get list of all parquet files in the directory
            parquet_files = glob.glob(os.path.join(path, "*.parquet"))
            parquet_file_name = f"{output_folder}calculated.parquet"
            for parquet_file in parquet_files:
                print(parquet_file)
                df = spark.read.parquet(parquet_file)
                # Create a window of 30 days
                window = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-29, 0)
                # Calculate the 30-day moving average of Volume for each Symbol
                df = df.withColumn("vol_moving_avg", avg(col("Volume")).over(window))
                # Calculate the rolling median of Adj Close for each Symbol using the window
                rolling_median = percentile_approx(col("Adj Close"), 0.5).over(window).alias("adj_close_rolling_med")
                df = df.withColumn("Adj_close_rolling_med", rolling_median)
                # Append to an existing one
                df.write.mode("append").parquet(parquet_file_name)
            print(f"Parquet file created at {parquet_file_name}")
            print(f"Total number of records: {df.count()}")

        except Exception as e:
            raise CustomException(e, sys)
