import unittest
from pyspark.sql import SparkSession
from calculation import FeatureEngineering
import os

class TestFeatureEngineering(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_feature_engineering").getOrCreate()
        self.config_dict = {
            "stocks_parquet_location": "stocks.parquet",
            "etfs_parquet_location": "etfs.parquet",
            "combined_parquet_location": "combined.parquet",
            "stocks_calculate": "stocks_MA_RM",
            "etfs_calculate": "etfs_MA_RM",
            "combined_calculated": "combined_MA_RM"
        }
        self.feature_engineering = FeatureEngineering()

    def test_calculate_moving_avg_and_rolling_median(self):
        # create a temporary test parquet file
        # test_parquet_location = "/path/to/test.parquet"
        test_parquet_location = data_dir = os.path.abspath(os.path.join(os.getcwd(), 'test.parquet'))
        df = self.spark.createDataFrame([(1, "ABC", 100), (1, "ABC", 200), (1, "ABC", 300)], ["Date", "Symbol", "Volume"])
        df.write.parquet(test_parquet_location)
        print(test_parquet_location)
        exit()
        # calculate moving average and rolling median
        self.feature_engineering.calculate_moving_avg_and_rolling_median(test_parquet_location)

        # read the resulting parquet file and check if the values are correct
        calculated_location = "/path/to/calculated.parquet"
        calculated_df = self.spark.read.parquet(calculated_location)
        expected_df = self.spark.createDataFrame([(1, "ABC", 100, 100, 100), (1, "ABC", 200, 150, 150), (1, "ABC", 300, 200, 200)], ["Date", "Symbol", "Volume", "vol_moving_avg", "Adj_close_rolling_med"])
        self.assertEqual(calculated_df.collect(), expected_df.collect())

    def tearDown(self):
        self.spark.stop()

if __name__ == '__main__':
    unittest.main()
