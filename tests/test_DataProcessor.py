import unittest
from unittest.mock import patch
from solution.DataProcessor import DataProcessor
from solution.DataProcessor import *
from pyspark.sql import SparkSession
import pandas as pd


class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.data_processor = DataProcessor(self.spark)

    def tearDown(self):
        self.spark.stop()

    @patch('solution.DataProcessor.urllib.request')
    @patch('solution.DataProcessor.tarfile')
    def test_download_and_extract_data(self, mock_urllib, mock_tarfile):
        url = 'http://example.com/data.tar.gz'
        destination = '/path/to/destination'
        self.data_processor.download_and_extract_data(url, destination)
        mock_urllib.urlretrieve.assert_called_once_with(url, f'{destination}/data.tar.gz')
        mock_tarfile.open.assert_called_once_with(f'{destination}/data.tar.gz', 'r:gz')
        mock_tarfile.open.return_value.extractall.assert_called_once_with(destination)

    def test_read_data_into_pandas(self):
        filepath = '/path/to/data.csv'
        pandas_df = self.data_processor.read_data_into_pandas(filepath)
        self.assertIsInstance(pandas_df, pd.DataFrame)

    def test_create_spark_dataframe(self):
        pandas_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        spark_df = self.data_processor.create_spark_dataframe(pandas_df)
        self.assertIsInstance(spark_df, self.spark.DataFrame)

    def test_process_data(self):
        psdf = ps.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        processed_psdf = self.data_processor.process_data(psdf)
        self.assertIsInstance(processed_psdf, ps.DataFrame)

    def test_save_dataframe_as_table(self):
        df = self.spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ['col1', 'col2'])
        table_name = 'my_table'
        self.data_processor.save_dataframe_as_table(df, table_name)
        self.spark.catalog.createTable.assert_called_once_with(table_name, df)

if __name__ == '__main__':
    unittest.main()
