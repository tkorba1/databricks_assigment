import urllib.request
import tarfile
import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, first_value, last_value, desc
from pyspark.sql.window import Window
import functools
import logging
import time

def logging_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        
        logger = logging.getLogger(func.__name__)
        logging.basicConfig(level=logging.INFO)
        logger.info("Function {} is being called".format(func.__name__))
        logging.basicConfig(level=logging.ERROR)
        
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        execution_time = end_time - start_time
        logging.basicConfig(level=logging.INFO)
        logger.info("Function {} has finished executing. Execution time: {:.2f} seconds".format(func.__name__, execution_time))
        logging.basicConfig(level=logging.ERROR)

        return result
    return wrapper

def count_processed_rows(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        
        logger = logging.getLogger(func.__name__)
        input_df = args[1]  
        num_rows = input_df.count()
        logging.basicConfig(level=logging.INFO)
        logger.info("Function {} processed {} rows".format(func.__name__, num_rows))
        logging.basicConfig(level=logging.ERROR)
        return func(*args, **kwargs)
    return wrapper

class DataProcessor:
    def __init__(self, spark):
        self.spark = spark

    @logging_decorator
    def download_and_extract_data(self, url, destination):
        urllib.request.urlretrieve(url, destination)
        with tarfile.open(destination, "r:gz") as tar:
            tar.extractall("/tmp")

    @logging_decorator
    def read_data_into_pandas(self, filepath):
        return pd.read_parquet(filepath)

    @logging_decorator
    def create_spark_dataframe(self, pandas_df):
        return self.spark.createDataFrame(pandas_df)

    @logging_decorator
    @count_processed_rows
    def process_data(self, psdf):
        oldestItemWindow = Window.partitionBy("entity_id").orderBy("month_id", "item_id")
        newestItemWindow = Window.partitionBy("entity_id").orderBy(desc("month_id"), "item_id")

        final_df = psdf.select(
            'entity_id',
            first_value('item_id').over(oldestItemWindow).alias('oldest_item_id'),
            first_value('item_id').over(newestItemWindow).alias('newest_item_id'),
            sum('signal_count').over(Window.partitionBy('entity_id')).alias('total_signals')
        ).distinct()

        return final_df

    @logging_decorator
    @count_processed_rows
    def save_dataframe_as_table(self, df, table_name):
        df.write.mode("overwrite").format('parquet').saveAsTable(table_name)


