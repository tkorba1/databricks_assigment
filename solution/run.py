from pyspark.sql import SparkSession
from DataProcessor import *

def main():
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Create a DataProcessor
    data_processor = DataProcessor(spark)

    # Download and extract the signals data
    data_processor.download_and_extract_data("https://drive.google.com/u/0/uc?id=1dMOv9BX6G4-hfAT-xiP-zID1_8sRmDNz&export=download", "/tmp/signals.tgz")

    # Read the signals data into a pandas DataFrame
    df = data_processor.read_data_into_pandas("/tmp/signals")

    # Create a Spark DataFrame from the pandas DataFrame
    psdf = data_processor.create_spark_dataframe(df)

    # Perform the transformations on the Spark DataFrame
    final_df = data_processor.process_data(psdf)

    # Save the final DataFrame as a table
    data_processor.save_dataframe_as_table(final_df, "results")

# Usage example
if __name__ == "__main__":
    main()


