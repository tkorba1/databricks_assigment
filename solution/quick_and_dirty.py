# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Data engineering assigmment

# COMMAND ----------
# MAGIC %md
# MAGIC ### Import libraries
# COMMAND ----------
import urllib.request
import tarfile
import pandas as pd
import pyspark.pandas as ps
from pyspark.sql.functions import sum, first_value, last_value, desc
import pyspark.sql.functions as sf
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md
# MAGIC ### Download and extract the signals data
# COMMAND ----------
urllib.request.urlretrieve("https://drive.google.com/u/0/uc?id=1dMOv9BX6G4-hfAT-xiP-zID1_8sRmDNz&export=download", "/tmp/signals.tgz")
with tarfile.open("/tmp/signals.tgz", "r:gz") as tar:
    tar.extractall("/tmp")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Read the signals data into a pandas DataFrame
# COMMAND ----------
df = pd.read_parquet("/tmp/signals")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Create a Spark DataFrame from the pandas DataFrame
# COMMAND ----------
psdf = spark.createDataFrame(df)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Define the window specifications

# COMMAND ----------
oldestItemWindow = Window.partitionBy("entity_id").orderBy("month_id", "item_id")
newestItemWindow = Window.partitionBy("entity_id").orderBy(desc("month_id"), "item_id")


# COMMAND ----------
# MAGIC %md
# MAGIC ### Perform the transformations on the Spark DataFrame

# COMMAND ----------
final_df = psdf.select(
    'entity_id',
    first_value('item_id').over(oldestItemWindow).alias('oldest_item_id'),
    first_value('item_id').over(newestItemWindow).alias('newest_item_id'),
    sum('signal_count').over(Window.partitionBy('entity_id')).alias('total_signals')
).distinct()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Save the final DataFrame as a table

# COMMAND ----------
final_df.write.mode("overwrite").format('parquet').saveAsTable("results")


# COMMAND ----------
# MAGIC %md
# MAGIC ### Execute a SQL query on the table

# COMMAND ----------
%sql
select * from results

