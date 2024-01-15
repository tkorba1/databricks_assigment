# Databricks notebook source
# MAGIC %md
# MAGIC Setup

# COMMAND ----------

import os

assignment_folder = "/databricks-results/assignment"
if not os.path.exists(assignment_folder):
    dbutils.fs.mkdirs(assignment_folder)

# COMMAND ----------

import requests

url = 'https://drive.google.com/u/0/uc?id=1dMOv9BX6G4-hfAT-xiP-zID1_8sRmDNz&export=download'
filename = '/tmp/signals.tgz'

response = requests.get(url)
with open(filename, 'wb') as f:
    f.write(response.content)


# COMMAND ----------

import tarfile
tar = tarfile.open(filename, "r:gz")
tar.extractall(path='/tmp')
tar.close()


# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/signals
# MAGIC rm /tmp/signals.tgz
# MAGIC

# COMMAND ----------

dbutils.fs.mv("file:/tmp/signals", "dbfs:/tmp/signals", recurse=True)

# COMMAND ----------

display(dbutils.fs.ls('/tmp/signals'))

# COMMAND ----------

#dbutils.fs.rm("/databricks-results/assignment/part-00000-8b43448f-2282-4fe6-a5cc-64cdf61b750e-c000.snappy.parquet")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/learning-spark-v2/people/people-10m.parquet'))

# COMMAND ----------

df = spark.read.parquet('/tmp/signals')
#df = spark.read.parquet('dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.parquet')
display(df)

# COMMAND ----------

from pyspark.sql.functions import (
    sum,
    first_value,
    last_value,
)
import pyspark.sql.functions as sf
from pyspark.sql.window import Window

oldestItemWindow = Window.partitionBy("entity_id").orderBy("month_id", "item_id")
newestItemWindow = Window.partitionBy("entity_id").orderBy(desc("month_id"), "item_id")

final_df=  df.select(
        'entity_id',
        first_value('item_id').over(oldestItemWindow).alias('oldest_item_id'),
        first_value('item_id').over(newestItemWindow).alias('newest_item_id'),
        sum('signal_count').over(Window.partitionBy('entity_id')).alias('total_signals')
        )\
        .distinct()



# COMMAND ----------

final_df.write.mode("overwrite").format('parquet').saveAsTable("output")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from output

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_DATABASE();
# MAGIC DESC EXTENDED OUTPUT
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/output'))
