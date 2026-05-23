# Databricks notebook source
import urllib
import os
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS 24280060_pa1;
# MAGIC USE CATALOG 24280060_pa1;
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Download the files to a temporary directory first. You need to perform this download within
# MAGIC your notebook using shell and/or dbutils commands.
# MAGIC 2. Load all the downloaded Parquet files into a single Spark DataFrame and add a new column
# MAGIC ingestion_timestamp using the current time.
# MAGIC 3. Write this DataFrame to a Delta table taxi_trips under bronze schema.
# MAGIC 4. Show the transaction log of your table.
# MAGIC 5. Demonstrate "Time Travel" by querying the table using a previous version number or
# MAGIC timestamp to show the state before all files were fully merged.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Create a volume
# MAGIC 2. Downloaded files into the vol
# MAGIC 3. Read from Volume
# MAGIC 4. Write Bronze table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show volumes

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze;
# MAGIC CREATE VOLUME IF NOT EXISTS raw_taxi_data_parquet;

# COMMAND ----------

## downloaded the data in the volume 
urls = [
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-09.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-10.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-11.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-12.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-01.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-02.parquet"
]

for url in urls:
    dbutils.fs.cp(
        url,
        "dbfs:/Volumes/24280060_pa1/bronze/raw_taxi_data_parquet"
    )

# COMMAND ----------

# loaded the data in spark dataframe
df = spark.read.parquet('/Volumes/24280060_pa1/bronze/raw_taxi_data_parquet/*parquet')

# COMMAND ----------

# added a column to the dataframe
df = df.withColumn("ingestion_timestamp", current_timestamp())
df.show()

# COMMAND ----------

# DBTITLE 1,Cell 7
df.columns

# COMMAND ----------

#%pip install dbdemos
#import dbdemos
#dbdemos.install('delta-lake')

# COMMAND ----------

# Write this DataFrame to a Delta table taxi_trips under bronze schema.
df.write.format("delta").mode("overwrite").saveAsTable("24280060_pa1.bronze.taxi_trips")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the transaction log of your table.
# MAGIC DESCRIBE HISTORY 24280060_pa1.bronze.taxi_trips;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- demonstrate "Time Travel" by querying the table using a previous version number or timestamp to show the state before all files were fully merged.
# MAGIC
# MAGIC SELECT * FROM `24280060_pa1`.bronze.taxi_trips VERSION AS OF 0;
# MAGIC DESCRIBE HISTORY 24280060_pa1.bronze.taxi_trips;
# MAGIC