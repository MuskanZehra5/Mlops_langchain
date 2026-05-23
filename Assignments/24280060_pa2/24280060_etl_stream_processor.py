# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC To implement a decoupled, real-time data pipeline, two separate notebooks were created: data_ingestion and etl_stream_processor. These were orchestrated using a Databricks Workflow Job.
# MAGIC A job was configured in the Databricks Workflows UI with two tasks linked in sequence. The first task runs the data_ingestion notebook, which continuously simulates flight data and CDC updates by writing batched .csv and .json files to their respective landing zones (flight_landing_zone_part2 and cdc_flight_landing_zone_part2). Upon successful completion of the ingestion task, the second task triggers the etl_stream_processor notebook, which reads from these landing zones through the Bronze, Silver, and Gold streaming layers and applies CDC changes to the Silver table via the merge_cdc_to_silver function.
# MAGIC The job was scheduled to run on an hourly interval, ensuring that new data is ingested and processed through the full Medallion architecture every hour without any manual intervention. This setup ensures fault tolerance, as Spark checkpointing guarantees that each stream resumes from where it left off in the event of a failure, and the job scheduler ensures continuous execution.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format, col, to_timestamp, concat, lit, concat_ws, when, array_sort, array, broadcast

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS 24280060_pa2;
# MAGIC USE CATALOG 24280060_pa2;
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS bronze.temp;

# COMMAND ----------

base_path = "/Volumes/24280060_pa2/bronze/temp"


# COMMAND ----------


dbutils.fs.mkdirs(f"{base_path}/flight_landing_zone")
dbutils.fs.mkdirs(f"{base_path}/flight_landing_zone_part2")
dbutils.fs.mkdirs(f"{base_path}/cdc_zone_part2")
dbutils.fs.mkdirs(f"{base_path}/flight_reference_data")
dbutils.fs.mkdirs(f"{base_path}/checkpoints")
dbutils.fs.mkdirs(f"{base_path}/schemas")

# COMMAND ----------

# MAGIC %md
# MAGIC # PART 1: Stream Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Scalable Ingestion with Auto Loader (Bronze Layer)

# COMMAND ----------

checkpoint_path = base_path + "/checkpoints"
schema_path = base_path + "/schemas"
flight_landing_zone_directory_path = base_path + "/flight_landing_zone"

# COMMAND ----------

path = '/databricks-datasets/flights/departuredelays.csv'
flight_landing_zone = base_path + '/flight_landing_zone/departuredelays.csv'

dbutils.fs.cp(path, flight_landing_zone)


# COMMAND ----------

flights_loader = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", f"{schema_path}/flight_landing_zone")
  .option("cloudFiles.schemaEvolutionMode", "rescue")
  .load(flight_landing_zone_directory_path)
)

# COMMAND ----------

flights_loader = flights_loader.withColumn("ingestion_timestamp", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

flights_loader.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpoint_path).trigger(availableNow=True).toTable("24280060_pa2.bronze.flights")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `24280060_pa2`.bronze.flights

# COMMAND ----------

# MAGIC %md
# MAGIC 1.  **What is schemaEvolutionMode used for, and under what circumstances should it be used?**
# MAGIC
# MAGIC schemaEvolutionMode is used to handle schema changes in incoming data files(e.g new columns). It is used when the structure of incoming data may change. For example, in rescue mode, when random new columns are added _rescued_data column so the streaming pipeline continues without failing.
# MAGIC
# MAGIC 2. **What kinds of trigger modes are there, and what is the purpose of each?**
# MAGIC
# MAGIC     4 types of trigger modes 
# MAGIC * processingTime: Runs the streaming query at fixed time intervals
# MAGIC * availableNow: Processes all currently available data and then stops
# MAGIC * once: Processes the available data one time and stops
# MAGIC * continuous: Provides very low latency continuous processing for real-time systems

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Data Cleansing, Enrichment, and Standardization (Silver Layer)

# COMMAND ----------

bronze_data = spark.readStream.table('24280060_pa2.bronze.flights')

# COMMAND ----------

cleaned_data = bronze_data.withColumn("delay", col("delay").cast("int")).withColumn("distance", col("distance").cast("int")).withColumn("origin", col("origin").cast("string")).withColumn("destination", col("destination").cast("string"))

# COMMAND ----------

date_clean = cleaned_data.withColumn("event_timestamp",  to_timestamp(concat(lit("2025"), col("date")), "yyyyMMddHHmm"))

# COMMAND ----------

checkpoint_path_silver = base_path + "/my_checkpoints_silver"


# COMMAND ----------


date_clean.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpoint_path_silver).trigger(availableNow=True).toTable("24280060_pa2.silver.flights_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `24280060_pa2`.silver.flights_silver limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2: Referential Enrichment and Routing Logic

# COMMAND ----------

airport_codes_path = '/databricks-datasets/flights/airport-codes-na.txt'

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets/flights/airport-codes-na.txt")
display(files)

# COMMAND ----------

airport_ref_data = spark.read.option("header", True).option("delimiter", "\t").csv(airport_codes_path)
display(airport_ref_data)

# COMMAND ----------

airport_ref_data = airport_ref_data.select(
    col("IATA").alias("airport_code"),
    col("City").alias("city"),
    col("State").alias("state")
)



# COMMAND ----------

silver_flights_data = spark.readStream.table('24280060_pa2.silver.flights_silver')

# COMMAND ----------

silver_flights_data.printSchema()

# COMMAND ----------


# origin join
my_flights = silver_flights_data.join(
    broadcast(airport_ref_data).alias("orign"),
    silver_flights_data.origin == col("orign.airport_code"),
    "left"
).withColumn(
    "departure",
    concat_ws(", ", col("orign.city"), col("orign.state"))
)

# destination join
my_flights = my_flights.join(
    broadcast(airport_ref_data).alias("dest"),
    my_flights.destination == col("dest.airport_code"),
    "left"
).withColumn(
    "arrival",
    concat_ws(", ", col("dest.city"), col("dest.state"))
)

# nullables filtering
my_flights = my_flights.filter((col("departure").isNotNull()) & (col("arrival").isNotNull())
)


# COMMAND ----------

my_flights.printSchema()

# COMMAND ----------

my_flights = my_flights.select(
    "date",
    "delay",
    "distance",
    "origin",
    "destination",
    "_rescued_data",
    "ingestion_timestamp",
    "event_timestamp",
    col("orign.airport_code").alias("airport_code"),
    col("orign.city").alias("city"),
    col("orign.state").alias("state"),
    "departure",
    "arrival"
)

# COMMAND ----------

my_flights.printSchema()

# COMMAND ----------

my_flights = my_flights.withColumn(
    "take_off_status",
    when(col("delay") < 0, "Early")
    .when((col("delay") >= 0) & (col("delay") < 10), "On Time")
    .when((col("delay") >= 10) & (col("delay") < 30), "Late")
    .otherwise("Delay")
)

# COMMAND ----------

my_flights = my_flights.withColumn(
    "standardized_route", concat_ws("-", array_sort(array(col("origin"), col("destination"))))
)
my_flights = my_flights.withColumn("seq", lit(3))


# COMMAND ----------

# final selection
my_flights = my_flights.select(
    "date",
    "departure",
    "arrival",
    "standardized_route",
    "distance",
    "delay",
    "take_off_status",
    "ingestion_timestamp",
    "event_timestamp",
    "seq"
)

# COMMAND ----------

my_flights.printSchema()

# COMMAND ----------

checkpoint_path_silver_new = base_path + "/my_new_checkpoints_silver1"


# COMMAND ----------

my_flights.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpoint_path_silver_new).trigger(availableNow=True).toTable("24280060_pa2.silver.flights")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `24280060_pa2`.silver.flights limit(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## cdc code

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

from delta.tables import DeltaTable
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window


# COMMAND ----------

cdc_path = "/Volumes/24280060_pa2/bronze/temp/cdc_flight_landing_zone_part2"

cdc_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{schema_path}/flight_landing_zone")
    .load(cdc_path)
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, row_number, from_json
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

## my payload schema for required columns
payload_schema = StructType([
    StructField("date", StringType(), True),
    StructField("departure", StringType(), True),
    StructField("arrival", StringType(), True),
    StructField("standardized_route", StringType(), True),
    StructField("distance", IntegerType(), True),
    StructField("delay", IntegerType(), True),
])

def merge_cdc_to_silver(batch_df, batch_id):

    ## parsing the payload column as JSON string into a struct, then expand as my payload that has standardized route is in json form 
    df = batch_df.select(
        col("action"),
        col("seq").cast("int"),
        from_json(col("payload").cast("string"), payload_schema).alias("payload")
    ).select(
        col("action"),
        col("seq"),
        col("payload.*") 
    )

    # standardized route partition
    window_spec = Window.partitionBy("standardized_route", "date").orderBy(col("seq").desc())

    df_dedup = df.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    silver_table = DeltaTable.forName(spark, "24280060_pa2.silver.flights")

    silver_table.alias("s").merge(
        df_dedup.alias("u"),
        "s.standardized_route = u.standardized_route AND s.date = u.date"
    ).whenMatchedUpdate(
        condition="u.action = 'UPDATE'",
        set={
            "delay": "u.delay",
            "distance": "u.distance",
            "seq": "u.seq"
        }
    ).whenMatchedDelete(condition="u.action = 'DELETE'") \
    .execute()

# COMMAND ----------


cdc_stream.writeStream.foreachBatch(merge_cdc_to_silver).option("checkpointLocation", "/Volumes/24280060_pa2/bronze/temp/checkpoints_cdc").option("cloudFiles.schemaLocation", f"{schema_path}/flight_landing_zone").trigger(availableNow=True).start()

# COMMAND ----------

# MAGIC %md
# MAGIC ## How does a broadcast join optimize performance?
# MAGIC
# MAGIC A broadcast join improves its performance by sending the smaller datasets to all worker nodes, this allows joins to happen locally without data shuffling. This reduces network overhead and speeds up join operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Requirement 1: Average Delay per Month

# COMMAND ----------

from pyspark.sql.functions import window, col, avg, sum, round, date_format, max_by, year, month,lpad

# COMMAND ----------

df_gold = spark.readStream.table("24280060_pa2.silver.flights")

# COMMAND ----------

df_gold.printSchema()

# COMMAND ----------

monthly_delay = df_gold.withColumn("delay_hours", col("delay")/60)

monthly_delay = monthly_delay.withWatermark("event_timestamp", "7 days").groupBy(window("event_timestamp", "30 days")).agg(round(avg("delay_hours"), 2).alias("moving_avg_delay")).selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "moving_avg_delay"
    )

# COMMAND ----------

monthly_delay.writeStream.format("delta").option("checkpointLocation", base_path + "/gold_monthly_checkpoint1").trigger(availableNow=True).toTable("24280060_pa2.gold.monthly_delays")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 24280060_pa2.gold.monthly_delays limit(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Requirement 2: Total Route Distance Every 2 Weeks

# COMMAND ----------

biweekly_distance = df_gold.withWatermark("event_timestamp", "7 days").groupBy(window("event_timestamp", "14 days")).agg(sum("distance").alias("total_distance")).selectExpr(
        "total_distance",
        "window.start as window_start",
        "window.end as window_end")

# COMMAND ----------

biweekly_distance.writeStream.format("delta").option("checkpointLocation", base_path + "/gold_biweekly_checkpoint1").trigger(availableNow=True).toTable("24280060_pa2.gold.biweekly_distance")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 24280060_pa2.gold.biweekly_distance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Requirement 3: Monthly Maximum Delay by Destination

# COMMAND ----------

from pyspark.sql.functions import max_by, max

# COMMAND ----------


monthly_max_delay = df_gold.withColumn("delay_hours", col("delay") / 60).withWatermark("event_timestamp", "7 days").groupBy(window("event_timestamp", "30 days")).agg(
        max_by(col("standardized_route"), col("delay_hours")).alias("worst_route"),
        round(max(col("delay_hours")), 2).alias("max_delay")
    ).selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "worst_route",
        "max_delay"
    )

# COMMAND ----------

monthly_max_delay.writeStream.format("delta").option("checkpointLocation", base_path + "/gold_monthly_max_delay_checkpoint1").trigger(availableNow=True).toTable("24280060_pa2.gold.monthly_max_delay")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 24280060_pa2.gold.monthly_max_delay

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Task: Tumbling Window Aggregation for Hourly Flight Counts (Gold Layer)

# COMMAND ----------

hourly_count = df_gold.withWatermark("event_timestamp", "1 hour").groupBy(window("event_timestamp", "1 hour")).count()

hourly_tumbling_window = hourly_count.selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "count as flight_count",
    )

# COMMAND ----------

hourly_tumbling_window.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", base_path + "/gold_checkpoint_hourly1") \
    .trigger(availableNow=True) \
    .toTable("24280060_pa2.gold.hourly_tumbling_flights")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 24280060_pa2.gold.hourly_tumbling_flights 