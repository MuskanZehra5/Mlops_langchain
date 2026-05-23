# Databricks notebook source
from pyspark.sql.functions import current_timestamp, date_format, col, to_timestamp, concat, lit, concat_ws, when, array_sort, array,  broadcast, avg, sum, count, max, max_by, round, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

base_path    = "/Volumes/24280060_pa2/bronze/temp"
schema_path  = base_path + "/schemas"
landing_zone = base_path + "/flight_landing_zone"


# COMMAND ----------

# MAGIC %md
# MAGIC ## EC1 - Spark Structured Streaming (15%)

# COMMAND ----------


csv_schema = StructType([
    StructField("date",        StringType(),  True),
    StructField("delay",       IntegerType(), True),
    StructField("distance",    IntegerType(), True),
    StructField("origin",      StringType(),  True),
    StructField("destination", StringType(),  True),
])
#reading data
structured_stream = (
    spark.readStream
    .format("csv")                   
    .schema(csv_schema)              
    .option("header", True)
    .option("latestFirst", False)  
    .load(landing_zone)
)

#ingestion timestamp
structured_stream = structured_stream.withColumn(
    "ingestion_timestamp",
    date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")
)

structured_stream.printSchema()

# COMMAND ----------

# write to a separate Bronze table

structured_stream.writeStream.format("delta").outputMode("append").option("checkpointLocation", base_path + "/checkpoints_structured_stream").trigger(availableNow=True).toTable("24280060_pa2.bronze.flights_structured_stream")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- comparison Auto Loader bronze vs Structured Streaming bronze
# MAGIC select 'Auto Loader' as source, COUNT(*) as row_count from 24280060_pa2.bronze.flights
# MAGIC union all
# MAGIC select 'Structured Streaming' as source, COUNT(*) as row_count from 24280060_pa2.bronze.flights_structured_stream

# COMMAND ----------

# MAGIC %md
# MAGIC ## EC3 - Spark Structured Streaming (15%)

# COMMAND ----------

from pyspark.sql.functions import window, col, avg, sum, round, date_format, max_by, year, month,lpad, count

# COMMAND ----------

df_gold = spark.readStream.table("24280060_pa2.silver.flights")

# COMMAND ----------

df_gold.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sliding Avg Delay

# COMMAND ----------


sliding_avg_delay = (
    df_gold
    .withWatermark("event_timestamp", "8 days")
    .groupBy(
        window("event_timestamp", "7 days", "1 day"))
    .agg(
        round(avg(col("delay") / 60), 2).alias("rolling_avg_delay_hours")
    )
    .selectExpr(
        "date_format(window.start, 'yyyy-MM-dd HH:mm:ss') as window_start",
        "date_format(window.end,   'yyyy-MM-dd HH:mm:ss') as window_end",
        "rolling_avg_delay_hours"
    )
)

# COMMAND ----------


sliding_avg_delay.writeStream.format("delta").outputMode("append").option("checkpointLocation", base_path + "/gold_sliding_avg_delay_checkpoint").trigger(availableNow=True).toTable("24280060_pa2.gold.sliding_avg_delay")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 24280060_pa2.gold.sliding_avg_delay limit(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sliding Route traffic

# COMMAND ----------


sliding_route_traffic = (
    df_gold
    .withWatermark("event_timestamp", "15 days")
    .groupBy(
        window("event_timestamp", "14 days", "7 days"), col("standardized_route")
    )
    .agg(
        count("*").alias("flight_count"),
        round(sum("distance"), 0).alias("total_distance")
    )
    .selectExpr(
        "date_format(window.start, 'yyyy-MM-dd HH:mm:ss') as window_start",
        "date_format(window.end,   'yyyy-MM-dd HH:mm:ss') as window_end",
        "standardized_route",
        "flight_count",
        "total_distance"
    )
)


# COMMAND ----------

sliding_route_traffic.writeStream.format("delta").outputMode("append").option("checkpointLocation", base_path + "/gold_sliding_route_traffic_checkpoint").trigger(availableNow=True).toTable("24280060_pa2.gold.sliding_route_traffic")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 24280060_pa2.gold.sliding_route_traffic

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sliding Hourly status

# COMMAND ----------

from pyspark.sql.functions import max_by, max

# COMMAND ----------

sliding_hourly_status = (
    df_gold
    .withWatermark("event_timestamp", "4 hours")
    .groupBy(
        window("event_timestamp", "3 hours", "1 hour"), col("take_off_status")
    )
    .agg(
        count("*").alias("flight_count")
    )
    .selectExpr(
        "date_format(window.start, 'yyyy-MM-dd HH:mm:ss') as window_start",
        "date_format(window.end,   'yyyy-MM-dd HH:mm:ss') as window_end",
        "take_off_status",
        "flight_count"
    )
)


# COMMAND ----------


sliding_hourly_status.writeStream.format("delta").outputMode("append").option("checkpointLocation", base_path + "/gold_sliding_hourly_status_checkpoint").trigger(availableNow=True).toTable("24280060_pa2.gold.sliding_hourly_status")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 24280060_pa2.gold.sliding_hourly_status