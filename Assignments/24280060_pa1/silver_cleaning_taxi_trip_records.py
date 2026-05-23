# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ## Task: Setup Silver Layer (Cleansing & Refining)
# MAGIC **Note: Data Dictionary for the Yellow Taxi Trips can be retrieved from the below link:** 
# MAGIC https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
# MAGIC ### Goal: Clean the data and ensure it is ready for analysis.
# MAGIC - Perform data quality checks: Filter out records where trip_distance is zero or negative, and
# MAGIC where total_amount is less than 0.
# MAGIC - Standardize the data types for pickup and drop-oƯ times and calculate a new column
# MAGIC trip_duration_minutes.
# MAGIC - Simulate a data update. Pick 10K random records and change their payment_type. Use the
# MAGIC MERGE INTO command to update these records in your Silver table while maintaining ACID
# MAGIC consistency.
# MAGIC - Add a new dummy column (e.g., driver_notes) to your Silver table. Configure your write
# MAGIC operation to allow for Schema Evolution so the table schema updates automatically. Write the
# MAGIC mapped value of RatecodeId to this column.
# MAGIC - Create delta table in your silver layer with data filtered based on vendor id. Name the table as
# MAGIC nyc_yellowtaxi_<vendor_id>. Show percentage of data belongs to each vendor in your
# MAGIC notebooks.
# MAGIC - What is the ratio of tip amount to the fare paid?
# MAGIC - Out of total taxis, what percentage of taxis stored the data in the vehicle memory before
# MAGIC forwarding the saved batch to the server?

# COMMAND ----------

from pyspark.sql.functions import col, unix_timestamp, round, rand, when, lit

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG 24280060_pa1;
# MAGIC USE SCHEMA silver;

# COMMAND ----------

# DBTITLE 1,Untitled
df = spark.table("24280060_pa1.bronze.taxi_trips")
cleaned_df = df.filter((col("trip_distance") > 0) & (col("total_amount") >= 0))


time_df = cleaned_df.withColumn(
    "trip_duration_minutes",
    round(
        (unix_timestamp(col("tpep_dropoff_datetime").cast("timestamp")) -
         unix_timestamp(col("tpep_pickup_datetime").cast("timestamp"))) / 60,
        2
    )
)

time_df = time_df.withColumn("airport_fee", lit(None).cast("double"))


# COMMAND ----------

time_df.columns

# COMMAND ----------

## writing the cleaned data in silver layer
time_df.write.format("delta").mode("overwrite").saveAsTable("24280060_pa1.silver.taxi_trips")

# COMMAND ----------

# Simulate a data update. Pick 10K random records and change their payment_type. 

updates_df = spark.table("24280060_pa1.silver.taxi_trips") \
    .orderBy(rand()) \
    .limit(10000) \
    .withColumn("payment_type", lit(99)) 

# COMMAND ----------

# Simulate a data update. Pick 10K random records and change their payment_type. limiting only on basis of payment type was time consuming so selected vendor id and other attributes to improve performance


updates_df = (
    spark.table("24280060_pa1.silver.taxi_trips").orderBy(rand())
    .limit(10000)
    .select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "payment_type"
    )
    .withColumn("payment_type", (col("payment_type") % 5) + 1)
)

updates_df.createOrReplaceTempView("updates_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize 24280060_pa1.silver.taxi_trips
# MAGIC zorder by (VendorID, tpep_pickup_datetime);

# COMMAND ----------

spark.table("24280060_pa1.silver.taxi_trips").printSchema()


# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists silver.taxi_trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into 24280060_pa1.silver.taxi_trips as target
# MAGIC using updates_view AS source
# MAGIC on target.VendorID = source.VendorID
# MAGIC and target.tpep_pickup_datetime = source.tpep_pickup_datetime
# MAGIC and target.tpep_dropoff_datetime = source.tpep_dropoff_datetime
# MAGIC when matched then
# MAGIC update set target.payment_type = source.payment_type;

# COMMAND ----------

# Add a new dummy column (e.g., driver_notes) to your Silver table. Configure your write operation to allow for Schema Evolution so the table schema updates automatically. Write the mapped value of RatecodeId to this column.
mapped_df = spark.table("24280060_pa1.silver.taxi_trips").withColumn(
        "driver_notes",
        when(col("RatecodeID") == 1, "Standard Rate")
        .when(col("RatecodeID") == 2, "JFK")
        .when(col("RatecodeID") == 3, "Newark")
        .when(col("RatecodeID") == 4, "Nassau/Westchester")
        .when(col("RatecodeID") == 5, "Negotiated Fare")
        .otherwise("Unknown")
    )

# COMMAND ----------

mapped_df.count()

# COMMAND ----------

mapped_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("24280060_pa1.silver.taxi_trips")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(VendorID) from 24280060_pa1.silver.taxi_trips;
# MAGIC
# MAGIC select distinct vendorid from 24280060_pa1.silver.taxi_trips;

# COMMAND ----------

# Create delta table in your silver layer with data filtered based on vendor id. Name the table as nyc_yellowtaxi_<vendor_id>. Show percentage of data belongs to each vendor in your notebooks.
vendors = [1, 2, 4, 5, 6]

for v in vendors:
    spark.sql(f"""
        CREATE OR REPLACE TABLE 24280060_pa1.silver.nyc_yellowtaxi_{v}
        USING DELTA
        AS
        SELECT *
        FROM 24280060_pa1.silver.taxi_trips
        WHERE VendorID = {v}
    """)

# COMMAND ----------

# DBTITLE 1,Show percentage of data belongs to each vendor
# MAGIC %sql
# MAGIC -- Show percentage of data belongs to each vendor in your notebooks.
# MAGIC
# MAGIC WITH total AS (
# MAGIC   SELECT COUNT(*) AS total_count
# MAGIC   FROM 24280060_pa1.silver.taxi_trips
# MAGIC )
# MAGIC SELECT VendorID, COUNT(*) AS vendor_count,
# MAGIC        COUNT(*) * 100.0 / total.total_count AS vendor_percentage
# MAGIC FROM 24280060_pa1.silver.taxi_trips
# MAGIC CROSS JOIN total
# MAGIC GROUP BY VendorID, total.total_count
# MAGIC ORDER BY VendorID

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What is the ratio of tip amount to the fare paid?
# MAGIC select sum(tip_amount)/sum(fare_amount) as ratio from 24280060_pa1.silver.taxi_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Out of total taxis, what percentage of taxis stored the data in the vehicle memory before forwarding the saved batch to the server?
# MAGIC select round(avg(case when store_and_fwd_flag = 'Y' then 1 else 0 end)* 100, 3) as vehicle_memory_pct from 24280060_pa1.silver.taxi_trips 
# MAGIC