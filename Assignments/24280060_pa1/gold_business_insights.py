# Databricks notebook source
# MAGIC %md
# MAGIC **Goal: Create high-value, aggregated tables for the business.**
# MAGIC 1. Create a table under gold schema that has the Average Fare Amount and Total Trip Count per
# MAGIC PULocationID (Pickup Zone) for each of the months. Name the table avg_fare.
# MAGIC 2. Join your taxi data with the Taxi Zone Lookup Table to replace IDs with actual Borough and Zone
# MAGIC names. Same your table as
# MAGIC 3. Based on the pickup location and drop of locations, group and aggregate data for each to-from
# MAGIC location (total fare paid, total tips paid, total tips, airport fee, etc). Then look up the IDs from the
# MAGIC respective data dictionary and replace location ID with the actual location name. Replace all
# MAGIC IDs with their respective values. Same the table as trip_aggregates_with_locations.
# MAGIC     - You will need the Taxi Zone Lookup Table to replace IDs with actual location and zone
# MAGIC name.
# MAGIC 4. Delta Lake Performance Optimization: Run the OPTIMIZE command on your Gold table and
# MAGIC apply Z-ORDER on a column to speed up time-based queries.
# MAGIC     - Clarify in the notebook which column you picked and why.
# MAGIC     - Demonstrate query timings updates before and after performing z-ordering.

# COMMAND ----------

import requests

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG 24280060_pa1;
# MAGIC USE SCHEMA gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a table under gold schema that has the Average Fare Amount and Total Trip Count per PULocationID (Pickup Zone) for each of the months. Name the table avg_fare.
# MAGIC
# MAGIC create or replace table `24280060_pa1`.gold.avg_fare
# MAGIC using delta 
# MAGIC select PULocationID, avg(fare_amount) as Average_Fare_Amount, count(*) as Total_Trip,
# MAGIC date_format(tpep_pickup_datetime, 'yyyy-MM') as month
# MAGIC from 24280060_pa1.silver.taxi_trips
# MAGIC group by PULocationID, date_format(tpep_pickup_datetime, 'yyyy-MM')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `24280060_pa1`.gold.avg_fare

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze;
# MAGIC CREATE VOLUME IF NOT EXISTS taxi_zone_lookup;

# COMMAND ----------

#downloaded  the data in my bronze volume 
url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

dbutils.fs.cp(url,"dbfs:/Volumes/24280060_pa1/bronze/taxi_zone_lookup")

# COMMAND ----------

lookup_df = spark.read.csv(
    "dbfs:/Volumes/24280060_pa1/bronze/taxi_zone_lookup",header=True,inferSchema=True
)
display(lookup_df)

# COMMAND ----------

lookup_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("24280060_pa1.bronze.taxi_zone_lookup")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 24280060_pa1.bronze.taxi_zone_lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join your taxi data with the Taxi Zone Lookup Table to replace IDs with actual Borough and Zone names. Same your table as Based on the pickup location and drop of locations, group and aggregate data for each to-from location (total fare paid, total tips paid, total tips, airport fee, etc). Then look up the IDs from the respective data dictionary and replace location ID with the actual location name. Replace all IDs with their respective values. Same the table as trip_aggregates_with_locations.You will need the Taxi Zone Lookup Table to replace IDs with actual location and zone name.
# MAGIC
# MAGIC create or replace table trip_aggregates_with_locations
# MAGIC as 
# MAGIC select 
# MAGIC pkp.Zone as pickup_zone,
# MAGIC pkp.Borough as pickup_borough,
# MAGIC dpf.Zone as dropoff_zone,
# MAGIC dpf.Borough as dropoff_borough,
# MAGIC
# MAGIC count(*) AS total_trips,
# MAGIC round(sum(tt.fare_amount), 3) as total_fare_amount,
# MAGIC round(sum(tt.airport_fee), 3) as total_airport_fee,
# MAGIC round(sum(tt.tip_amount),3) as total_tip_amount,
# MAGIC round(sum(tt.total_amount),3) as total_amount_paid,
# MAGIC round(sum(tt.tolls_amount), 3) as total_tolls_amount,
# MAGIC round(sum(tt.mta_tax), 3) as total_mta_tax
# MAGIC
# MAGIC from 
# MAGIC 24280060_pa1.silver.taxi_trips as tt
# MAGIC join 24280060_pa1.bronze.taxi_zone_lookup as pkp
# MAGIC on tt.PULocationID = pkp.LocationID
# MAGIC join 24280060_pa1.bronze.taxi_zone_lookup dpf
# MAGIC on tt.DOLocationID = dpf.LocationID
# MAGIC group by 
# MAGIC pkp.Zone,pkp.Borough , dpf.Zone,dpf.Borough

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 24280060_pa1.gold.trip_aggregates_with_locations
# MAGIC where pickup_zone = 'Little Italy/NoLiTa'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE 24280060_pa1.gold.trip_aggregates_with_locations
# MAGIC ZORDER BY (pickup_zone);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 24280060_pa1.gold.trip_aggregates_with_locations
# MAGIC where pickup_zone = 'Little Italy/NoLiTa'

# COMMAND ----------

# MAGIC %md
# MAGIC > _**I picked the pickup_ location column as in real world scenarios most businesses filter by pickup_location and also it improves the zone based filtering performance
# MAGIC > 
# MAGIC > On observing the query time
# MAGIC >  - before optimize -> 1.60s
# MAGIC >  - after optimize -> 1.43s
# MAGIC > 
# MAGIC > it is observed that the optimize query reduced the execution time due to data skipping and file compaction.**_
# MAGIC
# MAGIC