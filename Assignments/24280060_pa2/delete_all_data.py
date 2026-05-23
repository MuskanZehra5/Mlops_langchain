# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS 24280060_pa2.bronze.flights;
# MAGIC DROP TABLE IF EXISTS 24280060_pa2.silver.flights_silver;
# MAGIC DROP TABLE IF EXISTS 24280060_pa2.gold.monthly_max_delay;
# MAGIC DROP TABLE IF EXISTS 24280060_pa2.gold.hourly_flights;
# MAGIC DROP TABLE IF EXISTS 24280060_pa2.gold.monthly_delays;
# MAGIC DROP TABLE IF EXISTS 24280060_pa2.gold.biweekly_distance;

# COMMAND ----------

base_path = "/Volumes/24280060_pa2/bronze/temp"

# 🔥 Remove entire volume data
dbutils.fs.rm(base_path, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS 24280060_pa2 CASCADE;