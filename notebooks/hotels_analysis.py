# Databricks notebook source
import configparser

secret = dbutils.secrets.get(scope="abfs-access", key="storage-creds")

config = configparser.ConfigParser()
config.read_string(secret)

class AZStorage:
    IN_STORAGE_ACCOUNT = config["INPUT"]["AZ_STORAGE_ACCOUNT"]
    IN_CONTAINER = config["INPUT"]["AZ_CONTAINER"]
    IN_CLIENT_ID = config["INPUT"]["AZ_CLIENT_ID"]
    IN_CLIENT_SECRET = config["INPUT"]["AZ_CLIENT_SECRET"]
    IN_CLIENT_ENDPOINT = config["INPUT"]["AZ_CLIENT_ENDPOINT"]

    OUT_STORAGE_ACCOUNT = config["OUTPUT"]["AZ_STORAGE_ACCOUNT"]
    OUT_CONTAINER = config["OUTPUT"]["AZ_CONTAINER"]
    OUT_CLIENT_ID = config["OUTPUT"]["AZ_CLIENT_ID"]
    OUT_CLIENT_SECRET = config["OUTPUT"]["AZ_CLIENT_SECRET"]
    OUT_CLIENT_ENDPOINT = config["OUTPUT"]["AZ_CLIENT_ENDPOINT"]

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net", f"{AZStorage.IN_CLIENT_ID}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net", f"{AZStorage.IN_CLIENT_SECRET}")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net", f"{AZStorage.IN_CLIENT_ENDPOINT}")

spark.conf.set(f"fs.azure.account.auth.type.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net", f"{AZStorage.OUT_CLIENT_ID}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net", f"{AZStorage.OUT_CLIENT_SECRET}")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net", f"{AZStorage.OUT_CLIENT_ENDPOINT}")

# COMMAND ----------

IN_STORAGE_URI = f"abfss://{AZStorage.IN_CONTAINER}@{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net"
OUT_STORAGE_URI = f"abfss://{AZStorage.OUT_CONTAINER}@{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net"

# COMMAND ----------

expedia_raw = spark.read.format("avro").load(f"{IN_STORAGE_URI}/expedia")
hotel_weather_raw = spark.read.format("parquet").load(f"{IN_STORAGE_URI}/hotel-weather")

# COMMAND ----------

expedia_raw.write.format("delta").mode("ignore").save(f"{OUT_STORAGE_URI}/expedia-delta")
hotel_weather_raw.write.format("delta").mode("ignore").save(f"{OUT_STORAGE_URI}/hotel-weather-delta")

# COMMAND ----------

expedia_delta = spark.read.format("delta").load(f"{OUT_STORAGE_URI}/expedia-delta")
hotel_weather_delta = spark.read.format("delta").load(f"{OUT_STORAGE_URI}/hotel-weather-delta")

# COMMAND ----------

# Top 10 hotels with max absolute temperature difference by month.
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.functions import col

hotel_weather_cleaned = hotel_weather_delta \
  .select("id", "address", "month", "year", "avg_tmpr_c") \
  .withColumnRenamed("id", "hotel_id") \
  .withColumnRenamed("address", "hotel_name")

hotels_abs_tmpr_diff = hotel_weather_cleaned \
  .groupBy("hotel_id", "hotel_name", "month", "year") \
  .agg(f.max("avg_tmpr_c").alias("max_tmpr_c"), f.min(hotel_weather_cleaned.avg_tmpr_c).alias("min_tmpr_c")) \
  .withColumn("abs_tmpr_diff_c", f.round(f.abs(col("max_tmpr_c") - col("min_tmpr_c")), scale=1)) \
  .select("hotel_id", "hotel_name", "month", "year", "abs_tmpr_diff_c")

window = Window.partitionBy("month", "year").orderBy(col("abs_tmpr_diff_c").desc())

top_hotels_abs_tmpr_diff = hotels_abs_tmpr_diff \
  .withColumn("tmpr_diff_rank", f.dense_rank().over(window)) \
  .filter(col("tmpr_diff_rank") <= 10) \
  .orderBy("year", "month", "tmpr_diff_rank", "hotel_name")

top_hotels_abs_tmpr_diff.show()

# COMMAND ----------

# Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.functions import col

expedia_extended = expedia_delta \
  .select("id", "hotel_id", col("srch_ci").cast("date"), col("srch_co").cast("date")) \
  .filter(col("srch_co") >= col("srch_ci")) \
  .withColumn("stay_months", f.expr("sequence(srch_ci, srch_co, interval 1 month)")) \
  .withColumn("gen_date", f.explode("stay_months")) \
  .select("id", "hotel_id", f.month("gen_date").alias("stay_month"), f.year("gen_date").alias("stay_year"))

hotels_visits = expedia_extended \
  .groupBy("hotel_id", "stay_month", "stay_year") \
  .agg(f.count("id").alias("visits_count"))

window = Window.partitionBy("stay_month", "stay_year").orderBy(col("visits_count").desc())

top_hotels_by_visits = hotels_visits \
  .withColumn("visits_rank", f.dense_rank().over(window)) \
  .filter(col("visits_rank") <= 10) \
  .orderBy("stay_year", "stay_month", "visits_rank")

top_hotels_by_visits.show()

# COMMAND ----------

# For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.
from pyspark.sql import functions as f
from pyspark.sql.window import Window

extended_stays = expedia_delta \
  .select("id", "hotel_id", col("srch_ci").cast("date"), col("srch_co").cast("date")) \
  .withColumnRenamed("id", "visit_id") \
  .filter(col("srch_co") >= col("srch_ci")) \
  .filter(f.datediff("srch_co", "srch_ci") > 7)

hotel_weather_cleaned = hotel_weather_delta \
  .select("id", col("wthr_date").cast("date"), "avg_tmpr_c") \
  .withColumnRenamed("id", "hotel_id")

join_cond = [
  extended_stays.hotel_id == hotel_weather_cleaned.hotel_id,
  (hotel_weather_cleaned.wthr_date >= extended_stays.srch_ci) & (hotel_weather_cleaned.wthr_date <= extended_stays.srch_co)
]
extended_stays_with_weather = extended_stays \
  .hint("range_join", 24 * 60 * 60) \
  .join(hotel_weather_cleaned, join_cond, "inner") \
  .select("visit_id", "wthr_date", "avg_tmpr_c")

visits_weather_trends = extended_stays_with_weather \
  .groupBy("visit_id") \
  .agg(
    f.first("avg_tmpr_c").alias("fd_avg_tmpr_c"), 
    f.last("avg_tmpr_c").alias("ld_avg_tmpr_c"), 
    f.avg("avg_tmpr_c").alias("total_avg_tmpr_c")) \
  .select(
    "visit_id", 
    f.round(col("ld_avg_tmpr_c") - col("fd_avg_tmpr_c"), scale=1).alias("tmpr_trend"),
    f.round("total_avg_tmpr_c", scale=1).alias("total_avg_tmpr_c")) \
  .orderBy("visit_id")

visits_weather_trends.show()

# COMMAND ----------

# Store final DataMarts
top_hotels_abs_tmpr_diff.write \
  .format("delta") \
  .mode("ignore") \
  .partitionBy("year", "month") \
  .save(f"{OUT_STORAGE_URI}/top-hotels-abs-tmpr-diff")

top_hotels_by_visits.write \
  .format("delta") \
  .mode("ignore") \
  .partitionBy("stay_year", "stay_month") \
  .save(f"{OUT_STORAGE_URI}/top-hotels-by-visits")

visits_weather_trends.write \
  .format("delta") \
  .mode("ignore") \
  .save(f"{OUT_STORAGE_URI}/visits-weather-trends")
