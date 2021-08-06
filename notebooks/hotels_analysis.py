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

hotel_weather_delta \
  .select("id", "address", "wthr_date", "avg_tmpr_c", "geohash") \
  .withColumnRenamed("address", "name") \
  .withColumnRenamed("id", "hotel_id") \
  .withColumnRenamed("name", "hotel_name") \
  .show()

# COMMAND ----------

# Top 10 hotels with max absolute temperature difference by month
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.functions import col

window = Window.partitionBy("hotel_id")

hotel_weather_cleaned = hotel_weather_delta \
  .select("id", "address", "avg_tmpr_c") \
  .withColumnRenamed("id", "hotel_id") \
  .withColumnRenamed("address", "hotel_name")

hotels_abs_tmpr_diff = hotel_weather_cleaned \
  .withColumn("max_tmpr_c", f.max("avg_tmpr_c").over(window)) \
  .withColumn("min_tmpr_c", f.min("avg_tmpr_c").over(window)) \
  .withColumn("abs_tmpr_diff_c", f.round(f.abs(col("max_tmpr_c") - col("min_tmpr_c")), scale=1)) \
  .select("hotel_id", "hotel_name", "abs_tmpr_diff_c") \
  .dropDuplicates(["hotel_id", "hotel_name"])

window = Window.orderBy(col("abs_tmpr_diff_c").desc())

top_hotels_abs_tmpr_diff = hotels_abs_tmpr_diff \
  .withColumn("tmpr_diff_rank", f.dense_rank().over(window)) \
  .filter(col("tmpr_diff_rank") <= 10) \
  .orderBy(col("tmpr_diff_rank"))

top_hotels_abs_tmpr_diff.show()
