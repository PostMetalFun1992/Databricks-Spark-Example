# Databricks notebook source
import configparser

secret = dbutils.secrets.get(scope="abfs-access", key="storage-creds")

config = configparser.ConfigParser()
config.read_string(secret)

print(config.sections())

# COMMAND ----------


