# Databricks notebook source
dbutils.fs.ls("/tmp/pyspark-prototype")

# COMMAND ----------

dbutils.secrets.get(scope="adls", key="himanshu-adls-key")