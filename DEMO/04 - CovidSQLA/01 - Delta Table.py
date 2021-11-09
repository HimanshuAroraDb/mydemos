# Databricks notebook source
# MAGIC %md
# MAGIC ## DATASET: DAILY COVID-19 CASES IN US STATES
# MAGIC https://www.kaggle.com/sudalairajkumar/covid19-in-usa?select=us_states_covid19_daily.csv

# COMMAND ----------

# DBTITLE 1,Read data from ADLS
daily_covid_data_path = "abfss://demo-data@himanshudemostoageacc.dfs.core.windows.net/us_states_covid19_daily.csv"
daily_covid_df = spark.read.option("header", "true").csv(daily_covid_data_path)
display(daily_covid_df)


# COMMAND ----------

# DBTITLE 1,List databases
# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# DBTITLE 1,Create database for demo
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS himanshu_demo;
# MAGIC USE himanshu_demo;

# COMMAND ----------

# DBTITLE 1,Temp view in memory
daily_covid_df.createOrReplaceTempView("daily_covid_tmp_table")

# COMMAND ----------

# DBTITLE 1,Create delta table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS daily_covid_us_delta;
# MAGIC 
# MAGIC CREATE TABLE daily_covid_us_delta
# MAGIC USING delta
# MAGIC AS SELECT * FROM daily_covid_tmp_table;
# MAGIC 
# MAGIC -- View Delta Lake table
# MAGIC SELECT * FROM daily_covid_us_delta