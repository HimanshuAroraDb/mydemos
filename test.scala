// Databricks notebook source
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.himanshuteststorageacc.dfs.core.windows.net", "")

// COMMAND ----------

val df = spark.read.text("abfss://test@himanshuteststorageacc.dfs.core.windows.net/test.rtf")
display(df)