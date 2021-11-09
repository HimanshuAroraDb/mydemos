# Databricks notebook source
# MAGIC %md
# MAGIC ## Housekeeping

# COMMAND ----------

dbutils.fs.unmount("/mnt/demo-data1")
dbutils.fs.unmount("/mnt/demo-data2")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## ADLS Access via passthrough
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/security/credential-passthrough/adls-passthrough

# COMMAND ----------

# DBTITLE 1,Direct data access
wine_data_path = "abfss://demo-data@himanshudemostoageacc.dfs.core.windows.net/winequality-red.csv"
wine_df = spark.read.option("header", "true").option("delimiter", ";").csv(wine_data_path)
display(wine_df)

# COMMAND ----------

# DBTITLE 1,Mount ADLS to DBFS
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

dbutils.fs.mount(
  source = "abfss://demo-data@himanshudemostoageacc.dfs.core.windows.net/",
  mount_point = "/mnt/demo-data1",
  extra_configs = configs)

# COMMAND ----------

wine_df = spark.read.option("header", "true").option("delimiter", ";").csv("/mnt/demo-data1/winequality-red.csv")
display(wine_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## ADLS Access via service principal (OAuth 2.0) - *Recommended*
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access

# COMMAND ----------

# Please put client id, secret and tenant id in either databricks secret store or azure key vault
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "19e36f88-b7f5-4970-9f3c-9ff42e21e2ce", 
           "fs.azure.account.oauth2.client.secret": "Y3m5~873oyd~PxU38.DKUb.skc8tQKOOf5",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demo-data@himanshudemostoageacc.dfs.core.windows.net/",
  mount_point = "/mnt/demo-data2",
  extra_configs = configs)

# COMMAND ----------

wine_df = spark.read.option("header", "true").option("delimiter", ";").csv("/mnt/demo-data2/winequality-red.csv")
display(wine_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## ADLS Access via access key

# COMMAND ----------

# Please put access key in either databricks secret store or azure key vault
spark.sparkContext._jsc.hadoopConfiguration().set("fs.azure.account.key.himanshudemostoageacc.dfs.core.windows.net", "DH0lXYM2lq4QNC8rFi9NvVEkSv1cmfMKVzgWhMmXn8xkBRG7femc8QhMBmZes4OK+KZ7q27velq7On0rKoj7sw==")

# COMMAND ----------

wine_data_path = "abfss://demo-data@himanshudemostoageacc.dfs.core.windows.net/winequality-red.csv"
wine_df = spark.read.option("header", "true").option("delimiter", ";").csv(wine_data_path)
display(wine_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## ADLS Access via SAS token
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sas-access