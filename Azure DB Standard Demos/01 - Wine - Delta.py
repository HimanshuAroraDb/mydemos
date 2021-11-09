# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # What can you do with Delta Lake?
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" width=200/>
# MAGIC 
# MAGIC Delta Lake is an <a href="https://delta.io/" target="_blank">open-source</a> storage layer that brings Reliability and increased Performance to Apache Spark™ and big data workloads. 
# MAGIC 
# MAGIC ## In this notebook, we will create a simple multi-step data pipeline by demonstrating the following features:
# MAGIC 
# MAGIC 0. Reliable batch and streaming capability.
# MAGIC 0. Full DML support for Deletes, Updates and Merge statements.
# MAGIC 0. A <a href="https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html" target="_black"> Schema evolution </a> and enforcement scenario.
# MAGIC 0. Review the <a href="https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html" target="_black"> Time Travel</a> which automatically versions the data that you store in your data lake.
# MAGIC 0. Explore some of the key Delta Lake's <a href="https://docs.databricks.com/delta/optimizations/index.html" target="_black">Optimizations features</a>.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC * This notebook was last tested with *Databricks Community Edition: 6.4 ML, Python 3*

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Build a Reliable Data Lake at Scale
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/Delta Lake Light BG.png" alt='Make all your data ready for BI and ML' width=1000/> 
# MAGIC 
# MAGIC ## With ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake Optimizations
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/Delta medallion icon.png" alt='Make all your data ready for BI and ML' width=1000/>
# MAGIC                                                                                      
# MAGIC                                                                                      
# MAGIC                                                                                      

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## The Data
# MAGIC 
# MAGIC The data used is **red wine quality** data. The data contains many wine characteristics and quality score based on those. Let's read it to find out the structure of this data...

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

dbutils.fs.mount(
  source = "abfss://demo-data@himanshudemostoageacc.dfs.core.windows.net/",
  mount_point = "/mnt/himanshudemostorageacc1",
  extra_configs = configs)

# COMMAND ----------

wine_data_path = "abfss://demo-data@himanshudemostoageacc.dfs.core.windows.net/winequality-red.csv"
wine_df = spark.read.option("header", "true").option("delimiter", ";").csv(wine_data_path)
display(wine_df)

# COMMAND ----------

# MAGIC %md ## A few housekeeping tasks

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# DBTITLE 1,Set the database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS himanshu_demo;
# MAGIC USE himanshu_demo;

# COMMAND ----------

# DBTITLE 1,Create temp view from the wine dataframe
# Create table
wine_df = wine_df \
  .withColumnRenamed("fixed acidity", "fixed_acidity") \
  .withColumnRenamed("volatile acidity", "volatile_acidity") \
  .withColumnRenamed("citric acid", "citric_acid") \
  .withColumnRenamed("residual sugar", "residual_sugar") \
  .withColumnRenamed("free sulfur dioxide", "free_sulfur_dioxide") \
  .withColumnRenamed("total sulfur dioxide", "total_sulfur_dioxide") 
wine_df.createOrReplaceTempView("wine_tmp_table")

# COMMAND ----------

# MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Creating a Delta Lake Table is simple as ...
# MAGIC 
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/simplysaydelta.png" width=600/>

# COMMAND ----------

dbutils.fs.rm('/user/hive/warehouse/himanshu_demo.db/wine_delta', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS wine_delta;
# MAGIC 
# MAGIC CREATE TABLE wine_delta
# MAGIC USING delta
# MAGIC AS SELECT * FROM wine_tmp_table;
# MAGIC 
# MAGIC -- View Delta Lake table
# MAGIC SELECT * FROM wine_delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DETAIL wine_delta

# COMMAND ----------

# DBTITLE 1,Let's look under the hood
dbutils.fs.ls("dbfs:/user/hive/warehouse/himanshu_demo.db/wine_delta")

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/himanshu_demo.db/wine_delta/_delta_log/")

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/himanshu_demo.db/wine_delta/_delta_log/00000000000000000000.json"))

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Unified Batch and Streaming Source and Sink
# MAGIC 
# MAGIC These cells showcase streaming and batch concurrent queries (inserts and reads)
# MAGIC * This notebook will run an `INSERT` every 5s against our `loan_stats_delta` table
# MAGIC * We will run two streaming queries concurrently against this data
# MAGIC * Note, you can also use `writeStream` but this version is easier to run in DBCE

# COMMAND ----------

DELTALAKE_SILVER_PATH = "/user/hive/warehouse/himanshu_demo.db/wine_delta"

# COMMAND ----------

# Read the insertion of data
wine_readStream = spark.readStream.format("delta").load(DELTALAKE_SILVER_PATH)
wine_readStream.createOrReplaceTempView("wine_readStream")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wine_readStream where fixed_acidity > 14

# COMMAND ----------

# MAGIC %md **Wait** until the stream is up and running (map appears) before executing the code below

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Reliably writes to the Delta table in parallel while streaming

# COMMAND ----------

import time
i = 1
while i <= 6:
  # Execute Insert statement
  insert_sql = "INSERT INTO wine_delta VALUES ('100', null, null, null, null, null, null, null, null, null, null, null)"
  spark.sql(insert_sql)
  print('wine delta: inserted new row of data, loop: [%s]' % i)
    
  # Loop through
  i = i + 1
  time.sleep(1)

# COMMAND ----------

# MAGIC %md 
# MAGIC **Note**: Once the previous cell is finished, click *Cancel* in Cell 19 to stop the `readStream`.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Full DML Support
# MAGIC 
# MAGIC Delta Lake supports standard DML including UPDATE, DELETE and MERGE INTO providing developers more controls to manage large datasets.

# COMMAND ----------

# MAGIC %md Let's start by creating a traditional Parquet table

# COMMAND ----------

dbutils.fs.rm('/user/hive/warehouse/himanshu_demo.db/wine_parquet', True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Creating a new parquet table
# MAGIC DROP TABLE IF EXISTS wine_parquet;
# MAGIC 
# MAGIC CREATE TABLE wine_parquet
# MAGIC USING parquet
# MAGIC AS SELECT * FROM wine_delta;
# MAGIC 
# MAGIC -- View Parquet table
# MAGIC SELECT * FROM wine_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL wine_parquet

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) DELETE Support

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `DELETE` on the Parquet table
# MAGIC DELETE FROM wine_parquet WHERE fixed_acidity = 100

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the `DELETE` statements are not supported in Parquet, but are supported in Delta Lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running `DELETE` on the Delta Lake table
# MAGIC DELETE FROM wine_delta WHERE fixed_acidity = 100

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review wine delta lake table
# MAGIC select * from wine_delta where fixed_acidity = 100

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) UPDATE Support

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `UPDATE` on the Parquet table
# MAGIC UPDATE wine_parquet SET pH = 7 WHERE fixed_acidity = 100

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the `UPDATE` statements are not supported in Parquet, but are supported in Delta Lake.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running `UPDATE` on the Delta Lake table
# MAGIC INSERT INTO wine_delta VALUES ('100', null, null, null, null, null, null, null, null, null, null, null);
# MAGIC UPDATE wine_delta SET pH = 7 WHERE fixed_acidity = 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review wine delta lake table
# MAGIC select * from wine_delta where fixed_acidity = 100

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) MERGE INTO Support
# MAGIC 
# MAGIC #### INSERT or UPDATE parquet: 7-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif" alt='Merge process' width=700/>
# MAGIC 
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake
# MAGIC 
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`

# COMMAND ----------

# Let's create a simple table to merge
items = [
  ('200', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null', 'null'),
  ('100', 'null', 'null', 'null', 'null', 'null', 'null', 'null', '3', 'null', 'null', 'null')
]

cols = [
  'fixed_acidity', 'volatile_acidity', 'citric_acid', 'residual_sugar', 'chlorides', 'free_sulfur_dioxide', 'total_sulfur_dioxide', 'density', 'pH', 'sulphates', 'alcohol', 'quality'
]

merge_table = spark.createDataFrame(items, cols)
merge_table.createOrReplaceTempView("merge_table")
display(merge_table)

# COMMAND ----------

# MAGIC %md Instead of writing separate `INSERT` and `UPDATE` statements, we can use a `MERGE` statement. 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO wine_delta as d
# MAGIC USING merge_table as m
# MAGIC on d.fixed_acidity = m.fixed_acidity
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review wine delta lake table
# MAGIC select * from wine_delta where fixed_acidity >= 100

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Schema Evolution
# MAGIC With the `mergeSchema` option, you can evolve your Delta Lake table schema

# COMMAND ----------

# Generate a new/fake column
wine_quality = sql("select *, cast(rand(10) * 10000 * pH as double) as fake_col from wine_delta")
display(wine_quality)

# COMMAND ----------

# Let's write this data out to our Delta table
wine_quality.write.format("delta").mode("append").save(DELTALAKE_SILVER_PATH)

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the schema of our new data does not match the schema of our original data

# COMMAND ----------

# Add the mergeSchema option
wine_quality.write.option("mergeSchema","true").format("delta").mode("append").save(DELTALAKE_SILVER_PATH)

# COMMAND ----------

# MAGIC %md **Note**: With the `mergeSchema` option, we can merge these different schemas together.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review wine delta lake table
# MAGIC select * from wine_delta where fake_col is not null

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Let's Travel back in Time!
# MAGIC Databricks Delta’s time travel capabilities simplify building data pipelines for the following use cases. 
# MAGIC 
# MAGIC * Audit Data Changes
# MAGIC * Reproduce experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/timetravel.png?raw=true" width=250/>
# MAGIC 
# MAGIC You can query by:
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC 
# MAGIC using Python, Scala, and/or Scala syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY wine_delta

# COMMAND ----------

# MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Time Travel via Version Number
# MAGIC Below are SQL syntax examples of Delta Time Travel by using a Version Number

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wine_delta VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wine_delta VERSION AS OF 8

# COMMAND ----------

# MAGIC %md # ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake Optimizations
# MAGIC 
# MAGIC In this Section, we'll examine several features of Delta Lake that make it highly performant for reading data.
# MAGIC 
# MAGIC - Optimize
# MAGIC - Z-Ordering
# MAGIC - Caching
# MAGIC - Data Skipping
# MAGIC 
# MAGIC NOTE: Reference links at the end of the notebook for the complete list of optimization topics.

# COMMAND ----------

# MAGIC %md
# MAGIC ####RUN the OPTIMIZE command on our loan_by_state_delta table and let's see how many files are compacted!

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE wine_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Z-Ordering via Optimize <img src="https://pages.databricks.com/rs/094-YMS-629/images/zorro.jpg?raw=true" width=150/>
# MAGIC 
# MAGIC Zordering is a technique to colocate related information in the same set of files. Zordering maps multidimensional data to one dimension while preserving locality of the data points. Z ordering sorts the data based on the Zorder column specified with in a partition using the popular algorithm(z-order curve). Z order column should be different from the partition column.
# MAGIC 
# MAGIC ``
# MAGIC OPTIMIZE  loan_by_state_delta ZORDER BY (addr_state)
# MAGIC ``
# MAGIC In this example, we have specified only one column (addr_state) for Z-Ordering.  It is possible to specify multiple columns; however, the effectiveness of Z-Ordering diminishes rapidly once we get beyond three or four columns , Z-ordering Works best when data is Partitioned.
# MAGIC 
# MAGIC ## <img src="https://pages.databricks.com/rs/094-YMS-629/images/z-order.png?raw=true" width=450/> 
# MAGIC <a href="https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html" target="_blank"> Refer this blog</a> also note z-ordering via optimize is a databricks custom feature.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE wine_delta ZORDER BY fixed_acidity;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DETAIL wine_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Caching  <img src="https://pages.databricks.com/rs/094-YMS-629/images/treasure.png?raw=true" width=200 />
# MAGIC 
# MAGIC The <a href="https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html" target="_black"> Delta cache </a> accelerates data reads by creating copies of remote files in nodes’ local storage using a fast intermediate data format. The data is cached automatically whenever a file has to be fetched from a remote location. Successive reads of the same data are then performed locally, which results in significantly improved reading speed.
# MAGIC 
# MAGIC Is summary:
# MAGIC 
# MAGIC - __Spark caching__ takes place in __memory__
# MAGIC - __Delta Lake caching__ takes place on the __SSD drives__ of worker machines, and does not "steal" memory from Spark.  SSD drives provide very fast response times, and avoid network usage.

# COMMAND ----------

# MAGIC %md
# MAGIC Then open up __Spark Jobs__ in the cell above, click __View__ in any of the jobs, and click the __Storage__ tab. The data is read from the cache, as illustrated below:
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/cache_write_and_read.png?raw=true" width=800/>
# MAGIC 
# MAGIC For this small demo dataset, there isn't much response time difference.  However, the time savings can be significant for a large dataset, since a great deal of network traffic is eliminated.

# COMMAND ----------

# DBTITLE 1,Disabling Delta Cache
# MAGIC %scala
# MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

# DBTITLE 1,Verify the Delta caching settings
# MAGIC %scala
# MAGIC spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC After disabling the delta cache, open up the Spark Job and click "View" as shown below:
# MAGIC 
# MAGIC #<img src="https://pages.databricks.com/rs/094-YMS-629/images/disablecache.png?raw=true" width=850/>
# MAGIC 
# MAGIC Now go to the "Storage" tab.  You should see something similar to the image below:
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/cache_unused.png?raw=true" width=800 />

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Skipping <img src="https://pages.databricks.com/rs/094-YMS-629/images/skippingstone.jpg?raw=true" width=200/>
# MAGIC 
# MAGIC Databricks' Data Skipping feature takes advantage of the multi-file structure we saw above.  As new data is inserted into a Databricks Delta table, file-level min/max statistics are collected for all columns. Then, when there’s a lookup query against the table, Databricks Delta first consults these statistics in order to determine which files can safely be skipped.  The picture below illustrates the process:
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/dataskipping.png?raw=true" />
# MAGIC 
# MAGIC In this example, we skip file 1 because its minimum value is higher than our desired value.  Similarly, we skip file 3 based on its maximum value.  File 2 is the only one we need to access.
# MAGIC 
# MAGIC For certain classes of queries, Data Skipping can provide dramatically faster response times. Refer to this useful <a https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html</a>blog.