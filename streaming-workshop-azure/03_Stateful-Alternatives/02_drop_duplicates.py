# Databricks notebook source
# MAGIC %md # Stateful drop duplicates alternatives
# MAGIC Scenario: Suppose you would deduplicate incoming rows (maybe upstream does resend data using at-least-once guarantee), would there be an alternative option to do this without relying on the state store ?

# COMMAND ----------

# MAGIC %run "./includes/setup"

# COMMAND ----------

import pyspark.sql.types as T
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# Prevent the creation of unnecessary small files.
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", True)

# Use RocksDB as state store backend.
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# Clean out old runs.
clean_workspace(user_home)

# COMMAND ----------

# MAGIC %md-sandbox ## Stateful Operator Implementation
# MAGIC 
# MAGIC <img src="https://drive.google.com/uc?id=17boRYqUXexn56J8-Tp8CmG7p06FmeHM5" style="width:40%"/>

# COMMAND ----------

stateful_output_path = "{}/drop_duplicates_alternatives/stateful_operator".format(user_home)
stateful_output_checkpoint = "{}/_checkpoint".format(stateful_output_path)

until_stream_is_ready(
    spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 50)
    .load()
    .withColumn("key", F.col("value") % 50) # Keep generating repeating keys.
    .dropDuplicates(["key"])                # This uses the state store. We are not using a watermark so distinct rows are stored indefinitely to keep for deduplication (which does not scale, alternatively we could add a watermark but then we only partially dedup).
    .writeStream 
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", stateful_output_checkpoint) # State is stored here.
    .start(stateful_output_path)
  
)

# COMMAND ----------

# MAGIC %md ### Output

# COMMAND ----------

display(DeltaTable.forPath(spark, stateful_output_path).toDF().orderBy("key"))

# COMMAND ----------

# MAGIC %md ### Output directory (Delta Lake Table)

# COMMAND ----------

display(dbutils.fs.ls(stateful_output_path))

# COMMAND ----------

# MAGIC %md ### Checkpoint directory

# COMMAND ----------

display(dbutils.fs.ls(stateful_output_checkpoint))

# COMMAND ----------

# MAGIC %md ### State Store files for each of the 200 partitions (output shows partition 0)

# COMMAND ----------

display(dbutils.fs.ls("{}/state/0/0".format(stateful_output_checkpoint)))

# COMMAND ----------

# MAGIC %md-sandbox ## Alternative Implementation (using MERGE instead of Stateful Operators)
# MAGIC 
# MAGIC <img src="https://drive.google.com/uc?id=13Dq8rlNFQEcw5OFe0TTcF2Rzas7KTi0i" style="width:40%"/>

# COMMAND ----------

# MAGIC %md ### Create Delta table that stores state (instead of the state store)

# COMMAND ----------

alternative_output_path = "{}/drop_duplicates_alternatives/alternative".format(user_home)
alternative_output_checkpoint = "{}/_checkpoint".format(alternative_output_path)

alternative_delta_schema = (
  T.StructType([
    T.StructField("timestamp", T.TimestampType()),
    T.StructField("value", T.LongType()),
    T.StructField("key", T.LongType())
])
)

# Create an empty Delta table if it does not exist. This is required for the MERGE to work in the first mini batch.
if not DeltaTable.isDeltaTable(spark, alternative_output_path):
  (
    spark
      .createDataFrame([], alternative_delta_schema)
      .write
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .format("delta")
      .save(alternative_output_path)
  )

# COMMAND ----------

# MAGIC %md ### Define the alternative to stateful dropDuplicates

# COMMAND ----------

def drop_duplicates(updates_df: DataFrame, epoch_id: int):
  
  # Make sure the input is already deduped. 
  # This is also generally required for MERGE with WHEN MATCHED clause (we do not have that here, but do not want to insert duplicates from the same mini batch).
  deduped_df = updates_df.dropDuplicates(["key"])
    
  
  target_table = DeltaTable.forPath(spark, alternative_output_path)
  
  # We merge the new counts with the already existing counts.
  # Note that because this is still APPEND only, it will be implemented as LEFT ANTI JOIN under the hood (much better than full outer).
  (
    target_table.alias("target").merge(
      deduped_df.alias("source"),
      "target.key = source.key") 
    .whenNotMatchedInsertAll()
    .execute()
  )

# COMMAND ----------

# MAGIC %md ### Run the alternative

# COMMAND ----------

until_stream_is_ready(
    spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 50)
    .load()
    .withColumn("key", F.col("value") % 50) # Keep generating repeating keys.
    .writeStream 
    .foreachBatch(drop_duplicates)
    .outputMode("append")
    .option("checkpointLocation", alternative_output_checkpoint) # Only commits/offsets are stored here, not the actual dedup state.
    .start(alternative_output_path)
)

# COMMAND ----------

# MAGIC %md ### Alternative Output

# COMMAND ----------

display(DeltaTable.forPath(spark, alternative_output_path).toDF().orderBy("key"))

# COMMAND ----------

# MAGIC %md ### Alternative Output directory (Delta Lake Table)

# COMMAND ----------

display(dbutils.fs.ls(alternative_output_path))

# COMMAND ----------

# MAGIC %md ### Alternative Checkpoint directory (see that "state" sub directory is not present)

# COMMAND ----------

display(dbutils.fs.ls(alternative_output_checkpoint))

# COMMAND ----------

# MAGIC %md # Clean up

# COMMAND ----------

for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream

# Clean out old runs.
clean_workspace(user_home)

# COMMAND ----------

# MAGIC %md # Summary of benefits
# MAGIC 
# MAGIC ## Stateful Operator
# MAGIC 1. In-memory backed state store (RocksDB) which can be faster than going to disk.
# MAGIC 2. Code is more concise.
# MAGIC 3. Natively supports dropping of late data.
# MAGIC 
# MAGIC ## Alternative
# MAGIC 1. State is queryable as a Delta table.
# MAGIC 2. State will not need to be rebuilt after [making changes that require a wipe of the checkpoint](https://docs.databricks.com/spark/latest/structured-streaming/production.html#recover-after-changes-in-a-streaming-query).
# MAGIC 3. State can grow a lot without requiring a super big cluster.
# MAGIC 4. Can be cheaper as state store backing stateful operator generates a lot of small files, which can incur higher object store / list / put cost.