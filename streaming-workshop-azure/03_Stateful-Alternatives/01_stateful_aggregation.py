# Databricks notebook source
# MAGIC %md
# MAGIC # Stateful aggregation alternatives
# MAGIC Scenario: Suppose you would do a running count using stateful streaming aggregations, would there be an alternative option to do this without relying on the state store ?

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
# MAGIC This is roughly what is happening when trying to compute a **groupBy+count** using the stateful API. It will store intermediate counts in the state store and keep joining this to the new data coming in. The number of partitions used in performing the logical join between the new data and existing state will be fixed after stream start (tied to checkpoint), and is tunable using **spark.sql.shuffle.partitions** (default 200).
# MAGIC 
# MAGIC <img src="https://drive.google.com/uc?id=1a91U4JnnlCvbKsLyfPi43iZfM42ranFq" style="width:40%"/>

# COMMAND ----------

stateful_output_path = "{}/stateful_aggregation_alternatives/stateful_operator".format(user_home)
stateful_output_checkpoint = "{}/_checkpoint".format(stateful_output_path)

until_stream_is_ready(
    spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 50)
    .load()
    .withColumn("key", F.col("value") % 50) # Keep generating repeating keys.
    .groupBy("key") 
    .count()                                  # This is stateful aggregation relying on the state store. If checkpoint needs to be reset (for whatever reason) -> state gone, needs to be rebuild.
    .writeStream 
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", stateful_output_checkpoint) # State is stored here.
    .start(stateful_output_path)
  
)

# COMMAND ----------

# MAGIC %md ### Output

# COMMAND ----------

display(DeltaTable.forPath(spark, stateful_output_path).toDF())

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

t = DeltaTable.forPath(spark, stateful_output_path)
display(t.history())

# COMMAND ----------

# MAGIC %md-sandbox ## Alternative Implementation (using MERGE instead of Stateful Operators)
# MAGIC 
# MAGIC This implementation does not rely on the state store. Instead it MERGEs the input with the already existing output to update the counts.
# MAGIC 
# MAGIC <img src="https://drive.google.com/uc?id=108Hd86iqXlL3zJ1Fb_z-LHfqQ1KBWz73" style="width:30%" />

# COMMAND ----------

# MAGIC %md ### Create Delta table that stores state (instead of the state store)

# COMMAND ----------

alternative_output_path = "{}/stateful_aggregation_alternatives/alternative".format(user_home)
alternative_output_checkpoint = "{}/_checkpoint".format(alternative_output_path)

alternative_delta_schema = (
  T.StructType([
    T.StructField("key", T.LongType()),
    T.StructField("count", T.LongType())
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

# MAGIC %md ### Define the alternative to groupBy+count

# COMMAND ----------

def group_by_count(updates_df: DataFrame, epoch_id: int):
  
  # Count the new incoming keys.
  incoming_counts_df = updates_df.groupBy("key").count()
    
  
  target_table = DeltaTable.forPath(spark, alternative_output_path)
  
  # We merge the new counts with the already existing counts.
  (
    target_table.alias("target").merge(
      incoming_counts_df.alias("source"),
      "target.key = source.key") 
    .whenMatchedUpdate(set = { "count" : F.col("source.count") + F.col("target.count") } ) 
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
    .foreachBatch(group_by_count)
    .outputMode("append")
    .option("checkpointLocation", alternative_output_checkpoint) # Only commits/offsets are stored here, not the actual state of the counts.
    .start(alternative_output_path)
)

# COMMAND ----------

# MAGIC %md ### Alternative Output

# COMMAND ----------

display(DeltaTable.forPath(spark, alternative_output_path).toDF().orderBy("count", ascending=False))

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
# MAGIC 3. State can grow a lot without requiring a super big cluster (make sure it is manageable though, or you might end up rewriting a lot of data after every mini batch).
# MAGIC 4. Shuffle partitions used in MERGE can be adapted without requiring a checkpoint restrat (compared to them being fixed in the stateful operator implementation).
# MAGIC 4. Can be cheaper as state store backing stateful operator generates a lot of small files, which can incur higher object store / list / put cost.