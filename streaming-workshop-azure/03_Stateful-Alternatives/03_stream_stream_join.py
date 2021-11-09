# Databricks notebook source
# MAGIC %md # Stream-Stream join alternatives
# MAGIC 
# MAGIC **Note: stream-stream joins are stateful operators, while stream-static joins are not!**
# MAGIC 
# MAGIC Scenario: Suppose you have two streams of data coming in that you want to join, would there be an alternative option to do this without relying on the state store ?

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
# MAGIC Stream-stream joins rely heavily on the state store. Roughly speaking it looks like the below, storing both sides of the join in the state store in full until a watermark has passed. This means careful tuning is due or you will OOM your cluster.
# MAGIC 
# MAGIC <img src="https://drive.google.com/uc?id=1ILVdJ65nPjEDEyYhfS0Tjjj_Gbyg9R3h" style="width:50%" />

# COMMAND ----------

stateful_output_path = "{}/stream_stream_join_alternatives/stateful_operator".format(user_home)
stateful_output_checkpoint = "{}/_checkpoint".format(stateful_output_path)

sdf1 = (
    spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 50)
    .load()
    .withColumn("key", F.col("value") % 500) # Keep generating repeating keys.
    .withWatermark("timestamp", "0 seconds")
    .alias("left")
)  

sdf2 = (
    spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 50)
    .load()
    .withColumn("key", F.col("value") % 500) # Keep generating repeating keys.
    .withWatermark("timestamp", "0 seconds")
    .alias("right")
)  

  
until_stream_is_ready(
  sdf1.join(sdf2, F.expr("""
    left.key = right.key AND
    left.timestamp >= right.timestamp AND
    left.timestamp <= right.timestamp + interval 1 minutes
    """))                                                     # This is a stateful streaming operator. Note that in a production scenario we have to set watermarks otherwise state will be built indefinitely. We now match on a 1 minute range, and accept no late data.
    .select(
      F.col("left.timestamp").alias("left_time"),
      F.col("right.timestamp").alias("right_time"),
      F.col("left.key").alias("key"),
      F.col("left.value").alias("left_value"),
      F.col("right.value").alias("right_value")
    )
    .writeStream 
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", stateful_output_checkpoint) # State is stored here. Rows per second = 50, so 3000 per minute per stream (in a perfect world with no clock desyncs). In practice aggregation state trails around 7500 (instead of 6000) likely because of clock desyncs.
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

# MAGIC %md-sandbox ## Alternative Implementation 1 (using Stream-Static JOIN instead of Stateful Operators)
# MAGIC Do you really need a stream-stream join (**fast+fast**) or is your data more like joining **slow+fast** / **slow+slow** ?
# MAGIC 
# MAGIC A **stream-static** join can be used if it is OK that only 1 side of the join drives triggering of the output (e.g. a fact table stream that will be joined to a (series of) static dimension tables). 
# MAGIC 
# MAGIC **Note that Delta Lake is required to have the static side of the join always represent the most up to date data (parquet requires a stream restart).**
# MAGIC 
# MAGIC <img src="https://drive.google.com/uc?id=18T0GKyrwjKprCrXn0QzRIY6VYZMlzJbr" style="width:40%" />

# COMMAND ----------

# MAGIC %md ### Generate static side of the join as a Delta Lake table

# COMMAND ----------

alternative_static_input_path = "{}/stream_stream_join_alternatives/static".format(user_home)

(
  spark
  .range(500)
  .withColumnRenamed("id", "value")
  .withColumn("key",  F.col("value"))
  .write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(alternative_static_input_path)
)

# COMMAND ----------

# MAGIC %md ### Start the stream-static join stream

# COMMAND ----------

alternative_output_path = "{}/stream_stream_join_alternatives/alternative".format(user_home)
alternative_output_checkpoint = "{}/_checkpoint".format(alternative_output_path)

streaming_df = (
    spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 50)
    .load()
    .withColumn("key", F.col("value") % 500) # Keep generating repeating keys.
    .withWatermark("timestamp", "0 seconds")
    .alias("left")
)  

# When static side is a Delta Lake table then changes will be automatically picked up without having to restart the stream!
static_df = DeltaTable.forPath(spark, alternative_static_input_path).toDF().alias("right")

until_stream_is_ready(
    streaming_df
    .join(static_df, "key")
    .select(
      F.col("left.timestamp").alias("left_time"),
      F.col("left.key").alias("key"),
      F.col("left.value").alias("left_value"),
      F.col("right.value").alias("right_value")
    )
    .writeStream 
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", alternative_output_checkpoint) # Only commits/offsets are stored here, not the actual state of the join.
    .start(alternative_output_path)
)

# COMMAND ----------

# MAGIC %md ### Alternative 1 Output

# COMMAND ----------

display(DeltaTable.forPath(spark, alternative_output_path).toDF().orderBy("key"))

# COMMAND ----------

# MAGIC %md ### Alternative 1 Output directory (Delta Lake Table)

# COMMAND ----------

display(dbutils.fs.ls(alternative_output_path))

# COMMAND ----------

# MAGIC %md ### Alternative 1 Checkpoint directory (see that "state" sub directory is not present)

# COMMAND ----------

display(dbutils.fs.ls(alternative_output_checkpoint))

# COMMAND ----------

# MAGIC %md-sandbox ## Alternative Implementation 2 (two crossing Stream-Static JOINS)
# MAGIC The **stream-static** join in alternative 2 has the downside that only new data on the left side will trigger output. If you cannot rely on this mechanism alone, you can employ this trick: a **stream-static + static-stream (reverse)** combination. So that both tables can actually be triggering output (at the downside of potential duplicates when both sources update at close to identical intervals). Note that you should keep both streams append only to not generate conflicts between the two streams. The risk of duplicates is minimized when applying a DEDUP logic like in [dedup stream notebook]($./02_drop_duplicates)
# MAGIC 
# MAGIC 
# MAGIC <img src="https://drive.google.com/uc?id=1vgKB2HYeNd6cKHDhOy4cU0otc9MijG8q" style="width:40%" />

# COMMAND ----------

# left as exercise for the reader.

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
# MAGIC ## Stateful Operator (Stream-Stream JOIN) - Driving output with 2 sides of the join
# MAGIC 1. In-memory backed state store (RocksDB) which can be faster than going to disk.
# MAGIC 2. Code is more concise.
# MAGIC 3. Natively supports handling late data.
# MAGIC 4. Output will be triggered by new data coming in either side of the join.
# MAGIC 
# MAGIC ## Alternative 1 (Stream-Static JOIN) - Only driving output with 1 side of the join
# MAGIC 1. No State to manage
# MAGIC 2. State will not need to be rebuilt after [making changes that require a wipe of the checkpoint](https://docs.databricks.com/spark/latest/structured-streaming/production.html#recover-after-changes-in-a-streaming-query).
# MAGIC 3. Can be cheaper as state store backing stateful operator generates a lot of small files, which can incur higher object store / list / put cost.
# MAGIC 
# MAGIC ## Alternative 2 (Stream-Static JOIN + Static-Stream JOIN REVERSE) - Driving output in two separate stream processes
# MAGIC 1. Same as alternative 1, plus
# MAGIC 2. Can have either side of the conceptual join drive output, at the downside of introducing duplicates downstream if both jobs trigger around the same time interval (as Delta Lake does not support transactions cross jobs)