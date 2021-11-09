# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration
# MAGIC This notebook performs exploratory data analysis on the dataset.
# MAGIC To expand on the analysis, attach this notebook to the **mb-demo** cluster,
# MAGIC edit [the options of pandas-profiling](https://pandas-profiling.github.io/pandas-profiling/docs/master/rtd/pages/advanced_usage.html), and rerun it.
# MAGIC - Navigate to the parent notebook [here](#notebook/4092484852385397)
# MAGIC - Explore completed trials in the [MLflow experiment](#mlflow/experiments/4092484852385407/s?orderByKey=metrics.%60val_f1_score%60&orderByAsc=false)
# MAGIC 
# MAGIC Runtime Version: _8.3.x-cpu-ml-scala2.12_

# COMMAND ----------

import os
import uuid
import shutil
import pandas as pd

from mlflow.tracking import MlflowClient

# Download input data from mlflow into a pandas DataFrame
# create temp directory to download data
temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], str(uuid.uuid4())[:8])
os.makedirs(temp_dir)

# download the artifact and read it
client = MlflowClient()
training_data_path = client.download_artifacts("9af1cc7ad29d4a439c733d43430d62f9", "data", temp_dir)
df = pd.read_parquet(os.path.join(training_data_path, "training_data"))

# delete the temp data
shutil.rmtree(temp_dir)

target_col = "onVentilatorCumulative"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profiling Results

# COMMAND ----------

from pandas_profiling import ProfileReport
df_profile = ProfileReport(df, minimal=True, title="Profiling Report", progress_bar=False)
profile_html = df_profile.to_html()

displayHTML(profile_html)