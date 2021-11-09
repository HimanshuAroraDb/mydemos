# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration
# MAGIC This notebook performs exploratory data analysis on the dataset.
# MAGIC To expand on the analysis, attach this notebook to the **Test cluster** cluster,
# MAGIC edit [the options of pandas-profiling](https://pandas-profiling.github.io/pandas-profiling/docs/master/rtd/pages/advanced_usage.html), and rerun it.
# MAGIC - Navigate to the parent notebook [here](#notebook/190381220853884)
# MAGIC - Explore completed trials in the [MLflow experiment](#mlflow/experiments/190381220853885/s?orderByKey=metrics.%60val_f1_score%60&orderByAsc=false)
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
training_data_path = client.download_artifacts("2ee7f8ceccde427a83faf15a2c91fda4", "data", temp_dir)
df = pd.read_parquet(os.path.join(training_data_path, "training_data"))

# delete the temp data
shutil.rmtree(temp_dir)

target_col = "quality"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profiling Results

# COMMAND ----------

from pandas_profiling import ProfileReport
df_profile = ProfileReport(df, interactions={"continuous": False}, title="Profiling Report", progress_bar=False)
profile_html = df_profile.to_html()

displayHTML(profile_html)