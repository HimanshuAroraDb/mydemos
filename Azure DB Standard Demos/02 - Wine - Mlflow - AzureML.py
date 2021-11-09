# Databricks notebook source
# MAGIC %md
# MAGIC ## Train & Track model using Mlflow and Sklean & Deploy on Spark, Azure ML using ACI & AKS
# MAGIC <img src="https://github.com/HimanshuAroraDb/Images/blob/master/azureml.png?raw=true" />

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql USE himanshu_demo;

# COMMAND ----------

import pandas as pd

wine_df = spark.sql("SELECT * FROM wine_delta VERSION AS OF 0")
df = wine_df.toPandas()
display(df)

# COMMAND ----------

import os
import mlflow

model_path = "model"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training & tracking a model

# COMMAND ----------

import os
import warnings
import sys

import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet

import mlflow
import mlflow.sklearn

def eval_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2

# L1, L2 are regularisation parameters work as penalties in calculation of loss function.
# hyperparameter “alpha” is provided to assign how much weight is given to each of the L1 and L2 penalties.
def train_model(data, model_path, alpha, l1_ratio):
    warnings.filterwarnings("ignore")
    np.random.seed(40)

    # Split the data into training and test sets. (0.75, 0.25) split.
    train, test = train_test_split(data)

    # The predicted column is "quality" which is a scalar from [3, 9]
    train_x = train.drop(["quality"], axis=1)
    test_x = test.drop(["quality"], axis=1)
    train_y = train[["quality"]]
    test_y = test[["quality"]]

    with mlflow.start_run():
        lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        lr.fit(train_x, train_y)

        predicted_qualities = lr.predict(test_x)

        (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)

        print("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
        print("  RMSE: %s" % rmse)
        print("  MAE: %s" % mae)
        print("  R2: %s" % r2)

        mlflow.log_param("alpha", alpha)
        mlflow.log_param("l1_ratio", l1_ratio)
        mlflow.log_param("delta_version", 0)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)

        mlflow.sklearn.log_model(lr, model_path)
        
        return mlflow.active_run().info.run_uuid

# COMMAND ----------

alpha_1 = 0.75
l1_ratio = 0.25
run_id1 = train_model(data=df, model_path=model_path, alpha=alpha_1, l1_ratio=l1_ratio)

# COMMAND ----------

alpha_2 = 0.25
l1_ratio = 0.8
run_id2 = train_model(data=df, model_path=model_path, alpha=alpha_2, l1_ratio=l1_ratio)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Register models to MLflow Model Registry
# MAGIC 
# MAGIC The MLflow Model Registry component is a centralized model store, set of APIs, and UI, to collaboratively manage the full lifecycle of an MLflow Model. It provides model lineage (which MLflow experiment and run produced the model), model versioning, stage transitions (for example from staging to production), and annotations
# MAGIC 
# MAGIC ![](https://quentin-demo-resources.s3.eu-west-3.amazonaws.com/images/mlflow-repository.png)

# COMMAND ----------

#Get the model from "production" stage
from mlflow.tracking.client import MlflowClient

model_name = "WineModel"

model_production_uri = "models:/{model_name}/production".format(model_name=model_name)
print(model_production_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC ** Example to invoke Rest endpoint** 
# MAGIC 
# MAGIC [
# MAGIC    {
# MAGIC       "fixed_acidity":"6.1",
# MAGIC       "volatile_acidity":"6.1",
# MAGIC       "citric_acid":"6.1",
# MAGIC       "residual_sugar":"6.1",
# MAGIC       "chlorides":"6.1",
# MAGIC       "free_sulfur_dioxide":"6.1",
# MAGIC       "total_sulfur_dioxide":"6.1",
# MAGIC       "density":"6.1",
# MAGIC       "pH":"6.1",
# MAGIC       "sulphates":"6.1",
# MAGIC       "alcohol":"6.1"
# MAGIC    }
# MAGIC ]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Deploy in spark batch

# COMMAND ----------

# Load model as a Spark UDF.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri = model_production_uri)

# COMMAND ----------

display(wine_df.drop('quality').withColumn('my_predictions', loaded_model(
  'fixed_acidity',
  'volatile_acidity',
  'citric_acid',
  'residual_sugar',
  'chlorides',
  'free_sulfur_dioxide',
  'total_sulfur_dioxide',
  'density',
  'pH',
  'sulphates',
  'alcohol'
)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serving Models with Microsoft Azure ML

# COMMAND ----------

# MAGIC %md #### Install the Azure ML SDK
# MAGIC 
# MAGIC Once a cluster has been launched with the configuration described in **Launch an Azure Databricks cluster**, install the Azure Machine Learning SDK using the following steps:
# MAGIC 
# MAGIC 1. Create the library with the Source ``Upload Python Egg or PyPI`` and the Pip library name:
# MAGIC   - `azureml-sdk[databricks]`     
# MAGIC      
# MAGIC 2. Attach the library to the cluster.

# COMMAND ----------

# MAGIC %md #### Create or load an Azure ML Workspace
# MAGIC 
# MAGIC Before models can be deployed to Azure ML, an Azure ML Workspace must be created or obtained. The `azureml.core.Workspace.create()` function will load a workspace of a specified name or create one if it does not already exist. For more information about creating an Azure ML Workspace, see the [Azure ML Workspace management documentation](https://docs.microsoft.com/en-us/azure/machine-learning/service/how-to-manage-workspace).

# COMMAND ----------

import azureml
from azureml.core import Workspace

workspace_name = "himanshuamlws"
workspace_location = "westeurope"
resource_group = "himanshudemorg"
subscription_id = "3f2e4d32-8e8d-46d6-82bc-5bb8d962328b"

workspace = Workspace.create(name = workspace_name,
                             subscription_id = subscription_id,
                             resource_group = resource_group,
                             location = workspace_location,
                             exist_ok=True)

# COMMAND ----------

# MAGIC %md #### Building an Azure Container Image for model deployment
# MAGIC 
# MAGIC **Use MLflow to build a Container Image for the trained model**
# MAGIC 
# MAGIC We will use the `mlflow.azuereml.build_image` function to build an Azure Container Image for the trained MLflow model. This function also registers the MLflow model with a specified Azure ML workspace. The resulting image can be deployed to Azure Container Instances (ACI) or Azure Kubernetes Service (AKS) for real-time serving.

# COMMAND ----------

import mlflow.azureml

model_image, azure_model = mlflow.azureml.build_image(model_uri="models:/WineModel/production", 
                                                      workspace=workspace, 
                                                      model_name="wine-model",
                                                      image_name="wine-model-container-image",
                                                      description="wine quality prediction model",
                                                      synchronous=False)

# COMMAND ----------

model_image.wait_for_creation(show_output=True)

# COMMAND ----------

# MAGIC %md #### Deploying the model to dev using [Azure Container Instances (ACI)](https://docs.microsoft.com/en-us/azure/container-instances/)
# MAGIC 
# MAGIC The [ACI platform](https://docs.microsoft.com/en-us/azure/container-instances/) is the recommended environment for staging and developmental model deployments.

# COMMAND ----------

# MAGIC %md #### Create an ACI webservice deployment using the model's Container Image
# MAGIC 
# MAGIC Using the Azure ML SDK, we will deploy the Container Image that we built for the trained MLflow model to ACI.

# COMMAND ----------

from azureml.core.webservice import AciWebservice, Webservice

dev_webservice_name = "wine-model-ws"
dev_webservice_deployment_config = AciWebservice.deploy_configuration()
dev_webservice = Webservice.deploy_from_image(name=dev_webservice_name, image=model_image, deployment_config=dev_webservice_deployment_config, workspace=workspace)

# COMMAND ----------

dev_webservice.wait_for_deployment()

# COMMAND ----------

# MAGIC %md #### Querying the deployed model

# COMMAND ----------

# MAGIC %md #### Load a sample input vector from the dataset

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn import datasets

train, _ = train_test_split(df)
train_x = train.drop(["quality"], axis=1)
sample = train_x.iloc[[0]]
query_input = sample.to_json(orient='split')

print("Using input vector: {}".format(query_input))

# COMMAND ----------

# MAGIC %md #### Evaluate the sample input vector by sending an HTTP request
# MAGIC We will query the ACI webservice's scoring endpoint by sending an HTTP POST request that contains the input vector.

# COMMAND ----------

import requests
import json

def query_endpoint_example(scoring_uri, inputs, service_key=None):
  headers = {
    "Content-Type": "application/json",
  }
  if service_key is not None:
    headers["Authorization"] = "Bearer {service_key}".format(service_key=service_key)
    
  print("Sending batch prediction request with inputs: {}".format(inputs))
  response = requests.post(scoring_uri, data=inputs, headers=headers)
  print("Response: {}".format(response.text))
  preds = json.loads(response.text)
  print("Received response: {}".format(preds))
  return preds

# COMMAND ----------

#dev_scoring_uri = dev_webservice.scoring_uri
dev_scoring_uri = "http://576c3a0f-2471-43c2-b3e6-7501b7cf571d.westeurope.azurecontainer.io/score"

# COMMAND ----------

dev_prediction = query_endpoint_example(scoring_uri=dev_scoring_uri, inputs=query_input)

# COMMAND ----------

# MAGIC %md ## Deploying the model to production using [Azure Kubernetes Service (AKS)](https://azure.microsoft.com/en-us/services/kubernetes-service/)

# COMMAND ----------

# MAGIC %md #### Connect to an existing AKS cluster
# MAGIC Create an AKS cluster using portal and make sure it has required rights to pull image from ACR

# COMMAND ----------

from azureml.core.compute import AksCompute, ComputeTarget

cluster_name = "himanshu-aks"

# Attatch the cluster to your workspace
attach_config = AksCompute.attach_configuration(resource_group="himanshudemorg", cluster_name=cluster_name)
compute = ComputeTarget.attach(workspace, cluster_name, attach_config)

# Wait for the operation to complete
compute.wait_for_completion(True)
print(compute.provisioning_state)
print(compute.provisioning_errors)

# COMMAND ----------

# MAGIC %md #### Deploy to the model's image to the specified AKS cluster

# COMMAND ----------

from azureml.core.webservice import Webservice, AksWebservice

# Set configuration and service name
prod_webservice_name = "wine-model-ws-prod"
prod_webservice_deployment_config = AksWebservice.deploy_configuration()

# Deploy from image
prod_webservice = Webservice.deploy_from_image(workspace = workspace, 
                                               name = prod_webservice_name,
                                               image = model_image,
                                               deployment_config = prod_webservice_deployment_config,
                                               deployment_target = compute)

# COMMAND ----------

# Wait for the deployment to complete
prod_webservice.wait_for_deployment(show_output = True)

# COMMAND ----------

#prod_scoring_uri = prod_webservice.scoring_uri
#prod_service_key = prod_webservice.get_keys()[0] if len(prod_webservice.get_keys()) > 0 else None
prod_scoring_uri = "http://20.76.3.13:80/api/v1/service/wine-model-ws-prod/score"
prod_service_key = "Ioo5RhodWSOSu7MpOohE13rqGmL7EnDs"

# COMMAND ----------

prod_prediction = query_endpoint_example(scoring_uri=prod_scoring_uri, service_key=prod_service_key, inputs=query_input)