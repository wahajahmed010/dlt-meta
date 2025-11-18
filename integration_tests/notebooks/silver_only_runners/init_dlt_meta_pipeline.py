# Databricks notebook source
dlt_meta_whl = spark.conf.get("dlt_meta_whl")
%pip install $dlt_meta_whl # noqa : E999

# COMMAND ----------

layer = spark.conf.get("layer", None)
print(f"Initializing Silver-Only DLT Pipeline for layer: {layer}")

from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)

