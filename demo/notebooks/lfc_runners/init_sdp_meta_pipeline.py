# Databricks notebook source
sdp_meta_whl = spark.conf.get("sdp_meta_whl")
%pip install $sdp_meta_whl # noqa : E999

# COMMAND ----------

layer = spark.conf.get("layer", None)

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
from pyspark.sql import DataFrame


def bronze_transform(df: DataFrame, dataflowSpec) -> DataFrame:
    """Rename LFC SCD2 reserved column names and deduplicate for no-PK tables.

    DLT globally reserves __START_AT and __END_AT as system column names for
    SCD Type 2 tracking.  Any source table that already contains these columns
    (e.g. LFC SCD2 output tables like dtix) must have them renamed before DLT
    analyses the schema, otherwise DLT either raises DLTAnalysisException
    (for apply_changes) or silently drops them (for apply_changes_from_snapshot),
    making them unresolvable as keys.

    Deduplication is required for no-PK source tables (e.g. dtix) where the SQL
    Server source allows multiple fully-identical rows.  LFC preserves these
    duplicates verbatim; apply_changes_from_snapshot requires unique keys per
    snapshot, so we collapse identical rows to one before DLT processes them.
    """
    target_table = dataflowSpec.targetDetails.get("table", "") if dataflowSpec.targetDetails else ""
    if target_table == "dtix":
        if "__START_AT" in df.columns:
            df = df.withColumnRenamed("__START_AT", "lfc_start_at")
        if "__END_AT" in df.columns:
            df = df.withColumnRenamed("__END_AT", "lfc_end_at")
        df = df.dropDuplicates()
    return df


DataflowPipeline.invoke_dlt_pipeline(spark, layer, bronze_custom_transform_func=bronze_transform)
