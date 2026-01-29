"""Databricks Labs SDP-META Framework.

A metadata-driven framework for Databricks Lakeflow Declarative Pipelines (formerly DLT).

Example usage:
    from databricks.labs.sdpmeta.dataflow_pipeline import DataflowPipeline
    from databricks.labs.sdpmeta.cli import SDPMeta
    from databricks.labs.sdpmeta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec

Note: DataflowPipeline and related classes require the Databricks DLT runtime.
Import them directly from their modules when running in Databricks.
"""
from databricks.labs.sdpmeta.__about__ import __version__, __package_name__

__all__ = [
    "__version__",
    "__package_name__",
]
