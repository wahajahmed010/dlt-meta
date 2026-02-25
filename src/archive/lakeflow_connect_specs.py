"""
Create Lakeflow Connect pipeline specifications from flat config.
Returns tuple of (gateway_spec, ingestion_spec).

ARCHIVED: Not documented; only used by tests. Main flow uses resources.pipelines directly.
"""

from typing import Dict, Any, Tuple


def create_lakeflow_connect_specs(config: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Create Lakeflow Connect pipeline specifications based on configuration.
    Returns tuple of (gateway_spec, ingestion_spec).
    """
    
    # Extract configuration
    connection_name = config["connection_name"]
    gateway_storage_catalog = config["gateway_storage_catalog"]
    gateway_storage_schema = config["gateway_storage_schema"]
    pipeline_mode = config.get("pipeline_mode", "cdc")
    ingestion_objects = config.get("ingestion_objects", [])
    
    # Gateway pipeline specification
    gateway_spec = None
    if pipeline_mode == "cdc":
        gateway_spec = {
            "name": f"{connection_name}-gateway",
            "gateway_definition": {
                "connection_name": connection_name,
                "gateway_storage_catalog": gateway_storage_catalog,
                "gateway_storage_schema": gateway_storage_schema,
                "gateway_storage_name": f"{connection_name}-gateway"
            }
        }
    
    # Ingestion pipeline specification
    ingestion_spec = {
        "name": f"{connection_name}-ingestion",
        "ingestion_definition": {
            "objects": ingestion_objects
        }
    }
    
    # Configure ingestion based on mode
    if pipeline_mode == "cdc_single_pipeline":
        ingestion_spec.update({
            "pipeline_type": "MANAGED_INGESTION",
            "catalog": gateway_storage_catalog,
            "target": gateway_storage_schema,
            "configuration": {
                "pipelines.directCdc.minimumRunDurationMinutes": "1",
                "pipelines.directCdc.enableBoundedContinuousGraphExecution": True
            },
            "serverless": False,
            "development": True
        })
        ingestion_spec["ingestion_definition"].update({
            "connection_name": connection_name,
            "connector_type": "CDC"
        })
    
    elif pipeline_mode == "cdc":
        ingestion_spec["ingestion_definition"]["ingestion_gateway_id"] = "${gateway_pipeline_id}"
    
    elif pipeline_mode == "qbc":
        ingestion_spec["ingestion_definition"]["connection_name"] = connection_name
    
    return gateway_spec, ingestion_spec
