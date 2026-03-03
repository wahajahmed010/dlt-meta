# DLT-Meta Enhanced: YAML-Based Configuration (dbldatagen + Lakeflow Connect)

**Alternative approach:** For notebook-based synthetic data and Lakeflow Connect demos, see [dlt-meta-dab.md](dlt-meta-dab.md).

## TL;DR - Quick Start for Existing DLT-Meta Users

**New enhancements added to dlt-meta:**
- **Multi-section YAML support** - Single file with variables, generation config, and dataflows
- **`synthetic_data` source format** - Generate test data using Databricks Labs Data Generator  
- **`lakeflow_connect` source format** - Ingest from databases/SaaS using Lakeflow Connect
- **Enhanced CLI** - Processes multi-section YAML files with integrated data generation

### 🚀 Step 1: Data Generation Configuration (Copy/Paste Example)

### 🚀 Complete Configuration (Single YAML File)

```yaml
# complete_config.yaml - Multi-section YAML (NEW dlt-meta enhancement)
variables:  # NEW - Multi-section YAML enhancement
  # Default values (CLI parameters override these)
  uc_catalog_name: "dev_catalog"
  bronze_schema: "synthetic_bronze" 
  silver_schema: "synthetic_silver"
  uc_volume_path: "/Volumes/dev_catalog/dltmeta/dltmeta"  # Auto-created by dlt-meta

# Synthetic Data Generation Configuration
resources:  # NEW - DAB-style resources for data generation
  data_generation:
    config:
      output_location: "{uc_volume_path}/synthetic_data"
      output_format: "parquet"  # Valid: csv, parquet, delta, json, orc
      schema_output_location: "{uc_volume_path}/synthetic_data/schemas"
      
    tables:
      # Orders table (parent table)
      orders:
        rows: 10000
        partitions: 4
        columns:
          order_id:
            type: "long"
            unique_values: 10000
          customer_id:
            type: "long"
            min_value: 1
            max_value: 1000
          order_date:
            type: "timestamp"
            begin: "2023-01-01T00:00:00"
            end: "2024-12-31T23:59:59"
          order_amount:
            type: "decimal"
            precision: 10
            scale: 2
            min_value: 10.00
            max_value: 5000.00
      
      # Order details table (child table)
      order_details:
        rows: 25000  # 2.5 details per order on average
        partitions: 4
        # Depends on orders table being generated first for referential integrity
        depends_on: ["orders"]
        columns:
          order_id:
            type: "long"
            # dbldatagen API for referential relationships
            base_column: "order_id"
            base_column_type: "values"
          product_name:
            type: "string"
            values: ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]
            weights: [30, 20, 20, 20, 10]
          quantity:
            type: "int"
            min_value: 1
            max_value: 5
          unit_price:
            type: "decimal"
            precision: 8
            scale: 2
            min_value: 5.00
            max_value: 2000.00

# DLT-Meta Onboarding Configuration (Best Practice: Use dataflows section)
dataflows:  # OPTIONAL: Section name can be omitted, but content below is required
  # Entry 1: Orders table from synthetic data
  - data_flow_id: "100"
    data_flow_group: "A1"  # Required field (just metadata)
    source_format: "cloudFiles"  # Standard dlt-meta source format
    source_details:
      source_table: "orders"
      source_path_dev: "{uc_volume_path}/synthetic_data/orders"  # Points to generated data
    bronze_catalog_dev: "{uc_catalog_name}"
    bronze_database_dev: "{bronze_schema}"
    bronze_table: "orders"
    bronze_table_path_dev: "{uc_volume_path}/data/bronze/orders"
    bronze_reader_options:
      cloudFiles.format: "parquet"
      cloudFiles.schemaLocation: "{uc_volume_path}/synthetic_data/_schemas"
    bronze_database_quarantine_dev: "{uc_catalog_name}.{bronze_schema}"
    bronze_quarantine_table: "orders_quarantine"
    bronze_quarantine_table_path_dev: "{uc_volume_path}/data/bronze/orders_quarantine"
    silver_catalog_dev: "{uc_catalog_name}"
    silver_database_dev: "{silver_schema}"
    silver_table: "orders_clean"
    silver_table_path_dev: "{uc_volume_path}/data/silver/orders_clean"
    silver_transformation_yaml_dev: "{uc_volume_path}/demo/conf/silver_transformations.yaml"

  # Entry 2: Order details table from synthetic data (separate data flow)
  - data_flow_id: "101"
    data_flow_group: "A1"  # Required field (just metadata)
    source_format: "cloudFiles"  # Standard dlt-meta source format
    source_details:
      source_table: "order_details"
      source_path_dev: "{uc_volume_path}/synthetic_data/order_details"  # Points to generated data
    bronze_catalog_dev: "{uc_catalog_name}"
    bronze_database_dev: "{bronze_schema}"
    bronze_table: "order_details"
    bronze_table_path_dev: "{uc_volume_path}/data/bronze/order_details"
    bronze_reader_options:
      cloudFiles.format: "parquet"
      cloudFiles.schemaLocation: "{uc_volume_path}/synthetic_data/_schemas"
    bronze_database_quarantine_dev: "{uc_catalog_name}.{bronze_schema}"
    bronze_quarantine_table: "order_details_quarantine"
    bronze_quarantine_table_path_dev: "{uc_volume_path}/data/bronze/order_details_quarantine"
    silver_catalog_dev: "{uc_catalog_name}"
    silver_database_dev: "{silver_schema}"
    silver_table: "order_details_clean"
    silver_table_path_dev: "{uc_volume_path}/data/silver/order_details_clean"
    silver_transformation_yaml_dev: "{uc_volume_path}/demo/conf/silver_transformations.yaml"

# Alternative: Existing Customer Format (Backward Compatible)
# If 'dataflows:' section is omitted, the array starts directly:
# - data_flow_id: "100"
#   data_flow_group: "A1"
#   source_format: "cloudFiles"
#   # ... rest of configuration (same as above)
```

**Required Silver Transformations File:**
```yaml
# {uc_volume_path}/demo/conf/silver_transformations.yaml
- target_table: "orders"
  select_exp:
    - "order_id"
    - "customer_id" 
    - "order_date"
    - "order_amount"
    - "date_format(order_date, 'yyyy-MM') as order_month"
    - "case when order_amount > 1000 then 'High' else 'Standard' end as order_tier"
    - "_rescued_data"
  where_clause:
    - "order_id IS NOT NULL"
    - "order_amount > 0"

- target_table: "order_details"
  select_exp:
    - "order_id"
    - "product_name"
    - "quantity" 
    - "unit_price"
    - "quantity * unit_price as line_total"
    - "upper(product_name) as product_category"
    - "_rescued_data"
  where_clause:
    - "order_id IS NOT NULL"
    - "quantity > 0"
    - "unit_price > 0"
```

**Run Enhanced DLT-Meta Command for Synthetic Data:**
```bash
# Enhanced CLI processes synthetic data generation and DLT-Meta pipeline
dlt-meta onboard-enhanced \
  --config_file complete_config.yaml \
  --uc_catalog_name dev_catalog \
  --bronze_schema synthetic_bronze \
  --silver_schema synthetic_silver
# Creates: Synthetic Data → Bronze Tables → Silver Tables
```

### 🔗 Lakeflow Connect Example (Copy/Paste Example)

```yaml
# complete_lakeflow_config.yaml - Multi-section YAML for Lakeflow Connect
variables:  # NEW - Multi-section YAML enhancement
  # Default values (CLI parameters override these)
  uc_catalog_name: "dev_catalog"
  bronze_schema: "lakeflow_bronze" 
  silver_schema: "lakeflow_silver"
  staging_schema: "lakeflow_staging"
  uc_volume_path: "/Volumes/dev_catalog/dltmeta/dltmeta"

# Lakeflow Connect Configuration (DAB YAML Convention)
resources:  # NEW - DAB-style Lakeflow Connect resources
  connections:
    sqlserver-connection:
      name: "prod_sqlserver_db"
      connection_type: "SQLSERVER"
      options:
        host: "sqlserver.company.com"
        port: "1433"
        user: "{db_username}"
        password: "{db_password}"
        
  pipelines:
    gateway:
      name: "sqlserver-gateway"
      gateway_definition:
        connection_name: "prod_sqlserver_db"
        gateway_storage_catalog: "{uc_catalog_name}"
        gateway_storage_schema: "{staging_schema}"
        gateway_storage_name: "sqlserver-gateway"
      target: "{staging_schema}"
      catalog: "{uc_catalog_name}"

    pipeline_sqlserver:
      name: "sqlserver-ingestion-pipeline"
      ingestion_definition:
        ingestion_gateway_id: "{gateway_pipeline_id}"
        objects:
          # Individual table ingestion
          - table:
              source_catalog: "test"
              source_schema: "dbo"
              source_table: "customers"
              destination_catalog: "{uc_catalog_name}"
              destination_schema: "{staging_schema}"
          # Whole schema ingestion
          - schema:
              source_catalog: "test"
              source_schema: "sales"
              destination_catalog: "{uc_catalog_name}"
              destination_schema: "{staging_schema}"
      target: "{staging_schema}"
      catalog: "{uc_catalog_name}"

# DLT-Meta Onboarding Configuration
dataflows:  # OPTIONAL: For backward compatibility, this section can be omitted
  # Entry 1: Customers table from Lakeflow Connect
  - data_flow_id: "200"
    data_flow_group: "A1"  # Required field (just metadata)
    source_format: "lakeflow_connect"
    source_details:
      source_table: "customers"
      source_path_dev: "{uc_catalog_name}.{staging_schema}.customers"  # Lakeflow staging table
    bronze_catalog_dev: "{uc_catalog_name}"
    bronze_database_dev: "{bronze_schema}"
    bronze_table: "customers_from_sqlserver"
    bronze_table_path_dev: "{uc_volume_path}/data/bronze/customers_from_sqlserver"
    bronze_reader_options:
      format: "delta"
    bronze_database_quarantine_dev: "{uc_catalog_name}.{bronze_schema}"
    bronze_quarantine_table: "customers_quarantine"
    bronze_quarantine_table_path_dev: "{uc_volume_path}/data/bronze/customers_quarantine"
    silver_catalog_dev: "{uc_catalog_name}"
    silver_database_dev: "{silver_schema}"
    silver_table: "customers_clean"
    silver_table_path_dev: "{uc_volume_path}/data/silver/customers_clean"
    silver_transformation_yaml_dev: "{uc_volume_path}/demo/conf/silver_transformations.yaml"
```

**Run Enhanced DLT-Meta Command for Lakeflow Connect:**
```bash
# Enhanced CLI processes Lakeflow Connect configuration
dlt-meta onboard-enhanced \
  --config_file complete_lakeflow_config.yaml \
  --uc_catalog_name dev_catalog \
  --bronze_schema lakeflow_bronze \
  --silver_schema lakeflow_silver \
  --staging_schema lakeflow_staging
# Creates: UC Connection → Gateway Pipeline → Ingestion Pipeline → DLT Pipeline
```

## 🔄 Backward Compatibility for Existing Customers

**Enhanced CLI handles both formats:**
- **Without `dataflows:` section** → Treats as traditional array (existing format)
- **With `dataflows:` section** → Processes as multi-section YAML (new format)

### Traditional Format (Existing Customers)
```yaml
# onboarding.yaml - Traditional format (no dataflows section)
- data_flow_id: "100"
  data_flow_group: "A1"
  source_format: "cloudFiles"
  source_details:
    source_table: "orders"
    source_path_dev: "{uc_volume_path}/synthetic_data/orders"
  # ... rest of configuration
```

### Multi-Section Format (Best Practice)  
```yaml
# complete_config.yaml - Enhanced format with sections
variables:
  # ... variables
dataflows:  # Explicit section (recommended)
  - data_flow_id: "100"
    # ... same configuration as Option 1
```

**Current DLT-Meta CLI (Requires 2 Files):**
```bash
# Current dlt-meta expects separate files:
# 1. onboarding.yaml (extract dataflows section)
# 2. silver_transformations.json (create from transformations above)

dlt-meta onboard \
  --onboarding_file_path onboarding.yaml \
  --uc_catalog_name dev_catalog \
  --bronze_schema synthetic_bronze \
  --silver_schema synthetic_silver
```

**Enhanced DLT-Meta CLI (Proposed - Single File):**
```bash
# NEW: Enhanced CLI that processes multi-section YAML and creates required files
dlt-meta onboard-enhanced \
  --config_file complete_config.yaml \
  --uc_catalog_name dev_catalog \
  --bronze_schema synthetic_bronze \
  --silver_schema synthetic_silver
```

## Implementation Notes

### Recognized `source_format` Values
- `cloudFiles` - Cloud file ingestion (S3, ADLS, GCS)
- `eventhub` - Azure Event Hub streaming
- `kafka` - Kafka streaming  
- `delta` - Delta table sources
- `snapshot` - Snapshot-based ingestion
- `sqlserver` - SQL Server direct connection
- `lakeflow_connect` - **NEW** - Lakeflow Connect database/SaaS ingestion

### Key Implementation Requirements
1. **Multi-section YAML parsing** - Enhanced CLI to process `variables`, `resources`, and `dataflows` sections
2. **Backward compatibility** - Support existing single-array format without `dataflows:` section header
3. **Variable substitution** - Use existing dlt-meta `{variable}` syntax throughout
4. **DAB resource support** - Handle `resources:` section for data generation and Lakeflow Connect
5. **File generation** - Auto-create separate transformation files from multi-section YAML

### Development Workflow
1. **Phase 1 - Development**: Use synthetic data generation for testing and development
2. **Phase 2 - Production**: Switch to Lakeflow Connect for real data ingestion
3. **Same pipeline logic**: Both phases use identical DLT-Meta medallion architecture (Bronze → Silver → Gold)

## Testing

### Unit Tests

Unit tests are in the `tests/` folder. See [Contributing / Onboarding](content/contributing/onboarding/_index.md) (Step 4) for full setup.

**Run all unit tests:**
```bash
pytest
```

**Run a specific test:**
```bash
pytest -k "test_case_name"
```

**Run enhanced CLI tests** (synthetic data, Lakeflow Connect specs):
```bash
python test_enhanced_cli.py
```

### Integration Tests

Integration tests run from your laptop against a Databricks workspace. See [Integration Tests README](../integration_tests/README.md) or [Integration Tests (docs)](content/additionals/integration_tests.md) for full setup (venv, Databricks CLI auth, `PYTHONPATH`).

**Run integration tests** (after setup):
```bash
# CloudFiles (simplest - no external services)
python integration_tests/run_integration_tests.py --uc_catalog_name=<catalog> --source=cloudfiles --profile=DEFAULT

# Snapshot
python integration_tests/run_integration_tests.py --uc_catalog_name=<catalog> --source=snapshot --profile=DEFAULT

# Kafka (requires running Kafka instance)
python integration_tests/run_integration_tests.py --uc_catalog_name=<catalog> --source=kafka --kafka_source_topic=dlt-meta-integration-test --kafka_sink_topic=dlt-meta_inttest_topic --kafka_source_broker=host:9092 --profile=DEFAULT

# EventHub (requires EventHub instance and secrets)
python integration_tests/run_integration_tests.py --uc_catalog_name=<catalog> --source=eventhub --eventhub_name=iot --eventhub_secrets_scope_name=eventhubs_creds --eventhub_namespace=<namespace> --eventhub_port=9093 --eventhub_producer_accesskey_name=producer --eventhub_consumer_accesskey_name=consumer --profile=DEFAULT
```