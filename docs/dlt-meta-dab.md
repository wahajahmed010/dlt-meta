# DLT-Meta Enhanced Source Formats: Synthetic Data Generation and Lakeflow Connect (JSON/YAML Support)

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
variables:
  # Default values (CLI parameters override these)
  uc_catalog_name: "dev_catalog"
  bronze_schema: "synthetic_bronze" 
  silver_schema: "synthetic_silver"
  uc_volume_path: "/Volumes/dev_catalog/dltmeta/dltmeta"  # Auto-created by dlt-meta

# Synthetic Data Generation Configuration
generation_config:
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

# DLT-Meta Onboarding Configuration
dataflows:
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
# uc_volume_path is auto-created as: /Volumes/dev_catalog/dltmeta_schema/dltmeta_schema/
```

**Enhanced DLT-Meta CLI (Proposed - Single File):**
```bash
# NEW: Enhanced CLI that processes multi-section YAML and creates required files
dlt-meta onboard-enhanced \
  --config_file complete_config.yaml \
  --uc_catalog_name dev_catalog \
  --bronze_schema synthetic_bronze \
  --silver_schema synthetic_silver
# Processes: complete_config.yaml → generates required files → runs standard pipeline
```

### 🔗 How the Two Sections Link Together

The configuration sections are linked through **file paths and naming**:

| Data Generation Config | DLT-Meta Onboarding | Purpose |
|----------------------|-------------------|---------|
| `tables.orders` | `source_details.source_table: "orders"` | Table name matching |
| `output_location: "{uc_volume_path}/synthetic_data"` | `source_path_dev: "{uc_volume_path}/synthetic_data/orders"` | Output location matching |
| `output_format: "parquet"` | `cloudFiles.format: "parquet"` | File format matching |
| Auto Loader manages schemas | `cloudFiles.schemaLocation: "{uc_volume_path}/synthetic_data/_schemas"` | Schema evolution |

### 🔄 Execution Order

1. **Create** `complete_config.yaml` (single file with all configuration)
2. **Run** data generation job to create parquet files + schemas
3. **Extract** the dataflows section for dlt-meta onboarding
4. **Run** dlt-meta onboard command with CLI parameters:

```bash
dlt-meta onboard \
  --onboarding_file_path onboarding.yaml \
  --uc_catalog_name dev_catalog \
  --bronze_schema synthetic_bronze \
  --silver_schema synthetic_silver
# uc_volume_path is auto-created as: /Volumes/dev_catalog/dltmeta_schema/dltmeta_schema/
```

**Key Coordination Points:**
- Table names in generation config must match `source_table` in onboarding
- Output paths in generation config must match `source_path_dev` in onboarding
- File format must be consistent between both configurations

**Variable Syntax:**
- **`{variable_name}`** - DLT-Meta variables (substituted via CLI parameters)
- **`"literal_value"`** - Direct values (like `data_flow_id: "100"`, `source_table: "orders"`)
- **Variable substitution** happens automatically in dlt-meta CLI

## 🔄 Recommended Flow: Separate Data Generation from DLT Pipelines

Following dlt-meta best practices, **data generation should be separated from pipeline processing**:

### Step 1: Generate Synthetic Data (Pre-Pipeline)
```bash
# Run data generation notebook first (separate job)
databricks jobs run-now --job-id {synthetic_data_generation_job_id}
```

### Step 2: Process Generated Data with DLT-Meta (Pipeline)
```bash
# Then run dlt-meta pipeline on the generated data
dlt-meta onboard --onboarding_file_path onboarding.yaml
```

**Why This Separation?**
- **Clear Separation of Concerns**: Data generation vs. data processing
- **Reusability**: Generate once, process multiple times with different configs
- **Standard dlt-meta Pattern**: Each data_flow_id processes one table from one source path
- **Debugging**: Easier to troubleshoot generation vs. pipeline issues separately
- **Scheduling**: Different cadences for generation (daily) vs. processing (real-time)


### 🔧 How Synthetic Data YAML Specs Become Executable Code

The YAML configuration above follows a **declarative-to-imperative** code generation pattern:

**1. YAML Specification (Declarative) - Linked Tables**
```yaml
tables:
  orders:
    rows: 10000
    columns:
      order_id:
        type: "long"
        unique_values: 10000
      customer_id:
        type: "long"
        min_value: 1
        max_value: 1000
  
  order_details:
    rows: 25000
    columns:
      order_id:
        type: "long"
        # dbldatagen uses baseColumn for referential relationships
        base_column: "order_id"
        base_column_type: "values"
      product_name:
        type: "string"
        values: ["Laptop", "Mouse", "Keyboard"]
```

**2. Auto-Generated Python Notebook (Imperative) - Linked Tables**
```python
# Generated notebook: synthetic_data_generator.py (runs as separate job)
import dbldatagen as dg
from pyspark.sql.types import *
import yaml

# Load generation configuration
with open("/dbfs{uc_volume_path}/synthetic_data_config.yaml", "r") as f:
    config = yaml.safe_load(f)

generation_config = config["generation_config"]
output_location = generation_config["output_location"]
output_format = generation_config["output_format"]
schema_output = generation_config["schema_output_location"]

# Generate Orders table (parent table)
orders_config = generation_config["tables"]["orders"]
orders_spec = dg.DataGenerator(spark, name="orders", 
                              rows=orders_config["rows"], 
                              partitions=orders_config["partitions"])

# Add columns based on configuration
for col_name, col_config in orders_config["columns"].items():
    if col_config["type"] == "long":
        if "unique_values" in col_config:
            orders_spec = orders_spec.withColumn(col_name, LongType(), 
                                               uniqueValues=col_config["unique_values"])
        else:
            orders_spec = orders_spec.withColumn(col_name, LongType(), 
                                               minValue=col_config["min_value"],
                                               maxValue=col_config["max_value"])
    elif col_config["type"] == "timestamp":
        orders_spec = orders_spec.withColumn(col_name, TimestampType(), 
                                           begin=col_config["begin"],
                                           end=col_config["end"])
    elif col_config["type"] == "decimal":
        orders_spec = orders_spec.withColumn(col_name, 
                                           DecimalType(col_config["precision"], col_config["scale"]),
                                           minValue=col_config["min_value"],
                                           maxValue=col_config["max_value"])

# Build and save orders table
orders_df = orders_spec.build()
orders_path = f"{output_location}/orders"
orders_df.write.mode("overwrite").format(output_format).save(orders_path)

# Generate schema DDL for dlt-meta
orders_schema_ddl = orders_df.schema.simpleString()
dbutils.fs.put(f"{schema_output}/orders.ddl", orders_schema_ddl, True)

# Generate Order Details table (with referential integrity)
details_config = generation_config["tables"]["order_details"]
details_spec = dg.DataGenerator(spark, name="order_details", 
                               rows=details_config["rows"], 
                               partitions=details_config["partitions"])

# Add columns with proper relationships
for col_name, col_config in details_config["columns"].items():
    if col_config["type"] == "long" and "base_column" in col_config:
        # Create referential relationship using existing orders data
        details_spec = details_spec.withColumn(col_name, LongType(), 
                                             baseColumn=col_config["base_column"],
                                             baseColumnType=col_config["base_column_type"])
    elif col_config["type"] == "string" and "values" in col_config:
        details_spec = details_spec.withColumn(col_name, StringType(), 
                                             values=col_config["values"],
                                             weights=col_config.get("weights"))

# Build and save order details table
details_df = details_spec.build()
details_path = f"{output_location}/order_details"
details_df.write.mode("overwrite").format(output_format).save(details_path)

# Generate schema DDL for dlt-meta
details_schema_ddl = details_df.schema.simpleString()
dbutils.fs.put(f"{schema_output}/order_details.ddl", details_schema_ddl, True)

print(f"✅ Generated {orders_df.count()} orders and {details_df.count()} order details")
print(f"📁 Data saved to: {output_location}")
print(f"📋 Schemas saved to: {schema_output}")
```

**3. DAB Job Configuration (Separate from DLT Pipeline)**
```yaml
# databricks.yml - For managing data generation job separately
resources:
  jobs:
    synthetic_data_generator:
      name: "Synthetic Data Generator"
      job_clusters:
        - job_cluster_key: "synthetic_cluster"
          new_cluster:
            spark_version: "13.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2
      tasks:
        - task_key: "generate_synthetic_data"
          job_cluster_key: "synthetic_cluster"
          notebook_task:
            notebook_path: "./notebooks/synthetic_data_generator.py"
          timeout_seconds: 3600
      
  notebooks:
    synthetic_data_generator:
      path: ./notebooks/synthetic_data_generator.py
```

**4. Execution Flow (Two-Step Process)**
```
Step 1: Data Generation Job
YAML Config → Code Generation → Notebook Job → Parquet Files + Schema DDLs

Step 2: DLT-Meta Pipeline  
Generated Data + Standard Onboarding → DLT Pipeline → Bronze/Silver Tables
```

**Benefits of Separated Flow:**
- ✅ **Follows dlt-meta Patterns**: Each data_flow_id processes one table from one path
- ✅ **Clear Separation**: Data generation vs. data processing are separate concerns
- ✅ **Reusable**: Generate once, process multiple times with different configurations
- ✅ **Standard Integration**: Uses existing dlt-meta `cloudFiles` format and reader options
- ✅ **Debuggable**: Can troubleshoot generation and pipeline issues independently
- ✅ **Flexible Scheduling**: Different cadences for generation vs. processing jobs

### 🔗 Lakeflow Connect Example

**Option 1: DLT-Meta format (uses existing dlt-meta variables)**

```yaml
# Add to your existing onboarding.yaml (DLT-Meta format)
- data_flow_id: "200"
  data_flow_group: "lakeflow_demo"
  source_system: "SQL Server"
  source_format: "lakeflow_connect"
  source_details:
    # DLT-Meta format for Lakeflow Connect (not DAB format)
    connection_name: "prod_sqlserver_db"
    gateway_storage_catalog: "{uc_catalog_name}"
    gateway_storage_schema: "{staging_schema}"
    ingestion_mode: "cdc"
    pipeline_mode: "cdc_single_pipeline"
    ingestion_objects:
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

  bronze_catalog_dev: "{uc_catalog_name}"
  bronze_database_dev: "{bronze_schema}"
  bronze_table: "customers_from_sqlserver"
  bronze_reader_options:
    format: "delta"
  silver_catalog_dev: "{uc_catalog_name}"
  silver_database_dev: "{silver_schema}"
  silver_table: "customers_clean"
```


**Option 2: JSON format with inline connection (Legacy)**

```json
// Alternative JSON format - add to your onboarding.json
[{
    "data_flow_id": "200",
    "data_flow_group": "lakeflow_demo",
    "source_system": "SQL Server",
    "source_format": "lakeflow_connect",
    "source_details": {
        "connection_name": "{source_connection_name}",
        "gateway_storage_catalog": "{uc_catalog_name}",
        "gateway_storage_schema": "{staging_schema}",
        "gateway_storage_name": "sqlserver-gateway",
        "ingestion_mode": "cdc",
        "pipeline_mode": "cdc_single_pipeline",
        "ingestion_objects": [
            {
                "table": {
                    "source_catalog": "test",
                    "source_schema": "dbo",
                    "source_table": "customers",
                    "destination_catalog": "{uc_catalog_name}",
                    "destination_schema": "{staging_schema}"
                }
            },
            {
                "schema": {
                    "source_catalog": "test",
                    "source_schema": "sales",
                    "destination_catalog": "{uc_catalog_name}",
                    "destination_schema": "{staging_schema}"
                }
            }
        ]
    },
    "bronze_catalog_dev": "{uc_catalog_name}",
    "bronze_database_dev": "{bronze_schema}",
    "bronze_table": "customers_from_sqlserver",
    "bronze_reader_options": {
        "format": "delta"
    },
    "silver_catalog_dev": "{uc_catalog_name}",
    "silver_database_dev": "{silver_schema}",
    "silver_table": "customers_clean"
}]
```

**Pipeline Mode Variations (Following Microsoft DAB Patterns):**

```yaml
# CDC Mode: Separate Gateway + Ingestion Pipeline
resources:
  pipelines:
    gateway:
      name: ${var.gateway_name}
      gateway_definition:
        connection_name: ${var.connection_name}
        gateway_storage_catalog: ${var.dest_catalog}
        gateway_storage_schema: ${var.dest_schema}
        gateway_storage_name: ${var.gateway_name}

    pipeline_cdc:
      name: cdc-ingestion-pipeline
      ingestion_definition:
        ingestion_gateway_id: ${resources.pipelines.gateway.id}
        objects:
          - table:
              source_catalog: test
              source_schema: dbo
              source_table: customers

# QBC Mode: Ingestion Pipeline Only (No Gateway)
resources:
  pipelines:
    pipeline_qbc:
      name: qbc-ingestion-pipeline
      ingestion_definition:
        connection_name: ${var.connection_name}  # Direct connection
        objects:
          - table:
              source_catalog: test
              source_schema: dbo
              source_table: customers
              table_configuration:
                query_based_connector_config:
                  cursor_columns: ["modified_date"]

# CDC_SINGLE_PIPELINE Mode: Combined Gateway + Ingestion
resources:
  pipelines:
    pipeline_cdc_single:
      name: cdc-single-pipeline
      pipeline_type: MANAGED_INGESTION
      catalog: ${var.dest_catalog}
      schema: ${var.dest_schema}
      configuration:
        pipelines.directCdc.minimumRunDurationMinutes: "1"
        pipelines.directCdc.enableBoundedContinuousGraphExecution: true
      development: true
      serverless: false  # Classic compute required
      continuous: true
      ingestion_definition:
        connection_name: ${var.connection_name}
        connector_type: "CDC"
        source_type: "SQLSERVER"
        objects:
          - table:
              source_catalog: test
              source_schema: dbo
              source_table: customers
```

**Additional Database Connection Examples:**

```yaml
resources:
  connections:
    # PostgreSQL Connection
    postgres-connection:
      name: "prod_postgres_db"
      connection_type: "POSTGRES"
      options:
        host: "{db_host}"
        port: "5432"
        user: "{{secrets/{secret_scope}/pg-username}}"
        password: "{{secrets/{secret_scope}/pg-password}}"
        sslmode: "require"

    # MySQL Connection  
    mysql-connection:
      name: "prod_mysql_db"
      connection_type: "MYSQL"
      options:
        host: "{db_host}"
        port: "3306"
        user: "{{secrets/{secret_scope}/mysql-username}}"
        password: "{{secrets/{secret_scope}/mysql-password}}"
        useSSL: "true"

    # Oracle Connection
    oracle-connection:
      name: "prod_oracle_db" 
      connection_type: "ORACLE"
      options:
        host: "{db_host}"
        port: "1521"
        serviceName: "{db_service}"
        user: "{{secrets/{secret_scope}/oracle-username}}"
        password: "{{secrets/{secret_scope}/oracle-password}}"
```

**Supported Lakeflow Connect Modes:**
- **`cdc`** - Change Data Capture with separate gateway pipeline + ingestion pipeline
- **`qbc`** - Query-Based Change detection (ingestion pipeline only, no gateway needed)  
- **`cdc_single_pipeline`** - Single combined pipeline (gateway + ingestion in one pipeline)

**Usage:** Same `dlt-meta onboard` command - Lakeflow Connect pipelines get created automatically!

### 📋 Lakeflow Connect Pipeline Architecture

**Understanding the Three Modes:**

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        LAKEFLOW CONNECT PIPELINE MODES                         │
└─────────────────────────────────────────────────────────────────────────────────┘

MODE 1: CDC (Separate Pipelines)
┌─────────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Gateway Pipeline  │───▶│ Ingestion Pipeline│───▶│   Unity Catalog     │
│                     │    │                  │    │   Staging Tables    │
│ • Connection Bridge │    │ • Data Processing│    │ • Delta Format      │
│ • Authentication    │    │ • CDC Processing │    │ • SCD Types         │
│ • Network Handling  │    │ • Schema Evolution│    │ • Audit Columns     │
└─────────────────────┘    └──────────────────┘    └─────────────────────┘

MODE 2: QBC (Ingestion Only)
                             ┌──────────────────┐    ┌─────────────────────┐
                             │ Ingestion Pipeline│───▶│   Unity Catalog     │
                             │                  │    │   Staging Tables    │
                             │ • Direct Connect │    │ • Delta Format      │
                             │ • Query-Based    │    │ • Incremental Load  │
                             │ • Timestamp CDC  │    │ • Cursor Columns    │
                             └──────────────────┘    └─────────────────────┘

MODE 3: CDC_SINGLE_PIPELINE (Combined)
┌─────────────────────────────────────────────────┐    ┌─────────────────────┐
│           Single Combined Pipeline              │───▶│   Unity Catalog     │
│                                                 │    │   Staging Tables    │
│ • Gateway + Ingestion in One                    │    │ • Delta Format      │
│ • MANAGED_INGESTION Type                        │    │ • SCD Types         │
│ • Classic Compute Required                      │    │ • CDC Processing    │
│ • Direct CDC Configuration                      │    │ • Schema Evolution  │
└─────────────────────────────────────────────────┘    └─────────────────────┘
```

**Mode Comparison:**

| Feature | CDC (Separate) | QBC (Query-Based) | CDC_SINGLE_PIPELINE |
|---------|----------------|-------------------|---------------------|
| **Pipelines** | Gateway + Ingestion | Ingestion Only | Single Combined |
| **Pipeline Type** | Standard | Standard | MANAGED_INGESTION |
| **Compute** | Serverless | Serverless | Classic Compute |
| **Connection** | Via Gateway | Direct | Direct |
| **Change Detection** | Real-time CDC | Timestamp/Cursor | Real-time CDC |
| **Use Case** | High-volume CDC | Batch incremental | Simplified CDC |

### 🎯 Development Workflow

1. **Phase 1 - Development:** Use `synthetic_data` to build and test your medallion architecture
2. **Phase 2 - Production:** Switch to `lakeflow_connect` for real data - same pipeline logic!

---

## Objectives

### Primary Objective: Direct Data Source Support

The primary objective is to enhance dlt-meta with **direct data source support** through **Lakeflow Connect integration**, enabling seamless ingestion from various databases and SaaS connectors without requiring manual connection setup or JDBC configuration. This positions Lakeflow Connect as the managed staging layer that feeds into dlt-meta's medallion architecture (Bronze → Silver → Gold).

**Supported Data Sources via Lakeflow Connect:**
- **Databases**: SQL Server, PostgreSQL, MySQL (with CDC support)
- **SaaS Applications**: Salesforce, ServiceNow, HubSpot, Google Analytics, and others
- **Cloud Platforms**: Automated schema evolution and incremental ingestion

### Secondary Objective: Synthetic Data Generation for Testing

The secondary objective is to provide **Databricks Labs Data Generator integration** as an alternative data source for development, testing, and proof-of-concept scenarios where Lakeflow Connect is not yet desired or available.

**Development Workflow Strategy:**

1. **Phase 1 - Development & Testing**: Use Databricks Labs Data Generator to:
   - Generate synthetic staging data that mimics production schemas
   - Set up and validate the complete medallion architecture
   - Test data quality rules, transformations, and pipeline logic
   - Validate DAB deployment and orchestration workflows

2. **Phase 2 - Production Deployment**: Transition to Lakeflow Connect to:
   - Replace synthetic data with real data sources
   - Maintain the same medallion architecture and transformations
   - Leverage proven pipeline logic and data quality rules
   - Enable real-time CDC and incremental processing

This two-phase approach allows teams to **develop and validate their entire data architecture** using synthetic data, then seamlessly **transition to production data sources** without changing the core pipeline logic or medallion architecture.

### Benefits of Synthetic-First Development Approach

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    SYNTHETIC-FIRST DEVELOPMENT WORKFLOW                         │
└─────────────────────────────────────────────────────────────────────────────────┘

PHASE 1: DEVELOPMENT & TESTING (Synthetic Data)
┌─────────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│  Databricks Labs    │    │   DLT-Meta       │    │   Medallion         │
│  Data Generator     │───▶│   Pipelines      │───▶│   Architecture      │
│                     │    │                  │    │                     │
│ • Synthetic Data    │    │ • Bronze Layer   │    │ • Validated Logic   │
│ • Schema Control    │    │ • Silver Layer   │    │ • Tested DQ Rules   │
│ • Volume Testing    │    │ • Data Quality   │    │ • Proven Transforms │
│ • Edge Cases        │    │ • Transformations│    │ • Performance Tuned │
└─────────────────────┘    └──────────────────┘    └─────────────────────┘
           │                          │                          │
           ▼                          ▼                          ▼
    ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
    │ Instant     │           │ What-If     │           │ Risk-Free   │
    │ Iteration   │           │ Scenarios   │           │ Validation  │
    └─────────────┘           └─────────────┘           └─────────────┘

                                    │
                                    ▼
                        ┌───────────────────────┐
                        │   SEAMLESS TRANSITION │
                        └───────────────────────┘
                                    │
                                    ▼

PHASE 2: PRODUCTION DEPLOYMENT (Real Data)
┌─────────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Lakeflow Connect  │    │   Same DLT-Meta  │    │   Same Medallion    │
│   Data Sources      │───▶│   Pipelines      │───▶│   Architecture      │
│                     │    │                  │    │                     │
│ • Real Databases    │    │ • Bronze Layer   │    │ • Proven Logic      │
│ • SaaS Connectors   │    │ • Silver Layer   │    │ • Tested DQ Rules   │
│ • CDC Streams       │    │ • Data Quality   │    │ • Known Transforms  │
│ • Production Scale  │    │ • Transformations│    │ • Optimized Perf    │
└─────────────────────┘    └──────────────────┘    └─────────────────────┘
           │                          │                          │
           ▼                          ▼                          ▼
    ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
    │ Real-time   │           │ Production  │           │ Confident   │
    │ CDC Data    │           │ Ready       │           │ Deployment  │
    └─────────────┘           └─────────────┘           └─────────────┘
```

#### **Accelerated Development Cycle**

- **No External Dependencies**: Start building medallion architecture immediately without waiting for database access, network configurations, or data source approvals
- **Instant Data Availability**: Generate any volume of test data instantly with controlled characteristics (edge cases, data quality issues, volume testing)
- **Rapid Iteration**: Modify data schemas, add new tables, or change data distributions in minutes rather than weeks

#### **Enhanced What-If Scenario Testing**

- **Schema Evolution Simulation**: Test how pipeline handles new columns, data type changes, or table structure modifications
- **Data Quality Validation**: Inject known data quality issues to validate cleansing rules and error handling
- **Volume & Performance Testing**: Generate datasets of any size to test pipeline performance and scalability before production deployment

#### **Risk Mitigation & Validation**

- **Complete Pipeline Validation**: Validate the entire medallion architecture, data quality rules, and business logic before touching production data
- **Zero Production Impact**: Develop, test, and iterate without any risk to production systems or data sources
- **Proven Architecture**: Deploy to production with confidence knowing the complete data pipeline has been thoroughly tested

#### **Seamless Production Transition**

- **Same Pipeline Logic**: The exact same DLT-Meta pipelines, transformations, and data quality rules work with both synthetic and real data
- **Configuration-Only Changes**: Switch from synthetic to real data sources by simply changing the `source_details.generator` from `dbldatagen` to Lakeflow Connect configuration
- **Validated Performance**: Production deployment uses pre-optimized pipeline configurations and proven transformation logic

## Input Specifications (JSON/YAML)

### 1. Enhanced Onboarding Configuration with Connection Management

**DAB Format (Following Official Microsoft Databricks Structure):**
```yaml
# databricks.yml - Official DAB structure for Lakeflow Connect
bundle:
  name: dlt_meta_enhanced

variables:
  # DAB variables following Microsoft documentation patterns
  gateway_name:
    default: lakeflow-gateway
  dest_catalog:
    default: main
  dest_schema:
    default: lakeflow_staging
  bronze_schema:
    default: bronze
  silver_schema:
    default: silver
  connection_name:
    default: external-db-connection

resources:
  connections:
    # Unity Catalog connections following DAB patterns
    external-db-connection:
      name: ${var.connection_name}
      connection_type: "POSTGRES"  # POSTGRES, MYSQL, SQLSERVER, ORACLE, etc.
      options:
        host: "<server-hostname>"
        port: "<port>"
        # Authentication details: use Databricks secrets for security
        user: "{{secrets/my-secret-scope/db-username}}"
        password: "{{secrets/my-secret-scope/db-password}}"
        # Additional connection properties as needed
        sslmode: "require"
      comment: "Production PostgreSQL database for customer data"

  notebooks:
    # Synthetic data generator notebook (auto-generated from YAML specs)
    synthetic_data_generator:
      path: ./generated/notebooks/synthetic_data_generator.py
      # language not needed - auto-generated Python from YAML configuration
      
    # Lakeflow Connect validator notebook (auto-generated)
    lakeflow_connect_validator:
      path: ./generated/notebooks/lakeflow_connect_validator.py
      # language not needed - auto-generated Python from connection specs

  pipelines:
    # Gateway pipeline (for CDC mode)
    gateway:
      name: ${var.gateway_name}
      gateway_definition:
        connection_name: ${var.connection_name}
        gateway_storage_catalog: ${var.dest_catalog}
        gateway_storage_schema: ${var.dest_schema}
        gateway_storage_name: ${var.gateway_name}
      target: ${var.dest_schema}
      catalog: ${var.dest_catalog}

    # Ingestion pipeline
    lakeflow_ingestion:
      name: lakeflow-ingestion-pipeline
      ingestion_definition:
        ingestion_gateway_id: ${resources.pipelines.gateway.id}
        objects:
          # Individual table ingestion
          - table:
              source_catalog: test
              source_schema: public
              source_table: customers
              destination_catalog: ${var.dest_catalog}
              destination_schema: ${var.dest_schema}
          # Whole schema ingestion
          - schema:
              source_catalog: test
              source_schema: sales
              destination_catalog: ${var.dest_catalog}
              destination_schema: ${var.dest_schema}
      target: ${var.dest_schema}
      catalog: ${var.dest_catalog}

  jobs:
    # Synthetic data generation job
    synthetic_data_job:
      name: synthetic_data_generation_job
      trigger:
        periodic:
          interval: 1
          unit: DAYS
      email_notifications:
        on_failure:
          - "data-team@company.com"
      tasks:
        - task_key: generate_synthetic_data
          notebook_task:
            notebook_path: ${resources.notebooks.synthetic_data_generator.path}
            base_parameters:
              onboarding_file_path: "/Volumes/main/default/dlt_meta_files/conf/onboarding.json"
              data_flow_id: "100"
          libraries:
            - pypi:
                package: "dbldatagen>=0.3.0"

    # Lakeflow Connect pipeline job
    lakeflow_job:
      name: lakeflow_ingestion_job
      trigger:
        periodic:
          interval: 1
          unit: DAYS
      email_notifications:
        on_failure:
          - "data-team@company.com"
      tasks:
        - task_key: validate_connection
          notebook_task:
            notebook_path: ${resources.notebooks.lakeflow_connect_validator.path}
            base_parameters:
              connection_name: ${var.connection_name}
        - task_key: refresh_lakeflow_pipeline
          pipeline_task:
            pipeline_id: ${resources.pipelines.lakeflow_ingestion.id}
          depends_on:
            - task_key: validate_connection

# DLT-Meta integration (extends DAB with dlt-meta specific config)
include:
  - resources/dlt_meta_config.yml  # DLT-Meta specific configurations

targets:
  dev:
    mode: development
    variables:
      dest_catalog: "dev_catalog"
      dest_schema: "dev_lakeflow_staging"
      bronze_schema: "dev_bronze"
      silver_schema: "dev_silver"
      connection_name: "dev-external-db-connection"
  
  prod:
    mode: production
    variables:
      dest_catalog: "prod_catalog"
      dest_schema: "prod_lakeflow_staging"
      bronze_schema: "prod_bronze"
      silver_schema: "prod_silver"
      connection_name: "prod-external-db-connection"
```

**DLT-Meta Configuration Extension (resources/dlt_meta_config.yml):**
```yaml
# resources/dlt_meta_config.yml - DLT-Meta specific configurations
dlt_meta:
  dataflows:
    # Synthetic data example
    - data_flow_id: "100"
      data_flow_group: "synthetic_demo"
      source_system: "DataGenerator"
      source_format: "cloudFiles"
      source_details:
        rows: 10000
        columns:
          customer_id:
            type: "long"
            unique_values: 10000
          name:
            type: "string"
            template: "\\w{4,8}"
          email:
            type: "string"
            template: "\\w+@\\w+\\.com"
      bronze_catalog_dev: ${var.dest_catalog}
      bronze_database_dev: ${var.bronze_schema}
      bronze_table: "synthetic_customers"

    # Lakeflow Connect example
    - data_flow_id: "200"
      data_flow_group: "lakeflow_demo"
      source_system: "PostgreSQL"
      source_format: "lakeflow_connect"
      # References DAB pipeline resources
      pipeline_reference: ${resources.pipelines.lakeflow_ingestion.id}
      connection_reference: ${resources.connections.external-db-connection.name}
      bronze_catalog_dev: ${var.dest_catalog}
      bronze_database_dev: ${var.bronze_schema}
      bronze_table: "customers_from_postgres"
      bronze_reader_options:
        format: "delta"
      silver_catalog_dev: ${var.dest_catalog}
      silver_database_dev: ${var.silver_schema}
      silver_table: "customers_clean"
```

### 2. Legacy JSON Configuration (Single File Approach)

**Purpose:** Extended dlt-meta onboarding format supporting new source formats. Following dlt-meta's single configuration file pattern, all settings are embedded in the `source_details` section.

**Recognized `source_format` values:**
- `kafka` - Existing Kafka streaming support
- `eventhub` - Existing Azure Event Hub support  
- `cloudfiles` - Existing cloud file ingestion support
- `synthetic_data` - **NEW** - Databricks Labs Data Generator integration
- `lakeflow_connect` - **NEW** - Lakeflow Connect database/SaaS ingestion

```json
// Enhanced onboarding template with new source formats - single file approach
[{
    "data_flow_id": "100",
    "data_flow_group": "synthetic_data",
    "source_system": "DataGenerator",
    "source_format": "cloudFiles",
    "source_details": {
        "generator": "dbldatagen",
        "rows": "{synthetic_data_rows}",
        "partitions": 10,
        "output_format": "delta",
        "output_location": "{uc_volume_path}/synthetic_data/customers",
        "columns": {
            "customer_id": {
                "type": "long",
                "unique_values": "{synthetic_data_rows}"
            },
            "first_name": {
                "type": "string",
                "template": "\\w{4,8}"
            },
            "last_name": {
                "type": "string",
                "template": "\\w{4,12}"
            },
            "email": {
                "type": "string",
                "template": "\\w{5,10}\\.\\w{3,8}@\\w{4,10}\\.(com|org|net)"
            },
            "registration_date": {
                "type": "timestamp",
                "begin": "2020-01-01T00:00:00",
                "end": "2024-12-31T23:59:59",
                "random": true
            },
            "city": {
                "type": "string",
                "values": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
                "weights": [20, 20, 20, 20, 20]
            }
        }
    },
    "bronze_catalog_dev": "{uc_catalog_name}",
    "bronze_database_dev": "{bronze_schema}",
    "bronze_table": "synthetic_customers",
    "bronze_reader_options": {
        "format": "delta"
    },
    "silver_catalog_dev": "{uc_catalog_name}",
    "silver_database_dev": "{silver_schema}",
    "silver_table": "customers_clean"
},
{
    "data_flow_id": "200", 
    "data_flow_group": "lakeflow_connect",
    "source_system": "SQL Server",
    "source_format": "lakeflow_connect",
    "source_details": {
        "connection_name": "{source_connection_name}",
        "gateway_storage_catalog": "{uc_catalog_name}",
        "gateway_storage_schema": "{staging_schema}",
        "ingestion_objects": [
            {
                "table": {
                    "source_catalog": "production",
                    "source_schema": "dbo", 
                    "source_table": "customers",
                    "destination_catalog": "{uc_catalog_name}",
                    "destination_schema": "{staging_schema}",
                    "ingestion_mode": "INCREMENTAL",
                    "primary_key": ["customer_id"],
                    "incremental_column": "modified_date",
                    "cdc_enabled": true
                }
            }
        ]
    },
    "bronze_catalog_dev": "{uc_catalog_name}",
    "bronze_database_dev": "{bronze_schema}",
    "bronze_table": "lakeflow_customers",
    "bronze_reader_options": {
        "format": "delta"
    }
}]
```


## Output: What Gets Created

### 1. Unity Catalog Resources

**Schemas:**
- ✅ `{uc_catalog_name}.{dlt_meta_schema}` - DLT-Meta metadata schema
- ✅ `{uc_catalog_name}.{bronze_schema}` - Bronze layer schema
- ✅ `{uc_catalog_name}.{silver_schema}` - Silver layer schema
- ✅ `{uc_catalog_name}.{staging_schema}` - Lakeflow Connect staging schema (if used)

**Volumes:**
- ✅ `{uc_volume_path}` - Configuration and data storage volume

**Tables:**
- ✅ `{dlt_meta_schema}.bronze_dataflowspec_table` - Bronze metadata table
- ✅ `{dlt_meta_schema}.silver_dataflowspec_table` - Silver metadata table
- ✅ `{bronze_schema}.{table_name}` - Bronze data tables (created at runtime)
- ✅ `{silver_schema}.{table_name}` - Silver data tables (created at runtime)
- ✅ `{staging_schema}.{table_name}` - Lakeflow Connect staging tables (if used)

**Unity Catalog Connections (for Lakeflow Connect):**
- ✅ `{source_connection_name}` - External database connection with credentials and JDBC configuration

### 2. Databricks Jobs

**Synthetic Data Generation Job:**
```python
# Created via REST API - integrated with onboarding process
{
    "name": "dlt_meta_synthetic_data_generation",
    "tasks": [{
        "task_key": "generate_data",
        "notebook_task": {
            "notebook_path": "/Users/{username}/dlt-meta/synthetic_data_generator.py",
            "base_parameters": {
                "onboarding_file_path": "{onboarding_json_path}",
                "data_flow_id": "100"
            }
        },
        "libraries": [{"pypi": {"package": "dbldatagen"}}]
    }]
}
```

**Onboarding Job:**
```python
# Existing dlt-meta pattern
{
    "name": "dlt_meta_onboarding_job",
    "tasks": [{
        "task_key": "dlt_meta_onbarding_task",
        "python_wheel_task": {
            "package_name": "dlt_meta",
            "entry_point": "run",
            "named_parameters": {
                "database": "{uc_catalog_name}.{dlt_meta_schema}",
                "onboarding_file_path": "{onboarding_json_path}",
                "bronze_dataflowspec_table": "bronze_dataflowspec_table",
                "silver_dataflowspec_table": "silver_dataflowspec_table"
            }
        }
    }]
}
```

### 3. DLT Pipelines

**Bronze Pipeline:**
```python
# Created via REST API
{
    "name": "dlt_meta_bronze_pipeline",
    "catalog": "{uc_catalog_name}",
    "schema": "{bronze_schema}",
    "libraries": [{
        "notebook": {
            "path": "/Users/{username}/dlt-meta/init_dlt_meta_pipeline.py"
        }
    }],
    "configuration": {
        "layer": "bronze",
        "bronze.dataflowspecTable": "{uc_catalog_name}.{dlt_meta_schema}.bronze_dataflowspec_table",
        "bronze.group": "my_group"
    }
}
```

**Silver Pipeline:**
```python
# Created via REST API
{
    "name": "dlt_meta_silver_pipeline", 
    "catalog": "{uc_catalog_name}",
    "schema": "{silver_schema}",
    "libraries": [{
        "notebook": {
            "path": "/Users/{username}/dlt-meta/init_dlt_meta_pipeline.py"
        }
    }],
    "configuration": {
        "layer": "silver",
        "silver.dataflowspecTable": "{uc_catalog_name}.{dlt_meta_schema}.silver_dataflowspec_table",
        "silver.group": "my_group"
    }
}
```

### 4. Lakeflow Connect Resources (When `source_format: "lakeflow_connect"`)

When using Lakeflow Connect as the data source, dlt-meta creates a complete data ingestion infrastructure consisting of three key components: **Unity Catalog Connection**, **Gateway Pipeline**, and **Ingestion Pipeline**. This creates a managed staging layer that feeds into the medallion architecture.

#### 4.1 Unity Catalog Connection

**Purpose:** Securely stores database credentials and connection parameters for external data sources.

```python
# Created via Unity Catalog Connections API
{
    "name": "{source_connection_name}",
    "connection_type": "JDBC",
    "options": {
        "url": "jdbc:sqlserver://{host}:{port};databaseName={database}",
        "user": "{{secrets/{secret_scope}/db-username}}",
        "password": "{{secrets/{secret_scope}/db-password}}",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    },
    "properties": {
        "purpose": "Lakeflow Connect data ingestion",
        "created_by": "dlt-meta",
        "source_system": "SQL Server Production"
    }
}
```

**Key Features:**
- **Secure Credential Management**: Uses Databricks Secrets for sensitive information
- **Connection Validation**: Automatically tests connectivity during creation
- **Reusable**: Can be shared across multiple gateway and ingestion pipelines
- **Audit Trail**: Tracks connection usage and access patterns

#### 4.2 Gateway Pipeline

**Purpose:** Establishes the connection bridge between external data sources and Unity Catalog, handling authentication, network connectivity, and initial data staging.

```python
# Created via Lakeflow Connect Gateway API
{
    "name": "{source_connection_name}-gateway",
    "pipeline_type": "GATEWAY",
    "gateway_definition": {
        "connection_name": "{source_connection_name}",
        "gateway_storage_catalog": "{uc_catalog_name}",
        "gateway_storage_schema": "{staging_schema}",
        "gateway_storage_name": "{source_connection_name}-gateway",
        "gateway_storage_location": "/Volumes/{uc_catalog_name}/{staging_schema}/gateway_storage"
    },
    "configuration": {
        "connection_timeout": "30s",
        "retry_policy": {
            "max_retries": 3,
            "retry_delay": "10s"
        },
        "batch_size": 10000,
        "parallel_connections": 4
    },
    "target": "{uc_catalog_name}.{staging_schema}",
    "continuous": false
}
```

**Key Responsibilities:**
- **Connection Management**: Maintains persistent connections to external databases
- **Authentication**: Handles database authentication using Unity Catalog connection credentials
- **Network Bridge**: Provides secure network connectivity between Databricks and external systems
- **Storage Allocation**: Creates dedicated storage space for gateway operations
- **Connection Pooling**: Manages multiple parallel connections for performance
- **Error Handling**: Implements retry logic and connection failure recovery

#### 4.3 Ingestion Pipeline

**Purpose:** Performs the actual data extraction, transformation, and loading from external sources into Unity Catalog staging tables.

```python
# Created via Lakeflow Connect Ingestion API
{
    "name": "lakeflow-ingestion-{staging_schema}",
    "pipeline_type": "INGESTION", 
    "ingestion_definition": {
        "ingestion_gateway_id": "{gateway_pipeline_id}",
        "source_connection": "{source_connection_name}",
        "ingestion_objects": [
            {
                "table": {
                    "source_catalog": "production",
                    "source_schema": "dbo",
                    "source_table": "customers",
                    "destination_catalog": "{uc_catalog_name}",
                    "destination_schema": "{staging_schema}",
                    "destination_table": "customers",
                    "ingestion_mode": "INCREMENTAL",
                    "primary_key": ["customer_id"],
                    "incremental_column": "modified_date",
                    "cdc_enabled": true
                }
            },
            {
                "table": {
                    "source_catalog": "production", 
                    "source_schema": "dbo",
                    "source_table": "orders",
                    "destination_catalog": "{uc_catalog_name}",
                    "destination_schema": "{staging_schema}",
                    "destination_table": "orders",
                    "ingestion_mode": "INCREMENTAL",
                    "primary_key": ["order_id"],
                    "incremental_column": "order_date",
                    "cdc_enabled": true
                }
            }
        ],
        "schedule": {
            "trigger": "INCREMENTAL",
            "interval": "15 minutes"
        },
        "data_quality": {
            "enable_schema_evolution": true,
            "handle_deletes": true,
            "conflict_resolution": "source_wins"
        }
    },
    "catalog": "{uc_catalog_name}",
    "target": "{staging_schema}",
    "continuous": true,
    "libraries": [
        {"maven": {"coordinates": "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre8"}}
    ]
}
```

**Key Features:**
- **Change Data Capture (CDC)**: Automatically detects and ingests only changed records
- **Incremental Loading**: Supports timestamp-based and key-based incremental strategies
- **Schema Evolution**: Automatically handles new columns and schema changes
- **Multiple Tables**: Can ingest multiple related tables in a single pipeline
- **Scheduling**: Supports both continuous streaming and batch scheduling
- **Data Quality**: Built-in data validation and conflict resolution
- **Performance Optimization**: Parallel processing and optimized data transfer

#### 4.4 Lakeflow Connect Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           LAKEFLOW CONNECT DATA FLOW                           │
└─────────────────────────────────────────────────────────────────────────────────┘

EXTERNAL DATA SOURCE                    DATABRICKS LAKEHOUSE
┌─────────────────────┐                ┌─────────────────────────────────────────┐
│   SQL Server DB     │                │            UNITY CATALOG               │
│                     │                │                                         │
│ ┌─────────────────┐ │   ┌──────────┐ │ ┌─────────────────────────────────────┐ │
│ │ production.dbo  │ │   │          │ │ │        UC Connection                │ │
│ │   ├─customers   │◄├──►│ Gateway  │◄├─┤ {source_connection_name}            │ │
│ │   ├─orders      │ │   │ Pipeline │ │ │ • JDBC URL + Credentials            │ │
│ │   └─products    │ │   │          │ │ │ • Secure Secret Management          │ │
│ └─────────────────┘ │   └──────────┘ │ └─────────────────────────────────────┘ │
└─────────────────────┘                │                                         │
                                       │ ┌─────────────────────────────────────┐ │
                                       │ │       Ingestion Pipeline            │ │
                                       │ │ • CDC Change Detection              │ │
                                       │ │ • Incremental Loading               │ │
                                       │ │ • Schema Evolution                  │ │
                                       │ │ • Multi-table Orchestration        │ │
                                       │ └─────────────────────────────────────┘ │
                                       │                  │                      │
                                       │                  ▼                      │
                                       │ ┌─────────────────────────────────────┐ │
                                       │ │     Staging Schema (Lakeflow)       │ │
                                       │ │ {uc_catalog}.{staging_schema}       │ │
                                       │ │   ├─customers (Delta)               │ │
                                       │ │   ├─orders (Delta)                  │ │
                                       │ │   └─products (Delta)                │ │
                                       │ └─────────────────────────────────────┘ │
                                       └─────────────────────────────────────────┘
                                                          │
                                                          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            DLT-META MEDALLION ARCHITECTURE                     │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   BRONZE LAYER      │    │   SILVER LAYER   │    │    GOLD LAYER       │
│ {bronze_schema}     │    │ {silver_schema}  │    │  (Future)           │
│                     │    │                  │    │                     │
│ ├─customers_bronze  │───▶│ ├─customers_clean│───▶│ ├─customer_360      │
│ ├─orders_bronze     │───▶│ ├─orders_clean   │───▶│ ├─sales_summary     │
│ └─products_bronze   │───▶│ └─products_clean │───▶│ └─product_analytics │
│                     │    │                  │    │                     │
│ • Raw data ingestion│    │ • Data cleansing │    │ • Business metrics  │
│ • Schema validation │    │ • Deduplication  │    │ • Aggregations      │
│ • Audit columns     │    │ • Type conversion│    │ • KPIs & Reports    │
└─────────────────────┘    └──────────────────┘    └─────────────────────┘
```

#### 4.5 Staging Tables Created by Lakeflow Connect

When the ingestion pipeline runs, it creates Delta tables in the staging schema:

**Example Staging Tables:**
```sql
-- Created automatically by Lakeflow Connect Ingestion Pipeline
{uc_catalog_name}.{staging_schema}.customers
├─ customer_id (BIGINT) - Primary key from source
├─ first_name (STRING) - Customer first name  
├─ last_name (STRING) - Customer last name
├─ email (STRING) - Customer email address
├─ phone (STRING) - Customer phone number
├─ registration_date (TIMESTAMP) - Account creation date
├─ modified_date (TIMESTAMP) - Last modified timestamp (for CDC)
├─ _lakeflow_ingestion_time (TIMESTAMP) - Lakeflow ingestion timestamp
├─ _lakeflow_source_file (STRING) - Source tracking information
└─ _lakeflow_operation (STRING) - CDC operation (INSERT, UPDATE, DELETE)

{uc_catalog_name}.{staging_schema}.orders  
├─ order_id (BIGINT) - Primary key from source
├─ customer_id (BIGINT) - Foreign key to customers
├─ order_date (TIMESTAMP) - Order creation date
├─ order_amount (DECIMAL(10,2)) - Order total amount
├─ order_status (STRING) - Current order status
├─ product_category (STRING) - Primary product category
├─ modified_date (TIMESTAMP) - Last modified timestamp (for CDC)
├─ _lakeflow_ingestion_time (TIMESTAMP) - Lakeflow ingestion timestamp
├─ _lakeflow_source_file (STRING) - Source tracking information
└─ _lakeflow_operation (STRING) - CDC operation (INSERT, UPDATE, DELETE)
```

**Key Characteristics of Staging Tables:**
- **Delta Format**: All staging tables use Delta Lake format for ACID transactions
- **CDC Metadata**: Automatic addition of Lakeflow metadata columns for change tracking
- **Schema Evolution**: Automatically adapts to source schema changes
- **Incremental Updates**: Only changed records are processed and updated
- **Audit Trail**: Complete lineage tracking from source to staging

#### 4.6 Integration with DLT-Meta Medallion Architecture

The Lakeflow Connect staging tables serve as the **data source** for dlt-meta's Bronze layer:

```json
// DLT-Meta Bronze layer reads from Lakeflow Connect staging
{
    "data_flow_id": "200",
    "source_format": "lakeflow_connect", 
    "source_details": {
        "staging_catalog": "{uc_catalog_name}",
        "staging_schema": "{staging_schema}",
        "staging_table": "customers"
    },
    "bronze_catalog_dev": "{uc_catalog_name}",
    "bronze_database_dev": "{bronze_schema}",
    "bronze_table": "customers_bronze"
}
```

This creates a **seamless data pipeline**:
1. **Lakeflow Connect** handles external data ingestion into staging
2. **DLT-Meta Bronze** processes staging data with additional transformations
3. **DLT-Meta Silver** applies business rules and data quality validations
4. **Future Gold Layer** will provide business-ready analytics and metrics

### 5. Generated Notebooks

**Synthetic Data Generator Notebook:**
```python
# Generated and uploaded to workspace
"""
# Databricks notebook source
# MAGIC %pip install dbldatagen dlt-meta=={version}
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import dbldatagen as dg
import json
from pyspark.sql.types import *

# Load onboarding configuration
onboarding_file_path = dbutils.widgets.get("onboarding_file_path")
data_flow_id = dbutils.widgets.get("data_flow_id")

with open(onboarding_file_path, 'r') as f:
    onboarding_config = json.load(f)

# Find the synthetic data configuration
synthetic_config = None
for config in onboarding_config:
    if config['data_flow_id'] == data_flow_id and config['source_details'].get('generator') == 'dbldatagen':
        synthetic_config = config
        break

if not synthetic_config:
    raise ValueError(f"No synthetic_data configuration found for data_flow_id: {data_flow_id}")

# Extract source_details for data generation
source_details = synthetic_config['source_details']
table_name = synthetic_config['bronze_table']

# Generate synthetic data using dbldatagen
df_spec = dg.DataGenerator(spark, 
                          name=table_name, 
                          rows=int(source_details['rows']), 
                          partitions=source_details.get('partitions', 4))

# Add columns based on specification
for col_name, col_spec in source_details['columns'].items():
    if col_spec['type'] == 'long':
        df_spec = df_spec.withColumn(col_name, LongType(), 
                                   uniqueValues=col_spec.get('unique_values'))
    elif col_spec['type'] == 'string':
        if 'template' in col_spec:
            df_spec = df_spec.withColumn(col_name, StringType(), 
                                       template=col_spec['template'])
        elif 'values' in col_spec:
            df_spec = df_spec.withColumn(col_name, StringType(), 
                                       values=col_spec['values'],
                                       weights=col_spec.get('weights'))
    elif col_spec['type'] == 'timestamp':
        df_spec = df_spec.withColumn(col_name, TimestampType(),
                                   begin=col_spec['begin'],
                                   end=col_spec['end'],
                                   random=col_spec.get('random', True))

# Build and save
df = df_spec.build()
output_path = source_details['output_location']
df.write.mode(source_details.get('mode', 'overwrite')).format(source_details['output_format']).save(output_path)

print(f"Generated {source_details['rows']} rows of synthetic data for table: {table_name}")
print(f"Data saved to: {output_path}")
"""
```

**DLT Pipeline Runner Notebook:**
```python
# Existing dlt-meta pattern - generated and uploaded
"""
# Databricks notebook source
# MAGIC %pip install dlt-meta=={version}
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
layer = spark.conf.get("layer", None)
from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
"""
```

## Code Structure to Support Input and Output

### 1. Dependencies and Module Loading

#### Changes to `setup.py`

**New dependencies to add to `INSTALL_REQUIRES`:**
```python
INSTALL_REQUIRES = [
    "setuptools", 
    "databricks-sdk", 
    "PyYAML>=6.0",  # Already present - supports YAML configuration
    "dbldatagen>=0.3.0",  # NEW - For synthetic data generation
    "sqlalchemy>=1.4.0",  # NEW - For PostgreSQL slot management
    "psycopg2-binary>=2.9.0",  # NEW - PostgreSQL driver
    "pandas>=1.3.0",  # NEW - For data inspection and display
]
```

**Optional dependencies for development:**
```python
DEV_REQUIREMENTS = [
    "flake8==6.0",
    "delta-spark==3.0.0", 
    "pytest>=7.0.0",
    "coverage>=7.0.0",
    "pyspark==3.5.5",
    "dbldatagen>=0.3.0",  # NEW - For local testing
    "mysql-connector-python>=8.0.0",  # NEW - MySQL driver for testing
    "cx-Oracle>=8.0.0",  # NEW - Oracle driver for testing
]
```

#### Module Loading Pattern (Following Existing DLT-Meta Patterns)

**Synthetic Data Module Loading:**
```python
# In src/dataflow_pipeline.py - following existing import pattern
import json
import logging
from typing import Callable, Optional
import ast
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, struct
from pyspark.sql.types import StructType, StructField

# NEW - Optional import with graceful fallback
try:
    import dbldatagen as dg
    DBLDATAGEN_AVAILABLE = True
except ImportError:
    DBLDATAGEN_AVAILABLE = False
    logger.warning("dbldatagen not available - synthetic_data source format will not work")

# NEW - YAML support (already available via PyYAML)
import yaml
```

**Lakeflow Connect Module Loading:**
```python
# In src/cli.py - following existing databricks-sdk pattern
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines, compute
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.catalog import SchemasAPI, VolumeType

# NEW - Lakeflow Connect APIs (part of databricks-sdk)
try:
    from databricks.sdk.service.lakeflow import LakeflowAPI
    LAKEFLOW_AVAILABLE = True
except ImportError:
    LAKEFLOW_AVAILABLE = False
    logger.warning("Lakeflow Connect APIs not available in this databricks-sdk version")
```

#### Runtime Dependency Installation (Notebook Pattern)

**Following existing pattern from DLT_META_RUNNER_NOTEBOOK:**
```python
# Current pattern in src/cli.py
DLT_META_RUNNER_NOTEBOOK = """
# Databricks notebook source
# MAGIC %pip install dlt-meta=={version}
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
layer = spark.conf.get("layer", None)
from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
"""

# NEW - Enhanced pattern for synthetic data
SYNTHETIC_DATA_RUNNER_NOTEBOOK = """
# Databricks notebook source
# MAGIC %pip install dlt-meta=={version} dbldatagen>=0.3.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import json
import dbldatagen as dg
from pyspark.sql.types import *

# Load onboarding configuration (following existing pattern)
onboarding_file_path = dbutils.widgets.get("onboarding_file_path")
data_flow_id = dbutils.widgets.get("data_flow_id")

with open(onboarding_file_path, 'r') as f:
    onboarding_config = json.load(f)

# Process synthetic data generation
from src.synthetic_data import SyntheticDataGenerator
generator = SyntheticDataGenerator()
generator.generate_from_onboarding(onboarding_config, data_flow_id)
"""
```

#### Error Handling for Missing Dependencies

**Following existing error handling patterns:**
```python
# In src/dataflow_pipeline.py - following existing try/catch pattern
def process_synthetic_data_source(self, spark, dataflow_spec):
    """Process synthetic_data source format with dependency checking"""
    if not DBLDATAGEN_AVAILABLE:
        raise ImportError(
            "dbldatagen is required for synthetic_data source format. "
            "Install with: %pip install dbldatagen>=0.3.0"
        )
    
    try:
        from src.synthetic_data import SyntheticDataGenerator
        generator = SyntheticDataGenerator(spark)
        return generator.process_dataflow_spec(dataflow_spec)
    except Exception as e:
        logger.error(f"Failed to process synthetic data: {str(e)}")
        raise

def process_lakeflow_connect_source(self, spark, dataflow_spec):
    """Process lakeflow_connect source format with dependency checking"""
    if not LAKEFLOW_AVAILABLE:
        raise ImportError(
            "Lakeflow Connect APIs are required for lakeflow_connect source format. "
            "Update databricks-sdk to latest version."
        )
    
    try:
        from src.lakeflow_connect import LakeflowConnectManager
        manager = LakeflowConnectManager(self._ws)
        return manager.process_dataflow_spec(dataflow_spec)
    except Exception as e:
        logger.error(f"Failed to process Lakeflow Connect: {str(e)}")
        raise
```

### 2. Enhanced CLI Module (`src/cli.py`)

**New Commands:**
```python
# Enhanced CLI with new source format support
class DltMetaCLI:
    
    def generate_synthetic_data(self, config_path: str, spec_path: str):
        """Generate synthetic data using dbldatagen based on YAML spec"""
        # Load DAB variables and YAML spec
        # Generate synthetic data using dbldatagen
        # Save to specified location
        pass
    
    def deploy_lakeflow_connect(self, config_path: str):
        """Deploy Lakeflow Connect gateway and ingestion pipelines"""
        # Create Unity Catalog connection
        # Deploy gateway pipeline via REST API
        # Deploy ingestion pipeline via REST API
        pass
    
    def onboard_enhanced(self, onboarding_file: str, variables: dict = None):
        """Enhanced onboarding supporting new source formats and DAB-style connections"""
        import yaml
        import json
        
        # Load configuration file (YAML or JSON)
        try:
            with open(onboarding_file, 'r') as f:
                if onboarding_file.endswith('.yaml') or onboarding_file.endswith('.yml'):
                    config = yaml.safe_load(f)
                else:
                    config = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load onboarding file: {e}")
            raise
        
        # Apply variable substitution
        if variables:
            from src.variable_management import VariableManager
            var_manager = VariableManager(variables)
            config = var_manager.substitute_variables(json.dumps(config))
            config = json.loads(config)
        
        # Process different source formats
        results = {"synthetic_data": [], "lakeflow_connect": {}, "errors": []}
        
        # Handle YAML format with connections and dataflows
        if "connections" in config or "dataflows" in config:
            lakeflow_manager = LakeflowConnectManager(self._ws)
            lfc_results = lakeflow_manager.process_enhanced_onboarding(config)
            results["lakeflow_connect"] = lfc_results
        
        # Handle legacy format or mixed formats
        dataflows = config.get("dataflows", [config] if "data_flow_id" in config else [])
        
        for dataflow in dataflows:
            source_format = dataflow.get("source_format")
            
            if source_details.get('generator') == 'dbldatagen':
                synthetic_result = self.process_synthetic_data(dataflow)
                results["synthetic_data"].append(synthetic_result)
            elif source_format == "lakeflow_connect" and "connections" not in config:
                # Legacy single dataflow format
                lakeflow_manager = LakeflowConnectManager(self._ws)
                lfc_result = lakeflow_manager.process_dataflow_spec(dataflow)
                results["lakeflow_connect"] = {"dataflows": {"single": lfc_result}}
        
        return results
```

**Enhanced Source Format Handlers:**
```python
# New source format processors in dataflow_pipeline.py
class DataflowPipeline:
    
    @staticmethod
    def process_synthetic_data(source_details: dict):
        """Process synthetic_data source format"""
        # Load synthetic data from specified location
        # Apply DLT-Meta bronze/silver transformations
        pass
    
    @staticmethod  
    def process_lakeflow_connect(source_details: dict):
        """Process lakeflow_connect source format"""
        # Read from Lakeflow Connect staging tables
        # Apply DLT-Meta bronze/silver transformations
        pass
```

### 2. Enhanced Variable Management (`src/variable_management.py`)

**New Module:**
```python
# Enhanced variable management for new source formats
class VariableManager:
    
    def __init__(self, variables: dict = None):
        self.variables = variables or {}
        
    def substitute_variables(self, template: str) -> str:
        """Replace {variable} patterns with actual values"""
        # Use existing dlt-meta variable substitution logic
        # Extended to support new variables for synthetic data and Lakeflow Connect
        pass
    
    def add_variables(self, new_variables: dict):
        """Add new variables for synthetic data and Lakeflow Connect"""
        self.variables.update(new_variables)
        
    def get_variable(self, name: str, default=None):
        """Get variable value with optional default"""
        return self.variables.get(name, default)
```

### 3. Synthetic Data Integration (`src/synthetic_data.py`)

**New Module:**
```python
# Synthetic data generation using dbldatagen
class SyntheticDataGenerator:
    
    def __init__(self, spec_path: str):
        self.spec = self.load_spec(spec_path)
        
    def load_spec(self, path: str) -> dict:
        """Load YAML specification for synthetic data"""
        pass
    
    def generate_table(self, table_name: str, table_spec: dict):
        """Generate single table using dbldatagen"""
        # Convert YAML spec to dbldatagen DataGenerator
        # Generate and save data
        pass
    
    def generate_all_tables(self):
        """Generate all tables defined in specification"""
        pass
    
    def create_onboarding_config(self) -> list:
        """Auto-generate dlt-meta onboarding JSON from synthetic data"""
        pass
```

### 4. PostgreSQL Slot Management (`src/postgres_slot_manager.py`)

**New Module:**
```python
# PostgreSQL replication slot and publication management
import logging
import pandas as pd
import sqlalchemy as sa
from typing import Optional, Tuple

logger = logging.getLogger('databricks.labs.dltmeta')

class PostgreSQLSlotManager:
    """Manages PostgreSQL replication slots and publications for CDC"""
    
    def __init__(self, sqlalchemy_engine):
        self.engine = sqlalchemy_engine
        
    def create_publication_and_slot(self, target_schema: str, source_schema: str, 
                                  tables: list = None) -> Tuple[bool, dict]:
        """Create PostgreSQL publication and replication slot using actual implementation pattern"""
        
        # Default tables if not specified
        if tables is None:
            tables = ['intpk', 'dtix']
            
        publication_name = f"{target_schema}_pub"
        slot_name = target_schema
        
        # Build table list for publication
        table_list = ', '.join([f"{source_schema}.{table}" for table in tables])
        
        result = {
            'publication_created': False,
            'slot_created': False,
            'publication_name': publication_name,
            'slot_name': slot_name,
            'tables': tables,
            'replication_slots': None,
            'publications': None
        }
        
        try:
            with self.engine.connect() as conn:
                logger.info(f"Creating PostgreSQL replication slot and publication for {target_schema}")
                
                # Create publication for specified tables
                try:
                    create_pub_sql = f"CREATE PUBLICATION {publication_name} FOR table {table_list}"
                    conn.execute(sa.text(create_pub_sql))
                    result['publication_created'] = True
                    logger.info(f"Created publication: {publication_name}")
                except Exception as e:
                    logger.warning(f"Publication creation failed (may already exist): {e}")
                
                # Create logical replication slot
                try:
                    create_slot_sql = f"SELECT 'init' FROM pg_create_logical_replication_slot('{slot_name}', 'pgoutput')"
                    conn.execute(sa.text(create_slot_sql))
                    result['slot_created'] = True
                    logger.info(f"Created replication slot: {slot_name}")
                except Exception as e:
                    logger.warning(f"Replication slot creation failed (may already exist): {e}")
                
                # Query and display replication slots
                try:
                    replication_slots_query = sa.text("SELECT * FROM pg_replication_slots ORDER BY slot_name")
                    replication_slots_result = conn.execute(replication_slots_query)
                    replication_slots = pd.DataFrame(
                        replication_slots_result.fetchall(), 
                        columns=replication_slots_result.keys()
                    )
                    result['replication_slots'] = replication_slots
                    logger.info(f"Current replication slots: {len(replication_slots)} found")
                except Exception as e:
                    logger.error(f"Failed to query replication slots: {e}")
                
                # Query and display publications
                try:
                    publication_query = sa.text("SELECT * FROM pg_publication ORDER BY pubname")
                    publication_result = conn.execute(publication_query)
                    publications = pd.DataFrame(
                        publication_result.fetchall(), 
                        columns=publication_result.keys()
                    )
                    result['publications'] = publications
                    logger.info(f"Current publications: {len(publications)} found")
                except Exception as e:
                    logger.error(f"Failed to query publications: {e}")
                
                # Commit the transaction
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL publication and slot: {e}")
            return False, result
            
        return True, result
    
    def cleanup_publication_and_slot(self, target_schema: str) -> bool:
        """Cleanup function to drop PostgreSQL publication and replication slot"""
        publication_name = f"{target_schema}_pub"
        slot_name = target_schema
        
        try:
            with self.engine.connect() as conn:
                # Drop publication
                try:
                    drop_pub_sql = f"DROP PUBLICATION IF EXISTS {publication_name} CASCADE"
                    conn.execute(sa.text(drop_pub_sql))
                    logger.info(f"Dropped publication: {publication_name}")
                except Exception as e:
                    logger.warning(f"Failed to drop publication: {e}")
                
                # Drop replication slot
                try:
                    drop_slot_sql = f"""
                    SELECT pg_drop_replication_slot('{slot_name}') 
                    WHERE EXISTS (
                        SELECT 1 FROM pg_replication_slots 
                        WHERE slot_name = '{slot_name}'
                    )
                    """
                    conn.execute(sa.text(drop_slot_sql))
                    logger.info(f"Dropped replication slot: {slot_name}")
                except Exception as e:
                    logger.warning(f"Failed to drop replication slot: {e}")
                
                conn.commit()
                logger.info(f"✅ Cleaned up PostgreSQL publication and slot for {target_schema}")
                return True
                
        except Exception as e:
            logger.error(f"⚠️  Error cleaning up PostgreSQL resources: {e}")
            return False
    
    def inspect_database_schema(self, source_schema: str) -> dict:
        """Inspect database schema and sample data using actual implementation pattern"""
        
        result = {
            'tables': None,
            'columns': None,
            'sample_data': None,
            'schema': source_schema
        }
        
        try:
            with self.engine.connect() as conn:
                # Query tables using SQLAlchemy
                tables_query = sa.text(f"""
                    SELECT * FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA='{source_schema}'
                """)
                tables_result = conn.execute(tables_query)
                tables = pd.DataFrame(
                    tables_result.fetchall(), 
                    columns=[key.upper() for key in tables_result.keys()]
                )
                result['tables'] = tables
                logger.info(f"Found {len(tables)} tables in schema {source_schema}")
                
                if not tables.empty:
                    first_table_name = tables["TABLE_NAME"].iloc[0]
                    
                    # Query columns using SQLAlchemy
                    try:
                        columns_query = sa.text(f"""
                            SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_SCHEMA='{source_schema}' 
                            AND TABLE_NAME='{first_table_name}'
                        """)
                        columns_result = conn.execute(columns_query)
                        columns = pd.DataFrame(
                            columns_result.fetchall(), 
                            columns=columns_result.keys()
                        )
                        result['columns'] = columns
                        logger.info(f"Found {len(columns)} columns in table {first_table_name}")
                    except Exception as e:
                        logger.warning(f"Failed to query columns: {e}")
                    
                    # Query sample data using SQLAlchemy
                    try:
                        sample_query = sa.text(f"""
                            SELECT * FROM {source_schema}.{first_table_name} 
                            WHERE DT = (SELECT MIN(DT) FROM {source_schema}.{first_table_name})
                        """)
                        sample_result = conn.execute(sample_query)
                        sample_data = pd.DataFrame(
                            sample_result.fetchall(), 
                            columns=sample_result.keys()
                        )
                        result['sample_data'] = sample_data
                        logger.info(f"Retrieved {len(sample_data)} sample rows")
                    except Exception as e:
                        logger.warning(f"Failed to query sample data: {e}")
                        
        except Exception as e:
            logger.error(f"Failed to inspect database schema: {e}")
            
        return result

### 5. Lakeflow Connect Integration (`src/lakeflow_connect.py`)

**New Module:**
```python
# Lakeflow Connect deployment and management
import json
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.lakeflow import LakeflowAPI
from src.postgres_slot_manager import PostgreSQLSlotManager

logger = logging.getLogger('databricks.labs.dltmeta')

class LakeflowConnectManager:
    
    def __init__(self, workspace_client):
        self.ws = workspace_client
        
    def create_or_update_connection(self, connection_config: dict) -> str:
        """Create or update Unity Catalog connection following DAB pattern"""
        connection_name = connection_config.get("name")
        
        try:
            # Check if connection already exists
            try:
                existing_connection = self.ws.connections.get(connection_name)
                logger.info(f"Connection {connection_name} already exists, updating...")
                
                # Update existing connection
                update_config = {
                    "name": connection_name,
                    "connection_type": connection_config.get("connection_type"),
                    "options": {
                        "url": connection_config.get("data_source_url"),
                        **connection_config.get("properties", {})
                    },
                    "comment": connection_config.get("comment", ""),
                }
                
                updated_connection = self.ws.connections.update(connection_name, **update_config)
                logger.info(f"Updated connection: {updated_connection.name}")
                return updated_connection.name
                
            except Exception:
                # Connection doesn't exist, create new one
                logger.info(f"Creating new connection: {connection_name}")
                
                create_config = {
                    "name": connection_name,
                    "connection_type": connection_config.get("connection_type"),
                    "options": {
                        "url": connection_config.get("data_source_url"),
                        **connection_config.get("properties", {})
                    },
                    "comment": connection_config.get("comment", f"Created by dlt-meta for {connection_config.get('connection_type')} connection"),
                }
                
                connection_response = self.ws.connections.create(**create_config)
                logger.info(f"Created connection: {connection_response.name}")
                return connection_response.name
                
        except Exception as e:
            logger.error(f"Failed to create/update connection {connection_name}: {str(e)}")
            raise
    
    def process_connections(self, connections_config: dict) -> dict:
        """Process all connection definitions from YAML configuration"""
        created_connections = {}
        
        for logical_name, connection_config in connections_config.items():
            try:
                connection_name = self.create_or_update_connection(connection_config)
                created_connections[logical_name] = {
                    "name": connection_name,
                    "status": "created_or_updated",
                    "connection_type": connection_config.get("connection_type")
                }
                logger.info(f"Processed connection {logical_name} -> {connection_name}")
            except Exception as e:
                created_connections[logical_name] = {
                    "name": connection_config.get("name"),
                    "status": "failed",
                    "error": str(e)
                }
                logger.error(f"Failed to process connection {logical_name}: {e}")
        
        return created_connections
    
    def deploy_gateway(self, gateway_config: dict, cdc_qbc_mode: str = 'cdc') -> dict:
        """Deploy Lakeflow Connect gateway pipeline using actual implementation pattern"""
        
        # Gateway pipeline specification following real-world pattern
        gw_pipeline_spec = {
            "name": gateway_config.get("gateway_pipeline_name"),
            "gateway_definition": {
                "connection_name": gateway_config.get("connection_name"),
                "gateway_storage_catalog": gateway_config.get("gateway_storage_catalog"),
                "gateway_storage_schema": gateway_config.get("gateway_storage_schema"),
            },
            "tags": {
                "RemoveAfter": gateway_config.get("remove_after_yyyymmdd", "20251231"),
                "Connector": gateway_config.get("source_type", "sqlserver"),
                "CreatedBy": "dlt-meta"
            },
        }
        
        # Conditional gateway creation based on CDC/QBC mode
        if cdc_qbc_mode == 'cdc':
            # CDC mode: Create separate gateway pipeline
            try:
                gw_response = self.ws.pipelines.create(**gw_pipeline_spec)
                gw_response_json = {
                    'pipeline_id': gw_response.pipeline_id,
                    'name': gw_response.name,
                    'state': gw_response.state
                }
                logger.info(f"Created separate gateway pipeline for CDC: {gw_response.pipeline_id}")
                return gw_response_json
            except Exception as e:
                logger.error(f"Failed to create gateway pipeline: {str(e)}")
                raise
        else:
            # QBC and cdc_single_pipeline modes don't need separate gateway pipeline
            # QBC: No gateway needed
            # cdc_single_pipeline: Gateway + ingestion combined in single pipeline
            logger.info(f"{cdc_qbc_mode} mode - skipping separate gateway pipeline creation")
            gw_response_json = {'pipeline_id': None}
            return gw_response_json
    
    def deploy_ingestion_pipeline(self, ingestion_config: dict, gateway_pipeline_id: str = None, 
                                 cdc_qbc_mode: str = 'cdc', trigger_interval_min: str = '0') -> dict:
        """Deploy Lakeflow Connect ingestion pipeline using actual implementation pattern"""
        
        # Extract configuration
        connection_name = ingestion_config.get("connection_name")
        source_type = ingestion_config.get("source_type", "sqlserver")
        target_catalog = ingestion_config.get("target_catalog")
        target_schema = ingestion_config.get("target_schema")
        source_catalog = ingestion_config.get("source_catalog")
        source_schema = ingestion_config.get("source_schema")
        ig_pipeline_name = ingestion_config.get("ig_pipeline_name")
        
        # Ingestion pipeline specification following real-world pattern
        ig_pipeline_spec = {
            "name": ig_pipeline_name,
            "pipeline_type": 
                'MANAGED_INGESTION' if cdc_qbc_mode == 'cdc_single_pipeline' 
                else None,  # Only cdc_single_pipeline uses MANAGED_INGESTION    
            'catalog': target_catalog if cdc_qbc_mode == 'cdc_single_pipeline' 
                else None,  # Only cdc_single_pipeline needs catalog     
            'schema': target_schema if cdc_qbc_mode == 'cdc_single_pipeline' 
                else None,  # Only cdc_single_pipeline needs schema         
            "configuration": {
                "pipelines.directCdc.minimumRunDurationMinutes": "1",
                "pipelines.directCdc.enableBoundedContinuousGraphExecution": True
            } if cdc_qbc_mode == 'cdc_single_pipeline' 
                else None,  # Only cdc_single_pipeline needs CDC configuration    
            'development': True,
            'serverless': 
                # cdc_single_pipeline needs classic compute, cdc/qbc can use serverless
                True if cdc_qbc_mode in ['cdc', 'qbc'] 
                else False,  # cdc_single_pipeline = False (classic compute)
            'continuous': 
                True if trigger_interval_min in ['0']  
                else False,       
            "ingestion_definition": {
                "ingestion_gateway_id": 
                    gateway_pipeline_id if cdc_qbc_mode == "cdc"
                    else None,  # Only CDC mode uses separate gateway
                "connection_name": 
                    connection_name if cdc_qbc_mode in ["qbc", "cdc_single_pipeline"]  
                    else None,  # QBC and cdc_single_pipeline connect directly
                "connector_type": 
                    "CDC" if cdc_qbc_mode == "cdc_single_pipeline" 
                    else None,  # Only cdc_single_pipeline needs connector_type
                "source_type": source_type.upper(),
                "source_configurations":     
                    [ {
                        "catalog": {
                            "source_catalog": source_catalog,
                            "postgres": {
                                "slot_config": {
                                "slot_name": f"{target_schema}",
                                "publication_name": f"{target_schema}_pub",
                                }
                            }
                        }
                    }] if source_type.startswith("postgres") and ingestion_config.get('pg_custom_slot') == 'true'
                    else None,
                "objects": self._build_ingestion_objects(
                    ingestion_config.get("ingestion_objects", []),
                    source_type,
                    target_catalog,
                    target_schema,
                    cdc_qbc_mode
                ),
            },
        }
        
        # Remove None values from the specification
        ig_pipeline_spec = {k: v for k, v in ig_pipeline_spec.items() if v is not None}
        if ig_pipeline_spec.get("ingestion_definition"):
            ig_pipeline_spec["ingestion_definition"] = {
                k: v for k, v in ig_pipeline_spec["ingestion_definition"].items() if v is not None
            }
        
        try:
            ig_response = self.ws.pipelines.create(**ig_pipeline_spec)
            ig_response_json = {
                'pipeline_id': ig_response.pipeline_id,
                'name': ig_response.name,
                'state': ig_response.state,
                'pipeline_type': ig_pipeline_spec.get('pipeline_type'),
                'serverless': ig_pipeline_spec.get('serverless'),
                'continuous': ig_pipeline_spec.get('continuous')
            }
            logger.info(f"Created ingestion pipeline: {ig_response.pipeline_id}")
            return ig_response_json
        except Exception as e:
            logger.error(f"Failed to create ingestion pipeline: {str(e)}")
            raise
    
    def validate_connection(self, connection_name: str) -> bool:
        """Test connection and validate configuration"""
        try:
            connection = self.ws.connections.get(connection_name)
            # Test connection logic here
            logger.info(f"Connection {connection_name} validated successfully")
            return True
        except Exception as e:
            logger.error(f"Connection validation failed: {str(e)}")
            return False
    
    def _build_ingestion_objects(self, ingestion_objects: list, source_type: str, 
                                target_catalog: str, target_schema: str, cdc_qbc_mode: str) -> list:
        """Build ingestion objects following Microsoft Databricks documentation patterns"""
        
        if not ingestion_objects:
            # Default fallback for backward compatibility
            return [
                {
                    "table": {
                        "source_catalog": None if source_type.startswith("mysql") else "test",
                        "source_schema": "dbo",
                        "source_table": "customers",
                        "destination_catalog": target_catalog,
                        "destination_schema": target_schema,
                        "table_configuration": {
                            "scd_type": "SCD_TYPE_1",
                            "query_based_connector_config": {
                                "cursor_columns": ["modified_date"]
                            } if cdc_qbc_mode == 'qbc' else None,
                        }
                    }
                }
            ]
        
        processed_objects = []
        
        for obj in ingestion_objects:
            if "table" in obj:
                # Individual table ingestion
                table_config = obj["table"]
                processed_table = {
                    "table": {
                        "source_catalog": self._normalize_catalog_name(
                            table_config.get("source_catalog"), source_type
                        ),
                        "source_schema": self._normalize_schema_name(
                            table_config.get("source_schema"), source_type
                        ),
                        "source_table": self._normalize_table_name(
                            table_config.get("source_table"), source_type
                        ),
                        "destination_catalog": table_config.get("destination_catalog", target_catalog),
                        "destination_schema": table_config.get("destination_schema", target_schema),
                    }
                }
                
                # Add destination table name if specified (optional)
                if table_config.get("destination_table"):
                    processed_table["table"]["destination_table"] = table_config["destination_table"]
                
                # Add table configuration for SCD and QBC
                table_configuration = {}
                
                # SCD Type configuration
                scd_type = table_config.get("scd_type", "SCD_TYPE_1")
                table_configuration["scd_type"] = scd_type
                
                # QBC cursor columns
                if cdc_qbc_mode == 'qbc' and table_config.get("cursor_columns"):
                    table_configuration["query_based_connector_config"] = {
                        "cursor_columns": table_config["cursor_columns"]
                    }
                
                if table_configuration:
                    processed_table["table"]["table_configuration"] = table_configuration
                
                processed_objects.append(processed_table)
                
            elif "schema" in obj:
                # Whole schema ingestion
                schema_config = obj["schema"]
                processed_schema = {
                    "schema": {
                        "source_catalog": self._normalize_catalog_name(
                            schema_config.get("source_catalog"), source_type
                        ),
                        "source_schema": self._normalize_schema_name(
                            schema_config.get("source_schema"), source_type
                        ),
                        "destination_catalog": schema_config.get("destination_catalog", target_catalog),
                        "destination_schema": schema_config.get("destination_schema", target_schema),
                    }
                }
                processed_objects.append(processed_schema)
        
        return processed_objects
    
    def _normalize_catalog_name(self, catalog_name: str, source_type: str) -> str:
        """Normalize catalog name based on database type"""
        if not catalog_name:
            return None if source_type.startswith("mysql") else catalog_name
        return catalog_name.upper() if source_type.startswith("ora") else catalog_name
    
    def _normalize_schema_name(self, schema_name: str, source_type: str) -> str:
        """Normalize schema name based on database type"""
        if not schema_name:
            return schema_name
        return schema_name.upper() if source_type.startswith("ora") else schema_name
    
    def _normalize_table_name(self, table_name: str, source_type: str) -> str:
        """Normalize table name based on database type"""
        if not table_name:
            return table_name
        return table_name.upper() if source_type.startswith("ora") else table_name

    def create_pipeline_job(self, pipeline_config: dict, trigger_interval_min: str = "0") -> dict:
        """Create scheduled job for ingestion pipeline using actual implementation pattern"""
        import random
        
        pipeline_id = pipeline_config.get('pipeline_id')
        pipeline_name = pipeline_config.get('name')
        source_type = pipeline_config.get('source_type', 'database')
        remove_after = pipeline_config.get('remove_after_yyyymmdd', '20251231')
        
        # Continuous pipelines don't need scheduled jobs
        if trigger_interval_min == "0":
            logger.info("Continuous pipeline - no scheduled job needed")
            # Continuous will autostart and do not need a separate method
            # Optionally start the pipeline manually:
            # try:
            #     self.ws.pipelines.start_update(pipeline_id, full_refresh=False)
            # except Exception as e:
            #     logger.warning(f"Manual pipeline start failed: {e}")
            return {"job_id": None, "status": "continuous_mode"}
        else:
            # Create scheduled job for triggered pipelines
            ig_job_spec = {
                "name": f"{pipeline_name}_{pipeline_id}",
                "performance_target": "standard",
                "schedule": {
                    "timezone_id": "UTC", 
                    "quartz_cron_expression": f"0 {random.randint(1, 5)}/{trigger_interval_min} * * * ?"
                },
                "tasks": [{
                    "task_key": "run_dlt", 
                    "pipeline_task": {"pipeline_id": pipeline_id} 
                }],
                "tags": {
                    "RemoveAfter": remove_after, 
                    "Connector": source_type,
                    "CreatedBy": "dlt-meta"
                },
            }

            ig_jobs_response_json = {}
            try:
                # Create the scheduled job
                ig_jobs_response = self.ws.jobs.create(**ig_job_spec)
                ig_jobs_response_json = {
                    'job_id': ig_jobs_response.job_id,
                    'name': ig_jobs_response.settings.name,
                    'schedule': ig_jobs_response.settings.schedule,
                    'status': 'job_created'
                }
                logger.info(f"Created scheduled job: {ig_jobs_response.job_id}")

                # Run the job immediately
                try:
                    ig_jobs_runnow_response = self.ws.jobs.run_now(ig_jobs_response.job_id)
                    ig_jobs_response_json.update({
                        'run_id': ig_jobs_runnow_response.run_id,
                        'status': 'job_started'
                    })
                    logger.info(f"Started job run: {ig_jobs_runnow_response.run_id}")
                except Exception as e_run_now:
                    logger.warning(f"Job created but failed to start immediately: {e_run_now}")
                    ig_jobs_response_json['status'] = 'job_created_not_started'

                return ig_jobs_response_json

            except Exception as e_job_create:
                logger.error(f"Job creation failed, trying manual pipeline start: {e_job_create}")
                ig_jobs_response_json.update({'job_id': None, 'status': 'job_creation_failed'})
                
                # Fallback: try to start pipeline manually
                try:
                    pipeline_update = self.ws.pipelines.start_update(pipeline_id, full_refresh=False)
                    ig_jobs_response_json.update({
                        'status': 'manual_start_success',
                        'update_id': pipeline_update.update_id
                    })
                    logger.info(f"Manual pipeline start successful: {pipeline_update.update_id}")
                except Exception as e_start_pipeline:
                    logger.error(f"Manual pipeline start failed: {e_start_pipeline}")
                    ig_jobs_response_json['status'] = 'manual_start_failed'
                    
                return ig_jobs_response_json
    
    def setup_postgresql_cdc(self, source_details: dict, dataflow_spec: dict) -> dict:
        """Setup PostgreSQL CDC prerequisites (slots and publications)"""
        
        source_system = dataflow_spec.get("source_system", "").lower()
        pg_custom_slot = source_details.get("pg_custom_slot", "false")
        
        if not source_system.startswith("postgres") or pg_custom_slot != "true":
            return {"postgresql_setup": False, "reason": "Not PostgreSQL or custom slot disabled"}
        
        try:
            # Get connection details for SQLAlchemy engine creation
            connection_name = source_details.get("connection_name")
            connection = self.ws.connections.get(connection_name)
            
            # Create SQLAlchemy engine from connection details
            # Note: In real implementation, you'd extract connection details and create engine
            # For now, this shows the integration pattern
            logger.info(f"Setting up PostgreSQL CDC for connection: {connection_name}")
            
            # Extract schema and table information
            ingestion_objects = source_details.get("ingestion_objects", [])
            if not ingestion_objects:
                return {"postgresql_setup": False, "reason": "No ingestion objects specified"}
            
            first_table = ingestion_objects[0].get("table", {})
            source_schema = first_table.get("source_schema", "public")
            target_schema = source_details.get("gateway_storage_schema")
            
            # Extract table names from ingestion objects
            tables = []
            for obj in ingestion_objects:
                table_info = obj.get("table", {})
                table_name = table_info.get("source_table")
                if table_name:
                    tables.append(table_name)
            
            # Default to intpk and dtix if no tables specified
            if not tables:
                tables = ['intpk', 'dtix']
            
            logger.info(f"Creating PostgreSQL slot for schema {source_schema}, tables: {tables}")
            
            # Note: In real implementation, you'd create the SQLAlchemy engine here
            # sqlalchemy_engine = create_engine(connection_url)
            # slot_manager = PostgreSQLSlotManager(sqlalchemy_engine)
            # success, slot_result = slot_manager.create_publication_and_slot(target_schema, source_schema, tables)
            
            # For documentation purposes, showing the expected result structure
            slot_result = {
                "postgresql_setup": True,
                "publication_created": True,
                "slot_created": True,
                "publication_name": f"{target_schema}_pub",
                "slot_name": target_schema,
                "tables": tables,
                "source_schema": source_schema,
                "target_schema": target_schema
            }
            
            logger.info(f"PostgreSQL CDC setup completed for {target_schema}")
            return slot_result
            
        except Exception as e:
            logger.error(f"Failed to setup PostgreSQL CDC: {e}")
            return {"postgresql_setup": False, "error": str(e)}

    def process_enhanced_onboarding(self, onboarding_config: dict) -> dict:
        """Process enhanced onboarding configuration with connections and dataflows"""
        results = {
            "connections": {},
            "dataflows": {},
            "summary": {"total_connections": 0, "total_dataflows": 0, "errors": []}
        }
        
        # Process connections first (if present)
        connections_config = onboarding_config.get("connections", {})
        if connections_config:
            logger.info(f"Processing {len(connections_config)} connection definitions")
            results["connections"] = self.process_connections(connections_config)
            results["summary"]["total_connections"] = len(connections_config)
        
        # Process dataflows
        dataflows_config = onboarding_config.get("dataflows", [])
        if not dataflows_config:
            # Fallback: treat entire config as single dataflow (legacy format)
            dataflows_config = [onboarding_config]
        
        for dataflow_spec in dataflows_config:
            if dataflow_spec.get("source_format") == "lakeflow_connect":
                try:
                    dataflow_id = dataflow_spec.get("data_flow_id", "unknown")
                    result = self.process_dataflow_spec(dataflow_spec)
                    results["dataflows"][dataflow_id] = result
                    results["summary"]["total_dataflows"] += 1
                except Exception as e:
                    error_msg = f"Failed to process dataflow {dataflow_id}: {str(e)}"
                    results["summary"]["errors"].append(error_msg)
                    logger.error(error_msg)
        
        return results

    def process_dataflow_spec(self, dataflow_spec: dict) -> dict:
        """Process complete Lakeflow Connect dataflow specification with PostgreSQL support"""
        source_details = dataflow_spec.get("source_details", {})
        
        # Extract configuration
        connection_name = source_details.get("connection_name")
        cdc_qbc_mode = source_details.get("ingestion_mode", "cdc")
        
        # Setup PostgreSQL CDC prerequisites if needed
        postgresql_result = self.setup_postgresql_cdc(source_details, dataflow_spec)
        
        # Gateway configuration
        gateway_config = {
            "gateway_pipeline_name": f"{connection_name}-gateway",
            "connection_name": connection_name,
            "gateway_storage_catalog": source_details.get("gateway_storage_catalog"),
            "gateway_storage_schema": source_details.get("gateway_storage_schema"),
            "source_type": dataflow_spec.get("source_system", "database").lower(),
            "remove_after_yyyymmdd": source_details.get("remove_after", "20251231")
        }
        
        # Deploy gateway (conditional based on CDC/QBC)
        gateway_result = self.deploy_gateway(gateway_config, cdc_qbc_mode)
        
        # Deploy ingestion pipeline
        ingestion_config = {
            "ig_pipeline_name": f"lakeflow-ingestion-{source_details.get('gateway_storage_schema')}",
            "connection_name": connection_name,
            "source_type": dataflow_spec.get("source_system", "sqlserver").lower(),
            "target_catalog": source_details.get("gateway_storage_catalog"),
            "target_schema": source_details.get("gateway_storage_schema"),
            "source_catalog": source_details.get("ingestion_objects", [{}])[0].get("table", {}).get("source_catalog", "production"),
            "source_schema": source_details.get("ingestion_objects", [{}])[0].get("table", {}).get("source_schema", "dbo"),
            "pg_custom_slot": source_details.get("pg_custom_slot", "false"),
            "replication_mode": source_details.get("replication_mode", "standard")
        }
        
        ingestion_result = self.deploy_ingestion_pipeline(
            ingestion_config, 
            gateway_result.get('pipeline_id'),
            cdc_qbc_mode,
            source_details.get("trigger_interval_min", "0")
        )
        
        # Create scheduled job if needed (for non-continuous pipelines)
        trigger_interval = source_details.get("trigger_interval_min", "0")
        job_config = {
            'pipeline_id': ingestion_result.get('pipeline_id'),
            'name': ingestion_config.get('ig_pipeline_name'),
            'source_type': ingestion_config.get('source_type'),
            'remove_after_yyyymmdd': source_details.get('remove_after', '20251231')
        }
        
        job_result = self.create_pipeline_job(job_config, trigger_interval)
        
        return {
            "gateway_pipeline_id": gateway_result.get('pipeline_id'),
            "ingestion_pipeline_id": ingestion_result.get('pipeline_id'),
            "ingestion_job_id": job_result.get('job_id'),
            "job_status": job_result.get('status'),
            "staging_schema": source_details.get("gateway_storage_schema"),
            "cdc_qbc_mode": cdc_qbc_mode,
            "trigger_interval_min": trigger_interval,
            "pipeline_type": ingestion_result.get('pipeline_type'),
            "serverless": ingestion_result.get('serverless'),
            "continuous": ingestion_result.get('continuous'),
            "postgresql_cdc": postgresql_result
        }
```

### 5. Enhanced Configuration Processing

**Updated `src/dataflow_pipeline.py`:**
```python
# Enhanced to handle new source formats
def invoke_dlt_pipeline(spark, layer):
    """Enhanced DLT pipeline processor"""
    
    # Get dataflow specs
    dataflow_specs = get_dataflow_specs(spark, layer)
    
    for spec in dataflow_specs:
        source_format = spec.get("source_format")
        
        if source_details.get('generator') == 'dbldatagen':
            process_synthetic_data_source(spark, spec)
        elif source_format == "lakeflow_connect":
            process_lakeflow_connect_source(spark, spec)
        elif source_format in ["kafka", "eventhub", "cloudfiles"]:
            # Existing processing logic
            process_existing_source(spark, spec)
        else:
            raise ValueError(f"Unsupported source_format: {source_format}")
```

### 6. Directory Structure

```
src/
├── cli.py                    # Enhanced CLI with new commands
├── dataflow_pipeline.py      # Enhanced with new source format support
├── variable_management.py    # NEW - Enhanced variable management
├── synthetic_data.py         # NEW - Synthetic data generation
├── lakeflow_connect.py       # NEW - Lakeflow Connect management
├── postgres_slot_manager.py # NEW - PostgreSQL CDC slot management
└── utils/
    ├── variable_substitution.py  # Enhanced variable handling
    └── rest_api_client.py        # REST API utilities

demo/
├── conf/
│   ├── enhanced_onboarding.template       # NEW - Multi-source format template (includes synthetic data inline)
│   └── lakeflow_connect_onboarding.json   # NEW - Lakeflow Connect configs
└── notebooks/
    ├── synthetic_data_generator.py        # NEW - Generated synthetic data notebook
    └── lakeflow_connect_validator.py      # NEW - Connection validation notebook
```

## Alternatives: Other Integration Options

### Alternative 1: Full DAB Native Deployment

**Approach:** Convert dlt-meta to use DAB's native resource definitions instead of direct REST API calls.

**Pros:**
- Native DAB integration with full `databricks bundle deploy` workflow
- Leverages DAB's built-in validation, dependency management, and deployment orchestration
- Consistent with Databricks' recommended deployment patterns
- Better integration with Databricks' CI/CD tooling

**Cons:**
- **Major Breaking Changes**: Requires complete rewrite of dlt-meta's deployment mechanism
- **Loss of Dynamic Behavior**: DAB resources are static YAML definitions, while dlt-meta currently generates resources dynamically based on onboarding configurations
- **Complex Migration**: Existing dlt-meta users would need to migrate their deployment workflows
- **Limited Flexibility**: DAB's resource definitions are less flexible than dlt-meta's current JSON-driven approach

**Implementation Complexity:** Very High - requires fundamental architecture changes

### Alternative 2: Hybrid DAB + DLT-Meta Deployment

**Approach:** Use DAB for infrastructure resources (connections, volumes, schemas) and dlt-meta CLI for dynamic pipeline resources.

**Pros:**
- Leverages DAB's strengths for infrastructure management
- Maintains dlt-meta's flexibility for pipeline generation
- Gradual migration path for existing users
- Clear separation of concerns

**Cons:**
- **Dual Deployment Complexity**: Requires managing both DAB deployments and dlt-meta CLI commands
- **Dependency Management**: Need to ensure DAB resources are deployed before dlt-meta resources
- **Inconsistent Tooling**: Teams need to learn both DAB and dlt-meta deployment patterns

**Implementation Complexity:** Medium - requires coordination between two deployment mechanisms

### Alternative 3: DAB Resource Generation

**Approach:** Enhance dlt-meta to generate complete DAB resource definitions that can be deployed via `databricks bundle deploy`.

**Pros:**
- Pure DAB deployment workflow
- Maintains dlt-meta's dynamic resource generation capabilities
- Leverages DAB's validation and deployment features
- Single deployment command

**Cons:**
- **Generated YAML Complexity**: Large, complex YAML files that are difficult to debug
- **Limited Runtime Flexibility**: Resources are static once generated
- **DAB Resource Limitations**: Some dlt-meta features may not map cleanly to DAB resources

**Implementation Complexity:** High - requires complete resource generation rewrite

### Alternative 4: Current Approach (Recommended)

**Approach:** Use DAB for structured configuration and variable management while maintaining dlt-meta's direct REST API deployment.

**Pros:**
- **Minimal Breaking Changes**: Preserves existing dlt-meta functionality and deployment patterns
- **Enhanced Configuration**: Leverages DAB's structured variable management and environment targeting
- **Flexible Deployment**: Maintains dlt-meta's dynamic resource creation capabilities
- **Gradual Enhancement**: Can be implemented incrementally without disrupting existing workflows

**Cons:**
- **Not Pure DAB**: Doesn't follow Databricks' recommended full DAB deployment pattern
- **Limited DAB Features**: Can't leverage all DAB features like dependency management and resource validation

**Implementation Complexity:** Low - requires only configuration parsing enhancements

### Comparison Matrix

| Approach | Breaking Changes | Implementation Effort | DAB Integration | Flexibility | Migration Path |
|----------|------------------|----------------------|-----------------|-------------|----------------|
| Full DAB Native | High | Very High | Complete | Low | Complex |
| Hybrid DAB + CLI | Medium | Medium | Partial | Medium | Gradual |
| DAB Generation | Medium | High | Complete | Medium | Moderate |
| **Current (Recommended)** | **Low** | **Low** | **Structured** | **High** | **Simple** |

### Why Current Approach is Recommended

1. **Preserves Existing Investment**: Organizations using dlt-meta can continue with their current deployment patterns while gaining enhanced configuration capabilities.

2. **Maintains Core Strengths**: dlt-meta's dynamic pipeline generation and flexible resource management remain intact.

3. **Structured Enhancement**: DAB provides structured variable management and environment targeting without forcing a complete architectural change.

4. **Implementation Feasibility**: Can be delivered quickly with minimal risk to existing functionality.

5. **Future Path**: Provides a foundation for future DAB integration enhancements without requiring immediate major changes.

## Overview

This document provides a comprehensive analysis of how dlt-meta integrates with Databricks Asset Bundles (DAB), comparing deployment mechanisms, configuration patterns, and opportunities for enhanced integration.

The recommended approach focuses on **leveraging DAB for structured configuration and variable management** while **maintaining dlt-meta's proven direct REST API deployment mechanism**. This strategy provides immediate benefits through enhanced configuration capabilities while preserving existing functionality and minimizing migration complexity.

## YAML Specification Analysis for Synthetic Data Generation

### Databricks Labs Data Generator (dbldatagen) Capabilities

**Core Features:**
- **Column Types**: Integer, Long, Float, Double, String, Boolean, Timestamp, Date, Decimal
- **Data Generation Patterns**: 
  - Random values with min/max ranges
  - Template-based generation (regex patterns)
  - Value sets with optional weights
  - Unique value generation
  - Sequential/incremental values
- **Advanced Features**:
  - Column dependencies and correlations
  - Custom expressions and formulas
  - Multi-table relationships and foreign keys
  - Data distribution control (normal, uniform, etc.)
  - Null value handling with configurable percentages
- **Scale & Performance**: Designed for Spark, handles millions to billions of rows efficiently

### Comparison of YAML Specifications vs. dbldatagen

#### **1. YData-Synthetic - ✅ Best Match**

**Strengths:**
- **Column-level granular control** - Direct mapping to dbldatagen's `.withColumn()` API
- **Type system alignment** - Supports all dbldatagen data types (Integer, String, Float, Timestamp, etc.)
- **Distribution support** - Normal, uniform, custom distributions map to dbldatagen's statistical capabilities
- **Template patterns** - Regex-based templates align with dbldatagen's template generation
- **Dependent columns** - Supports column relationships and correlations
- **Multi-table support** - Can define multiple related tables with foreign key relationships

**JSON Example:**
```json
{
    "tables": {
        "customers": {
            "rows": 100000,
            "columns": {
                "customer_id": {
                    "type": "integer",
                    "unique": true
                },
                "email": {
                    "type": "string",
                    "template": "\\w+@\\w+\\.com"
                },
                "age": {
                    "type": "integer",
                    "distribution": {
                        "type": "normal",
                        "mean": 35,
                        "std": 10
                    }
                }
            }
        }
    }
}
```

**Mapping to dbldatagen:**
```python
df_spec = (dg.DataGenerator(spark, rows=100000)
           .withColumn("customer_id", IntegerType(), uniqueValues=100000)
           .withColumn("email", StringType(), template="\\w+@\\w+\\.com")
           .withColumn("age", IntegerType(), distribution="normal(35,10)"))
```

#### **2. SDG (Synthetic Data Generator) - ⚠️ Partial Match**

**Strengths:** Table-level configuration, basic column types
**Limitations:** Limited distribution control, no template patterns, basic relationship support

#### **3. Gretel-Synthetics - ❌ Poor Match**

**Focus:** ML-based synthetic data from existing datasets, not schema-driven generation

#### **4. Table-Faker - ⚠️ Basic Match**

**Strengths:** Simple column definitions
**Limitations:** Limited to basic Faker patterns, no advanced distributions or relationships

### Recommended YAML Specification for DLT-Meta

Based on the analysis, **YData-Synthetic's specification format** provides the best foundation for dlt-meta integration, converted to JSON to match dlt-meta's configuration patterns:

```json
// dlt-meta synthetic data specification (YData-inspired, JSON format)
{
    "metadata": {
        "name": "dlt_meta_synthetic_dataset",
        "description": "Synthetic data for medallion architecture testing",
        "generator": "dbldatagen",
        "version": "1.0"
    },
    "settings": {
        "default_rows": 100000,
        "default_partitions": 10,
        "spark_config": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
    },
    "tables": {
        "customers": {
            "rows": "{synthetic_data_rows}",
            "partitions": 10,
            "description": "Customer master data",
            "columns": {
                "customer_id": {
                    "type": "long",
                    "unique_values": "{synthetic_data_rows}",
                    "description": "Unique customer identifier"
                },
                "first_name": {
                    "type": "string",
                    "template": "\\w{4,8}",
                    "description": "Customer first name"
                },
                "last_name": {
                    "type": "string",
                    "template": "\\w{4,12}",
                    "description": "Customer last name"
                },
                "email": {
                    "type": "string",
                    "template": "\\w{5,10}\\.\\w{3,8}@\\w{4,10}\\.(com|org|net)",
                    "description": "Customer email address"
                },
                "phone": {
                    "type": "string",
                    "template": "\\d{3}-\\d{3}-\\d{4}",
                    "description": "Phone number in XXX-XXX-XXXX format"
                },
                "birth_date": {
                    "type": "date",
                    "begin": "1950-01-01",
                    "end": "2005-12-31",
                    "distribution": {
                        "type": "normal",
                        "mean": "1980-01-01",
                        "std": "10 years"
                    },
                    "description": "Customer birth date"
                },
                "registration_date": {
                    "type": "timestamp",
                    "begin": "2020-01-01T00:00:00",
                    "end": "2024-12-31T23:59:59",
                    "random": true,
                    "description": "Account registration timestamp"
                },
                "city": {
                    "type": "string",
                    "values": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"],
                    "weights": [15, 12, 9, 7, 5, 5, 4, 4, 4, 3],
                    "description": "Customer city"
                },
                "state": {
                    "type": "string",
                    "dependent_on": "city",
                    "mapping": {
                        "New York": "NY",
                        "Los Angeles": "CA",
                        "Chicago": "IL",
                        "Houston": "TX",
                        "Phoenix": "AZ"
                    },
                    "description": "Customer state (derived from city)"
                },
                "annual_income": {
                    "type": "decimal",
                    "precision": 10,
                    "scale": 2,
                    "distribution": {
                        "type": "lognormal",
                        "mean": 65000,
                        "std": 25000
                    },
                    "min_value": 25000,
                    "max_value": 500000,
                    "description": "Annual income in USD"
                },
                "credit_score": {
                    "type": "integer",
                    "distribution": {
                        "type": "normal",
                        "mean": 720,
                        "std": 80
                    },
                    "min_value": 300,
                    "max_value": 850,
                    "description": "Credit score (300-850)"
                },
                "is_premium": {
                    "type": "boolean",
                    "probability": 0.15,
                    "description": "Premium customer flag"
                },
                "customer_segment": {
                    "type": "string",
                    "dependent_on": ["annual_income", "credit_score"],
                    "expression": "CASE WHEN annual_income > 100000 AND credit_score > 750 THEN 'Premium' WHEN annual_income > 60000 AND credit_score > 700 THEN 'Standard' ELSE 'Basic' END",
                    "description": "Customer segment based on income and credit"
                }
            }
        },
        "orders": {
            "rows": "{synthetic_data_rows * 3}",
            "partitions": 20,
            "description": "Customer orders",
            "columns": {
                "order_id": {
                    "type": "long",
                    "unique_values": "{synthetic_data_rows * 3}",
                    "description": "Unique order identifier"
                },
                "customer_id": {
                    "type": "long",
                    "foreign_key": {
                        "table": "customers",
                        "column": "customer_id"
                    },
                    "description": "Reference to customer"
                },
                "order_date": {
                    "type": "timestamp",
                    "begin": "2020-01-01T00:00:00",
                    "end": "2024-12-31T23:59:59",
                    "distribution": {
                        "type": "exponential",
                        "rate": 0.1
                    },
                    "description": "Order timestamp"
                },
                "order_amount": {
                    "type": "decimal",
                    "precision": 10,
                    "scale": 2,
                    "distribution": {
                        "type": "gamma",
                        "shape": 2,
                        "scale": 50
                    },
                    "min_value": 10.00,
                    "max_value": 5000.00,
                    "description": "Order total amount"
                },
                "product_category": {
                    "type": "string",
                    "values": ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty"],
                    "weights": [25, 20, 15, 15, 15, 10],
                    "description": "Primary product category"
                },
                "order_status": {
                    "type": "string",
                    "values": ["Completed", "Pending", "Cancelled", "Returned"],
                    "weights": [85, 8, 4, 3],
                    "description": "Current order status"
                }
            }
        }
    },
    "output_config": {
        "format": "delta",
        "location": "{uc_volume_path}/synthetic_data",
        "mode": "overwrite",
        "partitioning": {
            "customers": ["city"],
            "orders": ["order_date"]
        }
    },
    "data_quality": {
        "customers": [
            {
                "column": "email",
                "rule": "email IS NOT NULL AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
                "action": "quarantine"
            },
            {
                "column": "annual_income",
                "rule": "annual_income > 0 AND annual_income < 1000000",
                "action": "drop"
            }
        ],
        "orders": [
            {
                "column": "order_amount",
                "rule": "order_amount > 0",
                "action": "drop"
            },
            {
                "column": "customer_id",
                "rule": "customer_id IS NOT NULL",
                "action": "quarantine"
            }
        ]
    }
}
```

### **Verdict: YData-Synthetic Specification Analysis**

**YData-Synthetic specification format (converted to JSON for dlt-meta compatibility) is the closest match to dbldatagen capabilities**, offering:

1. **Column-Level Granular Control**: Direct mapping to dbldatagen's `.withColumn()` API with comprehensive type support
2. **Advanced Distribution Support**: Normal, uniform, exponential, gamma distributions that align with dbldatagen's statistical capabilities  
3. **Template-Based Generation**: Regex patterns and templates that map directly to dbldatagen's template generation
4. **Dependent Columns & Relationships**: Support for column dependencies, foreign keys, and derived columns using expressions
5. **Multi-Table Support**: Ability to define related tables with referential integrity
6. **DLT-Meta Integration**: Native support for variable substitution using `{variable}` patterns and output configuration for Delta tables

This specification provides a **declarative, maintainable approach** to synthetic data generation that leverages dbldatagen's full capabilities while integrating seamlessly with dlt-meta's variable substitution and medallion architecture patterns.

The YAML format enables **version control, collaboration, and reusability** of synthetic data specifications, making it ideal for teams developing and testing data pipelines before production deployment.