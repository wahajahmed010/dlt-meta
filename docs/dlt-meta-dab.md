# DLT-Meta Enhanced Approach: Synthetic Data and Lakeflow Connect

## Overview

This document outlines the recommended approach for combining DLT-Meta with:

1. **Synthetic data generation** (dbldatagen) – for testing and development
2. **Lakeflow Connect (LFC) streaming tables** – as the bronze table source for production ingestion

For the YAML-based configuration approach (multi-section YAML, Enhanced CLI), see [dbldatagen-yaml.md](dbldatagen-yaml.md).

---

## Step 1: Synthetic Data for Testing

Use the **`synthetic_data.ipynb`** notebook to generate test data locally or on Databricks. It mirrors the logic in `src/synthetic_data.py` and produces `orders` and `order_details` tables suitable for DLT-Meta pipelines.

**Notebook:** [demo/notebooks/synthetic_data.ipynb](../demo/notebooks/synthetic_data.ipynb)

### Quick Start

1. Open the notebook in Databricks or Jupyter
2. (Databricks) Optionally set widget `output_location` (default: `/tmp/synthetic_data`)
3. Run all cells

### Output

- `{output_location}/orders` – Parquet data
- `{output_location}/order_details` – Parquet data
- `{output_location}/_schemas/` – Schema metadata

### Use with DLT-Meta

Configure DLT-Meta onboarding with `source_format: "cloudFiles"` and `source_path_dev` pointing to the generated paths (e.g. `{output_location}/orders`, `{output_location}/order_details`).

---

## Step 2: Lakeflow Connect Streaming Tables as Bronze Source

Reference implementation: [lfcdemo-database.ipynb](https://github.com/rsleedbx/lfcddemo-one-click-notebooks/blob/cleanup/lfc/db/lfcdemo-database.ipynb)

### Flow

```
Source database (SQL Server, PostgreSQL, MySQL)
    |
    v
Lakeflow Connect: Gateway + Ingestion pipelines
    |
    v
Streaming tables: {catalog}.{schema}.intpk, dtix, ...
    |
    v  source_format: "delta", source_path_dev: "catalog.schema.table"
DLT-Meta Bronze Tables
    |
    v
DLT-Meta Silver Tables
```

### Demo Notebook

[demo/notebooks/lfcdemo_lakeflow_connect.ipynb](../demo/notebooks/lfcdemo_lakeflow_connect.ipynb) shows how to configure DLT-Meta so that **LFC streaming tables** are the source for bronze tables.

### DLT-Meta Onboarding Config

```json
{
  "data_flow_id": "300",
  "data_flow_group": "A1",
  "source_format": "delta",
  "source_details": {
    "source_table": "intpk",
    "source_path_dev": "main.lfcdemo_staging.intpk"
  },
  "bronze_catalog_dev": "dev_catalog",
  "bronze_database_dev": "lfc_bronze",
  "bronze_table": "intpk_from_lfc",
  "bronze_reader_options": { "format": "delta" },
  "..."
}
```

Replace `main.lfcdemo_staging.intpk` with your LFC target catalog, schema, and table.

### Create LFC Pipelines

Run the [lfcdemo-database.ipynb](https://github.com/rsleedbx/lfcddemo-one-click-notebooks/blob/cleanup/lfc/db/lfcdemo-database.ipynb) notebook to create gateway and ingestion pipelines. It uses `lfcdemolib` to set up CDC or QBC pipelines that populate streaming tables in the target schema.

---

## Summary

| Phase              | Tool / Notebook                    | Output                                 |
|--------------------|------------------------------------|----------------------------------------|
| **Testing**        | `demo/notebooks/synthetic_data.ipynb` | Parquet files (orders, order_details)   |
| **LFC Setup**      | lfcdemo-database.ipynb             | Streaming tables in UC schema          |
| **Bronze/Silver**  | DLT-Meta onboard + deploy         | Bronze and silver Delta tables         |

For the full YAML-based configuration (variables, resources, dataflows), see [dbldatagen-yaml.md](dbldatagen-yaml.md).

## Testing

See the [Testing](dbldatagen-yaml.md#testing) section in [dbldatagen-yaml.md](dbldatagen-yaml.md) for unit and integration test commands.
