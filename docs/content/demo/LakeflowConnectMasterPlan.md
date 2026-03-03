---
title: "Lakeflow Connect Master Plan"
date: 2024-01-01T00:00:00-05:00
weight: 21
draft: false
---

### Lakeflow Connect + DLT-Meta Master Plan

This document outlines the master plan for integrating [Lakeflow Connect](https://docs.databricks.com/en/data-governance/lakeflow-connect/index.html) (LFC) with DLT-Meta across three demos:

---

## Overview

| Demo | Purpose | Source | Tables |
|------|---------|--------|--------|
| **[Techsummit](Techsummit.md)** | Cloudfiles + Auto-generated tables | CSV files, Autoloader | 100s |
| **[Lakeflow Connect Demo](LakeflowConnectDemo.md)** | Real LFC + DLT-Meta | LFC streaming tables | 1–1000s |
| **Simulation** (optional) | No real DB | dbldatagen in DLT | Few |

---

## Plan Phases

### Phase 1: Techsummit Demo (Cloudfiles)

**Goal:** Improve the Techsummit demo for clarity and reliability.

- **Status:** [Techsummit.md](Techsummit.md)
- **Flow:** dbldatagen → CSV → UC Volume → Autoloader → Bronze → Silver
- **Improvements:** Clearer structure, step numbering, optional local generation

---

### Phase 2: Lakeflow Connect Demo (Make It Work)

**Goal:** End-to-end demo with real Lakeflow Connect.

**Steps:**

1. **Create Lakeflow Connect** – Run [lfcdemo-database.ipynb](https://github.com/rsleedbx/lfcddemo-one-click-notebooks/blob/main/lfc/db/lfcdemo-database.ipynb) to create:
   - Gateway pipeline (CDC from SQL Server/PostgreSQL/MySQL)
   - Ingestion pipeline → streaming tables in `{catalog}.{schema}`

2. **Hook up DLT-Meta** – Configure onboarding to read from LFC streaming tables with `source_format: "delta"`.

3. **Deploy** – Run `dlt-meta onboard` and deploy bronze/silver pipelines.

**Details:** [LakeflowConnectDemo.md](LakeflowConnectDemo.md)

---

### Phase 3: Auto-Generate Onboarding for 100–1000 Tables

**Goal:** Handle databases with many tables without manual JSON authoring.

**Yes—auto-generating the DLT-Meta onboarding JSON is the right approach** for 100–1000 tables.

#### Approach

1. **Discover tables** – After LFC creates streaming tables, query the catalog:
   ```python
   tables = spark.catalog.listTables(catalog_name, schema_name)
   ```

2. **Template per table** – For each table, generate an onboarding entry:
   ```python
   {
     "data_flow_id": str(i),
     "data_flow_group": "A1",
     "source_format": "delta",
     "source_details": {
       "source_catalog_prod": catalog_name,
       "source_database": schema_name,
       "source_table": table_name
     },
     "bronze_database_prod": f"{catalog}.{bronze_schema}",
     "bronze_table": table_name,
     "silver_database_prod": f"{catalog}.{silver_schema}",
     "silver_table": f"{table_name}_clean",
     "silver_transformation_json_prod": f"{volume_path}/conf/silver_transformations.json",
     ...
   }
   ```

3. **Silver transformations** – Options:
   - **Pass-through:** `select_exp: ["*"]` for all tables
   - **Schema-derived:** Use `spark.table(f"{catalog}.{schema}.{table}").schema` to build `select_exp` from column names
   - **Config file:** Generate `silver_transformations.json` with one entry per table

4. **Script/notebook** – A Python script or notebook cell can:
   - List tables from the LFC target schema
   - Generate `onboarding.json` (array of entries)
   - Write `silver_transformations.json` if needed
   - Optionally save to a UC volume path for `dlt-meta onboard`

#### Example Skeleton

```python
def generate_lfc_onboarding(catalog: str, lfc_schema: str, bronze_schema: str, 
                            silver_schema: str, volume_path: str) -> list:
    tables = spark.catalog.listTables(catalog, lfc_schema)
    records = []
    for i, t in enumerate(tables, start=1):
        records.append({
            "data_flow_id": str(i),
            "data_flow_group": "A1",
            "source_format": "delta",
            "source_details": {
                "source_catalog_prod": catalog,
                "source_database": lfc_schema,
                "source_table": t.name
            },
            "bronze_database_prod": f"{catalog}.{bronze_schema}",
            "bronze_table": t.name,
            "silver_database_prod": f"{catalog}.{silver_schema}",
            "silver_table": f"{t.name}_clean",
            # ... other required fields
        })
    return records
```

---

## Reference Links

| Resource | URL |
|----------|-----|
| **LFC Database Notebook** | [lfcdemo-database.ipynb](https://github.com/rsleedbx/lfcddemo-one-click-notebooks/blob/main/lfc/db/lfcdemo-database.ipynb) |
| **LFC Docs** | [Lakeflow Connect](https://docs.databricks.com/en/data-governance/lakeflow-connect/index.html) |
| **DLT-Meta LFC Config** | [lfcdemo_lakeflow_connect.ipynb](../../../demo/notebooks/lfcdemo_lakeflow_connect.ipynb) |

---

## Summary

| Question | Answer |
|----------|--------|
| Revamp Techsummit? | Yes – improve structure and flow |
| Make LakeflowConnectDemo work? | Yes – real LFC + clear instructions |
| Auto-generate JSON for 100–1000 tables? | **Yes** – discover tables, template per table, generate onboarding + silver config |
