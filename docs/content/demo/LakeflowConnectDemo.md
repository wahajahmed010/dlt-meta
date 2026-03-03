---
title: "Lakeflow Connect Demo"
date: 2024-01-01T00:00:00-05:00
weight: 23
draft: false
---

### Lakeflow Connect + DLT-Meta Demo

This demo uses [Lakeflow Connect](https://docs.databricks.com/en/data-governance/lakeflow-connect/index.html) (LFC) to stream two tables — `intpk` and `dtix` — from a source database (SQL Server, PostgreSQL, or MySQL) into Databricks streaming tables, then feeds those directly into a DLT-Meta bronze and silver pipeline. No CSV files or Autoloader are involved; the bronze source is `delta` (streaming table reads).

---

### How the demo configures bronze (SCD type per table)

The LFC source tables can receive **inserts**, **updates**, and **deletes** (e.g. CDC MERGE). A DLT streaming read from a Delta table assumes an **append-only** source by default; if the source has a non-append commit (update/delete), the flow fails unless you either skip those commits or process them via the change data feed.

This demo **hardcodes** the behavior per table so you don’t have to choose at launch time:

| Table  | SCD type | Source behavior              | Bronze config |
|--------|----------|------------------------------|----------------------------------------------|
| **intpk** | Type 1   | Can have insert/update/delete | **Process** CDC: `bronze_reader_options: {"readChangeFeed": "true"}` and `bronze_cdc_apply_changes` (keys `id`, `sequence_by` `_commit_version`, `apply_as_deletes` `_change_type = 'delete'`, SCD type 1). LFC table must have **change data feed** enabled (`delta.enableChangeDataFeed = true`). |
| **dtix**  | Type 2   | Append-only                  | `bronze_reader_options: {}` and bronze DQE; no CDC apply. |

- **intpk** is treated as **SCD Type 1**: the source may have updates and deletes. The demo **processes** them by reading the Delta change data feed (`readChangeFeed: true`) and applying CDC with `bronze_cdc_apply_changes` (keys, `sequence_by`, `apply_as_deletes`, etc.), so bronze reflects inserts, updates, and deletes. The LFC-created streaming table for `intpk` must have change data feed enabled.
- **dtix** is treated as **SCD Type 2** (append-only): no updates/deletes in the source, so no change feed or CDC apply is needed.

This is wired in two places so they stay in sync:

1. **Launcher** (`demo/launch_lfc_demo.py`) — when it writes `onboarding.json` to the run’s volume, it sets for `intpk`: `bronze_reader_options: {"readChangeFeed": "true"}`, `bronze_cdc_apply_changes` (and no bronze DQE); for `dtix`: `bronze_reader_options: {}` and bronze DQE.
2. **LFC notebook** (`demo/lfcdemo-database.ipynb`) — after creating the LFC pipelines, it overwrites `conf/onboarding.json` on the same volume with the correct `source_database` (the LFC-created schema) and the same per-table bronze config (intpk = readChangeFeed + bronze_cdc_apply_changes, dtix = DQE only).

You do **not** pass SCD type on the command line; the demo uses this table-based setup by default. To **skip** changes instead of processing them (e.g. `skipChangeCommits: true` for intpk), change the onboarding config and remove `bronze_cdc_apply_changes` for that flow.

---

### Lakeflow Connect SCD type 2 and DLT-Meta

[Lakeflow Connect history tracking (SCD type 2)](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/scd) controls how LFC writes the **destination** streaming table:

- **SCD type 1** (history off): LFC overwrites rows as they are updated/deleted at the source; the destination has one row per key.
- **SCD type 2** (history on): LFC keeps history: it adds the update as a new row and marks the old row as inactive. The destination has **`__START_AT`** and **`__END_AT`** columns; the sequence column (e.g. for SQL Server you can set `sequence_by` in `table_configuration`) determines the time span each row version was active.

In this demo, the LFC notebook sets **intpk** to `SCD_TYPE_1` and **dtix** to `SCD_TYPE_2`. So the LFC-created table for **dtix** is a versioned table with `__START_AT`/`__END_AT`. When the source row changes, LFC inserts the new version and marks the previous row inactive (typically by updating `__END_AT`). That can produce **UPDATE** operations in the Delta log, so a plain `readStream` on that table can fail with "update or delete detected". If you see that on dtix, treat it like intpk: enable **change data feed** on the LFC table and use `readChangeFeed: true`; optionally use `bronze_cdc_apply_changes` with `scd_type: "2"`, `sequence_by: "__START_AT"` (or the column LFC uses), and `except_column_list` including `__START_AT`/`__END_AT` if you want DLT-Meta to re-apply SCD type 2 into bronze (DLT-Meta also adds `__START_AT`/`__END_AT` when `scd_type` is 2).

**Compatibility:** DLT-Meta’s `bronze_cdc_apply_changes` (and `create_auto_cdc_flow`) support SCD type 2 and add `__START_AT`/`__END_AT` to the target schema, so they work with LFC SCD type 2 output. Use the same key and sequence semantics as LFC (e.g. business key and the LFC sequence column). An actual LFC SCD type 2 table (schema + sample rows and, if possible, whether commits are append-only or include UPDATEs) helps confirm the exact `sequence_by` and reader options.

---

### Prerequisites

1. **Command prompt** – Terminal or PowerShell

2. **Databricks CLI** – Install and authenticate:
   - [Install Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
   - Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:

   ```commandline
   databricks auth login --host WORKSPACE_HOST
   ```

3. **Python packages**:
   ```commandline
   pip install "PyYAML>=6.0" setuptools databricks-sdk
   ```

4. **Clone dlt-meta**:
   ```commandline
   git clone https://github.com/databrickslabs/dlt-meta.git
   cd dlt-meta
   ```

5. **Set environment**:
   ```commandline
   export PYTHONPATH=$(pwd)
   ```

6. **A Databricks connection** to a source database (SQL Server, PostgreSQL, or MySQL) — see [Lakeflow Connect docs](https://docs.databricks.com/en/data-governance/lakeflow-connect/index.html). The demo uses pre-configured connections:
   - `lfcddemo-azure-sqlserver`
   - `lfcddemo-azure-mysql`
   - `lfcddemo-azure-pg`

---

### Step 1: Run the Demo

The launch script handles everything end-to-end: it uploads the LFC notebook to your workspace and creates a job that runs the LFC setup, onboards DLT-Meta metadata, and starts the bronze + silver pipelines.

```commandline
python demo/launch_lfc_demo.py \
  --uc_catalog_name=<catalog> \
  --connection_name=lfcddemo-azure-sqlserver \
  --uc_schema_name=lfcddemo \
  --cdc_qbc=cdc \
  --trigger_interval_min=5 \
  --profile=DEFAULT
```

**Parameters:**

| Parameter | Description | Default / Choices |
|-----------|-------------|-------------------|
| `uc_catalog_name` | Unity Catalog name — required for setup | — |
| `connection_name` | Databricks connection to source DB | `lfcddemo-azure-sqlserver` \| `lfcddemo-azure-mysql` \| `lfcddemo-azure-pg` |
| `uc_schema_name` | Schema where LFC writes streaming tables (`intpk`, `dtix`) | `lfcddemo` |
| `cdc_qbc` | LFC pipeline mode | `cdc` \| `qbc` \| `cdc_single_pipeline` |
| `trigger_interval_min` | LFC trigger interval in minutes (positive integer) | `5` |
| `profile` | Databricks CLI profile | `DEFAULT` |
| `run_id` | Existing `run_id` — presence implies incremental (re-trigger) mode | — |

**Re-triggering bronze/silver** (after initial setup, while the LFC ingestion job is still running):

```commandline
python demo/launch_lfc_demo.py --profile=DEFAULT --run_id=<run_id_from_setup>
```

Alternatively, click **Run now** on the `dlt-meta-lfc-demo-incremental-<run_id>` job in the Databricks Jobs UI — no CLI needed.

---

### What Happens When You Run the Command

**On your laptop (synchronous):**

1. **UC resources created** – Unity Catalog schemas (`dlt_meta_dataflowspecs_lfc_*`, `dlt_meta_bronze_lfc_*`, `dlt_meta_silver_lfc_*`) and a volume are created in your catalog.
2. **Config files uploaded to UC Volume** – `onboarding.json`, `silver_transformations.json`, and DQE configs are uploaded to the volume.
3. **Notebooks uploaded to Workspace** – Runner notebooks are uploaded to `/Users/<you>/dlt_meta_lfc_demo/<run_id>/runners/`.
4. **dlt_meta wheel uploaded** – The `dlt_meta` Python wheel is uploaded to the UC Volume for use by pipeline tasks.
5. **Bronze and silver pipelines created** – Two Lakeflow Declarative Pipelines are created in your workspace.
6. **Job created and started** – A job is created and `run_now` is triggered. The job URL opens in your browser.

**When the job runs on Databricks (asynchronous):**

1. **Metadata onboarded** – The `dlt_meta onboard` step loads metadata into dataflowspec tables from `onboarding.json`, which points to the two LFC streaming tables (`intpk`, `dtix`) as `source_format: delta`.
2. **Bronze pipeline runs** – The bronze pipeline reads from the LFC streaming tables via `spark.readStream.table()` and writes to bronze Delta tables. All rows pass through (no quarantine rules).
3. **Silver pipeline runs** – The silver pipeline applies pass-through transformations (`select *`) from the metadata and writes to silver tables.

---

### Onboarding Configuration

DLT-Meta is configured with `source_format: delta` and points directly at the LFC streaming tables. DQE rules are set to pass everything through.

**Per-table bronze config (demo default):**

- **intpk** — Process CDC: `bronze_reader_options: {"readChangeFeed": "true"}` and `bronze_cdc_apply_changes` (keys `id`, `sequence_by` `_commit_version`, `apply_as_deletes` `_change_type = 'delete'`, SCD type 1). LFC table must have change data feed enabled. No bronze DQE (pipeline uses CDC path).
- **dtix** — `bronze_reader_options: {}` and bronze DQE (Type 2 append-only).

`<lfc_schema>` is the schema where LFC created the streaming tables (e.g. `main.<user>_sqlserver_<id>`). The notebook overwrites `onboarding.json` with that schema and these options.

```json
[
  {
    "data_flow_id": "1",
    "data_flow_group": "A1",
    "source_format": "delta",
    "source_details": {
      "source_catalog_prod": "<catalog>",
      "source_database": "<lfc_schema>",
      "source_table": "intpk"
    },
    "bronze_database_prod": "<catalog>.dlt_meta_bronze_lfc_<run_id>",
    "bronze_table": "intpk",
    "bronze_reader_options": { "readChangeFeed": "true" },
    "bronze_cdc_apply_changes": {
      "keys": ["id"],
      "sequence_by": "_commit_version",
      "scd_type": "1",
      "apply_as_deletes": "_change_type = 'delete'",
      "except_column_list": ["_change_type", "_commit_version", "_commit_timestamp"]
    },
    "silver_database_prod": "<catalog>.dlt_meta_silver_lfc_<run_id>",
    "silver_table": "intpk",
    "silver_transformation_json_prod": "<volume_path>/conf/silver_transformations.json"
  },
  {
    "data_flow_id": "2",
    "data_flow_group": "A1",
    "source_format": "delta",
    "source_details": {
      "source_catalog_prod": "<catalog>",
      "source_database": "<lfc_schema>",
      "source_table": "dtix"
    },
    "bronze_database_prod": "<catalog>.dlt_meta_bronze_lfc_<run_id>",
    "bronze_table": "dtix",
    "bronze_reader_options": {},
    "bronze_data_quality_expectations_json_prod": "<volume_path>/conf/dqe/bronze_dqe.json",
    "silver_database_prod": "<catalog>.dlt_meta_silver_lfc_<run_id>",
    "silver_table": "dtix",
    "silver_transformation_json_prod": "<volume_path>/conf/silver_transformations.json"
  }
]
```

**Silver transformations** (`silver_transformations.json`) — pass-through for both tables:

```json
[
  { "target_table": "intpk", "select_exp": ["*"] },
  { "target_table": "dtix",  "select_exp": ["*"] }
]
```

**DQE** (`bronze_dqe.json`) — all rows pass:

```json
{
  "expect": {
    "valid_row": "true"
  }
}
```

---

### Flow Summary

```
Source DB (SQL Server / PostgreSQL / MySQL)
    |
    v
LFC Gateway + Ingestion  (lfcdemo-database.ipynb)
    |
    v
Streaming tables:  {catalog}.{lfc_schema}.intpk
                   {catalog}.{lfc_schema}.dtix
    |
    v  source_format: delta  (spark.readStream.table)
DLT-Meta Bronze
    |
    v
DLT-Meta Silver
```

---

### References

| Resource | Link |
|----------|------|
| **LFC Database Notebook** | [demo/lfcdemo-database.ipynb](../../../demo/lfcdemo-database.ipynb) |
| **LFC Docs** | [Lakeflow Connect](https://docs.databricks.com/en/data-governance/lakeflow-connect/index.html) |
| **DLT-Meta delta source** | [Metadata Preparation](../getting_started/metadatapreperation.md) |
| **Tech Summit Demo** | [Techsummit.md](Techsummit.md) |
