---
name: databricks-job-monitor
description: Monitor Databricks job runs and DLT pipeline status, and clean up all resources created by this project. Use when the user asks to check job status, watch pipeline progress, inspect run output, monitor or clean up the techsummit demo jobs, pipelines, schemas, volumes, tables, or workspace notebooks. Extracts job_id and run_id from terminal output and queries or modifies the Databricks workspace via the SDK or CLI.
---

# Databricks Job & Pipeline Monitor

## Extracting identifiers from terminal output

When reading terminal output from `launch_techsummit_demo.py` or `launch_lfc_demo.py`, look for:

```
Job created successfully. job_id=<JOB_ID>, url=<URL>
  run_id : <RUN_ID>
```

### Techsummit demo — name patterns

- Setup job: `dlt-meta-techsummit-demo-{run_id}`
- Incremental job: `dlt-meta-techsummit-demo-incremental-{run_id}`
- Bronze pipeline: `dlt-meta-bronze-{run_id}`
- Silver pipeline: `dlt-meta-silver-{run_id}`

### LFC demo — name patterns

DLT-Meta pipelines (created by `launch_lfc_demo.py`):
- Setup job: `dlt-meta-lfc-demo-{run_id}`
- Incremental job: `dlt-meta-lfc-demo-incremental-{run_id}`
- Bronze pipeline: `dlt-meta-lfc-bronze-{run_id}`
- Silver pipeline: `dlt-meta-lfc-silver-{run_id}`

**Lakeflow Connect pipelines** (created *inside* `lfcdemo-database.ipynb` via `lfcdemolib`):

The pipeline names are derived from the user's email prefix and a hex timestamp ID:

```
{firstname_lastname}_{source_type}_{nine_char_id}_gw   ← gateway pipeline (CDC only)
{firstname_lastname}_{source_type}_{nine_char_id}_ig   ← ingestion pipeline
```

Where:
- `firstname_lastname` = email address part before `@`, with `.` and `-` replaced by `_`
  - e.g. `robert.lee@databricks.com` → `robert_lee`
- `source_type` = lowercase DB type from the connection: `sqlserver`, `mysql`, or `postgresql`
- `nine_char_id` = 9-char hex of a nanosecond timestamp, e.g. `1a2b3c4d5`
- The UC schema where streaming tables land uses the same stem: `{firstname_lastname}_{source_type}_{nine_char_id}`

**Example** for `robert.lee@databricks.com` with `lfcddemo-azure-sqlserver`:
```
robert_lee_sqlserver_1a2b3c4d5_gw
robert_lee_sqlserver_1a2b3c4d5_ig
```

To look up these pipelines by ID, read them directly from the `lfc_setup` task output (printed at end of notebook) or from the DLT-Meta setup job's `bronze_dlt`/`silver_dlt` task `pipeline_task.pipeline_id` fields.

> **LFC pipeline startup takes 5+ minutes.** After `lfcdemo-database.ipynb` creates the ingestion pipeline and triggers it, expect at least 5 minutes before the pipeline reaches `RUNNING` and the streaming tables (`intpk`, `dtix`) become available. The `lfc_setup` notebook wait cell: **Gateway** is always continuous → exit when RUNNING; STOPPED/CANCELED/DELETED is also accepted (e.g. gateway was RUNNING and then stopped). **Ingestion**: continuous mode → exit when RUNNING; trigger mode → exit when latest update is `COMPLETED`. For ingestion, terminal state without COMPLETED raises; for gateway, STOPPED/CANCELED is OK.

**Bronze pipeline source schema (LFC demo):** The bronze DLT pipeline reads the LFC streaming tables (`intpk`, `dtix`) from the **schema created by `lfcdemo-database.ipynb`** (i.e. `d.target_schema`, e.g. `robert_lee_sqlserver_4207c5e3d`), **not** from `uc_schema_name` / `lfc_schema` (e.g. `lfcddemo`) passed to `launch_lfc_demo.py`. The launcher writes an initial `onboarding.json` with `source_database: lfc_schema`; the notebook **overwrites** `conf/onboarding.json` on the run's volume with `source_database: d.target_schema` so that `onboarding_job` and the bronze pipeline use the correct schema. If the bronze pipeline fails with "Failed to resolve flow" or "Failed to analyze flow" for flows like `main_dlt_meta_bronze_lfc_{run_id}_intpk_bronze_inputview`, the usual cause is that the **source** tables are missing from the schema in `onboarding.json` — e.g. the file was not overwritten by the notebook (notebook failed before the write, or `run_id`/`target_catalog` not passed), or an older run used a different schema. Confirm that `conf/onboarding.json` on the run's volume has `source_database` equal to the LFC-created schema name (from `conf/lfc_created.json` → `lfc_schema`).

**Storing job IDs for efficient lookup (LFC demo):** To avoid slow `jobs.list(name=...)` over the whole workspace, `launch_lfc_demo.py` stores setup and incremental job IDs in a workspace file and uses `jobs.get(job_id=...)` when possible. At **setup**, after creating the main job it writes `conf/setup_metadata.json` under the run's workspace path (`/Users/{user}/dlt_meta_lfc_demo/{run_id}/conf/setup_metadata.json`) with `job_id` and `uc_catalog_name`. On **incremental** runs it first tries to read that file; if `job_id` is present it calls `jobs.get(job_id=meta["job_id"])` (fast) instead of `jobs.list(name=..., limit=100)`. When the incremental job is created for the first time, the launcher writes the same file with `incremental_job_id` added; subsequent incremental runs then use `jobs.get(job_id=meta["incremental_job_id"])` and skip listing. For monitoring or scripts: **prefer reading `conf/setup_metadata.json` and using `jobs.get(job_id=...)`** when you have a run_id and the workspace path; fall back to `jobs.list(name=..., limit=JOBS_LIST_LIMIT)` only if the file is missing (e.g. runs from before this feature).

> **LFC notebook scheduler:** The notebook schedules auto-cleanup of LFC pipelines after 1 hour (configurable via `wait_sec`, default 3600 s). This scheduled job runs independently in Databricks. The DML loop against the source database (10 inserts/updates/deletes per table per minute) **stops when the notebook session ends**, but the LFC ingestion pipeline itself continues running independently until the cleanup job deletes it.

## Checking status via Databricks CLI

Use the `DEFAULT` profile (or whatever profile is in scope). Note: run-id and job-id are **positional** args, not flags.

```bash
# Most recent runs for a job (--job-id is a flag, not positional)
databricks jobs list-runs --job-id=<JOB_ID> --profile=DEFAULT -o json | python3 -c "
import json, sys
data = json.load(sys.stdin)
runs = data if isinstance(data, list) else data.get('runs', [])
for r in runs[:3]:
    state = r.get('state', {})
    print(r['run_id'], state.get('life_cycle_state'), state.get('result_state', '—'))
"

# Per-task detail for a run (run-id is positional)
databricks jobs get-run <RUN_ID> --profile=DEFAULT -o json | python3 -c "
import json, sys
r = json.load(sys.stdin)
state = r.get('state', {})
print(f\"life_cycle={state.get('life_cycle_state')}  result={state.get('result_state','—')}\")
for t in r.get('tasks', []):
    ts = t.get('state', {})
    print(f\"  task={t['task_key']}  {ts.get('life_cycle_state')}  {ts.get('result_state','—')}\")"

# DLT pipeline status by known ID (pipeline-id is positional)
# Check both pipeline-level state and update-level state (WAITING_FOR_RESOURCES vs RUNNING)
databricks pipelines get <PIPELINE_ID> --profile=DEFAULT -o json | python3 -c "
import json, sys
p = json.load(sys.stdin)
print(f\"name={p.get('name')}  state={p.get('state')}  health={p.get('health')}\")
for u in p.get('latest_updates', [])[:3]:
    print(f\"  update: state={u.get('state')}  creation_time={u.get('creation_time')}\")
"

# Get a specific update (state, cause, cluster_id) — does NOT include the failure message text
databricks pipelines get-update <PIPELINE_ID> <UPDATE_ID> --profile=DEFAULT -o json | python3 -c "
import json, sys
d = json.load(sys.stdin)
u = d.get('update', d)
print('state:', u.get('state'), ' cause:', u.get('cause'), ' cluster_id:', u.get('cluster_id'))
"

# Pipeline update failure message: use list-pipeline-events; ERROR events have the message
# e.g. \"Update 9ebc78 has failed. Failed to analyze flow '...' and 1 other flow(s)..\"
databricks pipelines list-pipeline-events <PIPELINE_ID> --profile=DEFAULT -o json --max-results 50 | python3 -c "
import json, sys
d = json.load(sys.stdin)
events = d.get('events', [])
for e in events:
    if e.get('level') == 'ERROR':
        print(e.get('message', e.get('error', '')))
"
```

## Checking status via Python SDK

```python
from integration_tests.run_integration_tests import get_workspace_api_client
ws = get_workspace_api_client("DEFAULT")  # replace with actual profile

RUN_ID = "<run_id>"

# ── Techsummit / LFC DLT-Meta job ─────────────────────────────────────────────
# Prefer job_id when available (fast). LFC demo stores IDs in workspace conf/setup_metadata.json.
USERNAME = ws.current_user.me().user_name
runners_path = f"/Users/{USERNAME}/dlt_meta_lfc_demo/{RUN_ID}"
setup_meta_path = f"{runners_path}/conf/setup_metadata.json"
job = None
try:
    with ws.workspace.download(setup_meta_path) as f:
        import json
        meta = json.load(f)
    if meta.get("job_id") is not None:
        jd = ws.jobs.get(job_id=meta["job_id"])
        if (jd.settings.name or "").endswith(RUN_ID):
            job = jd
except Exception:
    pass
if not job:
    job_name = f"dlt-meta-techsummit-demo-{RUN_ID}"  # or dlt-meta-lfc-demo-{RUN_ID}
    job = next((j for j in ws.jobs.list(name=job_name, limit=100) if j.settings.name == job_name), None)

# Job runs (limit=1 for latest)
if job:
    for run in ws.jobs.list_runs(job_id=job.job_id, limit=1):
        print(run.run_id, run.state.life_cycle_state, run.state.result_state)
        for t in (run.tasks or []):
            print(f"  {t.task_key}  {t.state.life_cycle_state}  {t.state.result_state or '—'}")

# ── DLT-Meta bronze/silver pipeline IDs ───────────────────────────────────────
# Read pipeline IDs directly from the job task definitions — fastest and avoids
# list_pipelines() which chokes on hyphens in names when using filter=
job_details = ws.jobs.get(job_id=job.job_id)
for t in job_details.settings.tasks:
    if t.task_key == "bronze_dlt" and t.pipeline_task:
        bronze_pipeline_id = t.pipeline_task.pipeline_id
    elif t.task_key == "silver_dlt" and t.pipeline_task:
        silver_pipeline_id = t.pipeline_task.pipeline_id

for label, pid in [("bronze", bronze_pipeline_id), ("silver", silver_pipeline_id)]:
    detail = ws.pipelines.get(pipeline_id=pid)
    print(f"{label}: state={detail.state}  health={detail.health}")

# ── LFC ingestion/gateway pipelines (created by lfcdemo-database.ipynb) ───────
# Names follow: {firstname_lastname}_{source_type}_{nine_char_id}_{gw|ig}
# e.g. robert_lee_sqlserver_1a2b3c4d5_gw / robert_lee_sqlserver_1a2b3c4d5_ig
#
# The nine_char_id and pipeline IDs are printed at the end of the notebook
# (search for "ingestion pipeline:" and "gateway pipeline:" in the lfc_setup task output).
#
# To look them up programmatically, filter by the user's name prefix:
import re
me = ws.current_user.me().user_name
name_prefix = re.sub(r"[.\-@]", "_", me.split("@")[0]).lower()  # e.g. "robert_lee"
lfc_pipelines = [
    p for p in ws.pipelines.list_pipelines()
    if (p.name or "").startswith(name_prefix) and (p.name or "").endswith(("_gw", "_ig"))
]
for p in sorted(lfc_pipelines, key=lambda x: x.name):
    detail = ws.pipelines.get(pipeline_id=p.pipeline_id)
    print(f"{p.name}: state={detail.state}  health={detail.health}")
```

> **Prefer job_id over jobs.list when possible** — `jobs.list()` with or without `name=` can paginate through the whole workspace and is slow. For LFC demo, read `conf/setup_metadata.json` from the run's workspace path and use `jobs.get(job_id=...)`; pass `limit=100` if you must list. For pipeline IDs, read from the job's task definitions, not `list_pipelines()`.

> **Avoid `list_pipelines(filter=name)` when the name contains hyphens** — the filter parameter uses a SQL-like expression parser and chokes on unquoted hyphens (`'IN' expected but '-' found`). Always read DLT-Meta pipeline IDs from the job's `pipeline_task.pipeline_id` fields instead. For LFC pipelines, filter by `name.startswith(firstname_lastname)` since those names use underscores only.

> **LFC pipelines take 5+ minutes to reach `RUNNING`** — poll with a sleep loop, don't expect immediate status.

## Querying row counts and per-run metrics

Use `DESCRIBE HISTORY` (not just `COUNT(*)`) to see rows written per pipeline update.
`COUNT(*)` only shows the current total — it cannot distinguish initial vs incremental rows.

```python
from integration_tests.run_integration_tests import get_workspace_api_client
from databricks.sdk.service.sql import StatementState
import time, json

ws = get_workspace_api_client("e2demofe")  # use actual profile
RUN_ID = "<run_id>"
CATALOG = "main"

wh_id = next(w for w in ws.warehouses.list() if str(w.state).endswith('RUNNING')).id

def q(sql):
    resp = ws.statement_execution.execute_statement(statement=sql, warehouse_id=wh_id, wait_timeout='30s')
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        resp = ws.statement_execution.get_statement(resp.statement_id)
    return resp.result.data_array or [] if resp.status.state == StatementState.SUCCEEDED else []

for layer, schema in [('Bronze', f'dlt_meta_bronze_demo_{RUN_ID}'), ('Silver', f'dlt_meta_silver_demo_{RUN_ID}')]:
    print(f'\n=== {layer} current totals ===')
    for row in q(f'SHOW TABLES IN {CATALOG}.{schema}'):
        tbl = row[1]
        count = q(f'SELECT COUNT(*) FROM {CATALOG}.{schema}.{tbl}')
        print(f'  {tbl}: {count[0][0] if count else "?"} rows (total)')

    print(f'\n=== {layer} per-run history (numOutputRows per STREAMING UPDATE) ===')
    for row in q(f'SHOW TABLES IN {CATALOG}.{schema}'):
        tbl = row[1]
        print(f'  -- {tbl} --')
        for hrow in q(f'DESCRIBE HISTORY {CATALOG}.{schema}.{tbl}'):
            version, ts, op, metrics_raw = hrow[0], hrow[1], hrow[4], hrow[12]
            try:
                m = json.loads(metrics_raw) if metrics_raw else {}
            except:
                m = {}
            if op == 'STREAMING UPDATE':
                print(f'    v{version}  {ts}  numOutputRows={m.get("numOutputRows","—")}')
```

**Key point:** Each `STREAMING UPDATE` in the history = one DLT pipeline update (either initial or incremental). `numOutputRows` tells you exactly how many rows that specific run wrote.

## Per-run row summary table

Run `demo/check_run_summary.py` to print a table of rows generated, bronze, and silver per job run:

```bash
# Edit PROFILE, RUN_ID, CATALOG at the top of the script first
python3 demo/check_run_summary.py
```

Output format:
```
Date/Time (UTC)         Type           Status      New CSVs   Generated    Bronze    Silver
───────────────────────────────────────────────────────────────────────────────────────────
2026-03-02 17:03:38     setup          SUCCESS            1          10         9         9
2026-03-02 17:30:32     incremental    SUCCESS          100        1000         9         9
```

- **New CSVs** — CSV files written to the UC Volume whose `last_modified` falls in the run's time window
- **Generated** — `New CSVs × table_data_rows_count` from the job's task parameters
- **Bronze / Silver** — `numOutputRows` from `DESCRIBE HISTORY … WHERE operation = 'STREAMING UPDATE'`

If **Bronze/Silver don't increase** after an incremental run, check:
1. `New CSVs > 0` — if 0, data generation didn't write to the volume (likely `uc_volume_path` was None)
2. The CSV files are in the path AutoLoader is watching (`source_path_prod` in the onboarding spec)

## Verifying source data generation

Before checking Delta history, confirm the data generator actually wrote new files to the UC Volume:

```python
RUN_ID = "<run_id>"
CATALOG = "main"
vol_base = f'/Volumes/{CATALOG}/dlt_meta_dataflowspecs_demo_{RUN_ID}/{CATALOG}_volume_{RUN_ID}/resources/data/input'

for tbl_dir in ws.files.list_directory_contents(vol_base):
    files = list(ws.files.list_directory_contents(tbl_dir.path))
    csv_files = [f for f in files if f.name and f.name.endswith('.csv')]
    print(f'  {tbl_dir.name}: {len(csv_files)} csv file(s)')
```

- **Initial run**: 1 CSV file per table (written with `mode=overwrite`)
- **After each incremental run**: +1 CSV file per table (written with `mode=append`, AutoLoader picks up new files)
- If count does NOT increase after an incremental run, data generation failed silently (check `base_input_path` parameter in the notebook task)

## Key status values

**Job `life_cycle_state`**: `PENDING` → `RUNNING` → `TERMINATED` (check `result_state`) or `SKIPPED` / `INTERNAL_ERROR`

**Job `result_state`**: `SUCCESS`, `FAILED`, `TIMEDOUT`, `CANCELED`

**DLT pipeline `state`** (pipeline-level): `IDLE`, `RUNNING`, `STOPPING`, `DELETED`

**DLT pipeline `health`**: `HEALTHY`, `UNHEALTHY`

**DLT pipeline update `state`** (per-update, in `latest_updates`): Per [Pipelines API GetUpdateResponse](https://docs.databricks.com/api/workspace/pipelines/getupdate), each update has a `state` field. The pipeline can be `RUNNING` while an update is still provisioning. **Only `COMPLETED` means the update finished successfully**; do not treat `RUNNING` as sufficient — if the only update is `RUNNING`, streaming tables may not exist yet.

| Update state | Meaning |
|--------------|--------|
| `QUEUED` | Update is queued. |
| `CREATED` | Update was created. |
| `WAITING_FOR_RESOURCES` | Cluster/resources being provisioned (LFC often 5+ min here). |
| `INITIALIZING`, `RESETTING`, `SETTING_UP_TABLES` | Update in progress. |
| `RUNNING` | Update is actively executing (tables may still be creating). |
| `STOPPING` | Update is stopping. |
| **`COMPLETED`** | **Update finished successfully; tables created/updated.** |
| `FAILED` | Update failed. |
| `CANCELED` | Update was canceled. |

For "can downstream (e.g. bronze) start?" require the **latest** update (first in `latest_updates`) to have state `COMPLETED` — otherwise an older run's `COMPLETED` can cause early exit while the current update is still `WAITING_FOR_RESOURCES`. The LFC demo notebook wait cell does not exit until each pipeline's latest update is `COMPLETED`; it is OK if the pipeline's current state is then `FAILED`, `STOPPED`, or `DELETED`. If the latest update never completes and the pipeline is in a terminal state, the cell raises.

### Pipeline update failure cause

- **`pipelines get-update PIPELINE_ID UPDATE_ID`** returns `state`, `cause` (e.g. `JOB_TASK`), `cluster_id`, and `config`; it does **not** include the human-readable failure message.
- **Failure message text** (e.g. *"Update 9ebc78 has failed. Failed to analyze flow 'main_dlt_meta_bronze_lfc_..._intpk_bronze_inputview' and 1 other flow(s).."*) comes from **[List pipeline events](https://docs.databricks.com/api/workspace/pipelines/listpipelineevents)**:
  - `databricks pipelines list-pipeline-events PIPELINE_ID --max-results 50 -o json`
  - Events with `level: "ERROR"` have a `message` field (and optionally `error`) containing the failure description. Scan the events array for `level == "ERROR"` and use `message` (or `error`) for the cause.
- **"Failed to resolve flow" / "Failed to analyze flow"** on the bronze pipeline usually means the **source** tables (`intpk`, `dtix`) are not in the schema specified in `onboarding.json`. For the LFC demo, `source_database` must be the **LFC-created schema** (from `lfcdemo-database.ipynb`), not `uc_schema_name`. See **Bronze pipeline source schema (LFC demo)** above; ensure the notebook has overwritten `conf/onboarding.json` with `source_database: d.target_schema`.

## Monitoring workflow

1. Read the terminal file (check `/Users/robert.lee/.cursor/projects/*/terminals/*.txt`) for `job_id` and `run_id`
2. Run the CLI command(s) above to get current status
3. Report: job name, run_id, life_cycle_state, result_state, and URL
4. For pipelines, also report health and last update time
5. If a **job** run is `FAILED`, fetch the error message: `databricks jobs get-run <RUN_ID> --profile=DEFAULT -o json | python3 -c "import json,sys; r=json.load(sys.stdin); [print(t['task_key'], t.get('state',{}).get('state_message','')) for t in r.get('tasks',[])]"`
6. If a **pipeline update** is `FAILED`, get the failure message from **list-pipeline-events** (see "Pipeline update failure cause" above); `pipelines get-update` does not return the message text.

---

## Objects created per setup run

> **The Unity Catalog itself (e.g., `main`) is NOT created by the demo — it is a pre-existing catalog supplied via `--uc_catalog_name`. Do NOT delete the catalog; only the schemas (and their contents) listed below are created and should be cleaned up.**

### Techsummit demo

Every `launch_techsummit_demo.py` setup run creates the following, all scoped by `{run_id}`:

| Object | Name / Path | Type |
|--------|-------------|------|
| UC Schema | `{catalog}.dlt_meta_dataflowspecs_demo_{run_id}` | Unity Catalog schema |
| UC Schema | `{catalog}.dlt_meta_bronze_demo_{run_id}` | Unity Catalog schema |
| UC Schema | `{catalog}.dlt_meta_silver_demo_{run_id}` | Unity Catalog schema |
| UC Volume | `{catalog}.dlt_meta_dataflowspecs_demo_{run_id}.{catalog}_volume_{run_id}` | Managed volume (inside dlt_meta schema) |
| UC Tables | all tables inside the bronze/silver schemas | Delta tables created by DLT |
| DLT Pipeline | `dlt-meta-bronze-{run_id}` | Lakeflow Declarative Pipeline |
| DLT Pipeline | `dlt-meta-silver-{run_id}` | Lakeflow Declarative Pipeline |
| Job | `dlt-meta-techsummit-demo-{run_id}` | Databricks job |
| Job | `dlt-meta-techsummit-demo-incremental-{run_id}` | Databricks job (created on first incremental run) |
| Workspace notebooks | `/Users/{user}/dlt_meta_techsummit_demo/{run_id}/` | Workspace directory |

### LFC demo

Every `launch_lfc_demo.py` setup run creates the following:

| Object | Name / Path | Type |
|--------|-------------|------|
| UC Schema | `{catalog}.dlt_meta_dataflowspecs_lfc_{run_id}` | Unity Catalog schema |
| UC Schema | `{catalog}.dlt_meta_bronze_lfc_{run_id}` | Unity Catalog schema |
| UC Schema | `{catalog}.dlt_meta_silver_lfc_{run_id}` | Unity Catalog schema |
| UC Volume | `{catalog}.dlt_meta_dataflowspecs_lfc_{run_id}.{catalog}_lfc_volume_{run_id}` | Managed volume |
| UC Tables | all tables inside the bronze/silver schemas | Delta tables created by DLT |
| DLT Pipeline | `dlt-meta-lfc-bronze-{run_id}` | Lakeflow Declarative Pipeline |
| DLT Pipeline | `dlt-meta-lfc-silver-{run_id}` | Lakeflow Declarative Pipeline |
| Job | `dlt-meta-lfc-demo-{run_id}` | Databricks job |
| Job | `dlt-meta-lfc-demo-incremental-{run_id}` | Databricks job (created on first incremental run) |
| Workspace notebooks | `/Users/{user}/dlt_meta_lfc_demo/{run_id}/` | Workspace directory |

In addition, `lfcdemo-database.ipynb` (the `lfc_setup` task) creates **LFC-managed objects** that have their own lifecycle:

| Object | Name / Path | Cleanup |
|--------|-------------|---------|
| LFC gateway pipeline | `{firstname_lastname}_{source_type}_{nine_char_id}_gw` | Auto-deleted after 1 hour by the notebook's scheduler job |
| LFC ingestion pipeline | `{firstname_lastname}_{source_type}_{nine_char_id}_ig` | Auto-deleted after 1 hour by the notebook's scheduler job |
| LFC scheduler job | `{firstname_lastname}_{source_type}_{nine_char_id}_ig_{pipeline_id}` | Auto-deletes itself after cleanup; delete manually if auto-cleanup didn't fire |
| UC schema (streaming tables) | `{catalog}.{firstname_lastname}_{source_type}_{nine_char_id}` | Auto-deleted after 1 hour |

## Cleanup

### Via Python SDK (recommended — handles all objects in one pass)

```python
from integration_tests.run_integration_tests import get_workspace_api_client
ws = get_workspace_api_client("DEFAULT")

RUN_ID = "<run_id>"
CATALOG = "<uc_catalog_name>"   # e.g. "main"
USERNAME = ws.current_user.me().user_name

# 1. Delete jobs
job_prefixes = [
    f"dlt-meta-techsummit-demo-{RUN_ID}",
    f"dlt-meta-techsummit-demo-incremental-{RUN_ID}",
]
for j in ws.jobs.list():
    if j.settings.name in job_prefixes:
        print(f"Deleting job: {j.settings.name} ({j.job_id})")
        ws.jobs.delete(j.job_id)

# 2. Delete DLT pipelines
pipeline_names = [f"dlt-meta-bronze-{RUN_ID}", f"dlt-meta-silver-{RUN_ID}"]
for p in ws.pipelines.list_pipelines():
    if p.name in pipeline_names:
        print(f"Deleting pipeline: {p.name} ({p.pipeline_id})")
        ws.pipelines.delete(p.pipeline_id)

# 3. Delete UC schemas (volumes and tables cascade via explicit delete first)
schemas_to_delete = [
    f"dlt_meta_dataflowspecs_demo_{RUN_ID}",
    f"dlt_meta_bronze_demo_{RUN_ID}",
    f"dlt_meta_silver_demo_{RUN_ID}",
]
for schema in ws.schemas.list(catalog_name=CATALOG):
    if schema.name in schemas_to_delete:
        for vol in ws.volumes.list(catalog_name=CATALOG, schema_name=schema.name):
            print(f"Deleting volume: {vol.full_name}")
            ws.volumes.delete(vol.full_name)
        for table in ws.tables.list(catalog_name=CATALOG, schema_name=schema.name):
            print(f"Deleting table: {table.full_name}")
            ws.tables.delete(table.full_name)
        print(f"Deleting schema: {schema.full_name}")
        ws.schemas.delete(schema.full_name)

# 4. Delete workspace notebooks directory
nb_path = f"/Users/{USERNAME}/dlt_meta_techsummit_demo/{RUN_ID}"
try:
    ws.workspace.delete(nb_path, recursive=True)
    print(f"Deleted workspace directory: {nb_path}")
except Exception as e:
    print(f"Workspace delete skipped: {e}")

print("Cleanup complete.")
```

#### LFC demo cleanup

For **step 1 (delete jobs)**, prefer reading `job_id` and `incremental_job_id` from workspace `conf/setup_metadata.json` (path: `/Users/{user}/dlt_meta_lfc_demo/{run_id}/conf/setup_metadata.json`) and calling `jobs.get(job_id=...)` then `jobs.delete(job_id=...)` — no list. Fall back to `jobs.list(name=..., limit=100)` only if the file is missing. Pipeline IDs for step 2 come from the setup job's task definitions — no slow `list_pipelines()` scan needed. LFC schemas contain **gateway staging volumes** and sometimes **streaming tables not visible via `ws.tables.list`** — always use `DROP SCHEMA ... CASCADE` via SQL to be safe.

```python
from integration_tests.run_integration_tests import get_workspace_api_client
from databricks.sdk.service.sql import StatementState
import re, time

ws = get_workspace_api_client("DEFAULT")

RUN_ID = "<run_id>"
CATALOG = "<uc_catalog_name>"
USERNAME = ws.current_user.me().user_name
name_prefix = re.sub(r"[.\-@]", "_", USERNAME.split("@")[0]).lower()  # e.g. "robert_lee"

wh_id = next(w for w in ws.warehouses.list() if str(w.state).endswith("RUNNING")).id
def sql(stmt):
    r = ws.statement_execution.execute_statement(statement=stmt, warehouse_id=wh_id, wait_timeout="30s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1); r = ws.statement_execution.get_statement(r.statement_id)
    return r.status.state

# 1. Delete DLT-Meta jobs (use exact name= filter — list() without filter is too slow)
for jname in [f"dlt-meta-lfc-demo-{RUN_ID}", f"dlt-meta-lfc-demo-incremental-{RUN_ID}"]:
    j = next((x for x in ws.jobs.list(name=jname) if x.settings.name == jname), None)
    if j:
        # Read pipeline IDs from job tasks before deleting the job
        jd = ws.jobs.get(job_id=j.job_id)
        pipeline_ids = [t.pipeline_task.pipeline_id for t in jd.settings.tasks if t.pipeline_task]
        ws.jobs.delete(j.job_id)
        print(f"Deleted job: {jname}  pipeline_ids={pipeline_ids}")
        # 2. Delete DLT-Meta bronze/silver pipelines
        for pid in pipeline_ids:
            try:
                ws.pipelines.delete(pid)
                print(f"  Deleted pipeline: {pid}")
            except Exception as e:
                print(f"  Pipeline {pid}: {e}")

# 3. Delete DLT-Meta UC schemas — volumes first, then tables, then schema
for sname in [
    f"dlt_meta_dataflowspecs_lfc_{RUN_ID}",
    f"dlt_meta_bronze_lfc_{RUN_ID}",
    f"dlt_meta_silver_lfc_{RUN_ID}",
]:
    s = next((x for x in ws.schemas.list(catalog_name=CATALOG) if x.name == sname), None)
    if s:
        for vol in ws.volumes.list(catalog_name=CATALOG, schema_name=sname):
            ws.volumes.delete(vol.full_name); print(f"  Deleted volume: {vol.full_name}")
        print(f"  DROP SCHEMA {s.full_name} CASCADE → {sql(f'DROP SCHEMA IF EXISTS {s.full_name} CASCADE')}")

# 4. Delete LFC streaming-table schemas (accumulated from all past runs for this user+source_type)
#    LFC schemas contain gateway staging volumes and streaming tables not always visible via SDK.
#    Use DROP SCHEMA ... CASCADE to handle all contents regardless of type.
#    WARNING: this deletes ALL accumulated schemas for this user+source combination, not just one run.
all_lfc_schemas = [s for s in ws.schemas.list(catalog_name=CATALOG)
                   if s.name.startswith(name_prefix) and "sqlserver" in s.name]
print(f"\nFound {len(all_lfc_schemas)} LFC streaming-table schemas to drop")
for s in all_lfc_schemas:
    print(f"  DROP SCHEMA {s.full_name} CASCADE → {sql(f'DROP SCHEMA IF EXISTS {s.full_name} CASCADE')}")

# 5. Delete LFC scheduler jobs + gateway/ingestion pipelines
#    Scheduler job name: {ig_pipeline_name}_{pipeline_id}
#    e.g. robert_lee_sqlserver_4207c3507_ig_4cb82ef4-8552-424c-91b7-5c11da11d641
lfc_jobs = [j for j in ws.jobs.list()
            if (j.settings.name or "").startswith(name_prefix) and "_ig_" in (j.settings.name or "")]
print(f"Found {len(lfc_jobs)} LFC scheduler job(s)")
for j in lfc_jobs:
    ws.jobs.delete(j.job_id); print(f"  Deleted job: {j.settings.name}")

lfc_pipelines = [p for p in ws.pipelines.list_pipelines()
                 if (p.name or "").startswith(name_prefix) and (p.name or "").endswith(("_gw", "_ig"))]
print(f"Found {len(lfc_pipelines)} LFC pipeline(s)")
for p in lfc_pipelines:
    ws.pipelines.delete(p.pipeline_id); print(f"  Deleted pipeline: {p.name}")

# 6. Delete workspace directory
nb_path = f"/Users/{USERNAME}/dlt_meta_lfc_demo/{RUN_ID}"
try:
    ws.workspace.delete(nb_path, recursive=True)
    print(f"\nDeleted workspace directory: {nb_path}")
except Exception as e:
    print(f"\nWorkspace delete skipped: {e}")

print("\nLFC cleanup complete.")
```

> **LFC schemas accumulate.** The `{firstname_lastname}_{source_type}_{nine_char_id}` schemas are created fresh per notebook run. If the lfcdemolib scheduler job fails to auto-clean (e.g. because the cluster was terminated), many orphaned schemas accumulate. Step 4 above clears them all at once. Use `DROP SCHEMA ... CASCADE` — some contain gateway staging volumes and streaming tables that are invisible to `ws.tables.list`.

> **Pipeline IDs:** Always read DLT-Meta pipeline IDs from the setup job's task `pipeline_task.pipeline_id` fields before deleting the job. Do NOT use `list_pipelines()` for this — it scans all pipelines and times out.

### Via CLI (per object type, useful for spot cleanup)

```bash
RUN_ID=<run_id>
CATALOG=<catalog>   # e.g. main
PROFILE=DEFAULT

# Delete jobs by name pattern
databricks jobs list --profile=$PROFILE -o json | python3 -c "
import json, sys
for j in json.load(sys.stdin):
    if '$RUN_ID' in j.get('settings',{}).get('name',''):
        print(j['job_id'], j['settings']['name'])
" | while read id name; do
    echo "Deleting job $name ($id)"
    databricks jobs delete $id --profile=$PROFILE
done

# Delete DLT pipelines by name pattern
databricks pipelines list-pipelines --profile=$PROFILE -o json | python3 -c "
import json, sys
for p in json.load(sys.stdin):
    if '$RUN_ID' in p.get('name',''):
        print(p['pipeline_id'], p['name'])
" | while read id name; do
    echo "Deleting pipeline $name ($id)"
    databricks pipelines delete $id --profile=$PROFILE
done

# UC schemas/tables/volumes — use SDK script above (CLI lacks bulk schema delete)
```

### Cleanup order

Always delete in this order to avoid dependency errors:
1. Jobs (stop any active runs first if needed)
2. DLT pipelines
3. UC volumes (inside schemas)
4. UC tables (inside schemas)
5. UC schemas
6. Workspace notebook directory
