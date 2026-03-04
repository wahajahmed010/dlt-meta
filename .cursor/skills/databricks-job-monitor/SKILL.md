---
name: databricks-job-monitor
description: Monitor Databricks job runs and DLT pipeline status, and clean up all resources created by this project. Use when the user asks to check job status, watch pipeline progress, inspect run output, monitor or clean up the techsummit demo jobs, pipelines, schemas, volumes, tables, or workspace notebooks. Extracts job_id and run_id from terminal output and queries or modifies the Databricks workspace via the SDK or CLI.
---

# Databricks Job & Pipeline Monitor

## Domain knowledge: LFC, SDP Meta, SCD Types, and column formats

### Lakeflow Connect (LFC)

LFC streams changes from an external source database (SQL Server, PostgreSQL, MySQL) into
Databricks **streaming tables** in Unity Catalog. Per source it creates two DLT pipelines:

| Pipeline | Name suffix | Role |
|----------|------------|------|
| Gateway | `*_gw` | Connects to source DB and captures change log |
| Ingestion | `*_ig` | Writes changes into UC streaming tables |

The demo streams two source tables: **`intpk`** (SCD Type 1) and **`dtix`** (SCD Type 2).

### SDP Meta (formerly DLT-Meta)

`databricks.labs.sdp_meta` is the metadata-driven framework that reads `onboarding.json` and
drives DLT pipelines. It creates:

| Pipeline | Name pattern |
|----------|-------------|
| Bronze | `sdp-meta-lfc-bronze-{run_id}` |
| Silver | `sdp-meta-lfc-silver-{run_id}` |

Bronze reads the LFC streaming tables via Change Data Feed (`readChangeFeed: true`) and applies
CDC via `bronze_cdc_apply_changes`. Silver applies pass-through transformations (`select *`).

### SCD Type 1 — `intpk`

LFC writes **SCD Type 1** for `intpk` (source primary key: `pk`):

- One row per `pk` in the streaming table — always the current state.
- Changes arrive as INSERT / UPDATE / DELETE in the Change Data Feed.
- No history columns (`__start_at` / `__end_at` are absent).

DLT-Meta CDC config:

| Layer | `keys` | `scd_type` | `sequence_by` |
|-------|--------|------------|---------------|
| Bronze | `["pk"]` | `"1"` | `"_commit_version"` |
| Silver | `["pk"]` | `"1"` | `"dt"` |

### SCD Type 2 — `dtix`

LFC writes **SCD Type 2** for `dtix` (index on `dt`; treated as no-PK by LFC since the source
has no explicit primary key). The streaming table holds full row history:

- Multiple rows per `dt` value — one per version.
- Active version: `__end_at = NULL`. Closed version: `__end_at` is set.
- When a source row changes, LFC performs:
  1. **UPDATE** old row → sets `__end_at` from `NULL` → struct value (closes version).
  2. **INSERT** new row → `__start_at` = new struct value, `__end_at = NULL` (opens version).

Because the LFC streaming table **already has** `__start_at` / `__end_at`, DLT-Meta must use
`scd_type: "1"` — **not** `"2"`. Using `scd_type: "2"` causes:

```
DLTAnalysisException: Please rename the following system reserved columns
in your source: __START_AT, __END_AT.
```

DLT-Meta CDC config:

| Layer | `keys` | `scd_type` | `sequence_by` |
|-------|--------|------------|---------------|
| Bronze | `["dt", "__start_at"]` | `"1"` | `"_commit_version"` |
| Silver | `["dt", "__start_at"]` | `"1"` | `"__start_at"` |

`"dt"` is the logical business key; `"__start_at"` distinguishes row-versions.
`sequence_by = "_commit_version"` (bronze): never NULL from CDF; the UPDATE that closes a version
always has a higher commit version than the original INSERT, so the final `__end_at` value wins.
`sequence_by = "__start_at"` (silver): always non-null; lexicographically monotone per version.

### `__start_at` / `__end_at` — struct type, not a timestamp

Both columns are **structs** with two sub-fields:

| Sub-field | Type | Example |
|-----------|------|---------|
| `__cdc_internal_value` | string | `"0000132800003360000D-00001328000033600002-00000000000000000001"` |
| `__cdc_timestamp_value` | string (ISO-8601) | `"2026-03-04T01:06:41.787Z"` |

Sample closed row in the `dtix` LFC streaming table (both fields populated):

```
__start_at = {
  __cdc_internal_value:  "0000132800003360000D-00001328000033600002-00000000000000000001",
  __cdc_timestamp_value: "2026-03-04T01:06:41.787Z"
}
__end_at = {
  __cdc_internal_value:  "00001328000033500013-00001328000033500011-00000000000000000010",
  __cdc_timestamp_value: "2026-03-04T01:06:41.363Z"
}
```

Active rows have `__end_at = NULL`. To filter for active rows: `WHERE __end_at IS NULL`.

`__cdc_internal_value` encodes a commit position and is lexicographically monotone — newer
row-versions always compare greater, making struct-level comparison safe for `sequence_by`.

---

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
- Setup job: `sdp-meta-lfc-demo-{run_id}`
- Incremental job: `sdp-meta-lfc-demo-incremental-{run_id}`
- Bronze pipeline: `sdp-meta-lfc-bronze-{run_id}`
- Silver pipeline: `sdp-meta-lfc-silver-{run_id}`

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

**Bronze pipeline source schema (LFC demo):** The bronze DLT pipeline reads the LFC streaming tables (`intpk`, `dtix`) from the **schema created by `lfcdemo-database.ipynb`** (i.e. `d.target_schema`, e.g. `robert_lee_sqlserver_4207c5e3d`), **not** from the `source_schema` (source DB schema) / launcher's `lfc_schema` (e.g. `lfcddemo`) passed to `launch_lfc_demo.py`. The launcher writes an initial `onboarding.json` with `source_database: lfc_schema`; the notebook **overwrites** `conf/onboarding.json` on the run's volume with `source_database: d.target_schema` so that `onboarding_job` and the bronze pipeline use the correct schema. If the bronze pipeline fails with "Failed to resolve flow" or "Failed to analyze flow" for flows like `main_dlt_meta_bronze_lfc_{run_id}_intpk_bronze_inputview`, the usual cause is that the **source** tables are missing from the schema in `onboarding.json` — e.g. the file was not overwritten by the notebook (notebook failed before the write, or `run_id`/`target_catalog` not passed), or an older run used a different schema. Confirm that `conf/onboarding.json` on the run's volume has `source_database` equal to the LFC-created schema name (from `conf/lfc_created.json` → `lfc_schema`).

**Storing job IDs for efficient lookup (LFC demo):** To avoid slow `jobs.list(name=...)` over the whole workspace, `launch_lfc_demo.py` stores setup and incremental job IDs in a workspace file and uses `jobs.get(job_id=...)` when possible. At **setup**, after creating the main job it writes `conf/setup_metadata.json` under the run's workspace path (`/Users/{user}/sdp_meta_lfc_demo/{run_id}/conf/setup_metadata.json`) with `job_id` and `uc_catalog_name`. On **incremental** runs it first tries to read that file; if `job_id` is present it calls `jobs.get(job_id=meta["job_id"])` (fast) instead of `jobs.list(name=..., limit=100)`. When the incremental job is created for the first time, the launcher writes the same file with `incremental_job_id` added; subsequent incremental runs then use `jobs.get(job_id=meta["incremental_job_id"])` and skip listing. For monitoring or scripts: **prefer reading `conf/setup_metadata.json` and using `jobs.get(job_id=...)`** when you have a run_id and the workspace path; fall back to `jobs.list(name=..., limit=JOBS_LIST_LIMIT)` only if the file is missing (e.g. runs from before this feature).

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
runners_path = f"/Users/{USERNAME}/sdp_meta_lfc_demo/{RUN_ID}"
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
    job_name = f"dlt-meta-techsummit-demo-{RUN_ID}"  # or sdp-meta-lfc-demo-{RUN_ID}
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
- **"Failed to resolve flow" / "Failed to analyze flow"** on the bronze pipeline usually means the **source** tables (`intpk`, `dtix`) are not in the schema specified in `onboarding.json`. For the LFC demo, `source_database` must be the **LFC-created schema** (from `lfcdemo-database.ipynb`), not the source DB schema (`source_schema`). See **Bronze pipeline source schema (LFC demo)** above; ensure the notebook has overwritten `conf/onboarding.json` with `source_database: d.target_schema`.

### Verifying causal relationship: did a job run cause the pipeline update to be canceled?

**`cause: JOB_TASK`** means the pipeline update was **started** by a job task. It does **not** by itself prove that the job run stopped or that the job’s cancellation caused the update to be canceled. To get **positive evidence** that a specific job run caused the cancel:

1. **Get the pipeline update** (state, cause, creation_time):
   ```bash
   databricks pipelines get-update <PIPELINE_ID> <UPDATE_ID> --profile=PROFILE -o json
   ```
   Note `state` (e.g. `CANCELED`), `cause` (e.g. `JOB_TASK`), and `creation_time` (ms since epoch).

2. **Find the job that runs this pipeline.** The Pipelines API does not return `job_run_id` in get-update. You must identify the job whose task has `pipeline_id` equal to this pipeline:
   ```bash
   databricks jobs get <JOB_ID> --profile=PROFILE -o json
   ```
   Inspect `settings.tasks[].pipeline_task.pipeline_id`. Only that job can have started this update. (Example: job 754356193445229 runs pipelines `633ca38c-...` and `f1777a92-...`; it does **not** run pipeline `809c9648-...`. Pipeline `809c9648-...` is likely an LFC ingestion/gateway pipeline — find the job/task that references it, e.g. from the notebook’s scheduler or the run’s `lfc_created.json`.)

3. **Get the job run** that started the update (same run that triggered the update; you may need to correlate by start_time vs update creation_time, or from run history):
   ```bash
   databricks jobs get-run <RUN_ID> --profile=PROFILE -o json
   ```
   Note `state.result_state`, `state.life_cycle_state`, `start_time`, `end_time`.

4. **Positive evidence that the job caused the cancel:**
   - `cause` is `JOB_TASK`, **and**
   - That job run has `result_state: CANCELED` (or `FAILED`/`TIMEDOUT` that stopped the run), **and**
   - Job run `end_time` is set and is before or within a short time of the pipeline update’s cancel time (cancel time from `pipelines list-pipeline-events` — look for the event "Update &lt;id&gt; is CANCELED").
   Then the job run’s termination caused the pipeline update to be canceled.

5. **If the job run is still RUNNING** (`end_time: 0`) or has `result_state: SUCCESS`, then the pipeline update was **not** canceled because the job stopped. It was likely canceled by something else (e.g. user canceled the update in the pipeline UI). `cause` remains `JOB_TASK` because the update was *started* by a job task.

## Monitoring workflow

1. Read the terminal file (check `/Users/robert.lee/.cursor/projects/*/terminals/*.txt`) for `job_id` and `run_id`
2. Run the CLI command(s) above to get current status
3. Report: job name, run_id, life_cycle_state, result_state, and URL
4. For pipelines, also report health and last update time
5. If a **job** run is `FAILED`, fetch the error message: `databricks jobs get-run <RUN_ID> --profile=DEFAULT -o json | python3 -c "import json,sys; r=json.load(sys.stdin); [print(t['task_key'], t.get('state',{}).get('state_message','')) for t in r.get('tasks',[])]"`
6. If a **pipeline update** is `FAILED`, get the failure message from **list-pipeline-events** (see "Pipeline update failure cause" above); `pipelines get-update` does not return the message text.

### Trigger task: "Job X does not exist" (InvalidParameterValue)

When running **incremental** (`launch_lfc_demo.py --run_id=...`), the first task runs `trigger_ingestion_and_wait` (or the equivalent notebook). That task reads **`conf/lfc_created.json`** from the run’s UC volume to get `lfc_scheduler_job_id` and calls `jobs.run_now(job_id=lfc_scheduler_job_id)`. If you see **`InvalidParameterValue: Job 893133786814806 does not exist`** (or similar), the cause is:

- **Stale `lfc_scheduler_job_id`:** The LFC scheduler job was created at setup and its ID was written to `lfc_created.json`. That job is often **deleted** by the notebook’s auto-cleanup (e.g. after 1 hour) or manually. The volume file is not updated when the job is deleted, so a later incremental run still has the old ID and `run_now` fails.

**Verify:**

1. **Confirm the job is missing:** `databricks jobs get <JOB_ID> --profile=PROFILE` → if you get "does not exist" or 404, the job was deleted.
2. **Confirm what’s on the volume:** The trigger task reads  
   `/Volumes/<catalog>/sdp_meta_dataflowspecs_lfc_<run_id>/<catalog>_lfc_volume_<run_id>/conf/lfc_created.json`.  
   It should contain `ig_pipeline_id` and `lfc_scheduler_job_id`. If `lfc_scheduler_job_id` points to a deleted job, that’s the cause.

**Fix (code):** `trigger_ingestion_and_wait.py` now catches "job does not exist"–style errors and **falls back** to `pipelines.start_update(pipeline_id=ig_pipeline_id)` so the ingestion pipeline is triggered directly and the incremental run can proceed. Redeploy/upload the updated notebook so the incremental job uses it.

**Fix (manual):** If the ingestion pipeline still exists, you can trigger it by hand: `databricks pipelines start-update <ig_pipeline_id> --profile=PROFILE`. Get `ig_pipeline_id` from the same `lfc_created.json` (or from the setup job’s lfc_setup output).

### Tracing jobs, runs, pipelines and the dependency graph

Use run and job APIs to trace which notebook/job created a given job or pipeline.

**1. From a run_id, get run metadata.** Run metadata often persists even after the job is deleted: `databricks jobs get-run <RUN_ID> --profile=PROFILE -o json`. From the response: **job_id** (parent job; may be deleted), **run_name** (e.g. LFC scheduler jobs use `{user}_{source}_{id}_ig_{pipeline_id}`), **tasks[]** with `task_key`, `pipeline_task.pipeline_id`, or `notebook_task.notebook_path`.

**2. If the job is deleted**, `jobs get JOB_ID` fails. Use the **run** to infer creator: **run_name** like `robert_lee_sqlserver_42086316e_ig_809c9648-872b-4402-bf15-48516b23dad3` → LFC **ingestion scheduler job**, created by **lfcdemo-database.ipynb** (lfc_setup task), in the cell that calls `d.jobs_create(ig_job_spec)`; single task `run_dlt` with `pipeline_task.pipeline_id` = ingestion pipeline. **run_name** `sdp-meta-lfc-demo-{run_id}` → created by **launch_lfc_demo.py**.

**3. Example: job 893133786814806, run 327800737236822** — `jobs get 893133786814806` → Job does not exist. `jobs get-run 327800737236822` → run_name `robert_lee_sqlserver_42086316e_ig_809c9648-...`, one task `run_dlt`, pipeline_id `809c9648-872b-4402-bf15-48516b23dad3`. So this job was the **LFC scheduler job** for ingestion pipeline 809c9648..., **created by lfcdemo-database.ipynb**; its job_id was written to `conf/lfc_created.json` as `lfc_scheduler_job_id`.

**4. LFC demo dependency graph:** Setup job → **lfc_setup** (lfcdemo-database.ipynb) → creates gateway + ingestion pipelines and **LFC scheduler job** (name `{user}_{source}_{id}_ig_{pipeline_id}`), writes **lfc_created.json** and overwrites onboarding.json → onboarding_job → bronze_dlt, silver_dlt. Incremental job → **trigger task** reads lfc_created.json, calls run_now(scheduler job) or start_update(ig_pipeline_id) → bronze_dlt → silver_dlt.

---

## AI-initiated test cycle: launch → monitor → fix → re-launch

This section documents the full workflow for launching, troubleshooting, fixing, and re-launching
the LFC demo from the AI agent. **Always work in `dlt-meta-lfc/` with `.venv_3_11` activated.**

### Prerequisites

```bash
cd /Users/robert.lee/github/dlt-meta-lfc
source .venv_3_11/bin/activate
```

Always set `PYTHONPATH` when calling the launcher directly:

```bash
PYTHONPATH="$(pwd):$(pwd)/src" python demo/launch_lfc_demo.py \
  --uc_catalog_name=main \
  --connection_name=lfcddemo-azure-sqlserver \
  --cdc_qbc=cdc \
  --trigger_interval_min=5 \
  --profile=e2demofe \
  --sequence_by_pk \
  --snapshot_method=cdf
```

#### `--snapshot_method` flag

Controls how the `dtix` (LFC SCD2, no-PK) table is processed by the bronze DLT pipeline.

| Value | Behaviour | When to use |
|-------|-----------|-------------|
| `cdf` **(default)** | Custom `next_snapshot_and_version` lambda. Checks the Delta table version first (O(1)). If nothing changed since the last run, skips immediately. If changed, reads the full table (O(n)). | Frequently-triggered pipelines where source changes infrequently. |
| `full` | Built-in view-based `apply_changes_from_snapshot`. Reads and materialises the full source table on every trigger (O(n) always). | Stable reference; use if the lambda causes issues. |

The value is passed as Spark conf `dtix_snapshot_method` to the bronze DLT pipeline; `init_sdp_meta_pipeline.py` reads it with `spark.conf.get("dtix_snapshot_method", "cdf")`.

**How `cdf` mode works internally:**
1. `init_sdp_meta_pipeline.py` defines `dtix_next_snapshot_and_version(latest_snapshot_version, dataflowSpec)`.
2. It's passed as `bronze_next_snapshot_and_version` to `DataflowPipeline.invoke_dlt_pipeline`.
3. `DataflowPipeline.is_create_view()` sees it's a snapshot spec with a custom lambda → returns `False` (no DLT view registered for `dtix`).
4. `apply_changes_from_snapshot()` uses the lambda as the DLT source directly.
5. At runtime: lambda does `DESCRIBE HISTORY <table> LIMIT 1` (O(1)), returns `None` if version unchanged, otherwise reads full table and renames `__START_AT`/`__END_AT` → `lfc_start_at`/`lfc_end_at`.

The launcher prints a `run_id` at the end — save it for all subsequent monitoring, incremental
runs, and cleanup.

### Monitoring after launch

After a successful launch, two jobs run in sequence:

| Job | Purpose | How to find |
|-----|---------|-------------|
| Job 1 (setup) | Runs `lfcdemo-database.ipynb` — creates LFC pipelines, waits for tables | `Job` URL printed by launcher |
| Job 2 (downstream) | `onboarding_job` → `bronze_dlt` → `silver_dlt` | `Downstream` URL printed by launcher |

Job 1 (setup) takes **~1 hour**; it triggers Job 2 automatically when it succeeds.
Job 2 (downstream) takes **~10 min** depending on data volume.
**Always monitor task-by-task** — don't poll only the top-level job state.
Extract `SETUP_JOB_ID` and `DOWNSTREAM_JOB_ID` from the launcher's `Job :` and `Downstream:` output lines.
Run the incremental only after Job 2 shows `SUCCESS`.

**Poll loop (recommended):**

```python
import sys, os, time
sys.path.insert(0, os.path.join(os.getcwd(), "src"))
from integration_tests.run_integration_tests import get_workspace_api_client

ws = get_workspace_api_client("e2demofe")
DOWNSTREAM_JOB_ID = <downstream_job_id>   # from launcher output

for attempt in range(25):
    time.sleep(60)
    runs = list(ws.jobs.list_runs(job_id=DOWNSTREAM_JOB_ID, limit=1))
    if not runs:
        print(f"{attempt+1}m: Job2 not triggered yet"); continue
    run = runs[0]; full = ws.jobs.get_run(run_id=run.run_id)
    for t in (full.tasks or []):
        print(f"  {t.task_key:25s}  {t.state.life_cycle_state}  {t.state.result_state or '—'}")
    bronze_task = next((t for t in (full.tasks or []) if 'bronze' in t.task_key and t.pipeline_task), None)
    if bronze_task:
        pid = bronze_task.pipeline_task.pipeline_id
        events = list(ws.pipelines.list_pipeline_events(pipeline_id=pid, max_results=30))
        errors = [e for e in events if "ERROR" in str(e.level or "").upper()]
        p = ws.pipelines.get(pipeline_id=pid)
        latest = p.latest_updates[0] if p.latest_updates else None
        print(f"  Bronze: {p.state}  {latest.state if latest else 'none'}")
        if errors:
            for e in errors[:1]:
                for ex in (e.as_dict() or {}).get('error', {}).get('exceptions', []):
                    print(f"  ERROR: {ex.get('class_name')}: {ex.get('message','')[:500]}")
            break
        if latest and str(latest.state) == "UpdateStateInfoState.COMPLETED":
            print("Bronze COMPLETED"); break
    if str(run.state.life_cycle_state) == "RunLifeCycleState.TERMINATED":
        print(f"Job2 finished: {run.state.result_state}"); break
```

### Error diagnosis playbook

**Always check the full exception from `list_pipeline_events`, not just the summary event.**

| Error | Root cause | Fix |
|-------|-----------|-----|
| `Snapshot reader function not provided!` | Wheel is old — cluster cached a Python env from a prior `0.0.11` build | Bump wheel version, rebuild, relaunch |
| `from src.dataflow_pipeline import DataflowPipeline` fails | `init_sdp_meta_pipeline.py` used old flat import; `build/lib/src/` artifact contaminated wheel | Change import to `from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline` |
| `UNRESOLVED_COLUMN __START_AT` in `apply_changes_from_snapshot` | DLT globally strips `__START_AT`/`__END_AT` (reserved) before resolving keys | Add `bronze_custom_transform` in `init_sdp_meta_pipeline.py` to rename to `lfc_start_at`/`lfc_end_at`; update keys |
| `DLTAnalysisException: system reserved columns __START_AT, __END_AT` | Same reservation, triggered by CDF-based `apply_changes` | Switch `dtix` to `source_format: snapshot` + `apply_changes_from_snapshot` |
| `[SCHEMA_NOT_FOUND]` or `Schema '...' does not exist` | Run ID schema was cleaned up (or old run_id reused) | Always do a fresh launch; don't reuse a cleaned run_id |
| `AttributeError: 'bytes' object has no attribute 'seekable'` | `ws.files.upload(contents=bytes)` — must wrap in `io.BytesIO` | Use `io.BytesIO(data)` |
| `DUPLICATE_KEY_VIOLATION` — 9 rows for key `{"dt":"...","lfc_start_at":"{null, null}"}` | No-PK source table has multiple rows with same `dt` and null `__START_AT`; key `(dt, lfc_start_at)` is non-unique | Change key to `["dt", "lfc_end_at"]` — LFC's `__END_AT` is always unique per row (unique `__cdc_internal_value`). Verify: `COUNT(*) == COUNT(DISTINCT struct(dt, __END_AT))` in source |
| `FileNotFoundError: Cannot read /Volumes/main/dlt_meta_dataflowspecs_lfc_...` | `trigger_ingestion_and_wait.py` uses stale `dlt_meta_` prefix | Line 32: change `dlt_meta_dataflowspecs_lfc_` → `sdp_meta_dataflowspecs_lfc_` |

### Checking what's in the deployed wheel

When the DLT pipeline uses the wrong code, verify the wheel that was uploaded:

```python
import zipfile

whl = "dist/databricks_labs_sdp_meta-0.0.12-py3-none-any.whl"

with zipfile.ZipFile(whl) as z:
    # Check top-level structure (should only have 'databricks/')
    from collections import Counter
    tops = Counter(n.split('/')[0] for n in z.namelist() if 'dist-info' not in n)
    for k, v in sorted(tops.items()):
        print(f"  {k}/: {v} files")

    # Verify our fix is in the wheel
    dp = z.read('databricks/labs/sdp_meta/dataflow_pipeline.py').decode()
    lines = dp.split('\n')
    for i in range(269, 276):
        print(f"{i+1}: {lines[i]}")
```

If the wheel contains `src/` alongside `databricks/`, there are **stale build artifacts**.
Fix: delete `build/` before rebuilding.

```bash
rm -rf build/
python -m build --wheel
```

### Wheel version bumping (force fresh cluster environment)

Databricks caches Python environments by wheel **filename**. If you rebuild the wheel with the
same version (e.g. `0.0.11`) the cluster reuses the cached env and your fix never runs.

Always bump the version when deploying a code fix:

```bash
# src/databricks/labs/sdp_meta/__about__.py
__version__ = '0.0.12'   # was 0.0.11

# setup.py
version="0.0.12",        # was 0.0.11
```

Then rebuild and relaunch. The new filename `databricks_labs_sdp_meta-0.0.12-py3-none-any.whl`
forces Databricks to create a fresh Python environment with the corrected code.

### Cleanup during AI-initiated testing

**When a run has failed and the output is no longer needed, always clean up before re-launching.**
Stale runs consume workspace resources and make it harder to correlate errors to a specific run.

```bash
# Clean up a specific failed run
python demo/cleanup_lfc_demo.py --profile=e2demofe --run_id=<failed_run_id>
```

`cleanup_lfc_demo.py` deletes all objects created for that run:
- Setup and incremental jobs
- Bronze and silver DLT-Meta pipelines
- UC schemas (`sdp_meta_dataflowspecs_lfc_*`, `sdp_meta_bronze_lfc_*`, `sdp_meta_silver_lfc_*`) and their volumes/tables
- Workspace notebooks under `/Users/{user}/sdp_meta_lfc_demo/{run_id}/`

To also clean up the LFC gateway/ingestion pipelines:

```bash
python demo/cleanup_lfc_demo.py --profile=e2demofe --run_id=<run_id> --include-all-lfc-pipelines
```

**Rule of thumb:** After every failed run that required a code fix, clean up the old run before
launching again. Accumulating stale runs makes it hard to know which schema/table you're looking at.

### Running the incremental test

After a successful full run, verify the incremental path by re-triggering bronze/silver with
the latest LFC data:

```bash
python demo/launch_lfc_demo.py --profile=e2demofe --run_id=<run_id>
```

For example, with run `7bc7086ff8324a33b0f16b6e7ed872a7`:

```bash
python demo/launch_lfc_demo.py --profile=e2demofe --run_id=7bc7086ff8324a33b0f16b6e7ed872a7
```

This:
1. Creates (or reuses) an incremental job named `sdp-meta-lfc-demo-incremental-{run_id}`
2. Triggers the LFC ingestion pipeline to ingest new rows from the source DB
3. Waits for the ingestion pipeline update to `COMPLETED`
4. Triggers bronze and silver DLT-Meta pipelines against the same run's schemas

Monitor the incremental run the same way as the initial setup run, using `DOWNSTREAM_JOB_ID` from
the incremental job output.

**Verify incremental rows were written:**

```python
from integration_tests.run_integration_tests import get_workspace_api_client
from databricks.sdk.service.sql import StatementState
import time

ws = get_workspace_api_client("e2demofe")
wh_id = next(w for w in ws.warehouses.list() if str(w.state).endswith('RUNNING')).id
RUN_ID = "7bc7086ff8324a33b0f16b6e7ed872a7"
CATALOG = "main"

def q(sql):
    s = ws.statement_execution.execute_statement(statement=sql, warehouse_id=wh_id)
    for _ in range(20):
        r = ws.statement_execution.get_statement(s.statement_id)
        if r.status.state in (StatementState.SUCCEEDED, StatementState.FAILED): break
        time.sleep(2)
    return r.result.data_array or [] if r.status.state == StatementState.SUCCEEDED else []

for layer, schema in [
    ('Bronze', f'sdp_meta_bronze_lfc_{RUN_ID}'),
    ('Silver', f'sdp_meta_silver_lfc_{RUN_ID}'),
]:
    print(f'\n=== {layer} ===')
    for tbl in ['intpk', 'dtix']:
        rows = q(f'SELECT COUNT(*) FROM {CATALOG}.{schema}.{tbl}')
        print(f'  {tbl}: {rows[0][0] if rows else "?"} rows')
        # DESCRIBE HISTORY shows per-update write counts
        for h in q(f'DESCRIBE HISTORY {CATALOG}.{schema}.{tbl} LIMIT 3'):
            print(f'    v{h[0]}  op={h[4]}  metrics={h[12]}')
```

### Full fix-and-relaunch example (the session that produced this skill entry)

The session that refined these patterns went through 3 successive errors before the pipeline ran
clean. Here is the full trace so you can recognize the same pattern quickly:

1. **First launch** (`run_id: c77bd542...`) — failed with
   `Exception: Snapshot reader function not provided!` at `dataflow_pipeline.py:275`.  
   **Cause:** The wheel was built while `build/lib/src/dataflow_pipeline.py` (stale artifact, OLD
   code) existed; that artifact was packaged as `src/dataflow_pipeline.py` in the wheel alongside
   the fixed `databricks/labs/sdp_meta/dataflow_pipeline.py`. The init notebook imported from
   `src.dataflow_pipeline` (old flat path) → loaded unfixed code.  
   **Fix:** (a) Deleted `build/`, (b) changed import in `init_sdp_meta_pipeline.py` from
   `from src.dataflow_pipeline` → `from databricks.labs.sdp_meta.dataflow_pipeline`, (c) bumped
   version to `0.0.12` to break the cluster env cache, (d) rebuilt wheel, cleaned up old run,
   relaunched.

2. **Second launch** (`run_id: 166b41513...`) — failed with
   `UNRESOLVED_COLUMN __START_AT. Did you mean ['data', 'dt']`.  
   **Cause:** `apply_changes_from_snapshot` reached the correct code path (bug fix worked!) but
   DLT globally strips `__START_AT`/`__END_AT` from the snapshot view schema (they are system-
   reserved names), making them invisible when resolving keys. The `['data', 'dt']` suggestion was
   from a different/cached schema context.  
   **Fix:** Added a `bronze_custom_transform` in `init_sdp_meta_pipeline.py` that renames
   `__START_AT` → `lfc_start_at` and `__END_AT` → `lfc_end_at` for the `dtix` table. Updated
   `LFC_DTIX_BRONZE_APPLY_CHANGES_FROM_SNAPSHOT` and silver counterpart keys to `["dt",
   "lfc_start_at"]`. Cleaned up old run, relaunched.

3. **Third launch** (`run_id: 7bc7086f...`) — **success**.  
   Bronze and silver completed; tables have columns `dt`, `lfc_start_at` (struct), `lfc_end_at`
   (struct).

4. **Incremental run on `7bc7086f...`** — failed with two new errors:

   a. `trigger_ingestion_and_wait.py` read `dlt_meta_dataflowspecs_lfc_...` (stale prefix).  
      **Fix:** `trigger_ingestion_and_wait.py` line 32 → change `dlt_meta_` to `sdp_meta_`.  
      Also: extended `_upload_trigger_ingestion_notebook` in `launch_lfc_demo.py` to also
      re-upload `init_sdp_meta_pipeline.py` on every incremental so local fixes are picked up
      without a full teardown.

   b. `[APPLY_CHANGES_FROM_SNAPSHOT_ERROR.DUPLICATE_KEY_VIOLATION]` — 9 rows per key
      `{"dt":"...","lfc_start_at":"{null, null}"}` in the internal materialization table.  
      **Root cause:** The `dtix` SQL Server source has no primary key; multiple rows can have
      the same `dt` value **and** a null `__START_AT` (initial-load rows LFC hasn't yet assigned
      a CDC start timestamp to). Key `(dt, lfc_start_at)` is therefore non-unique.  
      **Key insight:** LFC always assigns a unique `__END_AT.__cdc_internal_value` to every row,
      including initial-load rows where `__START_AT` is null. Querying the source confirmed:
      `COUNT(*) = COUNT(DISTINCT struct(dt, __END_AT))` — `(dt, __END_AT)` is globally unique
      across all 1900 rows (both historical and currently-active).  
      **Fix:** Change `apply_changes_from_snapshot` keys from `["dt", "lfc_start_at"]` to
      `["dt", "lfc_end_at"]` in `launch_lfc_demo.py`, `lfcdemo-database.ipynb`, and patch the
      live `onboarding.json` on the UC volume for the affected run.

   c. Attempting `full_refresh_selection` to clear the corrupted internal materialization was
      slow and ran into a `ResourceConflict` from the already-running failed update.  
      **Decision:** Clean up the failed run entirely and start a fresh launch. This is faster
      than waiting for selective full-refresh to complete on a pipeline with corrupted state.

5. **Fourth launch** (`run_id: cb89a69bd30c43c29dbb433ecc6ec7fb`) — initial `--snapshot_method=full` baseline success.  
   The **setup job** (`sdp-meta-lfc-demo-cb89a69bd30c43c29dbb433ecc6ec7fb`) takes **~1 hour**
   because `lfcdemo-database.ipynb` waits for LFC gateway/ingestion pipelines to finish their
   initial full load. Once the setup job finishes it automatically triggers the downstream job.  
   The **downstream job** (`sdp-meta-lfc-demo-cb89a69bd30c43c29dbb433ecc6ec7fb-downstream`)
   runs `onboarding_job` → `bronze_dlt` → `silver_dlt` and takes **~10 min** depending on
   data volume. Monitor task-by-task progress (see poll loop below) — don't just wait for
   the whole job.  
   Once downstream succeeds, run the incremental to validate the fixed `(dt, lfc_end_at)` key:
   ```bash
   python demo/launch_lfc_demo.py --profile=e2demofe --run_id=cb89a69bd30c43c29dbb433ecc6ec7fb
   ```

6. **Adding `--snapshot_method=cdf` (Option B) — run `41a635c00c864a51bc27dd11ceb749c5`**

   Added a `--snapshot_method` CLI flag to `launch_lfc_demo.py` with two options:
   - `cdf` (default): custom `next_snapshot_and_version` lambda; O(1) version-check fast skip
   - `full`: original view-based `apply_changes_from_snapshot` (O(n) always)

   **Bug fixes encountered during testing:**

   a. `AttributeError: 'DataflowPipeline' object has no attribute 'applyChangesFromSnapshot'`  
      **Cause:** New `is_create_view()` logic accessed `self.applyChangesFromSnapshot` for all
      specs (`intpk` doesn't have this attribute).  
      **Fix:** Use `getattr(self, "applyChangesFromSnapshot", None)` in `is_create_view()`.

   b. `TABLE_OR_VIEW_NOT_FOUND None.robert_lee_sqlserver_42093c22e.dtix`  
      **Cause:** `dtix_next_snapshot_and_version` used `dataflowSpec.sourceDetails.get("source_catalog_prod")`
      but DLT-Meta onboarding maps `source_catalog_prod` → `sourceDetails["catalog"]`.
      The raw key `source_catalog_prod` no longer exists in the processed dataflowSpec.  
      **Fix:** Changed to `dataflowSpec.sourceDetails.get("catalog")` and build
      `catalog_prefix = f"{catalog}." if catalog else ""`.

   **Results:**
   - Initial run: `onboarding_job` SUCCESS, `bronze_dlt` SUCCESS, `silver_dlt` SUCCESS ✓
   - Incremental run: `trigger_ingestion_and_wait` SUCCESS, `bronze_dlt` SUCCESS ✓
   - `silver_dlt` on incremental FAILED with `DELTA_SOURCE_TABLE_IGNORE_CHANGES` on `intpk`
     (see Known Issues below) — this is a **pre-existing** streaming issue unrelated to
     `--snapshot_method`.

   **Key DLT-Meta source details key mapping** (important for any custom lambda):
   | `onboarding.json` key | `dataflowSpec.sourceDetails` key (after onboarding_job) |
   |----------------------|--------------------------------------------------------|
   | `source_catalog_prod` | `catalog` |
   | `source_database` | `source_database` |
   | `source_table` | `source_table` |
   | `snapshot_format` | `snapshot_format` |

---

### `DELTA_SOURCE_TABLE_IGNORE_CHANGES` on silver `intpk` — **FIXED**

**Error:** `[STREAM_FAILED] DELTA_SOURCE_TABLE_IGNORE_CHANGES: Detected a data update (MERGE) in source table at version N.`  
**When:** Silver pipeline incremental run reads from bronze `intpk` as a streaming source.  
**Cause:** Bronze `intpk` CDC (`apply_changes`) writes MERGE operations to the bronze Delta table. Delta streaming cannot read a table with non-additive writes (MERGE/UPDATE/DELETE) unless CDF or skipChangeCommits is configured.  
**Fix (implemented):** Add `silver_reader_options: {"readChangeFeed": "true"}` to the `intpk` onboarding entry. Silver reads the Change Data Feed from bronze instead of the raw table files. CDF handles MERGE-producing sources correctly. The silver CDC config must also be CDF-aware:
```python
silver_reader_options = {"readChangeFeed": "true"}
silver_cdc_apply_changes = {
    "keys": ["pk"],
    "sequence_by": "_commit_version",   # always use _commit_version with CDF
    "scd_type": "1",
    "apply_as_deletes": "_change_type = 'delete'",
    "except_column_list": ["_change_type", "_commit_version", "_commit_timestamp"],
}
```
**Files changed:** `demo/launch_lfc_demo.py` (`LFC_INTPK_SILVER_READER_OPTIONS`, `LFC_INTPK_SILVER_CDC_APPLY_CHANGES`) and `demo/lfcdemo-database.ipynb` cell 20.  
**Verified:** Full test cycle (run `65b21620b71e4e46b3622d1ed1c85246`) — initial downstream SUCCESS, incremental `trigger_ingestion_and_wait` + `bronze_dlt` + `silver_dlt` all SUCCESS.

---

### When to start fresh vs. attempting in-place repair

| Situation | Recommended action |
|-----------|-------------------|
| Pipeline failed; `onboarding.json` key change needed | Patch volume JSON + `full_refresh_selection` on the pipeline **if** it's idle; otherwise clean up and relaunch |
| Internal materialization has duplicate rows (corrupted state) | Always clean up and relaunch — `full_refresh_selection` with a conflicting active update is unreliable |
| Any error involving stale `dlt_meta_` prefix paths | Check ALL notebook files; fix and re-upload. Use incremental launcher (it re-uploads both `trigger_ingestion_and_wait.py` and `init_sdp_meta_pipeline.py` every time) |
| Fix is taking too long or blocked by `ResourceConflict` | `cleanup_lfc_demo.py` + fresh `launch_lfc_demo.py` — setup job ~1 hour, then downstream ~10 min |

### Timing guide for the LFC demo

| Phase | Approximate duration |
|-------|---------------------|
| `launch_lfc_demo.py` script itself (UC setup, uploads, job creation) | ~30 s |
| **Setup job** `sdp-meta-lfc-demo-{run_id}` — `lfc_setup` task (LFC pipelines + initial full load) | **~1 hour** |
| **Downstream job** `sdp-meta-lfc-demo-{run_id}-downstream` — `onboarding_job` → `bronze_dlt` → `silver_dlt` | **~10 min** (data-dependent) |
| Incremental run (LFC trigger + bronze + silver) | ~5–8 min |

**Do not wait for the setup job to finish before starting to monitor.** Poll each job's tasks
individually as they progress — the downstream job starts automatically as soon as the setup job
succeeds, so you can start watching for it well before the 1-hour mark.

**Wait for the downstream job to succeed before running incremental.** Its name is
`sdp-meta-lfc-demo-{run_id}-downstream`; its URL is printed by the launcher on the `Downstream:`
line. Check task-level status, not just the overall job state:

```python
import sys, os, time
sys.path.insert(0, os.path.join(os.getcwd(), "src"))
from integration_tests.run_integration_tests import get_workspace_api_client

ws = get_workspace_api_client("e2demofe")
DOWNSTREAM_JOB_ID = 808917810045282   # from launcher "Downstream:" line
RUN_ID = "cb89a69bd30c43c29dbb433ecc6ec7fb"

_start = time.time()
while True:
    elapsed = int(time.time() - _start)
    runs = list(ws.jobs.list_runs(job_id=DOWNSTREAM_JOB_ID, limit=1))
    if not runs:
        print(f"[{elapsed:>4}s] downstream not triggered yet"); time.sleep(60); continue
    run = runs[0]
    full = ws.jobs.get_run(run_id=run.run_id)
    lc = str(run.state.life_cycle_state)
    rr = str(run.state.result_state or "—")
    print(f"\n[{elapsed:>4}s] downstream run={run.run_id}  {lc}/{rr}")
    for t in (full.tasks or []):
        ts = t.state
        print(f"  {t.task_key:35s} {str(ts.life_cycle_state):25s} {str(ts.result_state or '—')}")
    if "TERMINATED" in lc:
        if "SUCCESS" in rr:
            print("\nDownstream SUCCEEDED — ready to run incremental.")
            print(f"  python demo/launch_lfc_demo.py --profile=e2demofe --run_id={RUN_ID}")
        else:
            print(f"\nDownstream FAILED: {rr} — check errors above.")
        break
    time.sleep(60)
```

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
| UC Schema | `{catalog}.sdp_meta_dataflowspecs_lfc_{run_id}` | Unity Catalog schema |
| UC Schema | `{catalog}.sdp_meta_bronze_lfc_{run_id}` | Unity Catalog schema |
| UC Schema | `{catalog}.sdp_meta_silver_lfc_{run_id}` | Unity Catalog schema |
| UC Volume | `{catalog}.sdp_meta_dataflowspecs_lfc_{run_id}.{catalog}_lfc_volume_{run_id}` | Managed volume |
| UC Tables | all tables inside the bronze/silver schemas | Delta tables created by DLT |
| DLT Pipeline | `sdp-meta-lfc-bronze-{run_id}` | Lakeflow Declarative Pipeline |
| DLT Pipeline | `sdp-meta-lfc-silver-{run_id}` | Lakeflow Declarative Pipeline |
| Job | `sdp-meta-lfc-demo-{run_id}` | Databricks job |
| Job | `sdp-meta-lfc-demo-incremental-{run_id}` | Databricks job (created on first incremental run) |
| Workspace notebooks | `/Users/{user}/sdp_meta_lfc_demo/{run_id}/` | Workspace directory |

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

For **step 1 (delete jobs)**, prefer reading `job_id` and `incremental_job_id` from workspace `conf/setup_metadata.json` (path: `/Users/{user}/sdp_meta_lfc_demo/{run_id}/conf/setup_metadata.json`) and calling `jobs.get(job_id=...)` then `jobs.delete(job_id=...)` — no list. Fall back to `jobs.list(name=..., limit=100)` only if the file is missing. Pipeline IDs for step 2 come from the setup job's task definitions — no slow `list_pipelines()` scan needed. LFC schemas contain **gateway staging volumes** and sometimes **streaming tables not visible via `ws.tables.list`** — always use `DROP SCHEMA ... CASCADE` via SQL to be safe.

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
for jname in [f"sdp-meta-lfc-demo-{RUN_ID}", f"sdp-meta-lfc-demo-incremental-{RUN_ID}"]:
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
    f"sdp_meta_dataflowspecs_lfc_{RUN_ID}",
    f"sdp_meta_bronze_lfc_{RUN_ID}",
    f"sdp_meta_silver_lfc_{RUN_ID}",
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
nb_path = f"/Users/{USERNAME}/sdp_meta_lfc_demo/{RUN_ID}"
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
