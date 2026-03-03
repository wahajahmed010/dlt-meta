"""
Launch the DLT-Meta Lakeflow Connect (LFC) Demo.

Setup run (first time):
  1. Uploads demo/lfcdemo-database.ipynb to the Databricks workspace.
  2. Creates and runs a job that:
       a. lfc_setup        — runs lfcdemo-database.ipynb, which creates the LFC gateway +
                             ingestion pipelines and starts DML against the source DB.
       b. onboarding_job   — registers intpk and dtix as delta (streaming table) sources
                             in the DLT-Meta dataflow spec schema.
       c. bronze_dlt       — DLT-Meta bronze pipeline reads the LFC streaming tables.
       d. silver_dlt       — DLT-Meta silver pipeline (pass-through transformations).

Incremental run (re-trigger after initial setup):
  python demo/launch_lfc_demo.py --profile=DEFAULT --run_id=<run_id_from_setup>
  Re-triggers bronze_dlt → silver_dlt against the latest LFC streaming-table state.
  (LFC ingestion pipeline continuously updates the streaming tables while it is running.)
"""

import io
import json
import traceback
import uuid
import webbrowser
from dataclasses import dataclass

from databricks.sdk.service import compute, jobs
from databricks.sdk.service.workspace import ImportFormat

from src.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    DLTMETARunner,
    DLTMetaRunnerConf,
    get_workspace_api_client,
    process_arguments,
)

LFC_TABLES = ["intpk", "dtix"]
# Demo: intpk = process insert/update/delete via CDC apply + change data feed; dtix = append-only
LFC_TABLE_BRONZE_READER_OPTIONS = {"intpk": {"readChangeFeed": "true"}, "dtix": {}}
# intpk: bronze_cdc_apply_changes (process CDC). Uses Delta CDF columns: _change_type, _commit_version.
# LFC streaming table must have delta.enableChangeDataFeed = true for intpk.
LFC_INTPK_BRONZE_CDC_APPLY_CHANGES = {
    "keys": ["pk"],
    "sequence_by": "_commit_version",
    "scd_type": "1",
    "apply_as_deletes": "_change_type = 'delete'",
    "except_column_list": ["_change_type", "_commit_version", "_commit_timestamp"],
}
# Silver merge by pk so intpk silver accepts insert/update/delete (one row per pk)
LFC_INTPK_SILVER_CDC_APPLY_CHANGES = {
    "keys": ["pk"],
    "sequence_by": "dt",
    "scd_type": "1",
}
LFC_DEFAULT_SCHEMA = "lfcddemo"
# Cap jobs.list() to avoid slow full-workspace iteration (API returns 25 per page)
JOBS_LIST_LIMIT = 100


@dataclass
class LFCRunnerConf(DLTMetaRunnerConf):
    """Configuration for the LFC demo runner."""
    lfc_schema: str = None          # source schema on the source DB (passed to notebook as source_schema)
    connection_name: str = None     # Databricks connection name for the source DB
    cdc_qbc: str = "cdc"            # LFC pipeline mode
    trigger_interval_min: str = "5" # LFC trigger interval in minutes
    sequence_by_pk: bool = False   # if True, use primary key for CDC silver sequence_by; else use dt
    lfc_notebook_ws_path: str = None  # resolved workspace path of the uploaded LFC notebook
    setup_job_id: int = None       # setup job id (set when resolving incremental; used to write metadata)


class DLTMETALFCDemo(DLTMETARunner):
    """Run the DLT-Meta Lakeflow Connect Demo."""

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    # ── helpers ──────────────────────────────────────────────────────────────

    def _is_incremental(self) -> bool:
        """True when --run_id is supplied, implying incremental (re-trigger) mode."""
        return bool(self.args.get("run_id"))

    # ── configuration ────────────────────────────────────────────────────────

    def init_runner_conf(self) -> LFCRunnerConf:
        """
        Build an LFCRunnerConf from CLI args.
        In incremental mode the existing setup job is inspected to fill in
        any missing fields (uc_catalog_name, lfc_schema, pipeline IDs).
        """
        run_id = self.args["run_id"] if self._is_incremental() else uuid.uuid4().hex
        lfc_schema = self.args.get("source_schema") or LFC_DEFAULT_SCHEMA

        runner_conf = LFCRunnerConf(
            run_id=run_id,
            username=self._my_username(self.ws),
            dlt_meta_schema=f"dlt_meta_dataflowspecs_lfc_{run_id}",
            bronze_schema=f"dlt_meta_bronze_lfc_{run_id}",
            silver_schema=f"dlt_meta_silver_lfc_{run_id}",
            runners_full_local_path="demo/notebooks/lfc_runners",
            runners_nb_path=f"/Users/{self._my_username(self.ws)}/dlt_meta_lfc_demo/{run_id}",
            int_tests_dir="demo",
            env="prod",
            lfc_schema=lfc_schema,
            connection_name=self.args.get("connection_name"),
            cdc_qbc=self.args.get("cdc_qbc") or "cdc",
            trigger_interval_min=str(self.args.get("trigger_interval_min") or "5"),
            sequence_by_pk=bool(self.args.get("sequence_by_pk")),
        )

        if self.args.get("uc_catalog_name"):
            runner_conf.uc_catalog_name = self.args["uc_catalog_name"]
            runner_conf.uc_volume_name = f"{runner_conf.uc_catalog_name}_lfc_volume_{run_id}"

        if self._is_incremental():
            self._resolve_incremental_conf(runner_conf)

        return runner_conf

    def _setup_metadata_path(self, runner_conf: LFCRunnerConf) -> str:
        """Workspace path for conf/setup_metadata.json (job_id, uc_catalog_name, incremental_job_id)."""
        return f"{runner_conf.runners_nb_path}/conf/setup_metadata.json"

    def _read_setup_metadata(self, runner_conf: LFCRunnerConf) -> dict | None:
        """Read setup_metadata.json from workspace; returns None if missing."""
        path = self._setup_metadata_path(runner_conf)
        try:
            with self.ws.workspace.download(path) as f:
                return json.load(f)
        except Exception:
            return None

    def _write_setup_metadata(self, runner_conf: LFCRunnerConf, data: dict):
        """Write setup_metadata.json to workspace (creates conf dir if needed)."""
        path = self._setup_metadata_path(runner_conf)
        self.ws.workspace.mkdirs(f"{runner_conf.runners_nb_path}/conf")
        self.ws.workspace.upload(
            path=path,
            content=io.BytesIO(json.dumps(data, indent=2).encode("utf-8")),
            format=ImportFormat.AUTO,
        )

    def _job_set_no_retry(self, job_id: int):
        """Set job-level max_retries=0 so the job is not retried on failure (SDK has no job-level field).
        Note: Marking a job/task as 'production' in the UI does not change retry behavior; only this setting does."""
        try:
            self.ws.api_client.do(
                "POST",
                "/api/2.1/jobs/update",
                body={"job_id": job_id, "new_settings": {"max_retries": 0}},
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to set job {job_id} to max_retries=0 (job may retry on failure): {e}"
            ) from e

    def _resolve_incremental_conf(self, runner_conf: LFCRunnerConf):
        """
        Populate uc_catalog_name (if not supplied), lfc_schema, and bronze/silver
        pipeline IDs by inspecting the existing LFC setup job. Prefer job_id from
        setup_metadata.json (fast); fall back to jobs.list by name (slow).
        """
        setup_job_name = f"dlt-meta-lfc-demo-{runner_conf.run_id}"
        print(f"Looking up setup job '{setup_job_name}'...")
        setup_job = None
        meta = self._read_setup_metadata(runner_conf)
        if meta and meta.get("job_id") is not None:
            try:
                job_details = self.ws.jobs.get(job_id=meta["job_id"])
                if job_details.settings.name == setup_job_name:
                    setup_job = job_details
                    print(f"  Found job_id={setup_job.job_id} (from setup_metadata.json)")
            except Exception:
                pass
        if not setup_job:
            setup_job = next(
                (
                    j
                    for j in self.ws.jobs.list(name=setup_job_name, limit=JOBS_LIST_LIMIT)
                    if j.settings.name == setup_job_name
                ),
                None,
            )
            if not setup_job:
                raise ValueError(
                    f"Setup job '{setup_job_name}' not found in first {JOBS_LIST_LIMIT} jobs. "
                    "Ensure the original setup run completed successfully."
                )
            print(f"  Found job_id={setup_job.job_id}")
            job_details = self.ws.jobs.get(job_id=setup_job.job_id)
        else:
            job_details = setup_job
        runner_conf.setup_job_id = job_details.job_id

        if not runner_conf.uc_catalog_name:
            # Derive uc_catalog_name from the onboarding_job task's "database" parameter
            onboarding_task = next(
                (t for t in job_details.settings.tasks if t.task_key == "onboarding_job"),
                None,
            )
            if onboarding_task and onboarding_task.python_wheel_task:
                database = onboarding_task.python_wheel_task.named_parameters.get("database", "")
                runner_conf.uc_catalog_name = database.split(".")[0]
            if not runner_conf.uc_catalog_name:
                raise ValueError(
                    "Could not derive uc_catalog_name from the existing job. "
                    "Please supply --uc_catalog_name explicitly."
                )
            runner_conf.uc_volume_name = (
                f"{runner_conf.uc_catalog_name}_lfc_volume_{runner_conf.run_id}"
            )
            print(f"  Derived uc_catalog_name={runner_conf.uc_catalog_name}")

        # Always (re-)derive uc_volume_path — not set by initialize_uc_resources in incr. mode
        runner_conf.uc_volume_path = (
            f"/Volumes/{runner_conf.uc_catalog_name}/"
            f"{runner_conf.dlt_meta_schema}/{runner_conf.uc_volume_name}/"
        )

        # Derive lfc_schema and trigger_interval_min from the lfc_setup task if not supplied
        lfc_task = next(
            (t for t in job_details.settings.tasks if t.task_key == "lfc_setup"),
            None,
        )
        if lfc_task and lfc_task.notebook_task and lfc_task.notebook_task.base_parameters:
            params = lfc_task.notebook_task.base_parameters
            if not runner_conf.lfc_schema or runner_conf.lfc_schema == LFC_DEFAULT_SCHEMA:
                runner_conf.lfc_schema = params.get("source_schema") or runner_conf.lfc_schema
            runner_conf.trigger_interval_min = (
                params.get("trigger_interval_min") or runner_conf.trigger_interval_min
            )
        print(f"  Derived lfc_schema={runner_conf.lfc_schema}")
        print(f"  Derived trigger_interval_min={runner_conf.trigger_interval_min}")

        # Extract pipeline IDs directly from job task definitions
        print("Extracting pipeline IDs from setup job tasks...")
        for t in job_details.settings.tasks:
            if t.task_key == "bronze_dlt" and t.pipeline_task:
                runner_conf.bronze_pipeline_id = t.pipeline_task.pipeline_id
            elif t.task_key == "silver_dlt" and t.pipeline_task:
                runner_conf.silver_pipeline_id = t.pipeline_task.pipeline_id

        if not runner_conf.bronze_pipeline_id or not runner_conf.silver_pipeline_id:
            raise ValueError(
                f"Could not find pipeline IDs in setup job tasks for run_id={runner_conf.run_id}. "
                "Ensure the setup run completed successfully."
            )
        print(f"  bronze_pipeline_id={runner_conf.bronze_pipeline_id}")
        print(f"  silver_pipeline_id={runner_conf.silver_pipeline_id}")

    # ── resource creation ────────────────────────────────────────────────────

    def _write_conf_files_to_volume(self, runner_conf: LFCRunnerConf):
        """
        Write onboarding.json, silver_transformations.json, and bronze_dqe.json
        directly to the UC Volume via the Files API.
        DLT-Meta is configured with source_format=delta, pointing at the two
        streaming tables created by lfcdemo-database.ipynb (intpk, dtix).
        Demo: intpk = process insert/update/delete (bronze_cdc_apply_changes + readChangeFeed); dtix = append-only.
        """
        vol = runner_conf.uc_volume_path.rstrip("/")
        onboarding = []
        for i, tbl in enumerate(LFC_TABLES):
            entry = {
                "data_flow_id": str(i + 1),
                "data_flow_group": "A1",
                "source_format": "delta",
                "source_details": {
                    "source_catalog_prod": runner_conf.uc_catalog_name,
                    "source_database": runner_conf.lfc_schema,
                    "source_table": tbl,
                },
                "bronze_database_prod": (
                    f"{runner_conf.uc_catalog_name}.{runner_conf.bronze_schema}"
                ),
                "bronze_table": tbl,
                "bronze_reader_options": LFC_TABLE_BRONZE_READER_OPTIONS.get(tbl, {}),
                "bronze_database_quarantine_prod": (
                    f"{runner_conf.uc_catalog_name}.{runner_conf.bronze_schema}"
                ),
                "bronze_quarantine_table": f"{tbl}_quarantine",
                "silver_database_prod": (
                    f"{runner_conf.uc_catalog_name}.{runner_conf.silver_schema}"
                ),
                "silver_table": tbl,
                "silver_transformation_json_prod": (
                    f"{vol}/conf/silver_transformations.json"
                ),
                "silver_data_quality_expectations_json_prod": (
                    f"{vol}/conf/dqe/silver_dqe.json"
                ),
            }
            if tbl == "intpk":
                entry["bronze_cdc_apply_changes"] = LFC_INTPK_BRONZE_CDC_APPLY_CHANGES
                entry["bronze_data_quality_expectations_json_prod"] = (
                    f"{vol}/conf/dqe/bronze_dqe.json"
                )
                silver_seq = "pk" if runner_conf.sequence_by_pk else "dt"
                entry["silver_cdc_apply_changes"] = {
                    **LFC_INTPK_SILVER_CDC_APPLY_CHANGES,
                    "sequence_by": silver_seq,
                }
                # silver DQE already set above; pipeline uses DQE-then-CDC path for intpk
            else:
                entry["bronze_data_quality_expectations_json_prod"] = (
                    f"{vol}/conf/dqe/bronze_dqe.json"
                )
            onboarding.append(entry)

        # Pass-through: select all columns as-is
        silver_transformations = [
            {"target_table": tbl, "select_exp": ["*"]} for tbl in LFC_TABLES
        ]

        bronze_dqe = {"expect": {"valid_row": "true"}}
        silver_dqe = {"expect": {"valid_row": "true"}}

        uploads = {
            f"{vol}/conf/onboarding.json": onboarding,
            f"{vol}/conf/silver_transformations.json": silver_transformations,
            f"{vol}/conf/dqe/bronze_dqe.json": bronze_dqe,
            f"{vol}/conf/dqe/silver_dqe.json": silver_dqe,
        }
        print("Uploading DLT-Meta configuration to UC Volume...")
        for path, content in uploads.items():
            data = json.dumps(content, indent=2).encode()
            self.ws.files.upload(file_path=path, contents=io.BytesIO(data), overwrite=True)
            print(f"  Uploaded {path}")

    def _upload_init_and_lfc_notebooks(self, runner_conf: LFCRunnerConf) -> str:
        """
        Upload init_dlt_meta_pipeline.py, wait_for_lfc_pipelines.py, and
        lfcdemo-database.ipynb to the Databricks workspace.
        Returns the workspace path of the uploaded LFC notebook (without extension).
        """
        from databricks.sdk.service.workspace import Language

        print(f"Uploading notebooks to {runner_conf.runners_nb_path}...")
        self.ws.workspace.mkdirs(f"{runner_conf.runners_nb_path}/runners")

        for nb_file in (
            "demo/notebooks/lfc_runners/init_dlt_meta_pipeline.py",
            "demo/notebooks/lfc_runners/trigger_ingestion_and_wait.py",
        ):
            nb_name = nb_file.split("/")[-1]
            with open(nb_file, "rb") as f:
                self.ws.workspace.upload(
                    path=f"{runner_conf.runners_nb_path}/runners/{nb_name}",
                    format=ImportFormat.SOURCE,
                    language=Language.PYTHON,
                    content=f.read(),
                    overwrite=True,
                )
            print(f"  Uploaded {nb_name}")

        # lfcdemo-database.ipynb — runs LFC setup
        lfc_nb_ws_path = f"{runner_conf.runners_nb_path}/lfcdemo-database"
        with open("demo/lfcdemo-database.ipynb", "rb") as f:
            self.ws.workspace.upload(
                path=lfc_nb_ws_path,
                format=ImportFormat.JUPYTER,
                content=f.read(),
                overwrite=True,
            )
        print(f"  Uploaded lfcdemo-database.ipynb to {lfc_nb_ws_path}")
        return lfc_nb_ws_path

    def _upload_trigger_ingestion_notebook(self, runner_conf: LFCRunnerConf):
        """Ensure trigger_ingestion_and_wait.py exists in the run's workspace (for incremental job)."""
        from databricks.sdk.service.workspace import Language
        path = f"{runner_conf.runners_nb_path}/runners/trigger_ingestion_and_wait.py"
        self.ws.workspace.mkdirs(f"{runner_conf.runners_nb_path}/runners")
        with open("demo/notebooks/lfc_runners/trigger_ingestion_and_wait.py", "rb") as f:
            self.ws.workspace.upload(
                path=path,
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                content=f.read(),
                overwrite=True,
            )
        print(f"  Uploaded trigger_ingestion_and_wait.py for incremental run.")

    def create_bronze_silver_dlt(self, runner_conf: LFCRunnerConf):
        runner_conf.bronze_pipeline_id = self.create_dlt_meta_pipeline(
            f"dlt-meta-lfc-bronze-{runner_conf.run_id}",
            "bronze",
            "A1",
            runner_conf.bronze_schema,
            runner_conf,
        )
        runner_conf.silver_pipeline_id = self.create_dlt_meta_pipeline(
            f"dlt-meta-lfc-silver-{runner_conf.run_id}",
            "silver",
            "A1",
            runner_conf.silver_schema,
            runner_conf,
        )

    # ── run orchestration ────────────────────────────────────────────────────

    def run(self, runner_conf: LFCRunnerConf):
        """
        Setup path  : initialize UC resources → upload conf/notebooks/wheel →
                      create DLT pipelines → create and trigger the main job.
        Incremental : re-trigger bronze_dlt → silver_dlt against the current
                      state of the LFC streaming tables.
        """
        try:
            if self._is_incremental():
                self._run_incremental(runner_conf)
            else:
                # 1. Create UC catalog schemas + volume
                self.initialize_uc_resources(runner_conf)
                # 2. Write onboarding/transformation/DQE JSON to the volume
                self._write_conf_files_to_volume(runner_conf)
                # 3. Upload notebooks and wheel
                runner_conf.lfc_notebook_ws_path = self._upload_init_and_lfc_notebooks(runner_conf)
                print("Python wheel upload starting...")
                runner_conf.remote_whl_path = (
                    f"{self.wsi._upload_wheel(uc_volume_path=runner_conf.uc_volume_path)}"
                )
                print(f"Python wheel upload to {runner_conf.remote_whl_path} completed!!!")
                # 4. Create bronze + silver DLT pipelines
                self.create_bronze_silver_dlt(runner_conf)
                # 5. Create and trigger the orchestration job
                self.launch_workflow(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()

    def _run_incremental(self, runner_conf: LFCRunnerConf):
        """
        Create (first time) or reuse the incremental job, then trigger it.
        First task: trigger LFC ingestion (jobs.run_now) and wait for pipeline update;
        then bronze_dlt → silver_dlt.
        """
        self._upload_trigger_ingestion_notebook(runner_conf)
        incr_job_name = f"dlt-meta-lfc-demo-incremental-{runner_conf.run_id}"
        existing_job = next(
            (
                j
                for j in self.ws.jobs.list(name=incr_job_name, limit=JOBS_LIST_LIMIT)
                if j.settings.name == incr_job_name
            ),
            None,
        )
        if existing_job:
            incr_job = existing_job
        else:
            incr_job = self._create_incremental_workflow(runner_conf)
            print(f"Incremental job created. job_id={incr_job.job_id}")

        self.ws.jobs.run_now(job_id=incr_job.job_id)
        url = (
            f"{self.ws.config.host}/jobs/{incr_job.job_id}"
            f"?o={self.ws.get_workspace_id()}"
        )
        webbrowser.open(url)
        print(f"Incremental run triggered. job_id={incr_job.job_id}, url={url}")

    def launch_workflow(self, runner_conf: LFCRunnerConf):
        created_job = self._create_lfc_demo_workflow(runner_conf)
        runner_conf.job_id = created_job.job_id
        self._write_setup_metadata(
            runner_conf,
            {"job_id": created_job.job_id, "uc_catalog_name": runner_conf.uc_catalog_name},
        )
        self.ws.jobs.run_now(job_id=created_job.job_id)

        oid = self.ws.get_workspace_id()
        vol_url = (
            f"{self.ws.config.host}/explore/data/volumes/"
            f"{runner_conf.uc_catalog_name}/{runner_conf.dlt_meta_schema}/{runner_conf.uc_volume_name}"
            f"?o={oid}"
        )
        ws_url = f"{self.ws.config.host}/#workspace/Workspace{runner_conf.runners_nb_path}"
        job_url = f"{self.ws.config.host}/jobs/{created_job.job_id}?o={oid}"
        webbrowser.open(job_url)

        profile = self.args.get("profile") or "DEFAULT"
        print(
            f"\n  Volume    : {vol_url}"
            f"\n  Workspace : {ws_url}"
            f"\n  Job       : {job_url}"
            f"\n\nSetup complete!"
            f"\n  run_id : {runner_conf.run_id}"
            f"\nTo re-trigger bronze/silver with the latest LFC data, run:"
            f"\n  python demo/launch_lfc_demo.py --profile={profile} --run_id={runner_conf.run_id}"
            f"\nTo clean up all resources for this run:"
            f"\n  python demo/cleanup_lfc_demo.py --profile={profile} --run_id={runner_conf.run_id}"
        )

    # ── job definitions ──────────────────────────────────────────────────────

    def _create_lfc_demo_workflow(self, runner_conf: LFCRunnerConf):
        """
        Create the main setup job:
          lfc_setup → onboarding_job → bronze_dlt → silver_dlt
        """
        dltmeta_environments = [
            jobs.JobEnvironment(
                environment_key="dl_meta_int_env",
                spec=compute.Environment(
                    client="1",
                    dependencies=[runner_conf.remote_whl_path],
                ),
            )
        ]

        # Do not retry on failure: avoid a 2nd run that would create a 2nd set of LFC pipelines.
        tasks = [
            jobs.Task(
                task_key="lfc_setup",
                description=(
                    "Run lfcdemo-database.ipynb: creates LFC gateway + ingestion pipelines, "
                    "starts DML against the source DB, then blocks until pipelines are RUNNING"
                ),
                max_retries=0,
                timeout_seconds=0,
                notebook_task=jobs.NotebookTask(
                    notebook_path=runner_conf.lfc_notebook_ws_path,
                    base_parameters={
                        "connection":           runner_conf.connection_name,
                        "cdc_qbc":              runner_conf.cdc_qbc,
                        "trigger_interval_min": runner_conf.trigger_interval_min,
                        "target_catalog":       runner_conf.uc_catalog_name,
                        "source_schema":        runner_conf.lfc_schema,
                        "run_id":               runner_conf.run_id,
                        "sequence_by_pk":       str(runner_conf.sequence_by_pk).lower(),
                    },
                ),
            ),
            jobs.Task(
                task_key="onboarding_job",
                description="Register LFC streaming tables as DLT-Meta delta sources",
                depends_on=[jobs.TaskDependency(task_key="lfc_setup")],
                environment_key="dl_meta_int_env",
                max_retries=0,
                timeout_seconds=0,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name="dlt_meta",
                    entry_point="run",
                    named_parameters={
                        "onboard_layer":             "bronze_silver",
                        "database":                  (
                            f"{runner_conf.uc_catalog_name}.{runner_conf.dlt_meta_schema}"
                        ),
                        "onboarding_file_path":      (
                            f"{runner_conf.uc_volume_path}conf/onboarding.json"
                        ),
                        "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                        "silver_dataflowspec_path":  (
                            f"{runner_conf.uc_volume_path}data/dlt_spec/silver"
                        ),
                        "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                        "bronze_dataflowspec_path":  (
                            f"{runner_conf.uc_volume_path}data/dlt_spec/bronze"
                        ),
                        "import_author":             "dlt-meta-lfc",
                        "version":                   "v1",
                        "overwrite":                 "True",
                        "env":                       runner_conf.env,
                        "uc_enabled":                "True",
                    },
                ),
            ),
            jobs.Task(
                task_key="bronze_dlt",
                depends_on=[jobs.TaskDependency(task_key="onboarding_job")],
                max_retries=0,
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                ),
            ),
            jobs.Task(
                task_key="silver_dlt",
                depends_on=[jobs.TaskDependency(task_key="bronze_dlt")],
                max_retries=0,
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.silver_pipeline_id
                ),
            ),
        ]

        created = self.ws.jobs.create(
            name=f"dlt-meta-lfc-demo-{runner_conf.run_id}",
            environments=dltmeta_environments,
            tasks=tasks,
        )
        self._job_set_no_retry(created.job_id)
        return created

    def _create_incremental_workflow(self, runner_conf: LFCRunnerConf):
        """
        Create incremental job: trigger_ingestion_and_wait → bronze_dlt → silver_dlt.
        Trigger task runs the LFC scheduler job once (jobs.run_now) and waits for the
        ingestion pipeline's latest update to complete before bronze/silver run.
        """
        trigger_nb_path = f"{runner_conf.runners_nb_path}/runners/trigger_ingestion_and_wait.py"
        tasks = [
            jobs.Task(
                task_key="trigger_ingestion_and_wait",
                description="Trigger LFC ingestion (jobs.run_now) and wait for pipeline update to complete",
                max_retries=0,
                notebook_task=jobs.NotebookTask(
                    notebook_path=trigger_nb_path,
                    base_parameters={
                        "run_id": runner_conf.run_id,
                        "target_catalog": runner_conf.uc_catalog_name,
                        "trigger_interval_min": runner_conf.trigger_interval_min,
                    },
                ),
            ),
            jobs.Task(
                task_key="bronze_dlt",
                depends_on=[jobs.TaskDependency(task_key="trigger_ingestion_and_wait")],
                max_retries=0,
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                ),
            ),
            jobs.Task(
                task_key="silver_dlt",
                depends_on=[jobs.TaskDependency(task_key="bronze_dlt")],
                max_retries=0,
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.silver_pipeline_id
                ),
            ),
        ]
        created = self.ws.jobs.create(
            name=f"dlt-meta-lfc-demo-incremental-{runner_conf.run_id}",
            tasks=tasks,
        )
        self._job_set_no_retry(created.job_id)
        return created


lfc_args_map = {
    "--profile":              "Databricks CLI profile name (default: DEFAULT)",
    "--uc_catalog_name":      "Unity Catalog name — required for setup, derived from job in incremental mode",
    "--source_schema":        "Source schema on the source database (default: lfcddemo)",
    "--connection_name":      "Databricks connection name for source DB (e.g. lfcddemo-azure-sqlserver)",
    "--cdc_qbc":              "LFC pipeline mode: cdc | qbc | cdc_single_pipeline (default: cdc)",
    "--trigger_interval_min": "LFC trigger interval in minutes — positive integer (default: 5)",
    "--sequence_by_pk":       "Use primary key for CDC silver sequence_by; default: use dt column",
    "--run_id":               "Existing run_id to re-trigger bronze/silver; implies incremental mode",
}

lfc_mandatory_args = ["uc_catalog_name", "connection_name"]


def main():
    args = process_arguments()
    if not args.get("run_id"):
        for required in lfc_mandatory_args:
            if not args.get(required):
                raise SystemExit(
                    f"Error: --{required} is required for a new setup run. "
                    f"(Pass --run_id to resume an existing run.)"
                )
    ws = get_workspace_api_client(args["profile"])
    runner = DLTMETALFCDemo(args, ws, "demo")
    print("initializing complete")
    runner_conf = runner.init_runner_conf()
    runner.run(runner_conf)


if __name__ == "__main__":
    main()
