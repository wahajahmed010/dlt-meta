"""End-to-end demo for the new sdp-meta DAB template features.

For each scenario (`cloudfiles`, `kafka`, `eventhub`, or `all`) this script
exercises every feature added in the DAB-template work:

  STAGE 1 - bundle-init        scaffold a bundle from the packaged template
  STAGE 2 - prepare-wheel      (--apply-prepare-wheel) build + upload sdp-meta
                               to a UC volume; otherwise pin a fake placeholder
                               to demonstrate the sentinel guard rails
  STAGE 3 - bundle-add-flow    bulk-append flows from
                               demo/dab_template_demo/flows/<scenario>_extra.csv
  STAGE 4 - recipe             run the rendered recipe that fits the source:
                                 cloudfiles -> recipes/from_volume.py (dry-run)
                                 kafka      -> recipes/from_topics.py
                                 eventhub   -> recipes/from_topics.py
                                 (the last two consume topic lists from
                                 demo/dab_template_demo/topics/)
  STAGE 5 - bundle-validate    run sdp-meta sanity checks + (when CLI present)
                               `databricks bundle validate`
  STAGE 6 - deploy + run       (--apply-deploy) actually deploy the bundle and
                               run onboarding + pipelines against the workspace

The first 5 stages need NO workspace access (great for dev loops). Only the
last stage talks to Databricks. Use ``--apply-prepare-wheel`` on its own to
upload a real wheel without deploying.

Usage:
    # Local exploration only - no workspace access needed.
    python demo/launch_dab_template_demo.py --scenario all \\
        --uc-catalog-name main --out-dir demo_runs

    # Build + upload wheel, validate (still no deploy).
    python demo/launch_dab_template_demo.py --scenario cloudfiles \\
        --uc-catalog-name main --apply-prepare-wheel \\
        --uc-schema sdp_meta_dab_demo_cf --uc-volume sdp_meta_wheels \\
        --profile DEFAULT

    # Full end-to-end: scaffold, append, recipe, prepare wheel, validate, deploy, run.
    python demo/launch_dab_template_demo.py --scenario kafka \\
        --uc-catalog-name main --apply-prepare-wheel --apply-deploy \\
        --uc-schema sdp_meta_dab_demo_kafka --uc-volume sdp_meta_wheels \\
        --profile DEFAULT
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_DIR = REPO_ROOT / "demo" / "dab_template_demo"

# Allow importing the in-tree sdp-meta package directly (avoids requiring an
# editable install just to run the demo).
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Importing from src after path patching, so flake8 E402 is expected/intended.
from databricks.labs.sdp_meta.bundle import (  # noqa: E402
    BundleAddFlowCommand,
    BundleInitCommand,
    BundlePrepareWheelCommand,
    BundleValidateCommand,
    _flows_from_csv,
    _sdp_meta_sanity_checks,
    bundle_add_flow,
    bundle_init,
    bundle_prepare_wheel,
    bundle_validate,
)


# ---------------------------------------------------------------------------
# Scenario registry
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Scenario:
    name: str
    answers_file: Path
    extra_flows_csv: Path
    recipe_name: str
    recipe_args_template: List[str]  # supports {bundle_dir} and {uc_catalog_name} substitution
    description: str
    # If True, the recipe is only attempted when --apply-recipe is set, because
    # it needs a real WorkspaceClient (no offline dry-run path). Today this
    # only applies to the `delta` scenario (recipes/from_uc.py lists UC tables).
    recipe_requires_workspace: bool = False


SCENARIOS = {
    "cloudfiles": Scenario(
        name="cloudfiles",
        answers_file=DEMO_DIR / "answers" / "cloudfiles_split.json",
        extra_flows_csv=DEMO_DIR / "flows" / "cloudfiles_extra.csv",
        recipe_name="from_volume.py",
        # Run from a fake local directory tree so the demo works without UC.
        recipe_args_template=[
            "--volume-path", "{bundle_dir}/_demo_landing",
            "--bundle-dir", "{bundle_dir}",
        ],
        description=(
            "Split bronze+silver, cloudFiles autoloader, YAML onboarding."
            " Demonstrates pipeline_mode=split and the from_volume recipe."
        ),
    ),
    "cloudfiles_combined": Scenario(
        name="cloudfiles_combined",
        # Same demo data + recipe as `cloudfiles`, but the bundle is rendered
        # with `pipeline_mode=combined` so bronze + silver run inside ONE
        # Lakeflow Declarative Pipeline. Use this when you want to demo the
        # combined topology against real cloud-storage data (cloudfiles) and
        # not against placeholder Event Hubs / Kafka brokers.
        answers_file=DEMO_DIR / "answers" / "cloudfiles_combined.json",
        extra_flows_csv=DEMO_DIR / "flows" / "cloudfiles_extra.csv",
        recipe_name="from_volume.py",
        recipe_args_template=[
            "--volume-path", "{bundle_dir}/_demo_landing",
            "--bundle-dir", "{bundle_dir}",
        ],
        description=(
            "Combined bronze_silver in ONE Lakeflow Declarative Pipeline,"
            " cloudFiles autoloader, YAML onboarding. Same demo data as"
            " `cloudfiles`; only `pipeline_mode=combined` differs."
        ),
    ),
    "kafka": Scenario(
        name="kafka",
        answers_file=DEMO_DIR / "answers" / "kafka_bronze.json",
        extra_flows_csv=DEMO_DIR / "flows" / "kafka_extra.csv",
        recipe_name="from_topics.py",
        recipe_args_template=[
            "--source-format", "kafka",
            "--bootstrap-servers", "broker1.example.com:9092,broker2.example.com:9092",
            "--topics-file", str(DEMO_DIR / "topics" / "kafka_topics.txt"),
            "--bundle-dir", "{bundle_dir}",
        ],
        description=(
            "Bronze-only, Kafka, JSON onboarding. Demonstrates layer=bronze,"
            " bundle-add-flow CSV mode, and the from_topics recipe."
        ),
    ),
    "eventhub": Scenario(
        name="eventhub",
        answers_file=DEMO_DIR / "answers" / "eventhub_combined.json",
        extra_flows_csv=DEMO_DIR / "flows" / "eventhub_extra.csv",
        recipe_name="from_topics.py",
        recipe_args_template=[
            "--source-format", "eventhub",
            "--bootstrap-servers", "my-eh-namespace.servicebus.windows.net:9093",
            "--topics-file", str(DEMO_DIR / "topics" / "eventhub_topics.txt"),
            "--bundle-dir", "{bundle_dir}",
        ],
        description=(
            "Combined bronze_silver in ONE Lakeflow Declarative Pipeline,"
            " Event Hubs source. Demonstrates pipeline_mode=combined and the"
            " same from_topics recipe used for Event Hubs."
        ),
    ),
    "delta": Scenario(
        name="delta",
        answers_file=DEMO_DIR / "answers" / "delta_split.json",
        extra_flows_csv=DEMO_DIR / "flows" / "delta_extra.csv",
        recipe_name="from_uc.py",
        # from_uc.py mirrors every Delta table under <catalog>.<schema> into
        # the bundle's onboarding file. We point at a `staging` schema by
        # convention; override with --uc-source-schema if yours is named
        # differently. Only runs with --apply-recipe (needs a workspace).
        recipe_args_template=[
            "--source-catalog", "{uc_catalog_name}",
            "--source-schema", "staging",
            "--bundle-dir", "{bundle_dir}",
        ],
        description=(
            "Split bronze+silver, source_format=delta (upstream Delta tables)."
            " Demonstrates the from_uc recipe that mirrors every table under"
            " a UC schema. STAGE 4 needs --apply-recipe + --profile (workspace)."
        ),
        recipe_requires_workspace=True,
    ),
}

# Scenarios that share the cloudFiles demo data set + `from_volume.py`
# recipe. The launcher's data-seeding (UC volume upload + local
# `_demo_landing` tree for the recipe) and CSV `{demo_data_volume_path}`
# substitution all gate on this set so adding a new cloudFiles topology
# (eg. `cloudfiles_combined`) only requires registering the scenario above.
_CLOUDFILES_SCENARIO_NAMES = {"cloudfiles", "cloudfiles_combined"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _banner(stage: str, message: str) -> None:
    bar = "=" * 78
    print(f"\n{bar}\n{stage}: {message}\n{bar}", flush=True)


def _materialize_answers_file(scenario: Scenario, uc_catalog_name: str, dest_dir: Path) -> Path:
    """Substitute the {uc_catalog_name} placeholder and write to dest_dir."""
    if not scenario.answers_file.is_file():
        raise SystemExit(f"Answer file not found: {scenario.answers_file}")
    raw = scenario.answers_file.read_text()
    rendered_text = raw.replace("{uc_catalog_name}", uc_catalog_name)
    rendered = json.loads(rendered_text)
    rendered.pop("_comment", None)
    out = dest_dir / f"answers_{scenario.name}.json"
    out.write_text(json.dumps(rendered, indent=2))
    return out


def _materialize_csv(scenario: Scenario, uc_catalog_name: str, dest_dir: Path,
                     *, demo_data_volume_path: Optional[str] = None) -> Path:
    """Substitute placeholders in the flow CSV.

    Recognized placeholders:
      - ``{uc_catalog_name}`` -> ``--uc-catalog-name``.
      - ``{demo_data_volume_path}`` -> the UC-volume base path where this
        launcher uploaded the seed datasets (cloudfiles scenario only). When
        not provided (offline / no --apply-prepare-wheel), we fall back to a
        clearly fake placeholder under the same volume name so users can see
        the intended shape without the run blowing up.
    """
    raw = scenario.extra_flows_csv.read_text()
    rendered = raw.replace("{uc_catalog_name}", uc_catalog_name)
    if "{demo_data_volume_path}" in rendered:
        if demo_data_volume_path:
            rendered = rendered.replace("{demo_data_volume_path}", demo_data_volume_path)
        else:
            # Offline placeholder. Keeps the CSV parseable; the LDP pipeline
            # would obviously fail to read these paths if you tried to deploy
            # without --apply-prepare-wheel, but bundle-validate is happy.
            rendered = rendered.replace(
                "{demo_data_volume_path}",
                f"/Volumes/{uc_catalog_name}/__placeholder__/__placeholder__/demo_data",
            )
    out = dest_dir / scenario.extra_flows_csv.name
    out.write_text(rendered)
    return out


def _set_dependency_default(bundle_dir: Path, dependency: str) -> None:
    """Replace `__SET_ME__` placeholder in resources/variables.yml."""
    var_path = bundle_dir / "resources" / "variables.yml"
    doc = yaml.safe_load(var_path.read_text())
    doc["variables"]["sdp_meta_dependency"]["default"] = dependency
    var_path.write_text(yaml.safe_dump(doc, sort_keys=False))
    print(f"[STAGE 2] Pinned sdp_meta_dependency in {var_path}\n          -> {dependency}")


# Each entry maps a recipe-discovered subdir name to a shipped dataset that
# the launcher copies in to seed it. Picking real files (instead of empty
# dirs) means the recipe-generated flows have actual data once the launcher
# uploads the seeded landing tree to the UC volume in STAGE 6.
_DEMO_LANDING_SEEDS = {
    "customers_streaming": REPO_ROOT / "demo" / "resources" / "data" / "customers",
    "orders_streaming": REPO_ROOT / "demo" / "resources" / "data" / "transactions",
    "events_streaming": REPO_ROOT / "demo" / "resources" / "data" / "products",
}


def _seed_demo_landing(bundle_dir: Path) -> None:
    """Create a landing tree under <bundle_dir>/_demo_landing for the
    `recipes/from_volume.py` recipe to discover.

    Each subdir is populated with a copy of one of the shipped demo CSVs so
    the recipe-generated flows have REAL data behind them once the launcher
    uploads the tree to the UC volume during STAGE 6 (and rewrites the
    rendered onboarding.yml's local source_path_dev to the volume path).
    """
    landing = bundle_dir / "_demo_landing"
    landing.mkdir(exist_ok=True)
    for name, src_dir in _DEMO_LANDING_SEEDS.items():
        dst = landing / name
        dst.mkdir(exist_ok=True)
        if not src_dir.is_dir():
            continue
        for src_file in src_dir.iterdir():
            if not src_file.is_file():
                continue
            target = dst / src_file.name
            if not target.is_file():
                shutil.copyfile(src_file, target)
    print(f"[STAGE 4 prep] Seeded landing tree at {landing} (copied real CSVs from demo/resources/data)")


def _bundle_dir_from_scaffold(scenario: Scenario, out_dir: Path, uc_catalog_name: str) -> Path:
    """Return the path the template renders into for a given scenario."""
    if not scenario.answers_file.is_file():
        raise SystemExit(f"Answer file not found: {scenario.answers_file}")
    answers = json.loads(
        scenario.answers_file.read_text().replace("{uc_catalog_name}", uc_catalog_name)
    )
    return out_dir / answers["bundle_name"]


def _run_recipe(scenario: Scenario, bundle_dir: Path, *, apply: bool,
                uc_catalog_name: str, profile: Optional[str]) -> int:
    """Invoke the rendered Python recipe inside the bundle as a subprocess."""
    recipe = bundle_dir / "recipes" / scenario.recipe_name
    if not recipe.is_file():
        print(f"[STAGE 4] WARNING: recipe {recipe} not found; skipping.")
        return 0

    args = [
        a.replace("{bundle_dir}", str(bundle_dir)).replace("{uc_catalog_name}", uc_catalog_name)
        for a in scenario.recipe_args_template
    ]
    if apply:
        args.append("--apply")
    if profile and "--profile" not in args:
        args.extend(["--profile", profile])

    cmd = [sys.executable, str(recipe), *args]
    print("[STAGE 4] $ " + " ".join(cmd))
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC) + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(cmd, env=env)
    return result.returncode


def _print_onboarding_summary(bundle_dir: Path) -> None:
    """Pretty-print the onboarding file's per-flow IDs and source formats."""
    for ext in ("yml", "json"):
        onboarding = bundle_dir / "conf" / f"onboarding.{ext}"
        if not onboarding.is_file():
            continue
        text = onboarding.read_text()
        flows = yaml.safe_load(text) if ext == "yml" else json.loads(text)
        if not isinstance(flows, list):
            continue
        print(f"\n[STAGE 5 summary] conf/onboarding.{ext} -> {len(flows)} flow(s)")
        for f in flows:
            print(
                f"  - data_flow_id={f.get('data_flow_id'):>5}  "
                f"source_format={f.get('source_format'):<11}  "
                f"bronze_table={f.get('bronze_table', '-')}"
            )
        return


# ---------------------------------------------------------------------------
# Stage runners
# ---------------------------------------------------------------------------

def stage_bundle_init(scenario: Scenario, out_dir: Path, uc_catalog_name: str,
                      profile: Optional[str], *, clean: bool = True) -> Path:
    _banner("STAGE 1", f"databricks bundle init  (scenario={scenario.name})")
    if not shutil.which("databricks"):
        raise SystemExit(
            "The `databricks` CLI is not on PATH. Install it before running "
            "this demo: https://docs.databricks.com/dev-tools/cli/install.html"
        )

    # `databricks bundle init` refuses to overwrite an existing scaffold, so
    # the demo is non-idempotent by default. Re-running after a previous
    # attempt would error with "one or more files already exist". Wipe the
    # target directory first unless --no-clean was passed.
    bundle_dir = _bundle_dir_from_scaffold(scenario, out_dir, uc_catalog_name)
    if bundle_dir.exists():
        if clean:
            print(f"[STAGE 1] Removing existing scaffold at {bundle_dir} (use --no-clean to keep)")
            shutil.rmtree(bundle_dir)
        else:
            raise SystemExit(
                f"Bundle directory already exists at {bundle_dir}. "
                "Re-run without --no-clean (or delete it manually) to re-scaffold."
            )

    with tempfile.TemporaryDirectory(prefix="dab_demo_") as tmp:
        tmp = Path(tmp)
        answers = _materialize_answers_file(scenario, uc_catalog_name, tmp)
        rc = bundle_init(BundleInitCommand(
            output_dir=str(out_dir),
            config_file=str(answers),
            profile=profile,
        ))
    if rc != 0:
        raise SystemExit(f"bundle init failed with exit code {rc}")
    if not bundle_dir.is_dir():
        raise SystemExit(f"Expected scaffolded bundle at {bundle_dir}, but it does not exist.")
    print(f"\n[STAGE 1] Bundle scaffolded at {bundle_dir}")
    return bundle_dir


# Datasets shipped under demo/resources/data/ that we re-use for the
# cloudfiles scenario so flows point at REAL files instead of placeholder
# UC paths. Keep the keys aligned with rows in flows/cloudfiles_extra.csv.
_CLOUDFILES_DEMO_DATASETS = {
    "customers": REPO_ROOT / "demo" / "resources" / "data" / "customers",
    "transactions": REPO_ROOT / "demo" / "resources" / "data" / "transactions",
    "products": REPO_ROOT / "demo" / "resources" / "data" / "products",
    "stores": REPO_ROOT / "demo" / "resources" / "data" / "stores",
}


def _upload_cloudfiles_demo_data(uc_catalog: str, uc_schema: str, uc_volume: str,
                                 profile: Optional[str]) -> str:
    """Upload the four shipped CSV datasets into the user's UC volume.

    Returns the base path (``/Volumes/<cat>/<sch>/<vol>/demo_data``) that
    `_materialize_csv` substitutes into ``{demo_data_volume_path}``.

    The upload is idempotent (overwrite=True) so re-runs are cheap. Reusing
    the same volume that holds the wheel keeps the demo to a single UC
    permission requirement.
    """
    from databricks.sdk import WorkspaceClient  # local import: keeps unit tests light

    base = f"/Volumes/{uc_catalog}/{uc_schema}/{uc_volume}/demo_data"
    ws = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    print(f"[STAGE 2] Uploading cloudFiles demo datasets into {base}/ ...")
    for table, src_dir in _CLOUDFILES_DEMO_DATASETS.items():
        if not src_dir.is_dir():
            print(f"          - SKIP {table}: source dir {src_dir} not found")
            continue
        for src_file in sorted(src_dir.iterdir()):
            if not src_file.is_file():
                continue
            dst = f"{base}/{table}/{src_file.name}"
            with src_file.open("rb") as fh:
                ws.files.upload(file_path=dst, contents=fh, overwrite=True)
            print(f"          - {src_file.name} -> {dst}")
    return base


def stage_prepare_wheel(scenario: Scenario, bundle_dir: Path, *,
                        apply: bool, uc_catalog_name: str,
                        uc_schema: Optional[str], uc_volume: Optional[str],
                        profile: Optional[str],
                        pip_index_url: Optional[str] = None,
                        pip_extra_index_urls: Optional[List[str]] = None,
                        create_if_missing: bool = True) -> Optional[str]:
    """Returns the demo_data UC-volume base path (cloudfiles only, when
    --apply-prepare-wheel is set), otherwise None."""
    _banner("STAGE 2", "bundle prepare-wheel  (build + upload sdp-meta wheel)")
    if not apply:
        # Pin a fake placeholder that LOOKS like a UC volume path. The
        # bundle-validate guard rails accept this shape; the runner notebook
        # will refuse it at runtime if it sees the literal `__SET_ME__` -- which
        # is the point of this stage in --no-apply mode.
        placeholder = (
            f"/Volumes/{uc_catalog_name}/sdp_meta_dab_demo/sdp_meta_wheels/"
            "databricks_labs_sdp_meta-0.0.0-py3-none-any.whl"
        )
        print("[STAGE 2] --apply-prepare-wheel was NOT set; pinning a placeholder path.")
        print("          (sanity checks accept the shape; runtime would fail-fast if used.)")
        _set_dependency_default(bundle_dir, placeholder)
        return None

    if not (uc_schema and uc_volume):
        raise SystemExit("--apply-prepare-wheel requires --uc-schema and --uc-volume")

    volume_path = bundle_prepare_wheel(BundlePrepareWheelCommand(
        uc_catalog=uc_catalog_name,
        uc_schema=uc_schema,
        uc_volume=uc_volume,
        profile=profile,
        pip_index_url=pip_index_url,
        pip_extra_index_urls=pip_extra_index_urls,
        create_if_missing=create_if_missing,
    ))
    _set_dependency_default(bundle_dir, volume_path)

    # For the cloudfiles scenarios (split + combined), additionally seed the
    # four demo datasets into the same UC volume so the CSV flows point at
    # real data. Both scenarios share the same demo data and recipe; only
    # the rendered LDP topology differs.
    if scenario.name in _CLOUDFILES_SCENARIO_NAMES:
        return _upload_cloudfiles_demo_data(uc_catalog_name, uc_schema, uc_volume, profile)
    return None


def stage_add_flow(scenario: Scenario, bundle_dir: Path, uc_catalog_name: str,
                   demo_data_volume_path: Optional[str] = None) -> None:
    _banner("STAGE 3", f"bundle-add-flow --from-csv  ({scenario.extra_flows_csv.name})")
    with tempfile.TemporaryDirectory(prefix="dab_demo_csv_") as tmp:
        csv_path = _materialize_csv(
            scenario, uc_catalog_name, Path(tmp),
            demo_data_volume_path=demo_data_volume_path,
        )
        flows = _flows_from_csv(csv_path)
    rc = bundle_add_flow(BundleAddFlowCommand(
        bundle_dir=str(bundle_dir),
        flows=flows,
        dry_run=False,
    ))
    if rc != 0:
        raise SystemExit(f"bundle-add-flow failed with exit code {rc}")


def stage_recipe(scenario: Scenario, bundle_dir: Path, *, apply_recipe: bool,
                 uc_catalog_name: str, profile: Optional[str]) -> None:
    _banner("STAGE 4", f"recipes/{scenario.recipe_name}  ({'apply' if apply_recipe else 'dry-run'})")
    if scenario.recipe_requires_workspace and not apply_recipe:
        print(
            f"[STAGE 4] recipe {scenario.recipe_name} requires a live workspace "
            "(no offline dry-run path). Skipping. Re-run with --apply-recipe "
            "and --profile to exercise it."
        )
        return
    if scenario.name in _CLOUDFILES_SCENARIO_NAMES:
        _seed_demo_landing(bundle_dir)
    rc = _run_recipe(scenario, bundle_dir, apply=apply_recipe,
                     uc_catalog_name=uc_catalog_name, profile=profile)
    if rc != 0:
        # Recipe dry-run prints the proposed plan with rc=0; non-zero means a
        # real failure (eg. duplicate id, validation error).
        raise SystemExit(f"recipe {scenario.recipe_name} failed with exit code {rc}")


def stage_validate(bundle_dir: Path, profile: Optional[str]) -> None:
    _banner("STAGE 5", "bundle-validate  (sdp-meta sanity checks + databricks validate)")
    errors = _sdp_meta_sanity_checks(bundle_dir)
    if errors:
        print("[STAGE 5] sdp-meta sanity checks reported issues:")
        for e in errors:
            print(f"  - {e}")
    else:
        print("[STAGE 5] sdp-meta sanity checks: clean")
    rc = bundle_validate(BundleValidateCommand(
        bundle_dir=str(bundle_dir),
        profile=profile,
    ))
    if rc != 0:
        raise SystemExit(f"bundle-validate exited with code {rc}")
    _print_onboarding_summary(bundle_dir)


# Per-flow keys that store a path to another conf file. The launcher rewrites
# ``${workspace.file_path}/conf/...`` -> ``<uc_volume_conf_base>/...`` for
# each one, AND drops the key entirely when the referenced file is missing
# on disk (so the pipeline doesn't crash trying to read a non-existent DDL).
# These keys live both at the top of each flow and (for source_schema_path)
# nested under ``source_details``.
_PATH_KEYS_TOP_LEVEL = (
    "bronze_data_quality_expectations_json_dev",
    "silver_data_quality_expectations_json_dev",
    "silver_transformation_json_dev",
    "bronze_transformation_json_dev",
    "bronze_table_path_dev",
    "silver_table_path_dev",
)
_PATH_KEYS_SOURCE_DETAILS = (
    "source_schema_path",
)


def _rewrite_and_prune_flow_paths(flow: dict, conf_root: Path,
                                  workspace_conf_token: str,
                                  volume_conf_base: str) -> None:
    """Substitute workspace.file_path tokens with the UC-volume base, and drop
    path fields whose local file is missing.

    Mutates ``flow`` in place. Empty-string values (template defaults like
    ``bronze_table_path_dev: ''``) are left as-is — only fields that actually
    point at ``${workspace.file_path}/conf/...`` are touched.
    """
    def _maybe_rewrite(container: dict, key: str) -> None:
        val = container.get(key)
        if not isinstance(val, str) or not val.startswith(workspace_conf_token):
            return
        # Resolve the local path the value WOULD point at if the file existed.
        rel = val[len(workspace_conf_token):].lstrip("/")
        local = conf_root / rel
        if not local.is_file():
            # Reference is dangling locally -> the pipeline would fail to
            # read it. Drop the key so the engine falls back to defaults
            # (e.g. cloudFiles.inferColumnTypes for a missing source_schema_path).
            del container[key]
            return
        container[key] = f"{volume_conf_base}/{rel}"

    for k in _PATH_KEYS_TOP_LEVEL:
        _maybe_rewrite(flow, k)
    src_details = flow.get("source_details")
    if isinstance(src_details, dict):
        for k in _PATH_KEYS_SOURCE_DETAILS:
            _maybe_rewrite(src_details, k)

    # When DQE is kept, the engine accesses `bronze_quarantine_table` (and
    # similar) from the Spark Row unconditionally, raising
    # `PySparkValueError: bronze_quarantine_table` if the column is missing
    # from the inferred schema. Seed empty defaults so the column shows up
    # in the schema and the runtime check (`if row["..."]:`) is falsy.
    if flow.get("bronze_data_quality_expectations_json_dev"):
        flow.setdefault("bronze_database_quarantine_dev",
                        flow.get("bronze_database_dev", ""))
        flow.setdefault("bronze_quarantine_table", "")
        flow.setdefault("bronze_quarantine_table_path_dev", "")
        flow.setdefault("bronze_quarantine_table_partitions", "")
        flow.setdefault("bronze_quarantine_table_properties", {})
        flow.setdefault("bronze_quarantine_table_cluster_by", [])


def _materialize_local_source_paths_to_volume(flow: dict, bundle_dir: Path,
                                              ws, volume_data_base: str) -> Optional[bool]:
    """Detect a flow whose ``source_path_dev`` is a LOCAL filesystem path,
    upload its files to the UC volume, and rewrite the path. Also flips
    ``cloudFiles.format`` to match the file extension we just uploaded.

    Returns:
        True  -> rewrite succeeded; flow now points at a real UC volume path.
        False -> flow's source_path_dev is local but the dir is empty/missing.
                 Caller should drop the flow (no real data behind it).
        None  -> flow does not have a local source_path_dev (already a
                 ``/Volumes/...`` path or no source_path_dev at all). Caller
                 should leave the flow alone.

    Mirrors the pattern used by ``integration_tests/run_integration_tests.py``
    which uploads ``demo/resources/data/...`` into a UC volume so DLT
    pipelines can stream from a path that actually exists.
    """
    src_details = flow.get("source_details")
    if not isinstance(src_details, dict):
        return None
    raw = src_details.get("source_path_dev")
    if not isinstance(raw, str) or not raw:
        return None
    # Already a UC volume path: leave the rewrite to the existence-check below.
    if raw.startswith("/Volumes/"):
        return None

    local = Path(raw)
    if not local.is_absolute():
        local = bundle_dir / raw
    if not local.is_dir():
        return False
    files = sorted([p for p in local.iterdir() if p.is_file()])
    if not files:
        return False

    table_name = src_details.get("source_table") or local.name
    dst_base = f"{volume_data_base}/{table_name}"
    for src_file in files:
        with src_file.open("rb") as fh:
            ws.files.upload(file_path=f"{dst_base}/{src_file.name}",
                            contents=fh, overwrite=True)
    src_details["source_path_dev"] = f"{dst_base}/"

    # Make cloudFiles.format match the data we just uploaded so the pipeline
    # actually parses the rows. Defaults to json from the engine; flip to csv
    # / parquet / json based on the dominant suffix.
    suffix_counts: Dict[str, int] = {}
    for src_file in files:
        suffix_counts[src_file.suffix.lower()] = suffix_counts.get(src_file.suffix.lower(), 0) + 1
    dominant_suffix = max(suffix_counts, key=suffix_counts.get) if suffix_counts else ""
    fmt_by_suffix = {".csv": "csv", ".json": "json", ".parquet": "parquet", ".avro": "avro"}
    new_format = fmt_by_suffix.get(dominant_suffix)
    if new_format and isinstance(flow.get("bronze_reader_options"), dict):
        flow["bronze_reader_options"]["cloudFiles.format"] = new_format

    return True


def _uc_volume_path_exists(ws, path: str) -> bool:
    """Best-effort existence check for a UC volume directory. We only use
    this to drop flows pointing at the seeded TEMPLATE placeholder
    (``/Volumes/<cat>/landing/files/example_table/``) before runtime so the
    pipeline doesn't fail on a flow the user never wired up."""
    try:
        ws.files.get_directory_metadata(path)
        return True
    except Exception:
        try:
            # Some SDK versions expose listing instead of metadata.
            list(ws.files.list_directory_contents(path))
            return True
        except Exception:
            return False


def _stage_conf_to_uc_volume(bundle_dir: Path, uc_catalog: str, uc_schema: str,
                             uc_volume: str, profile: Optional[str]) -> str:
    """Upload bundle's conf/* tree to a UC volume and rewrite workspace refs.

    Why: the seeded onboarding job's `python_wheel_task` passes
    ``onboarding_file_path: ${workspace.file_path}/conf/onboarding.yml`` which
    DAB expands to ``/Workspace/Users/.../files/conf/onboarding.yml``. On
    serverless compute, Spark's text/json reader treats that as
    ``dbfs:/Workspace/...`` and fails with PATH_NOT_FOUND. Mirroring the
    pattern used by ``integration_tests/run_integration_tests.py``: stage the
    conf files into a UC volume and pass the volume path instead.

    For onboarding files (yml/json) we ALSO:
      * Rewrite ``${workspace.file_path}/conf/...`` -> ``<uc_volume_conf>/...``
        for every per-flow path key (DQE, silver_transformation, source
        schema DDL, ...), so the pipeline can resolve them at runtime.
      * Drop path keys whose local file does not exist, so the engine falls
        back to its defaults instead of erroring on a dangling reference
        (typical for the placeholder ``conf/schemas/example_table.ddl`` and
        per-table DQE files that recipes / bundle-add-flow do not emit).

    Returns the UC-volume base for conf, e.g.
    ``/Volumes/<cat>/<sch>/<vol>/conf``.
    """
    import io

    from databricks.sdk import WorkspaceClient

    conf_root = bundle_dir / "conf"
    if not conf_root.is_dir():
        raise SystemExit(f"Expected {conf_root} to exist after bundle init.")

    volume_conf_base = f"/Volumes/{uc_catalog}/{uc_schema}/{uc_volume}/conf"
    volume_data_base = f"/Volumes/{uc_catalog}/{uc_schema}/{uc_volume}/demo_data"
    workspace_conf_token = "${workspace.file_path}/conf"

    ws = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    print(f"[STAGE 6] Staging conf/ -> {volume_conf_base}/  (Spark-readable on serverless)")
    uploaded = 0
    pruned = 0
    repointed = 0
    dropped = 0
    for src_path in sorted(conf_root.rglob("*")):
        if not src_path.is_file():
            continue
        rel = src_path.relative_to(conf_root).as_posix()
        dst = f"{volume_conf_base}/{rel}"

        # Special-case the onboarding file: parse it, rewrite per-flow path
        # references, prune dangling ones, push local source data to UC, and
        # drop flows whose UC volume source path doesn't exist.
        is_onboarding = src_path.name.startswith("onboarding.") and src_path.parent == conf_root
        if is_onboarding and src_path.suffix.lower() in (".yml", ".yaml", ".json"):
            if src_path.suffix.lower() == ".json":
                doc = json.loads(src_path.read_text())
            else:
                doc = yaml.safe_load(src_path.read_text())
            if isinstance(doc, list):
                before = sum(_count_path_keys(f) for f in doc)
                kept: list = []
                for flow in doc:
                    _rewrite_and_prune_flow_paths(
                        flow, conf_root, workspace_conf_token, volume_conf_base,
                    )
                    # 1) Local source_path_dev -> upload + rewrite to UC.
                    local_status = _materialize_local_source_paths_to_volume(
                        flow, bundle_dir, ws, volume_data_base,
                    )
                    if local_status is True:
                        repointed += 1
                    elif local_status is False:
                        dropped += 1
                        print(
                            f"[STAGE 6] DROP flow data_flow_id="
                            f"{flow.get('data_flow_id')!r}: local source_path_dev "
                            f"{flow.get('source_details', {}).get('source_path_dev')!r} "
                            "is empty/missing"
                        )
                        continue
                    # 2) Drop flows pointing at non-existent UC volume paths
                    # (typically the seeded /Volumes/<cat>/landing/files/...
                    # template placeholder the user never created).
                    src_path_now = flow.get("source_details", {}).get("source_path_dev")
                    if (isinstance(src_path_now, str) and src_path_now.startswith("/Volumes/")
                            and not _uc_volume_path_exists(ws, src_path_now.rstrip("/"))):
                        dropped += 1
                        print(
                            f"[STAGE 6] DROP flow data_flow_id="
                            f"{flow.get('data_flow_id')!r}: UC source_path_dev "
                            f"{src_path_now!r} does not exist on the workspace"
                        )
                        continue
                    kept.append(flow)
                doc = kept
                after = sum(_count_path_keys(f) for f in doc)
                pruned += before - after
            if src_path.suffix.lower() == ".json":
                payload = json.dumps(doc, indent=2).encode("utf-8")
            else:
                payload = yaml.safe_dump(doc, sort_keys=False).encode("utf-8")
            ws.files.upload(file_path=dst, contents=io.BytesIO(payload), overwrite=True)
        elif src_path.suffix.lower() in (".yml", ".yaml", ".json"):
            # Sub-conf files (DQE, transformations, ...) get a plain text
            # rewrite — they may contain `${workspace.file_path}/conf` too
            # (e.g. nested transformation refs).
            text = src_path.read_text()
            patched = text.replace(workspace_conf_token, volume_conf_base)
            ws.files.upload(file_path=dst, contents=io.BytesIO(patched.encode("utf-8")),
                            overwrite=True)
        else:
            with src_path.open("rb") as fh:
                ws.files.upload(file_path=dst, contents=fh, overwrite=True)
        uploaded += 1
    msg = f"[STAGE 6] Uploaded {uploaded} conf file(s) under {volume_conf_base}/"
    extras = []
    if pruned:
        extras.append(f"pruned {pruned} dangling path ref(s)")
    if repointed:
        extras.append(f"uploaded local data + repointed {repointed} flow(s) to {volume_data_base}/")
    if dropped:
        extras.append(f"dropped {dropped} flow(s) with no real source data")
    if extras:
        msg += "  (" + "; ".join(extras) + ")"
    print(msg)
    return volume_conf_base


def _count_path_keys(flow: dict) -> int:
    """How many of the rewritable path keys are populated on this flow."""
    if not isinstance(flow, dict):
        return 0
    n = sum(1 for k in _PATH_KEYS_TOP_LEVEL
            if isinstance(flow.get(k), str) and flow.get(k))
    src_details = flow.get("source_details")
    if isinstance(src_details, dict):
        n += sum(1 for k in _PATH_KEYS_SOURCE_DETAILS
                 if isinstance(src_details.get(k), str) and src_details.get(k))
    return n


def _resolve_target_schemas(bundle_dir: Path) -> List[str]:
    """Return the list of UC schemas the rendered bundle expects to write to.

    Reading these from ``resources/variables.yml`` (instead of hard-coding
    them) means the helper still works when a user re-scaffolds the bundle
    with different answers.
    """
    var_path = bundle_dir / "resources" / "variables.yml"
    doc = yaml.safe_load(var_path.read_text())
    schemas = []
    for key in ("sdp_meta_schema", "bronze_target_schema", "silver_target_schema"):
        val = doc.get("variables", {}).get(key, {}).get("default")
        if val and val not in schemas:
            schemas.append(val)
    return schemas


def _ensure_target_schemas(bundle_dir: Path, uc_catalog: str,
                           profile: Optional[str]) -> None:
    """Create the bronze/silver/dataflowspec schemas if they don't exist.

    The dataflowspec table that the onboarding job writes to, and the
    bronze/silver target schemas that LDP writes its tables to, must
    already exist in Unity Catalog before either job runs (LDP refuses to
    auto-create schemas, and the onboarding job fails with SCHEMA_NOT_FOUND
    when it tries to MERGE INTO a missing schema).

    Mirrors the auto-create-on-demand behaviour of
    ``bundle_prepare_wheel`` for the wheel volume's schema.
    """
    from databricks.sdk import WorkspaceClient  # local import: keeps unit tests light
    from databricks.sdk.errors import AlreadyExists, ResourceAlreadyExists

    schemas = _resolve_target_schemas(bundle_dir)
    if not schemas:
        return
    print(f"[STAGE 6] Ensuring target schemas exist in {uc_catalog}: {schemas}")
    ws = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    try:
        ws.catalogs.get(uc_catalog)
    except Exception as e:
        raise SystemExit(
            f"Catalog '{uc_catalog}' is not accessible: {e}. "
            "Catalogs are never auto-created; create it first or use a different "
            "--uc-catalog-name."
        )
    for schema in schemas:
        try:
            ws.schemas.get(f"{uc_catalog}.{schema}")
            print(f"          - {uc_catalog}.{schema} (exists)")
            continue
        except Exception:
            pass
        try:
            ws.schemas.create(name=schema, catalog_name=uc_catalog)
            print(f"          - {uc_catalog}.{schema} (created)")
        except (AlreadyExists, ResourceAlreadyExists):  # pragma: no cover
            print(f"          - {uc_catalog}.{schema} (raced; ok)")
        except Exception as e:
            # Some SDK paths surface a concurrent-create race as a plain
            # Exception whose message carries the canonical Databricks
            # `ALREADY_EXISTS` error code rather than the typed AlreadyExists
            # subclass. We *only* tolerate this exact case, and we surface
            # the original exception text so an unexpected swallow is still
            # debuggable from the demo log.
            if "ALREADY_EXISTS" not in str(e):
                raise SystemExit(
                    f"Failed to create schema '{uc_catalog}.{schema}': {e}. "
                    "Make sure your principal has `USE CATALOG` + `CREATE SCHEMA` "
                    "on the catalog."
                )
            print(
                f"          - {uc_catalog}.{schema} (raced; treating as ok). "
                f"Underlying error: {e}"
            )


def _resolve_onboarding_file_name(bundle_dir: Path) -> str:
    """Read variables.yml to learn the onboarding file basename.

    The template defaults to onboarding.yml, but a user could rename it via
    --config-file or by editing resources/variables.yml. We don't want the
    demo to silently mis-target the wrong file.
    """
    var_path = bundle_dir / "resources" / "variables.yml"
    doc = yaml.safe_load(var_path.read_text())
    name = doc.get("variables", {}).get("onboarding_file_name", {}).get("default")
    if not name:
        raise SystemExit(
            f"Could not read onboarding_file_name default from {var_path}. "
            "Did the template change?"
        )
    return name


def stage_deploy_and_run(bundle_dir: Path, profile: Optional[str], *,
                         uc_catalog: Optional[str] = None,
                         uc_schema: Optional[str] = None,
                         uc_volume: Optional[str] = None) -> None:
    """Run `databricks bundle deploy` + `bundle run` for onboarding & pipelines.

    When uc_catalog/schema/volume are provided, ALSO stages the bundle's
    conf/ tree into ``/Volumes/<cat>/<sch>/<vol>/conf/`` and overrides the
    onboarding job's ``onboarding_file_path`` parameter so Spark on
    serverless can actually read the file. This is the workaround for
    PATH_NOT_FOUND when reading workspace files via Spark.
    """
    _banner("STAGE 6", "databricks bundle deploy + run  (this hits the workspace)")
    cli = shutil.which("databricks")
    if not cli:
        raise SystemExit("databricks CLI not on PATH; cannot deploy.")
    base = [cli]
    if profile:
        base.extend(["--profile", profile])

    # 0) ensure target schemas exist (LDP + onboarding fail if they don't).
    if uc_catalog:
        _ensure_target_schemas(bundle_dir, uc_catalog, profile)

    # 1) deploy.
    deploy_cmd = base + ["bundle", "deploy", "--target", "dev"]
    print("[STAGE 6] $ " + " ".join(deploy_cmd))
    rc = subprocess.run(deploy_cmd, cwd=bundle_dir).returncode
    if rc != 0:
        raise SystemExit(f"command bundle deploy --target dev failed with code {rc}")

    # 2) (optional) stage conf/ to UC volume + build the override params.
    onboarding_extra: List[str] = []
    if uc_catalog and uc_schema and uc_volume:
        volume_conf_base = _stage_conf_to_uc_volume(
            bundle_dir, uc_catalog, uc_schema, uc_volume, profile,
        )
        onboarding_file_name = _resolve_onboarding_file_name(bundle_dir)
        onboarding_extra = [
            "--python-named-params",
            f"onboarding_file_path={volume_conf_base}/{onboarding_file_name}",
        ]
        print(
            f"[STAGE 6] Overriding onboarding_file_path -> "
            f"{volume_conf_base}/{onboarding_file_name}"
        )
    else:
        print(
            "[STAGE 6] WARNING: --apply-prepare-wheel was not set with UC "
            "catalog/schema/volume; the onboarding job will read from "
            "${workspace.file_path}/conf/... which is known to fail on "
            "serverless Spark with PATH_NOT_FOUND."
        )

    for sub in (
        ["bundle", "run", "onboarding", "--target", "dev"] + onboarding_extra,
        ["bundle", "run", "pipelines", "--target", "dev"],
    ):
        cmd = base + sub
        print("[STAGE 6] $ " + " ".join(cmd))
        result = subprocess.run(cmd, cwd=bundle_dir)
        if result.returncode != 0:
            raise SystemExit(f"command {' '.join(sub)} failed with code {result.returncode}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _selected_scenarios(name: str) -> List[Scenario]:
    if name == "all":
        return list(SCENARIOS.values())
    if name not in SCENARIOS:
        raise SystemExit(f"Unknown scenario {name!r}. Choose one of: all, {', '.join(SCENARIOS)}")
    return [SCENARIOS[name]]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", maxsplit=1)[0])
    parser.add_argument("--scenario", default="all",
                        choices=["cloudfiles", "cloudfiles_combined", "kafka",
                                 "eventhub", "delta", "all"],
                        help="Which source scenario to run (default: all). "
                             "`cloudfiles` renders pipeline_mode=split; "
                             "`cloudfiles_combined` renders pipeline_mode=combined "
                             "(bronze+silver in ONE LDP pipeline, same demo data).")
    parser.add_argument("--uc-catalog-name", required=True,
                        help="Unity Catalog catalog the demo writes into. "
                             "Substituted into the answers files and CSVs.")
    parser.add_argument("--out-dir", default="demo_runs",
                        help="Where the scaffolded bundles get written (default: demo_runs/)")
    parser.add_argument("--profile", default=None,
                        help="Databricks CLI profile (used for deploy + prepare-wheel)")
    parser.add_argument("--uc-schema", default=None,
                        help="UC schema for prepare-wheel (only with --apply-prepare-wheel)")
    parser.add_argument("--uc-volume", default=None,
                        help="UC volume for prepare-wheel (only with --apply-prepare-wheel)")
    parser.add_argument("--apply-prepare-wheel", action="store_true",
                        help="Actually build + upload the sdp-meta wheel. "
                             "Without this flag, the demo pins a placeholder.")
    parser.add_argument("--pip-index-url", default=os.environ.get("PIP_INDEX_URL"),
                        help="Forwarded to `pip wheel` as --index-url. Use this "
                             "when pypi.org is not reachable from your network "
                             "(e.g. https://pypi-proxy.dev.databricks.com/simple). "
                             "Defaults to $PIP_INDEX_URL.")
    parser.add_argument("--pip-extra-index-url", action="append", default=None,
                        help="Forwarded to `pip wheel` as --extra-index-url. "
                             "Can be passed multiple times. Defaults to "
                             "$PIP_EXTRA_INDEX_URL (space-separated).")
    parser.add_argument("--apply-recipe", action="store_true",
                        help="Pass --apply to the recipe so it actually appends to onboarding. "
                             "Without this flag, the recipe runs in dry-run mode.")
    parser.add_argument("--apply-deploy", action="store_true",
                        help="Run STAGE 6 (deploy + run onboarding + pipelines). "
                             "Without this flag, the demo stops after STAGE 5.")
    parser.add_argument("--no-clean", dest="clean", action="store_false",
                        help="Do NOT remove an existing bundle directory before "
                             "STAGE 1 re-scaffolds. Default: clean (so re-runs are "
                             "idempotent).")
    parser.set_defaults(clean=True)
    parser.add_argument("--no-create-missing-uc", dest="create_missing_uc",
                        action="store_false",
                        help="Do NOT auto-create the UC schema / volume during "
                             "bundle-prepare-wheel. Default: create them if they "
                             "don't exist (catalogs are never auto-created).")
    parser.set_defaults(create_missing_uc=True)
    args = parser.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Demo output dir: {out_dir}")

    failures: List[str] = []
    for scenario in _selected_scenarios(args.scenario):
        print(f"\n\n{'#' * 78}\n# SCENARIO: {scenario.name}\n# {scenario.description}\n{'#' * 78}")
        try:
            bundle_dir = stage_bundle_init(
                scenario, out_dir, args.uc_catalog_name, args.profile,
                clean=args.clean,
            )
            extras = args.pip_extra_index_url
            if not extras and os.environ.get("PIP_EXTRA_INDEX_URL"):
                extras = [u for u in os.environ["PIP_EXTRA_INDEX_URL"].split() if u]
            demo_data_volume_path = stage_prepare_wheel(
                scenario, bundle_dir,
                apply=args.apply_prepare_wheel,
                uc_catalog_name=args.uc_catalog_name,
                uc_schema=args.uc_schema, uc_volume=args.uc_volume,
                profile=args.profile,
                pip_index_url=args.pip_index_url,
                pip_extra_index_urls=extras,
                create_if_missing=args.create_missing_uc,
            )
            stage_add_flow(
                scenario, bundle_dir, args.uc_catalog_name,
                demo_data_volume_path=demo_data_volume_path,
            )
            stage_recipe(
                scenario, bundle_dir,
                apply_recipe=args.apply_recipe,
                uc_catalog_name=args.uc_catalog_name,
                profile=args.profile,
            )
            stage_validate(bundle_dir, args.profile)
            if args.apply_deploy:
                stage_deploy_and_run(
                    bundle_dir, args.profile,
                    uc_catalog=args.uc_catalog_name,
                    uc_schema=args.uc_schema,
                    uc_volume=args.uc_volume,
                )
        except SystemExit as exc:
            print(f"\n[SCENARIO {scenario.name}] FAILED: {exc}")
            failures.append(scenario.name)

    print("\n" + "=" * 78)
    if failures:
        print(f"Done with errors. Failed scenarios: {', '.join(failures)}")
        return 1
    print("Done. All requested scenarios completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
