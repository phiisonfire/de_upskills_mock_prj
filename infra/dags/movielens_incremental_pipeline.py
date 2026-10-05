"""Daily MovieLens pipeline orchestration for Amazon MWAA (Airflow 2.11)."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from typing import Any

import boto3
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError


RUN_ID_DEFAULT = "movielens_t2013_daily_v2"
MANIFEST_KEY_SUFFIX = "landing/movielens/manifest.json"
STATE_KEY_SUFFIX = "control/airflow/processed_manifest_files.json"
GLUE_TERMINAL_FAILURES = {"FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"}
GLUE_TERMINAL_SUCCESS = "SUCCEEDED"


def runtime_config() -> dict[str, str]:
    run_id = Variable.get("MOVIELENS_RUN_ID", default_var=RUN_ID_DEFAULT)
    bucket = Variable.get("MOVIELENS_BUCKET")
    region = Variable.get("MOVIELENS_REGION", default_var=os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1"))
    return {
        "run_id": run_id,
        "bucket": bucket,
        "region": region,
        "prefix": f"runs/{run_id}",
    }


def s3_client(config: dict[str, str]):
    return boto3.client("s3", region_name=config["region"])


def glue_client(config: dict[str, str]):
    return boto3.client("glue", region_name=config["region"])


def read_state(client: Any, bucket: str, key: str) -> dict[str, Any]:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404", "NotFound"}:
            return {"processed": [], "gold_bootstrap_complete": False}
        raise


def write_state(client: Any, bucket: str, key: str, state: dict[str, Any]) -> None:
    body = json.dumps(state, sort_keys=True, indent=2).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def file_identity(entry: dict[str, Any]) -> str:
    return "|".join((entry["table"], entry["batch_id"], entry["path"], entry["sha256"]))


def landing_uri(entry: dict[str, Any], config: dict[str, str]) -> str:
    local_root = f"data/simulated/{config['run_id']}/"
    path = entry["path"]
    if not path.startswith(local_root):
        raise AirflowException(f"Manifest path is outside this run: {path}")
    key = path[len(local_root):]
    if not key.startswith("landing/") or ".." in key.split("/"):
        raise AirflowException(f"Manifest path is not a safe Landing object key: {path}")
    return f"s3://{config['bucket']}/{config['prefix']}/{key}"


def load_manifest(config: dict[str, str]) -> dict[str, Any]:
    response = s3_client(config).get_object(
        Bucket=config["bucket"], Key=f"{config['prefix']}/{MANIFEST_KEY_SUFFIX}"
    )
    manifest = json.loads(response["Body"].read().decode("utf-8"))
    if manifest.get("run_id") != config["run_id"]:
        raise AirflowException(
            f"Manifest run_id={manifest.get('run_id')!r} does not match {config['run_id']!r}"
        )
    return manifest


def start_and_wait_for_glue(
    client: Any,
    job_name: str,
    arguments: dict[str, str],
    timeout_seconds: int = 5 * 60 * 60,
) -> None:
    response = client.start_job_run(JobName=job_name, Arguments=arguments)
    run_id = response["JobRunId"]
    logging.info("Started Glue job %s run %s", job_name, run_id)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        result = client.get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=False)
        job_run = result["JobRun"]
        state = job_run["JobRunState"]
        if state == GLUE_TERMINAL_SUCCESS:
            logging.info("Glue job %s run %s succeeded", job_name, run_id)
            return
        if state in GLUE_TERMINAL_FAILURES:
            raise AirflowException(
                f"Glue job {job_name} run {run_id} ended {state}: "
                f"{job_run.get('ErrorMessage', 'no error message')}. "
                "A Silver blocking DQ failure is fail-fast and must be resolved before downstream stages."
            )
        time.sleep(30)
    client.batch_stop_job_run(JobName=job_name, JobRunIds=[run_id])
    raise AirflowException(f"Glue job {job_name} run {run_id} exceeded the MWAA wait timeout")


def process_file(
    entry: dict[str, Any], config: dict[str, str], state: dict[str, Any],
    force: bool = False, run_gold: bool = True,
) -> None:
    identity = file_identity(entry)
    if identity in state["processed"] and not force:
        logging.info("Already processed manifest file: %s", identity)
        return
    uri = landing_uri(entry, config)
    table = entry["table"]
    batch_id = entry["batch_id"]
    glue = glue_client(config)
    bronze_args = {"--source_table": table, "--batch_id": batch_id, "--input_uri": uri}
    common = {"--source_table": table, "--batch_id": batch_id}
    logging.info("Processing %s/%s from %s", table, batch_id, uri)
    start_and_wait_for_glue(glue, f"cineinsight-{config['run_id']}-bronze", bronze_args)
    start_and_wait_for_glue(glue, f"cineinsight-{config['run_id']}-silver", {**common, "--landing_uri": uri})
    if run_gold:
        start_and_wait_for_glue(glue, f"cineinsight-{config['run_id']}-gold", {**common, "--landing_uri": uri})
    if identity not in state["processed"]:
        state["processed"].append(identity)
    write_state(s3_client(config), config["bucket"], f"{config['prefix']}/{STATE_KEY_SUFFIX}", state)


def sorted_inputs(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(entries, key=lambda x: (x["table"], x["batch_id"], x["path"]))


def init_bootstrap(**_context: Any) -> None:
    config = runtime_config()
    manifest = load_manifest(config)
    client = s3_client(config)
    state_key = f"{config['prefix']}/{STATE_KEY_SUFFIX}"
    state = read_state(client, config["bucket"], state_key)
    snapshot_inputs = [item for item in manifest.get("source_files", []) if item.get("ingest_role") == "bootstrap_snapshot"]
    priority = {"movie": 0, "genome_tags": 1, "link": 2, "genome_scores": 3}
    for entry in sorted(snapshot_inputs, key=lambda x: (priority.get(x["table"], 99), x["path"])):
        process_file(entry, config, state, run_gold=entry["table"] != "genome_scores")


def init_history(**_context: Any) -> None:
    config = runtime_config()
    manifest = load_manifest(config)
    client = s3_client(config)
    state_key = f"{config['prefix']}/{STATE_KEY_SUFFIX}"
    state = read_state(client, config["bucket"], state_key)
    history = [
        item for item in manifest.get("arrival_files", [])
        if item.get("table") in {"rating", "tag"} and item.get("batch_id") == "batch_0000_history"
    ]
    for entry in sorted_inputs(history):
        process_file(entry, config, state, run_gold=False)


def run_gold_bootstrap(**_context: Any) -> None:
    config = runtime_config()
    client = s3_client(config)
    state_key = f"{config['prefix']}/{STATE_KEY_SUFFIX}"
    state = read_state(client, config["bucket"], state_key)
    if state.get("gold_bootstrap_complete"):
        logging.info("Gold bootstrap is already complete")
        return
    start_and_wait_for_glue(
        glue_client(config), f"cineinsight-{config['run_id']}-gold",
        {"--source_table": "bootstrap", "--batch_id": "gold_bootstrap", "--landing_uri": "bootstrap"},
    )
    state["gold_bootstrap_complete"] = True
    write_state(client, config["bucket"], state_key, state)


def batch_for_date(manifest: dict[str, Any], day: date) -> str:
    cutoff = date.fromisoformat(manifest["cutoff"])
    if day < cutoff:
        return "batch_0000_history"
    window_days = int(manifest.get("window_days", 1))
    number = (day - cutoff).days // window_days + 1
    return f"batch_{number:04d}"


def selected_batch(manifest: dict[str, Any], context: dict[str, Any]) -> tuple[str | None, bool]:
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    if conf.get("process_all"):
        return None, True
    if conf.get("batch_id"):
        return str(conf["batch_id"]), True
    run_type = str(getattr(context.get("dag_run"), "run_type", ""))
    if "BACKFILL" in run_type.upper():
        logical_date = context.get("logical_date") or context.get("execution_date")
        return batch_for_date(manifest, logical_date.date()), True
    # Daily polling processes any newly published manifest entries, independent
    # of this simulator's historic event-time dates.
    return None, False


def process_incremental_events(**context: Any) -> None:
    config = runtime_config()
    manifest = load_manifest(config)
    client = s3_client(config)
    state_key = f"{config['prefix']}/{STATE_KEY_SUFFIX}"
    state = read_state(client, config["bucket"], state_key)
    target, force = selected_batch(manifest, context)
    events = [item for item in manifest.get("arrival_files", []) if item.get("table") in {"rating", "tag"} and item.get("batch_id") != "batch_0000_history"]
    if target:
        events = [item for item in events if item["batch_id"] == target]
    for entry in sorted_inputs(events):
        process_file(entry, config, state, force=force)


def process_movie_cdc(**context: Any) -> None:
    config = runtime_config()
    manifest = load_manifest(config)
    client = s3_client(config)
    state_key = f"{config['prefix']}/{STATE_KEY_SUFFIX}"
    state = read_state(client, config["bucket"], state_key)
    target, force = selected_batch(manifest, context)
    entries = sorted_inputs(manifest.get("movie_cdc_files", []))
    if target:
        entries = [item for item in entries if item["batch_id"] == target]
    for entry in entries:
        process_file(entry, config, state, force=force)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    logging.error("MovieLens MWAA pipeline SLA missed. Tasks: %s", task_list)


DEFAULT_ARGS = {
    "owner": "cineinsight-data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=36),
    "sla": timedelta(hours=24),
}


with DAG(
    dag_id="movielens_incremental_pipeline",
    description="Daily manifest polling and incremental MovieLens Bronze, Silver, and Gold loads on AWS Glue/MWAA.",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=sla_miss_callback,
    tags=["movielens", "aws-glue", "iceberg", "backfill"],
) as dag:
    bootstrap_sources = PythonOperator(task_id="bootstrap_source_dimensions", python_callable=init_bootstrap)
    history_events = PythonOperator(task_id="load_rating_tag_history", python_callable=init_history)
    gold_bootstrap = PythonOperator(task_id="initialize_gold_star_schema", python_callable=run_gold_bootstrap)
    incremental_events = PythonOperator(task_id="process_incremental_rating_tag_files", python_callable=process_incremental_events)
    movie_cdc = PythonOperator(task_id="apply_movie_cdc_in_order", python_callable=process_movie_cdc)

    bootstrap_sources >> history_events >> gold_bootstrap >> incremental_events >> movie_cdc
