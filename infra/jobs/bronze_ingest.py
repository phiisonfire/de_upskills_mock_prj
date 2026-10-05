"""Ingest one manifest-listed MovieLens file into an Iceberg Bronze table."""

import hashlib
import json
import sys
from urllib.parse import urlparse

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


REQUIRED_ARGS = [
    "JOB_NAME",
    "run_id",
    "source_table",
    "batch_id",
    "input_uri",
    "database_name",
    "warehouse_uri",
]


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"Expected an s3://bucket/key URI, received {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def sha256_s3_object(s3_client, bucket: str, key: str) -> tuple[str, int]:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    digest = hashlib.sha256()
    byte_count = 0
    body = response["Body"]
    try:
        while True:
            chunk = body.read(8 * 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            byte_count += len(chunk)
    finally:
        body.close()
    return digest.hexdigest(), byte_count


def read_json_s3(s3_client, bucket: str, key: str) -> dict:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def resolve_manifest_file(
    manifest: dict, input_relative_key: str, source_table: str, batch_id: str
) -> dict:
    candidates = []
    candidates.extend(manifest.get("arrival_files", []))
    candidates.extend(manifest.get("movie_cdc_files", []))
    candidates.extend(
        item
        for item in manifest.get("source_files", [])
        if item.get("ingest_role") == "bootstrap_snapshot"
    )
    expected_suffix = f"/landing/movielens/{input_relative_key}"
    matches = [
        item
        for item in candidates
        if item.get("table") == source_table
        and item.get("batch_id") == batch_id
        and item.get("path", "").endswith(expected_suffix)
    ]
    if len(matches) != 1:
        raise ValueError(
            "Input must match exactly one eligible manifest entry; "
            f"found {len(matches)} for table={source_table}, batch_id={batch_id}, "
            f"key={input_relative_key}"
        )
    return matches[0]


def quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def merge_control_record(spark, table_name: str, record: dict) -> None:
    control_schema = StructType(
        [
            StructField("record_id", StringType(), False),
            StructField("run_id", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("batch_id", StringType(), False),
            StructField("landing_uri", StringType(), False),
            StructField("source_file", StringType(), False),
            StructField("expected_sha256", StringType(), True),
            StructField("actual_sha256", StringType(), True),
            StructField("expected_size_bytes", LongType(), True),
            StructField("actual_size_bytes", LongType(), True),
            StructField("expected_row_count", LongType(), True),
            StructField("actual_row_count", LongType(), True),
            StructField("reconciliation_status", StringType(), False),
            StructField("status", StringType(), False),
            StructField("attempt", LongType(), False),
            StructField("error_message", StringType(), True),
            StructField("updated_at", TimestampType(), False),
        ]
    )
    source = spark.createDataFrame([record], schema=control_schema)
    source.createOrReplaceTempView("bronze_control_update")
    columns = [field.name for field in control_schema.fields]
    update_set = ", ".join(
        f"target.{quote_identifier(name)} = source.{quote_identifier(name)}"
        for name in columns
        if name != "record_id"
    )
    insert_columns = ", ".join(quote_identifier(name) for name in columns)
    insert_values = ", ".join(
        f"source.{quote_identifier(name)}" for name in columns
    )
    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING bronze_control_update AS source
        ON target.record_id = source.record_id
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
    )


def main() -> None:
    args = getResolvedOptions(sys.argv, REQUIRED_ARGS)
    run_id = args["run_id"]
    source_table = args["source_table"]
    batch_id = args["batch_id"]
    database_name = args["database_name"]
    warehouse_uri = args["warehouse_uri"].rstrip("/") + "/"

    bucket, input_key = parse_s3_uri(args["input_uri"])
    run_landing_prefix = f"runs/{run_id}/landing/movielens/"
    if not input_key.startswith(run_landing_prefix):
        raise ValueError(f"Input URI must be under s3://{bucket}/{run_landing_prefix}")
    input_relative_key = input_key[len(run_landing_prefix) :]
    manifest_key = f"{run_landing_prefix}manifest.json"
    s3_client = boto3.client("s3")
    manifest = read_json_s3(s3_client, bucket, manifest_key)
    if manifest.get("run_id") != run_id:
        raise ValueError(
            f"Manifest run_id {manifest.get('run_id')!r} does not match {run_id!r}"
        )
    manifest_entry = resolve_manifest_file(
        manifest, input_relative_key, source_table, batch_id
    )

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    catalog = "glue_catalog"
    bronze_table = f"{catalog}.{database_name}.bronze_{source_table}"
    control_table = f"{catalog}.{database_name}.bronze_ingestion_control"
    bronze_location = f"{warehouse_uri}iceberg/bronze_{source_table}/"
    control_location = f"{warehouse_uri}iceberg/bronze_ingestion_control/"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {control_table} (
          record_id string,
          run_id string,
          source_table string,
          batch_id string,
          landing_uri string,
          source_file string,
          expected_sha256 string,
          actual_sha256 string,
          expected_size_bytes bigint,
          actual_size_bytes bigint,
          expected_row_count bigint,
          actual_row_count bigint,
          reconciliation_status string,
          status string,
          attempt bigint,
          error_message string,
          updated_at timestamp
        ) USING iceberg
        LOCATION '{control_location}'
        TBLPROPERTIES ('format-version'='2')
        """
    )

    input_uri = args["input_uri"]
    record_id = hashlib.sha256(
        f"{run_id}\0{source_table}\0{batch_id}\0{input_uri}".encode("utf-8")
    ).hexdigest()
    previous_rows = (
        spark.table(control_table)
        .where(F.col("record_id") == record_id)
        .limit(1)
        .collect()
    )
    previous = previous_rows[0].asDict() if previous_rows else None
    attempt = int(previous["attempt"] if previous else 0) + 1
    expected_sha = manifest_entry["sha256"]
    expected_size = int(manifest_entry["size_bytes"])
    expected_rows = int(
        manifest_entry.get("expected_batch_rows", manifest_entry["row_count"])
    )

    def write_control(
        status: str,
        reconciliation_status: str,
        actual_sha: str | None,
        actual_size: int | None,
        actual_rows: int | None,
        error_message: str | None = None,
    ) -> None:
        from datetime import datetime, timezone

        merge_control_record(
            spark,
            control_table,
            {
                "record_id": record_id,
                "run_id": run_id,
                "source_table": source_table,
                "batch_id": batch_id,
                "landing_uri": input_uri,
                "source_file": input_key.rsplit("/", 1)[-1],
                "expected_sha256": expected_sha,
                "actual_sha256": actual_sha,
                "expected_size_bytes": expected_size,
                "actual_size_bytes": actual_size,
                "expected_row_count": expected_rows,
                "actual_row_count": actual_rows,
                "reconciliation_status": reconciliation_status,
                "status": status,
                "attempt": attempt,
                "error_message": error_message[:4000] if error_message else None,
                "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
            },
        )

    previously_validated_checksum = bool(
        previous
        and previous.get("actual_sha256")
        and previous.get("actual_sha256") == previous.get("expected_sha256")
    )
    if (
        previous
        and previously_validated_checksum
        and previous.get("expected_sha256") != expected_sha
    ):
        write_control(
            "FAILED", "MANIFEST_CHECKSUM_CHANGED", previous.get("actual_sha256"),
            previous.get("actual_size_bytes"), previous.get("actual_row_count"),
            "The manifest checksum changed after this URI was previously validated; refusing to merge potentially different content under the same row identities.",
        )
        raise ValueError("Manifest checksum changed after this URI was previously validated")
    try:
        actual_sha, actual_size = sha256_s3_object(s3_client, bucket, input_key)
    except Exception as error:
        write_control("FAILED", "READ_FAILED", None, None, None, str(error))
        raise
    if actual_sha != expected_sha or actual_size != expected_size:
        message = (
            f"Landing file mismatch: expected {expected_size} bytes/{expected_sha}, "
            f"found {actual_size} bytes/{actual_sha}"
        )
        write_control(
            "FAILED", "CHECKSUM_OR_SIZE_MISMATCH", actual_sha, actual_size, None, message
        )
        raise ValueError(message)
    if previous and previous.get("status") == "SUCCEEDED":
        print(f"Already reconciled; skipping {input_uri}")
        return
    write_control("RUNNING", "PENDING", actual_sha, actual_size, None)

    control_failure_recorded = False
    try:
        raw_df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "FAILFAST")
            .option("multiLine", "false")
            .load(input_uri)
        )
        source_columns = raw_df.columns
        if not source_columns:
            raise ValueError("CSV input has no header columns")
        indexed_schema = StructType(
            list(raw_df.schema.fields)
            + [StructField("_row_number", LongType(), nullable=False)]
        )
        indexed_rdd = raw_df.rdd.zipWithIndex().map(
            lambda item: tuple(item[0]) + (int(item[1]) + 1,)
        )
        indexed_df = spark.createDataFrame(indexed_rdd, schema=indexed_schema)
        actual_rows = indexed_df.count()

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {bronze_table} (
              {", ".join(f"{quote_identifier(name)} string" for name in source_columns)},
              _run_id string,
              _batch_id string,
              _source_table string,
              _source_file string,
              _landing_uri string,
              _file_checksum string,
              _ingested_at timestamp,
              _row_number bigint,
              _record_hash string,
              _ingestion_id string,
              _scenario string,
              _source_event_batch_id string,
              _ingest_role string
            ) USING iceberg
            PARTITIONED BY (_batch_id)
            LOCATION '{bronze_location}'
            TBLPROPERTIES ('format-version'='2')
            """
        )

        bronze_df = indexed_df
        for name, value in [
            ("_run_id", run_id),
            ("_batch_id", batch_id),
            ("_source_table", source_table),
            ("_source_file", input_key.rsplit("/", 1)[-1]),
            ("_landing_uri", input_uri),
            ("_file_checksum", actual_sha),
            ("_scenario", str(manifest_entry.get("scenario", ""))),
            (
                "_source_event_batch_id",
                manifest_entry.get("source_event_batch_id"),
            ),
            ("_ingest_role", manifest_entry.get("ingest_role", "event_arrival")),
        ]:
            bronze_df = bronze_df.withColumn(name, F.lit(value).cast(StringType()))
        bronze_df = bronze_df.withColumn("_ingested_at", F.current_timestamp())
        bronze_df = bronze_df.withColumn(
            "_record_hash",
            F.sha2(F.to_json(F.struct(*[F.col(quote_identifier(c)) for c in source_columns])), 256),
        )
        bronze_df = bronze_df.withColumn(
            "_ingestion_id",
            F.sha2(
                F.concat_ws(
                    "\u001f",
                    F.col("_run_id"),
                    F.col("_source_table"),
                    F.col("_batch_id"),
                    F.col("_landing_uri"),
                    F.col("_row_number").cast("string"),
                ),
                256,
            ),
        )
        bronze_df.createOrReplaceTempView("bronze_input_rows")
        target_columns = source_columns + [
            "_run_id", "_batch_id", "_source_table", "_source_file",
            "_landing_uri", "_file_checksum", "_ingested_at", "_row_number",
            "_record_hash", "_ingestion_id", "_scenario",
            "_source_event_batch_id", "_ingest_role",
        ]
        insert_columns = ", ".join(quote_identifier(c) for c in target_columns)
        insert_values = ", ".join(
            f"source.{quote_identifier(c)}" for c in target_columns
        )
        spark.sql(
            f"""
            MERGE INTO {bronze_table} AS target
            USING bronze_input_rows AS source
            ON target._ingestion_id = source._ingestion_id
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
            """
        )

        if actual_rows != expected_rows:
            message = f"Row-count mismatch: expected {expected_rows}, found {actual_rows}"
            write_control(
                "FAILED", "ROW_COUNT_MISMATCH", actual_sha, actual_size,
                actual_rows, message,
            )
            control_failure_recorded = True
            raise ValueError(message)
        write_control(
            "SUCCEEDED", "RECONCILED", actual_sha, actual_size, actual_rows
        )
        print(
            f"Reconciled {source_table}/{batch_id}: {actual_rows} rows from {input_uri}"
        )
    except Exception as error:
        if "actual_rows" not in locals():
            actual_rows = None
        if not control_failure_recorded:
            write_control(
                "FAILED", "PROCESSING_FAILED", actual_sha, actual_size,
                actual_rows, str(error),
            )
        raise


if __name__ == "__main__":
    main()
