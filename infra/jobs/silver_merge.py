"""Validate and incrementally merge one Bronze file into MovieLens Silver."""

import hashlib
import sys
import unicodedata
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)


ARGS = ["JOB_NAME", "run_id", "source_table", "batch_id", "landing_uri", "database_name", "warehouse_uri"]
SOURCE_SYSTEM = "movielens"


def qi(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def source_column(name: str) -> F.Column:
    return F.col(qi(name))


def make_table(spark: SparkSession, table: str, columns: str, location: str, partition: str | None = None) -> None:
    partition_sql = f" PARTITIONED BY ({partition})" if partition else ""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({columns}) USING iceberg{partition_sql} "
        f"LOCATION '{location}' TBLPROPERTIES ('format-version'='2')"
    )


def merge_rows(
    spark: SparkSession,
    table: str,
    rows: DataFrame,
    keys: list[str],
    update_columns: list[str] | None = None,
) -> None:
    if not rows.columns:
        return
    view = "silver_merge_source"
    rows.createOrReplaceTempView(view)
    match = " AND ".join(f"target.{qi(key)} = source.{qi(key)}" for key in keys)
    cols = rows.columns
    insert_cols = ", ".join(qi(col) for col in cols)
    insert_values = ", ".join(f"source.{qi(col)}" for col in cols)
    update_cols = update_columns or [col for col in cols if col not in keys]
    update_sql = ""
    if update_cols:
        update_sql = "WHEN MATCHED THEN UPDATE SET " + ", ".join(
            f"target.{qi(col)} = source.{qi(col)}" for col in update_cols
        )
    spark.sql(
        f"MERGE INTO {table} AS target USING {view} AS source ON {match} "
        f"{update_sql} WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})"
    )


def add_dq_codes(rows: DataFrame, rules: list[tuple[str, str, str, str]]) -> DataFrame:
    """Attach pipe-delimited blocking, quarantine-warning, and advisory codes."""
    blocking, quarantine_warning, advisory = [], [], []
    for code, condition, severity, _reason in rules:
        expression = F.when(F.expr(condition), F.lit(code))
        if severity == "BLOCKING":
            blocking.append(expression)
        elif severity == "QUARANTINE_WARNING":
            quarantine_warning.append(expression)
        else:
            advisory.append(expression)
    return (
        rows.withColumn("_blocking_codes", F.concat_ws("|", *blocking))
        .withColumn("_quarantine_warning_codes", F.concat_ws("|", *quarantine_warning))
        .withColumn("_advisory_codes", F.concat_ws("|", *advisory))
    )


def write_dq_results(
    spark: SparkSession,
    dq_table: str,
    batch_key: str,
    batch_id: str,
    source_table: str,
    landing_uri: str,
    evaluated: int,
    rows: DataFrame,
    rules: list[tuple[str, str, str, str]],
) -> None:
    # Aggregate all already-evaluated rule codes together in one scan.
    aggregates = [
        F.sum(
            F.when(
                F.array_contains(
                    F.split(F.concat_ws("|", "_blocking_codes", "_quarantine_warning_codes", "_advisory_codes"), "\\|"),
                    code,
                ),
                1,
            ).otherwise(0)
        ).alias(code)
        for code, _condition, _severity, _reason in rules
    ]
    counts = rows.agg(*aggregates).first().asDict() if aggregates else {}
    records = [
        (
            batch_key + ":" + code,
            batch_key,
            source_table,
            batch_id,
            landing_uri,
            code,
            severity,
            evaluated,
            int(counts.get(code) or 0),
            reason,
            datetime.now(timezone.utc).replace(tzinfo=None),
        )
        for code, _condition, severity, reason in rules
    ]
    if not records:
        return
    schema = StructType(
        [
            StructField("dq_result_id", StringType(), False),
            StructField("batch_key", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("batch_id", StringType(), False),
            StructField("landing_uri", StringType(), False),
            StructField("rule_id", StringType(), False),
            StructField("severity", StringType(), False),
            StructField("evaluated_rows", LongType(), False),
            StructField("failed_rows", LongType(), False),
            StructField("rule_description", StringType(), False),
            StructField("evaluated_at", TimestampNTZType(), False),
        ]
    )
    merge_rows(spark, dq_table, spark.createDataFrame(records, schema), ["dq_result_id"])


def write_quarantine(
    spark: SparkSession,
    quarantine_table: str,
    rows: DataFrame,
    batch_id: str,
    source_table: str,
    landing_uri: str,
    rules: list[tuple[str, str, str, str]],
) -> int:
    bad = rows.filter(
        (F.length(F.col("_blocking_codes")) > 0)
        | (F.length(F.col("_quarantine_warning_codes")) > 0)
    )
    count = bad.count()
    if not count:
        return 0
    codes = F.concat_ws("|", "_blocking_codes", "_quarantine_warning_codes")
    reasons = F.concat_ws(
        "; ",
        *[
            F.when(F.array_contains(F.split(codes, "\\|"), code), F.lit(reason))
            for code, _condition, _severity, reason in rules
        ],
    )
    payload_columns = [col for col in rows.columns if not col.startswith("_")]
    lineage_cols = [c for c in ["_run_id", "_batch_id", "_source_file", "_landing_uri", "_file_checksum", "_row_number", "_record_hash"] if c in rows.columns]
    quarantined = (
        bad.withColumn("quarantine_id", F.sha2(F.concat_ws("|", "_ingestion_id", codes), 256))
        .withColumn("source_table", F.lit(source_table))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("landing_uri", F.lit(landing_uri))
        .withColumn("rule_codes", codes)
        .withColumn("reason", reasons)
        .withColumn("severity", F.when(F.length(F.col("_blocking_codes")) > 0, "BLOCKING").otherwise("WARNING"))
        .withColumn("raw_payload", F.to_json(F.struct(*[F.col(qi(c)) for c in payload_columns])))
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("reprocess_status", F.lit("PENDING"))
        .select(
            "quarantine_id", "source_table", "batch_id", "landing_uri", "rule_codes",
            "reason", "severity", "raw_payload", "quarantined_at", "reprocess_status",
            *[F.col(qi(c)) for c in lineage_cols],
        )
    )
    merge_rows(spark, quarantine_table, quarantined, ["quarantine_id"])
    return count


def ensure_tables(spark: SparkSession, db: str, root: str) -> dict[str, str]:
    run_root = root.rstrip("/").rsplit("/silver", 1)[0]
    definitions = {
        "silver_interaction_event": ("event_id string, source_system string, event_type string, party_source_id bigint, content_source_id bigint, event_value_numeric double, event_value_unit string, event_value_text_raw string, tag_text_normalized string, tag_normalization_version string, event_time_source timestamp_ntz, event_time_raw string, record_hash string, batch_id string, source_file string, landing_uri string, source_row_number bigint, bronze_ingestion_id string", None),
        "silver_party": ("source_system string, source_party_id bigint, first_seen_batch string, source_file string, landing_uri string, record_hash string", None),
        "silver_content": ("source_system string, source_content_id bigint, title_raw string, display_title string, release_year int, title_parse_status string, genres_raw string, has_no_genres_listed boolean, is_deleted boolean, previous_display_title string, title_changed_at timestamp_ntz, source_changed_at timestamp_ntz, source_batch_id string, source_file string, landing_uri string, record_hash string", None),
        "silver_content_genre": ("source_system string, source_content_id bigint, genre string, is_current boolean, source_batch_id string, source_file string, landing_uri string, record_hash string", None),
        "silver_movie_cdc": ("cdc_event_id string, source_system string, source_content_id bigint, title_raw string, genres_raw string, is_deleted boolean, op_type string, changed_at timestamp_ntz, scenario string, record_hash string, batch_id string, source_file string, landing_uri string, source_row_number bigint", None),
        "silver_external_content_id": ("source_system string, source_content_id bigint, provider string, provider_id string, provider_url string, record_hash string, batch_id string, source_file string, landing_uri string", None),
        "silver_signal_taxonomy": ("source_system string, source_taxonomy_id bigint, signal_name string, record_hash string, batch_id string, source_file string, landing_uri string", None),
        "silver_content_tag_signal": ("source_system string, source_content_id bigint, source_taxonomy_id bigint, relevance double, signal_unit string, record_hash string, batch_id string, source_file string, landing_uri string, source_row_number bigint", None),
        "silver_quarantine": ("quarantine_id string, source_table string, batch_id string, landing_uri string, rule_codes string, reason string, severity string, raw_payload string, quarantined_at timestamp, reprocess_status string, _run_id string, _batch_id string, _source_file string, _landing_uri string, _file_checksum string, _row_number bigint, _record_hash string", "batch_id"),
        "silver_dq_result": ("dq_result_id string, batch_key string, source_table string, batch_id string, landing_uri string, rule_id string, severity string, evaluated_rows bigint, failed_rows bigint, rule_description string, evaluated_at timestamp_ntz", "batch_id"),
        "silver_batch_control": ("batch_key string, run_id string, source_table string, batch_id string, landing_uri string, status string, rows_read bigint, rows_merged bigint, rows_quarantined bigint, blocking_failures bigint, started_at timestamp_ntz, completed_at timestamp_ntz, error_message string", None),
    }
    tables = {}
    for name, (columns, partition) in definitions.items():
        table = f"glue_catalog.{db}.{name}"
        if name == "silver_quarantine":
            location = f"{run_root}/quarantine/iceberg/{name}/"
        elif name in ("silver_dq_result", "silver_batch_control"):
            location = f"{run_root}/control/iceberg/{name}/"
        else:
            location = f"{root.rstrip('/')}/iceberg/{name}/"
        make_table(spark, table, columns, location, partition)
        tables[name] = table
    return tables


def input_rows(spark: SparkSession, bronze_table: str, batch_id: str, landing_uri: str) -> DataFrame:
    rows = spark.table(bronze_table).where(
        (F.col("_batch_id") == batch_id) & (F.col("_landing_uri") == landing_uri)
    )
    if not rows.take(1):
        raise ValueError(f"No Bronze rows found for batch={batch_id}, landing_uri={landing_uri}")
    return rows


def movie_title_fields(rows: DataFrame, title_col: str) -> DataFrame:
    title = F.col(title_col)
    year_match = F.regexp_extract(title, r"\((\d{4})\)\)?$", 1)
    range_match = F.regexp_extract(title, r"\(\d{4}\s*[-–]\s*\d{0,4}\)\)?$", 0)
    return (
        rows.withColumn("_title_raw", title)
        .withColumn("_title_trimmed", F.trim(title))
        .withColumn("_release_year", F.when(year_match != "", year_match.cast(IntegerType())))
        .withColumn(
            "_display_title",
            F.when(year_match != "", F.regexp_replace(F.trim(title), r"\s*\(\d{4}\)\)?$", ""))
            .otherwise(F.trim(title)),
        )
        .withColumn(
            "_title_parse_status",
            F.when(year_match != "", "parsed_terminal_year")
            .when(range_match != "", "year_range_review")
            .otherwise("no_recognizable_terminal_year"),
        )
    )


def main() -> None:
    args = getResolvedOptions(sys.argv, ARGS)
    run_id, source_table, batch_id, landing_uri = (
        args["run_id"], args["source_table"], args["batch_id"], args["landing_uri"]
    )
    db = args["database_name"]
    root = args["warehouse_uri"].rstrip("/") + "/"
    spark = SparkSession.builder.getOrCreate()
    # The source timestamps have no zone marker. Use Spark's local timestamp
    # type for event_time_source; never convert these values to UTC.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    tables = ensure_tables(spark, db, root)
    bronze_table = f"glue_catalog.{db}.bronze_{source_table}"
    identity = hashlib.sha256(
        f"{run_id}\0{source_table}\0{batch_id}\0{landing_uri}".encode("utf-8")
    ).hexdigest()
    prior = (
        spark.table(tables["silver_batch_control"])
        .where(F.col("batch_key") == identity)
        .limit(1)
        .collect()
    )
    if prior and prior[0]["status"] == "SUCCEEDED":
        print(f"Silver batch already committed: {landing_uri}")
        return

    started = datetime.now(timezone.utc).replace(tzinfo=None)
    raw = input_rows(spark, bronze_table, batch_id, landing_uri)
    read_count = raw.count()
    dq_rules: list[tuple[str, str, str, str]] = []
    quarantined_rows: DataFrame | None = None
    report_rows: DataFrame | None = None
    merged_count = 0
    blocking_failures = 0
    rows_read = raw

    if source_table in ("rating", "tag"):
        normalized = raw.withColumn("_party_id", F.expr("try_cast(`userId` as bigint)"))
        normalized = normalized.withColumn("_content_id", F.expr("try_cast(`movieId` as bigint)"))
        normalized = normalized.withColumn(
            "_event_time_source",
            F.expr("to_timestamp_ntz(`timestamp`, 'yyyy-MM-dd HH:mm:ss')"),
        )
        if source_table == "rating":
            normalized = normalized.withColumn("_rating_value", F.expr("try_cast(`rating` as double)"))
            valid_domains = "_rating_value < 0.5 OR _rating_value > 5.0 OR abs(_rating_value * 2 - round(_rating_value * 2)) > 0.000001"
            dq_rules = [
                ("REQUIRED_IDS", "_party_id IS NULL OR _content_id IS NULL OR _party_id <= 0 OR _content_id <= 0", "BLOCKING", "Rating requires positive integer userId and movieId."),
                ("TIMESTAMP_PARSE", "_event_time_source IS NULL", "BLOCKING", "Timestamp must parse as yyyy-MM-dd HH:mm:ss; timezone remains unspecified."),
                ("RATING_DOMAIN", "_rating_value IS NULL OR " + valid_domains, "BLOCKING", "Rating must be one of the half-star values from 0.5 through 5.0."),
            ]
        else:
            normalize_tag = F.udf(
                lambda value: " ".join(unicodedata.normalize("NFC", value).strip().lower().split())
                if value is not None
                else None,
                StringType(),
            )
            normalized = normalized.withColumn("_tag_normalized", normalize_tag(source_column("tag")))
            dq_rules = [
                ("REQUIRED_IDS", "_party_id IS NULL OR _content_id IS NULL OR _party_id <= 0 OR _content_id <= 0", "BLOCKING", "Tag requires positive integer userId and movieId."),
                ("TIMESTAMP_PARSE", "_event_time_source IS NULL", "BLOCKING", "Timestamp must parse as yyyy-MM-dd HH:mm:ss; timezone remains unspecified."),
                ("TAG_BLANK", "_tag_normalized IS NULL OR length(_tag_normalized) = 0", "QUARANTINE_WARNING", "Blank or whitespace-only user tags are quarantined; processing continues."),
            ]
        if source_table == "rating":
            normalized = normalized.withColumn(
                "_business_event_id",
                F.sha2(F.concat_ws("|", F.lit(SOURCE_SYSTEM), F.col("_party_id"), F.col("_content_id")), 256),
            )
        else:
            normalized = normalized.withColumn(
                "_business_event_id",
                F.sha2(F.concat_ws("|", F.lit(SOURCE_SYSTEM), F.col("_party_id"), F.col("_content_id"), source_column("tag"), source_column("timestamp")), 256),
            )
        # Hash the source business payload rather than ingestion metadata.
        business_fields = [source_column(c) for c in raw.columns if not c.startswith("_")]
        normalized = normalized.withColumn("_silver_hash", F.sha2(F.to_json(F.struct(*business_fields)), 256))
        content_keys = spark.table(tables["silver_content"]).select(
            F.col("source_content_id").alias("_known_content_id")
        ).distinct()
        normalized = normalized.join(
            content_keys,
            normalized["_content_id"] == content_keys["_known_content_id"],
            "left",
        )
        dq_rules.append(("CONTENT_REFERENCE", "_known_content_id IS NULL", "BLOCKING", "Event movieId must exist in the bootstrapped Silver content table."))
        normalized = add_dq_codes(normalized, dq_rules)

        # Conflicting event keys are quarantined; equal key/hash rows are exact replays.
        collision_keys = (
            normalized.groupBy("_business_event_id")
            .agg(F.countDistinct("_silver_hash").alias("_hash_count"))
            .where(F.col("_hash_count") > 1)
            .select("_business_event_id")
        )
        normalized = normalized.join(collision_keys.withColumn("_in_batch_conflict", F.lit(True)), "_business_event_id", "left")
        normalized = normalized.withColumn(
            "_blocking_codes",
            F.concat_ws("|", "_blocking_codes", F.when(F.col("_in_batch_conflict"), "EVENT_KEY_CONFLICT")),
        ).withColumn(
            "_blocking_codes",
            F.concat_ws("|", "_blocking_codes", F.when(F.col("_known_content_id").isNull(), F.lit("CONTENT_REFERENCE"))),
        )
        target = spark.table(tables["silver_interaction_event"]).select(
            "event_id", F.col("record_hash").alias("_target_hash")
        )
        normalized = normalized.join(target, normalized["_business_event_id"] == target["event_id"], "left")
        normalized = normalized.withColumn(
            "_blocking_codes",
            F.concat_ws("|", "_blocking_codes", F.when(F.col("_target_hash").isNotNull() & (F.col("_target_hash") != F.col("_silver_hash")), "EVENT_KEY_CONFLICT")),
        )
        dq_rules.append(("EVENT_KEY_CONFLICT", "_in_batch_conflict IS NOT NULL OR (_target_hash IS NOT NULL AND _target_hash <> _silver_hash)", "BLOCKING", "A source event key with a different record hash is a conflict; it is quarantined rather than resolved by arrival order."))
        normalized = normalized.withColumn(
            "_replay", F.col("_target_hash").isNotNull() & (F.col("_target_hash") == F.col("_silver_hash"))
        )
        report_rows = normalized
        quarantined_rows = normalized.filter(
            (F.length("_blocking_codes") > 0) | (F.length("_quarantine_warning_codes") > 0)
        )
        good = normalized.filter((F.length("_blocking_codes") == 0) & (F.length("_quarantine_warning_codes") == 0) & ~F.col("_replay"))
        good = good.dropDuplicates(["_business_event_id", "_silver_hash"])
        event_type = "rating" if source_table == "rating" else "user_tag"
        event_rows = good.select(
            F.col("_business_event_id").alias("event_id"), F.lit(SOURCE_SYSTEM).alias("source_system"),
            F.lit(event_type).alias("event_type"), F.col("_party_id").alias("party_source_id"),
            F.col("_content_id").alias("content_source_id"),
            (F.col("_rating_value") if source_table == "rating" else F.lit(None).cast(DoubleType())).alias("event_value_numeric"),
            F.lit("rating_0_5" if source_table == "rating" else None).cast(StringType()).alias("event_value_unit"),
            (source_column("tag") if source_table == "tag" else F.lit(None).cast(StringType())).alias("event_value_text_raw"),
            (F.col("_tag_normalized") if source_table == "tag" else F.lit(None).cast(StringType())).alias("tag_text_normalized"),
            F.lit("nfc_trim_lower_whitespace_v1" if source_table == "tag" else None).alias("tag_normalization_version"),
            F.col("_event_time_source").alias("event_time_source"), source_column("timestamp").alias("event_time_raw"),
            F.col("_silver_hash").alias("record_hash"), F.col("_batch_id").alias("batch_id"),
            F.col("_source_file").alias("source_file"), F.col("_landing_uri").alias("landing_uri"),
            F.col("_row_number").alias("source_row_number"), F.col("_ingestion_id").alias("bronze_ingestion_id"),
        )
        merged_count = good.count()
        party_candidates = normalized.filter(
            (F.length("_blocking_codes") == 0)
            & (F.length("_quarantine_warning_codes") == 0)
        )
        party_rows = party_candidates.select(
            F.lit(SOURCE_SYSTEM).alias("source_system"), F.col("_party_id").alias("source_party_id"),
            F.col("_batch_id").alias("first_seen_batch"), F.col("_source_file").alias("source_file"),
            F.col("_landing_uri").alias("landing_uri"),
            F.sha2(F.concat_ws("|", F.lit(SOURCE_SYSTEM), F.col("_party_id")), 256).alias("record_hash"),
        ).dropDuplicates(["source_system", "source_party_id"])
        merge_rows(spark, tables["silver_party"], party_rows, ["source_system", "source_party_id"], [])
        merge_rows(spark, tables["silver_interaction_event"], event_rows, ["event_id"], [])

    elif source_table == "movie":
        movies = movie_title_fields(raw, "title")
        movies = movies.withColumn("_movie_id", F.expr("try_cast(`movieId` as bigint)"))
        movies = movies.withColumn("_source_hash", F.col("_record_hash"))
        unknown_genres = F.when(
            source_column("genres") == "(no genres listed)",
            F.expr("CAST(array() AS ARRAY<STRING>)"),
        ).otherwise(
            F.expr("filter(split(`genres`, '\\|'), g -> NOT array_contains(array('Action','Adventure','Animation','Children','Comedy','Crime','Documentary','Drama','Fantasy','Film-Noir','Horror','Musical','Mystery','Romance','Sci-Fi','Thriller','War','Western','IMAX'), trim(g)))")
        )
        movies = movies.withColumn("_unknown_genres", unknown_genres)
        dq_rules = [
            ("MOVIE_ID", "_movie_id IS NULL OR _movie_id <= 0", "BLOCKING", "movieId must be a positive integer."),
            ("MOVIE_REQUIRED", "title IS NULL OR genres IS NULL", "BLOCKING", "Movie title and genres are required; preserve the source row in quarantine."),
            ("UNKNOWN_GENRE", "size(_unknown_genres) > 0", "BLOCKING", "Every ordinary genre must match the documented MovieLens genre list."),
            ("TITLE_YEAR_REVIEW", "_title_parse_status <> 'parsed_terminal_year'", "WARNING", "Missing or ranged release year is retained with parse status for review."),
        ]
        movies = add_dq_codes(movies, dq_rules)
        duplicate_movie_ids = (
            movies.groupBy("_movie_id")
            .agg(F.countDistinct("_source_hash").alias("_movie_hash_count"))
            .where(F.col("_movie_hash_count") > 1)
            .select("_movie_id")
            .withColumn("_duplicate_movie_key", F.lit(True))
        )
        movies = movies.join(duplicate_movie_ids, "_movie_id", "left")
        movies = movies.withColumn(
            "_blocking_codes",
            F.concat_ws("|", "_blocking_codes", F.when(F.col("_duplicate_movie_key"), "MOVIE_KEY_CONFLICT")),
        )
        target = spark.table(tables["silver_content"]).where(F.col("source_system") == SOURCE_SYSTEM).select(
            F.col("source_content_id").alias("_known_movie_id"), F.col("record_hash").alias("_existing_hash")
        )
        movies = movies.join(target, movies["_movie_id"] == target["_known_movie_id"], "left")
        movies = movies.withColumn("_blocking_codes", F.concat_ws("|", "_blocking_codes", F.when(F.col("_existing_hash").isNotNull() & (F.col("_existing_hash") != F.col("_source_hash")), "MOVIE_KEY_CONFLICT")))
        dq_rules.append(("MOVIE_KEY_CONFLICT", "_duplicate_movie_key IS NOT NULL OR (_existing_hash IS NOT NULL AND _existing_hash <> _source_hash)", "BLOCKING", "A movie key with differing payloads is quarantined rather than resolved by row order."))
        report_rows = movies
        quarantined_rows = movies.filter((F.length("_blocking_codes") > 0))
        good = movies.filter(F.length("_blocking_codes") == 0)
        content = good.filter(F.col("_existing_hash").isNull()).select(
            F.lit(SOURCE_SYSTEM).alias("source_system"), F.col("_movie_id").alias("source_content_id"),
            F.col("_title_raw").alias("title_raw"), F.col("_display_title").alias("display_title"),
            F.col("_release_year").alias("release_year"), F.col("_title_parse_status").alias("title_parse_status"),
            source_column("genres").alias("genres_raw"), (source_column("genres") == "(no genres listed)").alias("has_no_genres_listed"),
            F.lit(False).alias("is_deleted"), F.lit(None).cast(StringType()).alias("previous_display_title"),
            F.lit(None).cast(TimestampNTZType()).alias("title_changed_at"), F.lit(None).cast(TimestampNTZType()).alias("source_changed_at"),
            F.col("_batch_id").alias("source_batch_id"), F.col("_source_file").alias("source_file"),
            F.col("_landing_uri").alias("landing_uri"), F.col("_source_hash").alias("record_hash"),
        )
        content = content.dropDuplicates(["source_system", "source_content_id"])
        merged_count = content.count()
        merge_rows(spark, tables["silver_content"], content, ["source_system", "source_content_id"], [])
        genres = (
            good.filter(source_column("genres") != "(no genres listed)")
            .withColumn("genre", F.explode(F.split(source_column("genres"), "\\|")))
            .withColumn("genre", F.trim("genre"))
            .select(
                F.lit(SOURCE_SYSTEM).alias("source_system"), F.col("_movie_id").alias("source_content_id"),
                "genre", F.lit(True).alias("is_current"), F.col("_batch_id").alias("source_batch_id"),
                F.col("_source_file").alias("source_file"), F.col("_landing_uri").alias("landing_uri"),
                F.col("_source_hash").alias("record_hash"),
            )
        )
        merge_rows(spark, tables["silver_content_genre"], genres.dropDuplicates(["source_system", "source_content_id", "genre"]), ["source_system", "source_content_id", "genre"], [])

    elif source_table == "movie_cdc":
        cdc = movie_title_fields(raw, "title")
        cdc = cdc.withColumn("_movie_id", F.expr("try_cast(`movieId` as bigint)"))
        cdc = cdc.withColumn("_changed_at", F.expr("to_timestamp_ntz(`changed_at`, 'yyyy-MM-dd HH:mm:ss')"))
        cdc = cdc.withColumn("_deleted_text", F.lower(F.trim(source_column("is_deleted"))))
        cdc = cdc.withColumn("_deleted", F.col("_deleted_text").isin("true", "1", "yes"))
        cdc = cdc.withColumn("_op", F.upper(source_column("op_type")))
        cdc = cdc.withColumn("_source_hash", F.col("_record_hash"))
        cdc = cdc.withColumn("_cdc_event_id", F.sha2(F.concat_ws("|", F.lit(SOURCE_SYSTEM), F.col("_movie_id"), F.col("_changed_at").cast("string"), F.col("_op"), F.col("_source_hash")), 256))
        known = spark.table(tables["silver_content"]).where(F.col("source_system") == SOURCE_SYSTEM).select(
            F.col("source_content_id").alias("_known_movie_id"), F.col("record_hash").alias("_existing_hash"),
            F.col("source_changed_at").alias("_existing_changed_at"), F.col("display_title").alias("_existing_display_title"),
            F.col("previous_display_title").alias("_existing_previous_display_title"),
            F.col("title_changed_at").alias("_existing_title_changed_at"),
        )
        cdc = cdc.join(known, cdc["_movie_id"] == known["_known_movie_id"], "left")
        dq_rules = [
            ("CDC_REQUIRED", "_movie_id IS NULL OR _movie_id <= 0 OR title IS NULL OR genres IS NULL OR _changed_at IS NULL", "BLOCKING", "CDC requires a positive movieId, title, genres, and parseable changed_at."),
            ("CDC_OPERATION", "COALESCE(_op, '') NOT IN ('I','U','D')", "BLOCKING", "CDC op_type must be I, U, or D."),
            ("CDC_DELETE_FLAG_INVALID", "_deleted_text IS NULL OR _deleted_text NOT IN ('true','false','1','0','yes','no')", "BLOCKING", "CDC is_deleted must be a boolean-like value."),
            ("CDC_DELETE_FLAG", "(_op = 'D' AND NOT _deleted) OR (_op IN ('I','U') AND _deleted)", "BLOCKING", "D must carry is_deleted=true; I/U must carry is_deleted=false."),
            ("CDC_UNKNOWN_GENRE", "size(_unknown_genres) > 0", "BLOCKING", "CDC genres must use the documented MovieLens genre values or sentinel."),
            ("CDC_TITLE_YEAR_REVIEW", "_title_parse_status <> 'parsed_terminal_year'", "WARNING", "Unparseable or ranged title year is retained for review."),
        ]
        cdc = cdc.withColumn(
            "_unknown_genres",
            F.when(source_column("genres") == "(no genres listed)", F.expr("CAST(array() AS ARRAY<STRING>)"))
            .otherwise(F.expr("filter(split(`genres`, '\\|'), g -> NOT array_contains(array('Action','Adventure','Animation','Children','Comedy','Crime','Documentary','Drama','Fantasy','Film-Noir','Horror','Musical','Mystery','Romance','Sci-Fi','Thriller','War','Western','IMAX'), trim(g)))")),
        )
        cdc = add_dq_codes(cdc, dq_rules)
        conflicting_cdc_keys = (
            cdc.groupBy("_movie_id", "_changed_at")
            .agg(F.countDistinct("_source_hash").alias("_cdc_hash_count"))
            .where(F.col("_cdc_hash_count") > 1)
            .select("_movie_id", "_changed_at")
            .withColumn("_duplicate_cdc_key", F.lit(True))
        )
        cdc = cdc.join(conflicting_cdc_keys, ["_movie_id", "_changed_at"], "left")
        cdc = cdc.withColumn("_blocking_codes", F.concat_ws("|", "_blocking_codes", F.when((F.col("_op") == "I") & F.col("_existing_hash").isNotNull() & (F.col("_existing_hash") != F.col("_source_hash")), "CDC_INSERT_EXISTS")))
        cdc = cdc.withColumn("_blocking_codes", F.concat_ws("|", "_blocking_codes", F.when((F.col("_op").isin("U", "D")) & F.col("_existing_hash").isNull(), "CDC_TARGET_MISSING")))
        cdc = cdc.withColumn("_blocking_codes", F.concat_ws("|", "_blocking_codes", F.when(F.col("_existing_changed_at").isNotNull() & (F.col("_changed_at") < F.col("_existing_changed_at")), "CDC_OUT_OF_ORDER")))
        cdc = cdc.withColumn("_blocking_codes", F.concat_ws("|", "_blocking_codes", F.when((F.col("_existing_changed_at") == F.col("_changed_at")) & (F.col("_existing_hash") != F.col("_source_hash")), "CDC_CONFLICT")))
        cdc = cdc.withColumn("_blocking_codes", F.concat_ws("|", "_blocking_codes", F.when(F.col("_duplicate_cdc_key"), "CDC_CONFLICT")))
        cdc = cdc.withColumn("_already_applied", F.col("_existing_hash") == F.col("_source_hash"))
        dq_rules.append(("CDC_CONFLICT", "_duplicate_cdc_key IS NOT NULL OR ((_existing_changed_at = _changed_at) AND (_existing_hash <> _source_hash))", "BLOCKING", "Conflicting CDC payloads for the same movie and effective timestamp are quarantined."))
        dq_rules.extend([
            ("CDC_INSERT_EXISTS", "_op = 'I' AND _existing_hash IS NOT NULL AND _existing_hash <> _source_hash", "BLOCKING", "CDC insert must not replace an existing movie unless it is an exact replay."),
            ("CDC_TARGET_MISSING", "_op IN ('U','D') AND _existing_hash IS NULL", "BLOCKING", "CDC update/delete target must already exist in Silver content."),
            ("CDC_OUT_OF_ORDER", "_existing_changed_at IS NOT NULL AND _changed_at < _existing_changed_at", "BLOCKING", "CDC changes must be applied in changed_at order for each movie."),
        ])
        report_rows = cdc
        quarantined_rows = cdc.filter(F.length("_blocking_codes") > 0)
        # Include exact replays here so dependent Silver genre rows can be repaired if
        # a previous job failed after updating content but before updating its bridge.
        good = cdc.filter(F.length("_blocking_codes") == 0)
        cdc_log = good.select(
            F.col("_cdc_event_id").alias("cdc_event_id"), F.lit(SOURCE_SYSTEM).alias("source_system"),
            F.col("_movie_id").alias("source_content_id"), F.col("_title_raw").alias("title_raw"),
            source_column("genres").alias("genres_raw"), F.col("_deleted").alias("is_deleted"),
            F.col("_op").alias("op_type"), F.col("_changed_at").alias("changed_at"),
            source_column("scenario").alias("scenario"), F.col("_source_hash").alias("record_hash"),
            F.col("_batch_id").alias("batch_id"), F.col("_source_file").alias("source_file"),
            F.col("_landing_uri").alias("landing_uri"), F.col("_row_number").alias("source_row_number"),
        )
        merge_rows(spark, tables["silver_movie_cdc"], cdc_log, ["cdc_event_id"], [])
        content = good.select(
            F.lit(SOURCE_SYSTEM).alias("source_system"), F.col("_movie_id").alias("source_content_id"),
            F.col("_title_raw").alias("title_raw"), F.col("_display_title").alias("display_title"),
            F.col("_release_year").alias("release_year"), F.col("_title_parse_status").alias("title_parse_status"),
            source_column("genres").alias("genres_raw"), (source_column("genres") == "(no genres listed)").alias("has_no_genres_listed"),
            F.col("_deleted").alias("is_deleted"),
            F.when((source_column("scenario") == "scd_type_3_display_title_change") & (F.col("_existing_display_title") != F.col("_display_title")), F.col("_existing_display_title"))
            .otherwise(F.col("_existing_previous_display_title")).alias("previous_display_title"),
            F.when((source_column("scenario") == "scd_type_3_display_title_change") & (F.col("_existing_display_title") != F.col("_display_title")), F.col("_changed_at"))
            .otherwise(F.col("_existing_title_changed_at")).alias("title_changed_at"),
            F.col("_changed_at").alias("source_changed_at"), F.col("_batch_id").alias("source_batch_id"),
            F.col("_source_file").alias("source_file"), F.col("_landing_uri").alias("landing_uri"),
            F.col("_source_hash").alias("record_hash"),
        )
        merged_count = content.count()
        merge_rows(spark, tables["silver_content"], content, ["source_system", "source_content_id"])
        # The current Silver genre bridge follows the current content row. CDC history is
        # preserved separately in silver_movie_cdc for Gold SCD Type 2 processing.
        changed_ids = [int(row[0]) for row in content.select("source_content_id").distinct().collect()]
        for movie_id in changed_ids:
            current = content.where(F.col("source_content_id") == movie_id).select("genres_raw", "is_deleted").first()
            new_genres = current["genres_raw"] if current else None
            is_deleted = bool(current["is_deleted"]) if current else False
            if is_deleted or not new_genres or new_genres == "(no genres listed)":
                spark.sql(f"UPDATE {tables['silver_content_genre']} SET is_current=false WHERE source_content_id={movie_id} AND is_current=true")
            else:
                genre_literals = ",".join("'" + genre.replace("'", "''") + "'" for genre in new_genres.split("|"))
                spark.sql(f"UPDATE {tables['silver_content_genre']} SET is_current=false WHERE source_content_id={movie_id} AND is_current=true AND genre NOT IN ({genre_literals})")
        genres = (
            content.filter(~F.col("has_no_genres_listed"))
            .withColumn("genre", F.explode(F.split("genres_raw", "\\|")))
            .withColumn("genre", F.trim("genre"))
            .select("source_system", "source_content_id", "genre", F.lit(True).alias("is_current"),
                    F.col("source_batch_id"), "source_file", "landing_uri", "record_hash")
        )
        merge_rows(spark, tables["silver_content_genre"], genres, ["source_system", "source_content_id", "genre"], ["is_current", "source_batch_id", "source_file", "landing_uri", "record_hash"])

    elif source_table == "link":
        ids = raw.withColumn("_movie_id", F.expr("try_cast(`movieId` as bigint)"))
        ids = ids.withColumn("_imdb", F.expr("try_cast(`imdbId` as bigint)"))
        ids = ids.withColumn("_tmdb", F.expr("try_cast(`tmdbId` as bigint)"))
        content_ids = spark.table(tables["silver_content"]).select(F.col("source_content_id").alias("_known_movie_id")).distinct()
        ids = ids.join(content_ids, ids["_movie_id"] == content_ids["_known_movie_id"], "left")
        ids = ids.withColumn("_known", F.col("_known_movie_id").isNotNull())
        dq_rules = [
            ("LINK_MOVIE_REFERENCE", "_movie_id IS NULL OR NOT _known", "BLOCKING", "Link movieId must reference Silver content."),
            ("IMDB_ID_REQUIRED", "_imdb IS NULL OR _imdb <= 0", "BLOCKING", "IMDb identifier is required and must be positive."),
            ("TMDB_ID_MISSING", "_tmdb IS NULL", "WARNING", "TMDb ID is optional external enrichment."),
        ]
        ids = add_dq_codes(ids, dq_rules)
        report_rows = ids
        quarantined_rows = ids.filter(F.length("_blocking_codes") > 0)
        good = ids.filter(F.length("_blocking_codes") == 0)
        provider_rows = good.select(
            F.lit(SOURCE_SYSTEM).alias("source_system"), F.col("_movie_id").alias("source_content_id"),
            F.lit("imdb").alias("provider"), F.lpad(F.col("_imdb").cast("string"), 7, "0").alias("provider_id"),
            F.concat(F.lit("https://www.imdb.com/title/tt"), F.lpad(F.col("_imdb").cast("string"), 7, "0"), F.lit("/")).alias("provider_url"),
            F.col("_record_hash").alias("record_hash"), F.col("_batch_id").alias("batch_id"), F.col("_source_file").alias("source_file"), F.col("_landing_uri").alias("landing_uri"),
        ).unionByName(good.filter(F.col("_tmdb").isNotNull()).select(
            F.lit(SOURCE_SYSTEM).alias("source_system"), F.col("_movie_id").alias("source_content_id"),
            F.lit("tmdb").alias("provider"), F.col("_tmdb").cast("string").alias("provider_id"),
            F.concat(F.lit("https://www.themoviedb.org/movie/"), F.col("_tmdb").cast("string")).alias("provider_url"),
            F.col("_record_hash").alias("record_hash"), F.col("_batch_id").alias("batch_id"), F.col("_source_file").alias("source_file"), F.col("_landing_uri").alias("landing_uri"),
        ))
        merged_count = provider_rows.count()
        merge_rows(spark, tables["silver_external_content_id"], provider_rows, ["source_system", "source_content_id", "provider"], [])

    elif source_table == "genome_tags":
        tags = raw.withColumn("_tag_id", F.expr("try_cast(`tagId` as bigint)"))
        tag_collisions = tags.groupBy("_tag_id").agg(F.countDistinct("_record_hash").alias("_hash_count")).where(F.col("_hash_count") > 1).select("_tag_id").withColumn("_in_batch_conflict", F.lit(True))
        tags = tags.join(tag_collisions, "_tag_id", "left")
        known_tags = spark.table(tables["silver_signal_taxonomy"]).select(F.col("source_taxonomy_id").alias("_known_tag_id"), F.col("record_hash").alias("_existing_hash"))
        tags = tags.join(known_tags, tags["_tag_id"] == known_tags["_known_tag_id"], "left")
        dq_rules = [
            ("TAXONOMY_REQUIRED", "_tag_id IS NULL OR _tag_id <= 0 OR tag IS NULL OR length(trim(tag)) = 0", "BLOCKING", "Genome taxonomy requires a positive tagId and a nonblank label."),
            ("TAXONOMY_KEY_CONFLICT", "_in_batch_conflict IS NOT NULL OR (_existing_hash IS NOT NULL AND _existing_hash <> _record_hash)", "BLOCKING", "A taxonomy ID with a different label/hash is quarantined."),
        ]
        tags = add_dq_codes(tags, dq_rules)
        report_rows = tags
        quarantined_rows = tags.filter(F.length("_blocking_codes") > 0)
        good = tags.filter(F.length("_blocking_codes") == 0).select(
            F.lit(SOURCE_SYSTEM).alias("source_system"), F.col("_tag_id").alias("source_taxonomy_id"),
            source_column("tag").alias("signal_name"), F.col("_record_hash").alias("record_hash"),
            F.col("_batch_id").alias("batch_id"), F.col("_source_file").alias("source_file"), F.col("_landing_uri").alias("landing_uri"),
        )
        good = good.dropDuplicates(["source_system", "source_taxonomy_id", "record_hash"])
        merged_count = good.count()
        merge_rows(spark, tables["silver_signal_taxonomy"], good, ["source_system", "source_taxonomy_id"], [])

    elif source_table == "genome_scores":
        scores = raw.withColumn("_movie_id", F.expr("try_cast(`movieId` as bigint)"))
        scores = scores.withColumn("_tag_id", F.expr("try_cast(`tagId` as bigint)"))
        scores = scores.withColumn("_relevance", F.expr("try_cast(`relevance` as double)"))
        content_ids = spark.table(tables["silver_content"]).select("source_content_id").distinct()
        taxonomy_ids = spark.table(tables["silver_signal_taxonomy"]).select("source_taxonomy_id").distinct()
        scores = scores.join(content_ids.withColumnRenamed("source_content_id", "_known_movie"), F.col("_movie_id") == F.col("_known_movie"), "left")
        scores = scores.join(taxonomy_ids.withColumnRenamed("source_taxonomy_id", "_known_tag"), F.col("_tag_id") == F.col("_known_tag"), "left")
        score_collisions = scores.groupBy("_movie_id", "_tag_id").agg(F.countDistinct("_record_hash").alias("_hash_count")).where(F.col("_hash_count") > 1).select("_movie_id", "_tag_id").withColumn("_in_batch_conflict", F.lit(True))
        scores = scores.join(score_collisions, ["_movie_id", "_tag_id"], "left")
        existing_scores = spark.table(tables["silver_content_tag_signal"]).select(
            F.col("source_content_id").alias("_existing_movie_id"),
            F.col("source_taxonomy_id").alias("_existing_tag_id"),
            F.col("record_hash").alias("_existing_hash"),
        )
        scores = scores.join(existing_scores, (scores["_movie_id"] == existing_scores["_existing_movie_id"]) & (scores["_tag_id"] == existing_scores["_existing_tag_id"]), "left")
        dq_rules = [
            ("GENOME_CONTENT_REFERENCE", "_known_movie IS NULL", "BLOCKING", "Genome movieId must reference Silver content."),
            ("GENOME_TAXONOMY_REFERENCE", "_known_tag IS NULL", "BLOCKING", "Genome tagId must reference Silver taxonomy."),
            ("GENOME_RELEVANCE", "_relevance IS NULL OR _relevance < 0 OR _relevance > 1", "BLOCKING", "Genome relevance must be in the inclusive range [0,1]."),
            ("GENOME_SCORE_KEY_CONFLICT", "_in_batch_conflict IS NOT NULL OR (_existing_hash IS NOT NULL AND _existing_hash <> _record_hash)", "BLOCKING", "A movie/tag genome score key with a different record hash is quarantined."),
        ]
        scores = add_dq_codes(scores, dq_rules)
        report_rows = scores
        quarantined_rows = scores.filter(F.length("_blocking_codes") > 0)
        good = scores.filter(F.length("_blocking_codes") == 0).select(
            F.lit(SOURCE_SYSTEM).alias("source_system"), F.col("_movie_id").alias("source_content_id"),
            F.col("_tag_id").alias("source_taxonomy_id"), F.col("_relevance").alias("relevance"),
            F.lit("relevance_0_1").alias("signal_unit"), F.col("_record_hash").alias("record_hash"),
            F.col("_batch_id").alias("batch_id"), F.col("_source_file").alias("source_file"),
            F.col("_landing_uri").alias("landing_uri"), F.col("_row_number").alias("source_row_number"),
        )
        good = good.dropDuplicates(["source_system", "source_content_id", "source_taxonomy_id", "record_hash"])
        merged_count = good.count()
        merge_rows(spark, tables["silver_content_tag_signal"], good, ["source_system", "source_content_id", "source_taxonomy_id"], [])
    else:
        raise ValueError(f"Unsupported Silver source_table: {source_table}")

    quarantined_count = 0
    if quarantined_rows is not None:
        # Preserve the raw Bronze columns in the quarantine payload and make the reason explicit.
        if "_blocking_codes" not in quarantined_rows.columns:
            quarantined_rows = quarantined_rows.withColumn("_blocking_codes", F.lit("DQ_BLOCKING"))
        if "_quarantine_warning_codes" not in quarantined_rows.columns:
            quarantined_rows = quarantined_rows.withColumn("_quarantine_warning_codes", F.lit(""))
        if "_ingestion_id" not in quarantined_rows.columns:
            quarantined_rows = quarantined_rows.withColumn("_ingestion_id", F.col("_record_hash"))
        quarantined_count = write_quarantine(
            spark, tables["silver_quarantine"], quarantined_rows,
            batch_id, source_table, landing_uri, dq_rules,
        )
    if dq_rules and report_rows is not None:
        write_dq_results(
            spark, tables["silver_dq_result"], identity, batch_id, source_table,
            landing_uri, read_count, report_rows, dq_rules,
        )

    blocking_failures = quarantined_rows.filter(F.length("_blocking_codes") > 0).count() if quarantined_rows is not None else 0
    status = "BLOCKED" if blocking_failures else "SUCCEEDED"
    control_schema = StructType([
        StructField("batch_key", StringType(), False), StructField("run_id", StringType(), False),
        StructField("source_table", StringType(), False), StructField("batch_id", StringType(), False),
        StructField("landing_uri", StringType(), False), StructField("status", StringType(), False),
        StructField("rows_read", LongType(), False), StructField("rows_merged", LongType(), False),
        StructField("rows_quarantined", LongType(), False), StructField("blocking_failures", LongType(), False),
        StructField("started_at", TimestampNTZType(), False), StructField("completed_at", TimestampNTZType(), False),
        StructField("error_message", StringType(), True),
    ])
    control_record = [(identity, run_id, source_table, batch_id, landing_uri, status, read_count, merged_count, quarantined_count, blocking_failures, started, datetime.now(timezone.utc).replace(tzinfo=None), None if not blocking_failures else "Blocking DQ rows were quarantined; see silver_quarantine and silver_dq_result.")]
    merge_rows(spark, tables["silver_batch_control"], spark.createDataFrame(control_record, control_schema), ["batch_key"])
    if blocking_failures:
        raise RuntimeError(f"Blocking Silver DQ failed for {source_table}/{batch_id}: {blocking_failures} rows quarantined")
    print(f"Silver {source_table}/{batch_id}: read={read_count}, merged={merged_count}, quarantined={quarantined_count}, status={status}")


if __name__ == "__main__":
    main()
