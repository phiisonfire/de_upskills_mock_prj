"""Build and incrementally merge dimensional Gold tables from Silver Iceberg."""

import hashlib
import sys
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampNTZType


ARGS = ["JOB_NAME", "run_id", "source_table", "batch_id", "landing_uri", "database_name", "warehouse_uri"]
SYSTEM = "movielens"
BASELINE = "1900-01-01 00:00:00"
GENRES = [
    "Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary",
    "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance",
    "Sci-Fi", "Thriller", "War", "Western", "IMAX", "NO_GENRE_LISTED",
]


def q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def ensure_table(spark: SparkSession, db: str, name: str, columns: str, root: str) -> str:
    table = f"glue_catalog.{db}.{name}"
    if name == "gold_batch_control":
        location = f"{root.rstrip('/').rsplit('/gold', 1)[0]}/control/iceberg/{name}/"
    else:
        location = f"{root.rstrip('/')}/{name}/"
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({columns}) USING iceberg "
        f"LOCATION '{location}' TBLPROPERTIES ('format-version'='2')"
    )
    return table


def merge(spark: SparkSession, table: str, rows: DataFrame, keys: list[str]) -> None:
    if not rows.columns:
        return
    rows.createOrReplaceTempView("gold_merge_source")
    condition = " AND ".join(f"t.{q(key)} = s.{q(key)}" for key in keys)
    columns = ", ".join(q(column) for column in rows.columns)
    values = ", ".join(f"s.{q(column)}" for column in rows.columns)
    updates = ", ".join(f"t.{q(column)} = s.{q(column)}" for column in rows.columns if column not in keys)
    update_sql = f"WHEN MATCHED THEN UPDATE SET {updates} " if updates else ""
    spark.sql(
        f"MERGE INTO {table} t USING gold_merge_source s ON {condition} "
        f"{update_sql}WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})"
    )


def build_tables(spark: SparkSession, db: str, root: str) -> dict[str, str]:
    definitions = {
        "gold_dim_content": "content_sk string, source_system string, source_content_id bigint, display_title string, title_raw string, release_year int, title_parse_status string, genres_raw string, has_no_genres_listed boolean, is_deleted boolean, is_inferred boolean, previous_display_title string, title_changed_at timestamp_ntz, effective_from timestamp_ntz, effective_to timestamp_ntz, is_current boolean, version int, scd_change_type string, record_hash string, batch_id string, source_file string, landing_uri string",
        "gold_dim_party": "party_sk string, source_system string, source_party_id bigint, first_seen_batch string, record_hash string, batch_id string, source_file string, landing_uri string",
        "gold_dim_date": "date_key int, calendar_date date, calendar_year int, calendar_quarter int, calendar_month int, month_name string, day_of_month int, day_of_week int, iso_week int",
        "gold_dim_genre": "genre_sk string, genre string, is_sentinel boolean",
        "gold_bridge_content_genre": "content_sk string, genre_sk string, genre string, is_no_genres_listed boolean, effective_from timestamp_ntz, effective_to timestamp_ntz, is_current boolean, record_hash string, batch_id string, source_file string, landing_uri string",
        "gold_dim_signal_tag": "signal_tag_sk string, source_system string, source_taxonomy_id bigint, signal_name string, record_hash string, batch_id string, source_file string, landing_uri string",
        "gold_dim_external_content_id": "external_content_id_sk string, source_system string, source_content_id bigint, provider string, provider_id string, provider_url string, record_hash string, batch_id string, source_file string, landing_uri string",
        "gold_fact_rating": "rating_event_sk string, source_event_id string, content_sk string, party_sk string, date_key int, rating double, rating_percent double, event_time_source timestamp_ntz, event_time_raw string, source_system string, source_content_id bigint, source_party_id bigint, batch_id string, source_file string, landing_uri string, record_hash string",
        "gold_fact_user_tag": "tag_event_sk string, source_event_id string, content_sk string, party_sk string, date_key int, tag_text_raw string, tag_text_normalized string, tag_normalization_version string, event_time_source timestamp_ntz, event_time_raw string, source_system string, source_content_id bigint, source_party_id bigint, batch_id string, source_file string, landing_uri string, record_hash string",
        "gold_fact_genome_score": "genome_score_sk string, content_sk string, signal_tag_sk string, source_content_id bigint, source_taxonomy_id bigint, relevance double, signal_unit string, batch_id string, source_file string, landing_uri string, record_hash string",
        "gold_batch_control": "batch_key string, run_id string, source_table string, batch_id string, landing_uri string, status string, rows_merged bigint, started_at timestamp_ntz, completed_at timestamp_ntz",
    }
    return {name: ensure_table(spark, db, name, columns, root) for name, columns in definitions.items()}


def make_content_history(spark: SparkSession, db: str, tables: dict[str, str]) -> DataFrame:
    """Reconstruct Type 2 genre/delete states; apply current Type 1/3 title attributes."""
    base = spark.table(f"glue_catalog.{db}.bronze_movie").select(
        F.lit(SYSTEM).alias("source_system"), F.expr("try_cast(`movieId` as bigint)").alias("source_content_id"),
        F.col("title").alias("title_raw"), F.col("genres").alias("genres_raw"),
        F.lit(False).alias("is_deleted"), F.to_timestamp_ntz(F.lit(BASELINE)).alias("effective_from"),
        F.col("_record_hash").alias("record_hash"), F.col("_batch_id").alias("batch_id"),
        F.col("_source_file").alias("source_file"), F.col("_landing_uri").alias("landing_uri"),
        F.lit("BASELINE").alias("scd_change_type"),
    )
    changes = spark.table(tables["silver_movie_cdc"])
    order = Window.partitionBy("source_content_id").orderBy("changed_at", "cdc_event_id")
    original_genres = spark.table(f"glue_catalog.{db}.bronze_movie").select(
        F.expr("try_cast(`movieId` as bigint)").alias("source_content_id"), F.col("genres").alias("_baseline_genres")
    )
    changed = changes.withColumn("_previous_genres", F.lag("genres_raw").over(order)).join(original_genres, "source_content_id", "left").where(
        (F.col("op_type") == "I") | (F.col("op_type") == "D") |
        (F.col("_previous_genres").isNotNull() & (F.col("genres_raw") != F.col("_previous_genres"))) |
        (F.col("_previous_genres").isNull() & F.col("_baseline_genres").isNotNull() & (F.col("genres_raw") != F.col("_baseline_genres")))
    )
    changed = changed.select(
        "source_system", "source_content_id", F.col("title_raw"), "genres_raw", "is_deleted",
        F.col("changed_at").alias("effective_from"), "record_hash", "batch_id", "source_file", "landing_uri",
        F.when(F.col("op_type") == "I", "INSERT")
        .when(F.col("op_type") == "D", "SOFT_DELETE").otherwise("GENRE_CHANGE").alias("scd_change_type"),
    )
    states = base.unionByName(changed)
    latest = spark.table(tables["silver_content"]).select(
        "source_system", "source_content_id", "display_title", "release_year", "title_parse_status",
        "previous_display_title", "title_changed_at",
    )
    states = states.join(latest, ["source_system", "source_content_id"], "left")
    states = states.withColumn("has_no_genres_listed", F.col("genres_raw") == "(no genres listed)")
    states = states.withColumn("_state_hash", F.sha2(F.concat_ws("|", "source_system", "source_content_id", "effective_from", "genres_raw", "is_deleted"), 256))
    states = states.dropDuplicates(["_state_hash"])
    timeline = Window.partitionBy("source_system", "source_content_id").orderBy("effective_from", "_state_hash")
    states = states.withColumn("effective_to", F.lead("effective_from").over(timeline))
    states = states.withColumn("is_current", F.col("effective_to").isNull())
    states = states.withColumn("version", F.row_number().over(timeline).cast("int"))
    states = states.withColumn("content_sk", F.sha2(F.concat_ws("|", "source_system", "source_content_id", "effective_from", "_state_hash"), 256))
    return states.withColumn("is_inferred", F.lit(False)).select(
        "content_sk", "source_system", "source_content_id", "display_title", "title_raw", "release_year",
        "title_parse_status", "genres_raw", "has_no_genres_listed", "is_deleted", "is_inferred",
        "previous_display_title", "title_changed_at", "effective_from", "effective_to", "is_current",
        "version", "scd_change_type", "record_hash", "batch_id", "source_file", "landing_uri",
    )


def merge_dimensions(spark: SparkSession, db: str, tables: dict[str, str], source_table: str) -> None:
    history = make_content_history(spark, db, tables)
    merge(spark, tables["gold_dim_content"], history, ["content_sk"])
    parties = spark.table(tables["silver_party"]).select(
        F.sha2(F.concat_ws("|", "source_system", "source_party_id"), 256).alias("party_sk"),
        "source_system", "source_party_id", "first_seen_batch", "record_hash",
        F.col("first_seen_batch").alias("batch_id"), "source_file", "landing_uri",
    )
    merge(spark, tables["gold_dim_party"], parties, ["party_sk"])
    genres = spark.createDataFrame([(name,) for name in GENRES], ["genre"]).withColumn(
        "genre_sk", F.sha2(F.concat(F.lit("genre|"), F.col("genre")), 256)
    ).withColumn("is_sentinel", F.col("genre") == "NO_GENRE_LISTED").select("genre_sk", "genre", "is_sentinel")
    merge(spark, tables["gold_dim_genre"], genres, ["genre_sk"])
    signal = spark.table(tables["silver_signal_taxonomy"]).select(
        F.sha2(F.concat_ws("|", "source_system", "source_taxonomy_id"), 256).alias("signal_tag_sk"),
        "source_system", "source_taxonomy_id", "signal_name", "record_hash", "batch_id", "source_file", "landing_uri",
    )
    merge(spark, tables["gold_dim_signal_tag"], signal, ["signal_tag_sk"])
    external = spark.table(tables["silver_external_content_id"]).select(
        F.sha2(F.concat_ws("|", "source_system", "source_content_id", "provider"), 256).alias("external_content_id_sk"),
        "source_system", "source_content_id", "provider", "provider_id", "provider_url", "record_hash", "batch_id", "source_file", "landing_uri",
    )
    merge(spark, tables["gold_dim_external_content_id"], external, ["external_content_id_sk"])
    # Use separate branches to avoid exploding the sentinel into a normal genre.
    ordinary = history.where(~F.col("has_no_genres_listed")).withColumn("genre", F.explode(F.split("genres_raw", "\\|"))).withColumn("genre", F.trim("genre"))
    sentinel = history.where(F.col("has_no_genres_listed")).withColumn("genre", F.lit("NO_GENRE_LISTED"))
    genre_bridge = ordinary.unionByName(sentinel).join(
        genres.select("genre", "genre_sk"), "genre", "inner"
    ).select("content_sk", "genre_sk", "genre", F.col("has_no_genres_listed").alias("is_no_genres_listed"), "effective_from", "effective_to", "is_current", "record_hash", "batch_id", "source_file", "landing_uri")
    merge(spark, tables["gold_bridge_content_genre"], genre_bridge.dropDuplicates(["content_sk", "genre_sk"]), ["content_sk", "genre_sk"])


def merge_dates(spark: SparkSession, table: str, events: DataFrame) -> None:
    bounds = events.agg(F.min(F.to_date("event_time_source")).alias("lo"), F.max(F.to_date("event_time_source")).alias("hi")).first()
    if not bounds["lo"]:
        return
    dates = spark.range(1).select(F.explode(F.sequence(F.lit(bounds["lo"]), F.lit(bounds["hi"]), F.expr("interval 1 day"))).alias("calendar_date"))
    rows = dates.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"), "calendar_date",
        F.year("calendar_date").alias("calendar_year"), F.quarter("calendar_date").alias("calendar_quarter"),
        F.month("calendar_date").alias("calendar_month"), F.date_format("calendar_date", "MMMM").alias("month_name"),
        F.dayofmonth("calendar_date").alias("day_of_month"), F.dayofweek("calendar_date").alias("day_of_week"),
        F.weekofyear("calendar_date").alias("iso_week"),
    )
    merge(spark, table, rows, ["date_key"])


def ensure_inferred_members(spark: SparkSession, tables: dict[str, str], events: DataFrame) -> None:
    content_keys = spark.table(tables["gold_dim_content"]).select("source_system", "source_content_id").distinct()
    candidates = events.select("source_system", "content_source_id", "batch_id", "source_file", "landing_uri", "record_hash").distinct().alias("e")
    content_keys = content_keys.alias("c")
    missing_content = candidates.join(content_keys,
        (F.col("e.source_system") == F.col("c.source_system")) & (F.col("e.content_source_id") == F.col("c.source_content_id")), "left_anti")
    missing_content = missing_content.select("e.*")
    # Resolve a source-unique inferred member so facts retain valid foreign keys.
    inferred_content = missing_content.select(
        F.sha2(F.concat_ws("|", "source_system", "content_source_id", F.lit("inferred")), 256).alias("content_sk"),
        "source_system", F.col("content_source_id").alias("source_content_id"), F.lit(None).cast("string").alias("display_title"),
        F.lit(None).cast("string").alias("title_raw"), F.lit(None).cast("int").alias("release_year"),
        F.lit("inferred_member").alias("title_parse_status"), F.lit(None).cast("string").alias("genres_raw"),
        F.lit(False).alias("has_no_genres_listed"), F.lit(False).alias("is_deleted"), F.lit(True).alias("is_inferred"),
        F.lit(None).cast("string").alias("previous_display_title"), F.lit(None).cast(TimestampNTZType()).alias("title_changed_at"),
        F.to_timestamp_ntz(F.lit(BASELINE)).alias("effective_from"), F.lit(None).cast(TimestampNTZType()).alias("effective_to"),
        F.lit(True).alias("is_current"), F.lit(1).cast("int").alias("version"), F.lit("INFERRED").alias("scd_change_type"),
        "record_hash", "batch_id", "source_file", "landing_uri",
    )
    merge(spark, tables["gold_dim_content"], inferred_content, ["content_sk"])
    party_keys = spark.table(tables["gold_dim_party"]).select("source_system", "source_party_id").distinct()
    party_candidates = events.select("source_system", "party_source_id", "batch_id", "source_file", "landing_uri", "record_hash").distinct().alias("e")
    missing_party = party_candidates.join(party_keys.alias("p"),
        (F.col("e.source_system") == F.col("p.source_system")) & (F.col("e.party_source_id") == F.col("p.source_party_id")), "left_anti")
    missing_party = missing_party.select("e.*")
    inferred_party = missing_party.select(
        F.sha2(F.concat_ws("|", "source_system", "party_source_id"), 256).alias("party_sk"),
        "source_system", F.col("party_source_id").alias("source_party_id"), F.col("batch_id").alias("first_seen_batch"),
        "record_hash", "batch_id", "source_file", "landing_uri",
    )
    merge(spark, tables["gold_dim_party"], inferred_party, ["party_sk"])


def process_interactions(spark: SparkSession, tables: dict[str, str], selector: str, batch_id: str, landing_uri: str) -> int:
    silver = spark.table(tables["silver_interaction_event"])
    if selector == "bootstrap":
        events = silver
    elif selector == "movie_cdc":
        affected = spark.table(tables["silver_movie_cdc"]).where(F.col("batch_id") == batch_id).select("source_content_id").distinct()
        events = silver.join(affected, silver["content_source_id"] == affected["source_content_id"], "inner")
    else:
        events = silver.where((F.col("batch_id") == batch_id) & (F.col("landing_uri") == landing_uri))
    if selector in ("rating", "bootstrap", "movie_cdc"):
        ratings = events.where(F.col("event_type") == "rating")
        merge_dates(spark, tables["gold_dim_date"], ratings)
        ensure_inferred_members(spark, tables, ratings)
        contents = spark.table(tables["gold_dim_content"]).select("content_sk", "source_system", "source_content_id", "effective_from", "effective_to")
        parties = spark.table(tables["gold_dim_party"]).select("party_sk", "source_system", "source_party_id")
        resolved = ratings.alias("e").join(contents.alias("c"),
            (F.col("e.source_system") == F.col("c.source_system")) & (F.col("e.content_source_id") == F.col("c.source_content_id")) &
            (F.col("c.effective_from") <= F.col("e.event_time_source")) & (F.col("c.effective_to").isNull() | (F.col("e.event_time_source") < F.col("c.effective_to"))), "left")
        resolved = resolved.join(parties.alias("p"), (F.col("e.source_system") == F.col("p.source_system")) & (F.col("e.party_source_id") == F.col("p.source_party_id")), "left")
        fact = resolved.select(
            F.sha2(F.concat(F.lit("rating|"), F.col("e.event_id")), 256).alias("rating_event_sk"), F.col("e.event_id").alias("source_event_id"),
            "c.content_sk", "p.party_sk", F.date_format(F.col("e.event_time_source"), "yyyyMMdd").cast("int").alias("date_key"),
            F.col("e.event_value_numeric").alias("rating"), (F.col("e.event_value_numeric") * F.lit(20.0)).alias("rating_percent"),
            F.col("e.event_time_source"), F.col("e.event_time_raw"), F.col("e.source_system"), F.col("e.content_source_id"), F.col("e.party_source_id"),
            F.col("e.batch_id"), F.col("e.source_file"), F.col("e.landing_uri"), F.col("e.record_hash"),
        )
        merge(spark, tables["gold_fact_rating"], fact, ["rating_event_sk"])
    if selector in ("tag", "bootstrap", "movie_cdc"):
        tags = events.where(F.col("event_type") == "user_tag")
        merge_dates(spark, tables["gold_dim_date"], tags)
        ensure_inferred_members(spark, tables, tags)
        contents = spark.table(tables["gold_dim_content"]).select("content_sk", "source_system", "source_content_id", "effective_from", "effective_to")
        parties = spark.table(tables["gold_dim_party"]).select("party_sk", "source_system", "source_party_id")
        resolved = tags.alias("e").join(contents.alias("c"),
            (F.col("e.source_system") == F.col("c.source_system")) & (F.col("e.content_source_id") == F.col("c.source_content_id")) &
            (F.col("c.effective_from") <= F.col("e.event_time_source")) & (F.col("c.effective_to").isNull() | (F.col("e.event_time_source") < F.col("c.effective_to"))), "left")
        resolved = resolved.join(parties.alias("p"), (F.col("e.source_system") == F.col("p.source_system")) & (F.col("e.party_source_id") == F.col("p.source_party_id")), "left")
        fact = resolved.select(
            F.sha2(F.concat(F.lit("tag|"), F.col("e.event_id")), 256).alias("tag_event_sk"), F.col("e.event_id").alias("source_event_id"),
            "c.content_sk", "p.party_sk", F.date_format(F.col("e.event_time_source"), "yyyyMMdd").cast("int").alias("date_key"),
            F.col("e.event_value_text_raw").alias("tag_text_raw"), F.col("e.tag_text_normalized"), F.col("e.tag_normalization_version"),
            F.col("e.event_time_source"), F.col("e.event_time_raw"), F.col("e.source_system"), F.col("e.content_source_id"), F.col("e.party_source_id"),
            F.col("e.batch_id"), F.col("e.source_file"), F.col("e.landing_uri"), F.col("e.record_hash"),
        )
        merge(spark, tables["gold_fact_user_tag"], fact, ["tag_event_sk"])
    return events.count()


def process_genome_scores(spark: SparkSession, tables: dict[str, str], selector: str, batch_id: str, landing_uri: str) -> int:
    scores = spark.table(tables["silver_content_tag_signal"])
    if selector in ("genome_scores",):
        scores = scores.where((F.col("batch_id") == batch_id) & (F.col("landing_uri") == landing_uri))
    elif selector == "movie_cdc":
        affected = spark.table(tables["silver_movie_cdc"]).where(F.col("batch_id") == batch_id).select("source_content_id").distinct()
        scores = scores.join(affected, scores["source_content_id"] == affected["source_content_id"], "inner").drop(affected["source_content_id"])
    content = spark.table(tables["gold_dim_content"]).where(F.col("is_current")).select("source_system", F.col("source_content_id").alias("_movie"), "content_sk")
    taxonomy = spark.table(tables["gold_dim_signal_tag"]).select("source_system", F.col("source_taxonomy_id").alias("_tag"), "signal_tag_sk")
    fact = scores.join(content, (scores["source_system"] == content["source_system"]) & (scores["source_content_id"] == content["_movie"]), "inner")
    fact = fact.join(taxonomy, (fact["source_system"] == taxonomy["source_system"]) & (fact["source_taxonomy_id"] == taxonomy["_tag"]), "inner")
    fact = fact.select(
        F.sha2(F.concat_ws("|", F.lit(SYSTEM), "source_content_id", "source_taxonomy_id"), 256).alias("genome_score_sk"), "content_sk", "signal_tag_sk",
        "source_content_id", "source_taxonomy_id", "relevance", "signal_unit", "batch_id", "source_file", "landing_uri", "record_hash",
    )
    merge(spark, tables["gold_fact_genome_score"], fact, ["genome_score_sk"])
    return fact.count()


def main() -> None:
    args = getResolvedOptions(sys.argv, ARGS)
    selector, batch_id, landing_uri, run_id = args["source_table"], args["batch_id"], args["landing_uri"], args["run_id"]
    db = args["database_name"]
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    tables = build_tables(spark, db, args["warehouse_uri"])
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    rows_merged = 0
    if selector in ("bootstrap", "movie", "movie_cdc", "rating", "tag", "link", "genome_tags", "genome_scores"):
        merge_dimensions(spark, db, tables, selector)
    if selector in ("bootstrap", "rating", "tag", "movie_cdc"):
        rows_merged += process_interactions(spark, tables, selector, batch_id, landing_uri)
    if selector in ("bootstrap", "genome_scores", "movie_cdc"):
        rows_merged += process_genome_scores(spark, tables, selector, batch_id, landing_uri)
    identity = hashlib.sha256(f"{run_id}\0{selector}\0{batch_id}\0{landing_uri}".encode()).hexdigest()
    control_schema = StructType([
        StructField("batch_key", StringType(), False), StructField("run_id", StringType(), False),
        StructField("source_table", StringType(), False), StructField("batch_id", StringType(), False),
        StructField("landing_uri", StringType(), False), StructField("status", StringType(), False),
        StructField("rows_merged", LongType(), False), StructField("started_at", TimestampNTZType(), False),
        StructField("completed_at", TimestampNTZType(), False),
    ])
    record = [(identity, run_id, selector, batch_id, landing_uri, "SUCCEEDED", rows_merged, started, datetime.now(timezone.utc).replace(tzinfo=None))]
    merge(spark, tables["gold_batch_control"], spark.createDataFrame(record, control_schema), ["batch_key"])
    print(f"Gold {selector}/{batch_id}: rows_merged={rows_merged}; status=SUCCEEDED")


if __name__ == "__main__":
    main()
