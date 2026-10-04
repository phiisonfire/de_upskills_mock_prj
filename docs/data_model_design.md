# MovieLens Data Model and Medallion Design

## 1. Purpose and design basis

This design translates the MovieLens 20M source files into an auditable lakehouse and a source-independent interaction model. It uses the assignment's contracts: immutable Landing, append-only Bronze, incremental/validated Silver, and dimensional Gold with SCD and point-in-time queries.

The design is based on the generated profiles in `data/investigation/`:

- `rating`: 20,000,263 rows; inferred `userId`/`movieId` Int64, `rating` Float64, `timestamp` String; no profiled nulls. `(userId, movieId)` has 20,000,263 distinct keys and no duplicates.
- `tag`: 465,564 rows; inferred IDs Int64, `tag` and `timestamp` String; no profiled nulls. `(userId, movieId, tag, timestamp)` is unique in this snapshot.
- `movie`: 27,278 rows; `movieId` Int64, `title`/`genres` String; no profiled nulls. `movieId` unique.
- `link`: 27,278 rows; `movieId`, `imdbId`, `tmdbId` inferred Int64. `movieId` unique and complete relative to movie in both directions; `tmdbId` has 252 nulls; `imdbId` has no profiled nulls.
- `genome_scores`: 11,709,768 rows; IDs Int64, relevance Float64; no profiled nulls. `(movieId, tagId)` unique; observed relevance range 0.00025–1.0.
- `genome_tags`: 1,128 rows; unique `tagId`, non-null string `tag`.
- All profiled foreign-key anti-joins returned zero orphan rows.
- Rating values range from 0.5 to 5.0; timestamps are strings in the source and lexically range from `1995-01-09 11:46:44` to `2015-03-31 06:40:02`.
- Movie title profiling found year parsing exceptions; the inspected unmatched-title output includes missing-year titles, year ranges, and malformed parentheses. Preserve and flag these rather than dropping the records.

Profiles do not yet establish timestamp timezone, detailed rating distribution/skew, exact invalid rating increments, all genre frequencies, exact duplicate rows for every table beyond the stated candidate keys, or the real-world validity of identifiers. Do not claim these checks passed until measured. In particular, profile results describe this static snapshot; the synthetic future CDC batches need their own checks.

## 2. Source-to-CDM mapping

### 2.1 Core CDM concepts

Use source-scoped identifiers so values from separate providers cannot collide. A source crosswalk can later resolve multiple source IDs to one enterprise entity.

| CDM entity | Grain / key | Purpose |
|---|---|---|
| `Content` | One source content record; `(source_system, source_content_id)` | Conformed movie identity and source-independent content attributes. MovieLens `movieId` maps to `source_content_id`. |
| `Party` | One source user identity; `(source_system, source_party_id)` | Actor who rates or submits tags. MovieLens `userId` maps to `source_party_id`; no PII is present in this dataset. |
| `InteractionEvent` | One user-submitted event; `event_id` | Common representation for rating and user-tag events. Keep event types distinct. |
| `ContentGenre` | One content-to-genre association | Normalize the pipe-delimited genre list into a bridge. Preserve an explicit `NO_GENRE_LISTED` state for the sentinel. |
| `ExternalContentId` | One provider identifier per content/provider | IMDb and TMDb IDs/URLs, nullable when absent. |
| `ContentTagSignal` | One `(content, signal taxonomy tag)` measurement | Algorithm-derived genome relevance. Separate from user-submitted tags. |

### 2.2 Field mapping

| Source field | Silver/CDM field | Transformation and rule |
|---|---|---|
| `rating.userId` | `party_source_id` | Cast to Int64; pair with `source_system='movielens'`. Do not treat numeric IDs as globally unique. |
| `rating.movieId` | `content_source_id` | Cast to Int64; pair with source system; validate against content key. |
| `rating.rating` | `event_value_numeric`, `event_value_unit='rating_0_5'` and optionally `rating_percent` | Preserve source value on 0.5–5.0 scale. For cross-source comparison, normalize to 0–100 by `rating * 20`; retain original value and scale. Do not replace source value with normalized value. |
| `rating.timestamp` | `event_time_utc` plus `event_time_source` | Parse exact `%Y-%m-%d %H:%M:%S`; confirm the source timezone before labeling UTC. Preserve the raw source timestamp. |
| `tag.userId`, `tag.movieId` | `party_source_id`, `content_source_id` | Same source-scoped key rules as ratings. |
| `tag.tag` | `event_value_text_raw`, `tag_text_normalized` | Preserve raw text. Create a normalized value by trimming, Unicode normalization, case folding, and whitespace collapse. Keep punctuation policy explicit; do not merge semantically different tags automatically. |
| `tag.timestamp` | `event_time_utc` plus `event_time_source` | Parse while retaining source text; timezone needs confirmation. |
| `movie.movieId` | `content_source_id` | Source-scoped natural key. |
| `movie.title` | `title_raw`, `display_title`, `release_year`, `title_parse_status` | Extract terminal year where valid; preserve raw title. Classify unmatched values as missing year, year range, malformed suffix, or other. Do not invent years. |
| `movie.genres` | `ContentGenre` associations | Split on `|`, trim values, validate against the documented genre set. Convert `(no genres listed)` into an explicit sentinel/status and no ordinary genre association. |
| `link.imdbId` | `ExternalContentId(provider='imdb', provider_id)` | Retain integer ID and construct URL with 7-digit zero padding: `tt` + padded ID. |
| `link.tmdbId` | `ExternalContentId(provider='tmdb', provider_id)` | Nullable; URL is `https://www.themoviedb.org/movie/{tmdbId}` only when present. |
| `genome_tags.tagId` | `signal_taxonomy_source_id` | Source-scoped taxonomy key. |
| `genome_tags.tag` | `signal_name` | Preserve taxonomy text. This is an algorithmic/content taxonomy label, not a user tag. |
| `genome_scores.movieId`, `tagId` | `content_source_id`, `signal_taxonomy_source_id` | Grain is one content × taxonomy tag. Validate both references. |
| `genome_scores.relevance` | `signal_value`, `signal_unit='relevance_0_1'` | Retain as Float64; enforce [0,1]. Clearly label as derived score, not a user-provided rating. |

### 2.3 Interaction event identifiers and deduplication

For the current immutable snapshot, a deterministic `event_id` can be a SHA-256 hash of canonicalized source identity plus source business-key fields and original values. For ratings, the measured `(userId, movieId)` pair is unique in this snapshot, so it can be the source event key; however, the project should still include rating, timestamp, source system, and source file in the record hash/audit lineage. For tags, use `(userId, movieId, tag, timestamp)` as the snapshot event key because that composite was measured unique. Do not assume the same uniqueness in future batches or other sources.

Deduplication precedence in Silver: exact same source key and same canonical record hash is replay and is ignored; same source key with differing payload is a conflict and is quarantined or resolved using an explicitly documented source update/version rule. Never use arbitrary last-row order. Late events are accepted based on event time and merge key; ingestion batch/time is tracked separately.

## 3. Medallion architecture

### AWS implementation recommendation

- S3 prefixes/bucket zones: `landing/`, `bronze/`, `silver/`, `gold/`, `quarantine/`, and `control/`.
- AWS Glue Spark jobs perform parsing, DQ, and Iceberg `MERGE`; Glue Data Catalog stores table metadata; Athena queries Gold and selected Silver tables.
- Apache Iceberg is the proposed table format because the pipeline needs transactional incremental writes/MERGE and Athena consumption. Keep the same conceptual layer contracts if a different format is chosen.
- Trigger Glue Workflow for scheduled/event-driven operation; the assignment explicitly asks for an Airflow DAG, so use MWAA if grading requires Airflow itself. Otherwise document why Glue Workflow was selected.

### Landing

- Store every original CSV or simulated CDC batch unchanged and immutably; include source file, batch ID, received time, and checksum in object metadata/control records, not by modifying the source file.
- Partition object paths by source and batch, e.g. `landing/movielens/rating/batch_id=.../rating.csv`; do not partition raw files by user/movie ID.
- Record file size, checksum, expected and observed row count, schema fingerprint, arrival time, and source version in `control.file_manifest`.
- Replay contract: Bronze/Silver/Gold can be rebuilt deterministically from Landing and versioned transformation code/configuration.

### Bronze

One append-only Iceberg table per source (`bronze_rating`, `bronze_tag`, `bronze_movie_cdc`, `bronze_link`, `bronze_genome_scores`, `bronze_genome_tags`). Preserve source field names/values; minimally cast IDs and measures only when safe, while retaining raw timestamp strings and parse errors. Add:

`source_system`, `source_file`, `landing_uri`, `landing_checksum`, `source_row_number`, `batch_id`, `ingested_at`, `record_hash`, `schema_version`, `parse_status`.

Append only. Reconciliation records compare Landing and Bronze row counts plus file checksums where applicable. Source CDC operations are appended as records (`I/U/D`); never update/delete Bronze history.

### Silver

Silver is the validated conformed layer and the only layer that applies business cleansing. Suggested tables:

- `silver_party` — source-scoped user identities inferred from rating/tag references.
- `silver_content` — one current content record per source identity, raw title retained, extracted name/year and parse status.
- `silver_content_genre` — normalized one-row-per-content/genre association plus explicit no-genre status.
- `silver_interaction_event` — CDM rating and user-tag event rows, with typed event fields and source lineage.
- `silver_external_content_id` — IMDb/TMDb crosswalk with nullable provider IDs.
- `silver_content_tag_signal` and `silver_signal_taxonomy` — genome scores and taxonomy kept distinct from user tags.
- `silver_quarantine` — original row/payload, source lineage, rule ID, severity, error code, reason, first-seen batch, and reprocess status.

Use Iceberg `MERGE` on stable source-scoped business keys and record hash. Exact replays no-op; late event records are merged idempotently; conflicting payloads follow the conflict policy above. Store schema and CDM mapping version on records or batch control metadata.

DQ candidates (severity finalized after measured rule results):

| Rule | Candidate severity | Basis |
|---|---|---|
| Required IDs and event timestamp parse | Blocking for Silver event facts | Without these, records cannot be keyed/ordered reliably; profile shows no nulls, but parsing check still needs execution. |
| Rating in [0.5,5.0], half-star increments | Blocking | Assignment defines valid domain; measured min/max match bounds but increment distribution was not in these result files. |
| Genome relevance in [0,1] | Blocking | Assignment defines range; observed min/max fit, but range rule must still be checked row-wise. |
| Orphan movie/tag references | Blocking | All measured orphan counts are zero; monitor every batch. |
| Duplicate event key with differing hash | Blocking/quarantine | Avoid silent event loss; exact measured keys unique in this snapshot. |
| Missing TMDb ID | Warning | 252 of 27,278 link rows; TMDb is nullable external enrichment, not required for core content identity. |
| Unparseable/missing release year | Warning + review queue | Title remains usable; do not drop movie. Preserve raw title and parse status. |
| `(no genres listed)` | Valid explicit state, not null | Assignment identifies it as sentinel. |
| Blank/whitespace user tag | Warning or quarantine | Free text is user-entered; measure count before choosing severity. |

### Gold

Use surrogate keys for dimensions and explicit fact grains. Suggested core star schema:

| Gold table | Grain | Key design / purpose |
|---|---|---|
| `dim_content` | One version of a content entity | `content_sk` surrogate key; natural key `(source_system, source_content_id)`; title fields and effective dates. |
| `dim_party` | One source-scoped user identity | `party_sk`; natural key `(source_system, source_party_id)`. Avoid adding unsupported user attributes. |
| `dim_date` | One calendar date | Date key plus calendar attributes. |
| `dim_genre` | One canonical genre | `genre_sk`; stable label/code. |
| `bridge_content_genre` | One content dimension version × genre association | Supports multi-valued genres and historical genre classification. |
| `dim_signal_tag` | One genome taxonomy tag | Distinct from user-entered tag values. |
| `fact_rating` | One rating event | `rating_event_sk`, `content_sk`, `party_sk`, date key, original rating, normalized 0–100 rating, event timestamp, source event ID and lineage. |
| `fact_user_tag` | One user tag submission | `tag_event_sk`, content/party/date keys, raw and normalized text or normalized tag dimension key. |
| `fact_genome_score` | One content × genome tag score | Grain `(content_sk, signal_tag_sk)` for a given source snapshot/version; store relevance. |
| `dim_external_content_id` | One content × provider identifier | Provider ID and constructed URL; nullable provider IDs omitted or represented as null. |

Avoid a single wide fact that combines ratings, tags, and genome scores: they have different grains and origins. Core facts/dimensions should not introduce new business rules beyond Silver; marts can aggregate them for ranking, genre analytics, tag correlation, genome coverage, and hidden gems.

## 4. SCD policy

| Attribute | Policy | Reason |
|---|---|---|
| Movie genres / genre associations | Type 2 | Business-relevant classification changes; retain effective periods and support point-in-time genre joins. Use a versioned content surrogate key and/or effective-dated bridge. |
| Corrected formatting/encoding in a title normalization field | Type 1 | Technical cleanup should not imply the movie itself changed. Preserve `title_raw` for source audit. |
| Display title when a deliberate title change needs current-versus-previous comparison | Type 3 (demonstration/use only when justified) | Store `current_display_title`, `previous_display_title`, and `title_changed_at`; supports one-step comparison but not full history. If every title version is analytically important, use Type 2 instead. |
| External IDs (IMDb/TMDb) | Type 1 unless provider ID reassignment history is a stated need | Current identifier enrichment is sufficient for assignment analytics; raw Bronze preserves changes. |
| Release year | Type 1 for correction of extraction/error; Type 2 only if business wants historical catalog assertions | Release year is an attribute, not a dynamic state in the source. Keep extraction status and raw title. |
| User attributes | No SCD table until attributes exist | This dataset contains user IDs only; do not invent demographics. |

The assignment asks to implement SCD Types 1, 2, and 3. Demonstrate Type 1 on a controlled technical correction, Type 2 on a meaningful synthetic genre change, and Type 3 on a synthetic deliberate display-title change. Clearly label these as simulated CDC examples. Type 3 is intentionally limited to previous/current values and must not be presented as full history.

For Type 2, use `effective_from`, `effective_to`, `is_current`, `version`, `content_sk`, and `record_hash`. For point-in-time joins, join an event timestamp where `effective_from <= event_time < effective_to` (open-ended `effective_to` for current rows). A late-arriving dimension change requires deterministic effective-time ordering and re-keying/reconciliation of affected fact surrogate keys; use an inferred content member when an event arrives before its content record.

## 5. Incremental controls, partitioning, and lineage

- `control.batch_run`: `batch_id`, source, batch window, input watermark, output watermark, start/end times, status, read/write/quarantine counts, code/config version.
- `control.file_manifest`: landing URI, checksum, file size, row count, schema fingerprint, first-seen batch, replay status.
- `control.watermark`: source/table, committed event-time watermark, tie-breaker, last successful batch. Advance only after downstream writes and DQ commit successfully.
- `control.dq_result`: rule ID/version, batch ID, evaluated/failed counts, severity, status, sample references.
- Late data policy: use an overlap/lookback window when reading event time, then MERGE using stable event identity; periodically reconcile older windows. The source snapshot is static, so document the synthetic late-arrival injection and expected outcomes.
- Idempotency: same `batch_id` and record hashes produce no extra Silver/Gold rows; control records distinguish a rerun from a new source arrival.
- Lineage columns in Silver and Gold retain `source_system`, source key, `batch_id`, `source_file`/Landing URI, and record hash or originating event ID.

Partitioning recommendation (validate with query patterns and file sizes):

| Table family | Partitioning / layout |
|---|---|
| Landing CSV | Prefix by source and batch; immutable file objects. |
| Bronze event tables | Partition by ingestion date or batch for replay/operations; avoid user/movie high-cardinality partitions. |
| Silver rating/tag events | Event month (or event date if data volume per date justifies it); assess Iceberg hidden partition transforms and avoid tiny partitions. |
| Silver movie/link/taxonomy dimensions | Usually unpartitioned at these sizes. |
| Genome scores | 11.7M rows: partition by a low-cardinality strategy such as movie ID bucket/hash if measurements justify it, or leave unpartitioned and sort/cluster by movie/tag keys; do not partition by individual `tagId` without workload evidence. |
| Gold rating fact | Event month is a reasonable starting point for temporal analytics; tune based on query scans and file sizes. |
| Gold small dimensions | Unpartitioned. |

## 6. Decisions still to document from profiling

Before finalizing the design, add measured results for: parsed timestamp failure counts and timezone interpretation; rating half-step violations and rating distribution/skew; exact duplicate rows (especially if batches can repeat); tag blank/whitespace counts and normalization impact; genre counts and sentinel count; title parse status totals; genome score bounds; and approximate file/table sizes after conversion. These are not included in the four result CSVs read for this design.
