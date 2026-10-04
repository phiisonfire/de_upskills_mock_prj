# MovieLens Data Model and Medallion Design

## 1. Purpose and design basis

This design translates the MovieLens 20M source files into an auditable lakehouse and a source-independent interaction model. It uses the assignment's contracts: immutable Landing, append-only Bronze, incremental/validated Silver, and dimensional Gold with SCD and point-in-time queries.

The design is based on the generated profiles in `data/investigation/`:

- `rating`: 20,000,263 rows; inferred `userId`/`movieId` Int64, `rating` Float64, `timestamp` String; no profiled nulls. `(userId, movieId)` has 20,000,263 distinct keys and no duplicates. Full-row duplicate excess is zero. All values are in [0.5, 5.0] and on half-star increments. Most common values are 4.0 (5,561,926), 3.0 (4,291,193), and 5.0 (2,898,660).
- Rating counts are highly skewed: ratings/movie median 18, p95 3,614, p99 14,396, max 67,310; ratings/user median 68, p95 520, p99 1,113, max 9,254. Avoid user/movie partitioning and account for skewed keys during joins/aggregations.
- `tag`: 465,564 rows; inferred IDs Int64, `tag` and `timestamp` String; no profiled nulls. `(userId, movieId, tag, timestamp)` is unique in this snapshot. Full-row duplicate excess is zero. Conservative NFC + trim + lowercase + whitespace-collapse normalization changes 162,625 rows (about 34.9%); distinct raw values fall from 38,644 to 35,162 (3,482 fewer, about 9.0%). Seven rows are blank or whitespace. These collisions represent formatting variants under this rule, not proof that semantic synonyms are equivalent.
- `movie`: 27,278 rows; `movieId` Int64, `title`/`genres` String; no profiled nulls. `movieId` unique.
- `link`: 27,278 rows; `movieId`, `imdbId`, `tmdbId` inferred Int64. `movieId` unique and complete relative to movie in both directions; `tmdbId` has 252 nulls; `imdbId` has no profiled nulls.
- `genome_scores`: 11,709,768 rows; IDs Int64, relevance Float64; no profiled nulls. `(movieId, tagId)` unique; full-row duplicate excess is zero; every relevance is within [0,1] and observed range is 0.00025–1.0. Scores cover 10,381 of 27,278 catalog movies (38.06%); each covered movie has exactly 1,128 scores. Missing genome rows mean “not covered” and must not be treated as zero relevance.
- `genome_tags`: 1,128 rows; unique `tagId`, non-null string `tag`.
- All profiled foreign-key anti-joins returned zero orphan rows. Full-row duplicate excess is zero for all six source files.
- Rating timestamps all parse using `%Y-%m-%d %H:%M:%S` and range from `1995-01-09 11:46:44` to `2015-03-31 06:40:02`; tag timestamps all parse and range from `2005-12-24 13:00:10` to `2015-03-31 03:09:12`. Neither file has timezone markers. The naive values do not establish timezone semantics, so the CDM must not call them UTC until the source timezone is confirmed.
- The movie profile finds 246 `(no genres listed)` sentinels (0.90% of movies), no null genres, and no genre labels outside the documented set. Drama (13,344 associations) and Comedy (8,374) are the most common labels; a multi-genre movie contributes one association to each label.
- Title profiling classified 27,256 titles with a terminal four-digit year, 3 as year ranges, and 19 with no recognizable terminal year. Preserve raw titles and parse status. The extra-closing-parenthesis case is accepted by the permissive year extractor; retain a malformed-title flag if you need to distinguish it from clean parses.
- Combined source CSV size is about 928.5 MB; `rating.csv` is 690.4 MB and `genome_scores.csv` is 214.3 MB. Measure converted columnar sizes after a representative write rather than extrapolating from CSV bytes.

The static snapshot results do not prove how future/simulated batches behave. Re-run the same DQ and idempotency checks on each batch; do not infer timezone from the timestamp strings or semantic tag equivalence from formatting collisions.

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
| `ContentTagSignal` | One `(content, signal taxonomy tag)` measurement | Algorithm-derived genome relevance. Separate from user-submitted tags. Genome data covers only 38.06% of catalog movies, so coverage must be explicit. |

### 2.2 Field mapping

| Source field | Silver/CDM field | Transformation and rule |
|---|---|---|
| `rating.userId` | `party_source_id` | Cast to Int64; pair with `source_system='movielens'`. Do not treat numeric IDs as globally unique. |
| `rating.movieId` | `content_source_id` | Cast to Int64; pair with source system; validate against content key. |
| `rating.rating` | `event_value_numeric`, `event_value_unit='rating_0_5'` and optionally `rating_percent` | Preserve source value on 0.5–5.0 scale. For cross-source comparison, normalize to 0–100 by `rating * 20`; retain original value and scale. Do not replace source value with normalized value. |
| `rating.timestamp` | `event_time_source` and, after timezone confirmation, `event_time_utc` | Parse exact `%Y-%m-%d %H:%M:%S`; profile found zero parse failures and no timezone markers. Preserve the raw source timestamp. Do not apply a timezone conversion until verified from source documentation. |
| `tag.userId`, `tag.movieId` | `party_source_id`, `content_source_id` | Same source-scoped key rules as ratings. |
| `tag.tag` | `event_value_text_raw`, `tag_text_normalized` | Preserve raw text. Conservative NFC + trim + lowercase + whitespace collapse changes 34.9% of rows and reduces distinct values by 3,482; seven rows normalize to blank. Keep raw values and a normalization version. Do not merge punctuation variants or semantic synonyms without a curated mapping. |
| `tag.timestamp` | `event_time_source` and, after timezone confirmation, `event_time_utc` | Parse while retaining source text; all rows parse, but no timezone marker is present and timezone needs source confirmation. |
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

For this snapshot, the measured rating pair and tag composite key are unique, and all source files have zero exact duplicate rows. Use those keys as source event identities for the initial model, but still compute record hashes and test repeated deliveries in the simulated batches. Deduplication precedence in Silver: exact same source key and same canonical record hash is replay and is ignored; same source key with differing payload is a conflict and is quarantined or resolved using an explicitly documented source update/version rule. Never use arbitrary last-row order. Late events are accepted based on event time and merge key; ingestion batch/time is tracked separately.

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

DQ rules for the supplied snapshot (recheck these on every simulated/new batch):

| Rule | Severity | Basis |
|---|---|---|
| Required IDs and event timestamp parse | Blocking for Silver event facts | No null IDs/timestamps or timestamp parse failures were found in the static snapshot. Timezone meaning remains unconfirmed. |
| Rating in [0.5,5.0], half-star increments | Blocking | All 20,000,263 ratings passed both checks. Keep the row-level rule for future batches. |
| Genome relevance in [0,1] | Blocking | All 11,709,768 rows passed; min/max were 0.00025 and 1.0. |
| Orphan movie/tag references | Blocking | All measured orphan counts are zero; monitor every batch. |
| Duplicate event key with differing hash | Blocking/quarantine | Avoid silent event loss; exact measured keys unique in this snapshot. |
| Missing TMDb ID | Warning | 252 of 27,278 link rows; TMDb is nullable external enrichment, not required for core content identity. |
| Unparseable/missing release year | Warning + review queue | 22 titles need review: 3 year ranges and 19 without a recognizable terminal year. Title remains usable; do not drop movie. Preserve raw title and parse status. |
| `(no genres listed)` | Valid explicit state, not null | 246 rows; keep as explicit status/sentinel and do not represent it as a normal genre. |
| Blank/whitespace user tag | Row-level quarantine, pipeline continues | Seven rows; they are unusable as tag labels but should not fail the whole batch. Preserve them in Bronze and record quarantine reason in Silver. |
| Rating and per-entity skew | Operational warning/optimization signal | Strongly skewed counts (movie p99 14,396/max 67,310); monitor runtime and shuffle skew, but do not reject records. |
| Genome coverage | Completeness metric, not a row-level failure | Only 38.06% of catalog movies have scores; do not impute absent scores as 0. |

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
| `dim_user_tag` (optional) | One normalized user tag value | Use only for tag frequency/correlation marts; retain raw submitted text on `fact_user_tag`, and do not merge semantic synonyms without governance. |
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
| Silver rating/tag events | Event month as initial layout, especially for ratings; inspect file sizes and query scans after conversion. Daily partitions could create small files; assess Iceberg hidden partition transforms. |
| Silver movie/link/taxonomy dimensions | Usually unpartitioned at these sizes. |
| Genome scores | Leave unpartitioned initially or use a movie-ID bucket/sort layout only if measured query performance requires it. The 11.7M rows are dense: exactly 1,128 tags for each of 10,381 covered movies. Do not partition by individual `tagId`; missing catalog coverage is 61.94%, not zero relevance. |
| Gold rating fact | Event month is a reasonable starting point for temporal analytics; tune based on query scans and file sizes. |
| Gold small dimensions | Unpartitioned. |

## 6. Remaining design follow-ups

The static-source investigations are complete for the listed measures and are saved in `data/investigation/`. The remaining evidence/decisions are:

1. **Timezone semantics:** confirm the source timezone from MovieLens documentation or instructor guidance; the strings contain no offsets, so code cannot infer UTC. Until confirmed, use `event_time_source` and avoid UTC conversions.
2. **Title parsing QA:** inspect the 22 review cases and separate the extra-closing-parenthesis title from genuine missing years/year ranges. Preserve source title in every case.
3. **Tag vocabulary governance:** inspect `tag_normalization_collision_examples.csv`; the normalization is intentionally conservative and does not equate punctuation changes or semantic synonyms. Decide whether any curated synonym mapping is justified. Keep raw and normalized values.
4. **Converted storage sizing and partition tuning:** measure Parquet/Iceberg output bytes, file counts, and query scans after a representative write. The CSV source totals about 928.5 MB, dominated by ratings (690.4 MB) and genome scores (214.3 MB).
5. **Incremental behavior:** the clean initial snapshot does not test replay, late arrivals, conflicting payloads, or watermark behavior. Test those using the synthetic batch generator; exact duplicate rows across delivered batches are still possible even though none exist within these source files.
6. **SCD CDC semantics:** confirm synthetic `changed_at` timestamps and define effective-time ordering for genre changes so point-in-time joins have deterministic results.
