# MovieLens batch simulator

## Purpose

`de_upskills_mock_prj.simulator` turns the static MovieLens snapshot into a reproducible local Landing fixture for incremental pipeline development. It never modifies files under `data/MovieLens/`. Each simulation run writes a new immutable output directory and refuses to overwrite an existing run ID.

## Choose the cutoff

First run the final **Choose the simulator cutoff and event window** section in `analytics.ipynb`. Review `data/investigation/monthly_event_volume.csv` for both ratings and tags. Choose a date boundary `T` that gives a substantial historical batch and enough later daily windows. Timestamp strings have no timezone marker; the simulator uses their displayed source time consistently and does not claim it is UTC.

The first fixture, `movielens_t2013_qtr_v1`, used `T = 2013-01-01` and quarterly windows and remains unchanged. The daily fixture at `data/simulated/movielens_t2013_daily_v1/` uses the same cutoff with one-day windows. Because history is exclusive of `T`, later windows contain 1,446,101 ratings and 132,612 tags. Each event table has 819 populated event-time batch files total (history plus daily arrivals), with daily IDs extending through `batch_0820`.

## Generate a run

From the repository root, set `YYYY-MM-DD` to the selected cutoff and use a new run ID for each generated run:

```bash
uv run python -m de_upskills_mock_prj.simulator \
  --cutoff 2013-01-01 \
  --window-days 1 \
  --scenario-rows 5 \
  --run-id movielens_t2013_daily_v1
```

Output is written under `data/simulated/<run-id>/`; the generated files are ignored by Git. A second run with the same ID fails instead of replacing immutable files. Use `--window-days 1` for daily arrivals; larger values group consecutive days into multi-day windows.

## Output layout and scenarios

- `landing/movielens/source_snapshot/` contains byte-for-byte copies of all six original CSVs. The manifest marks movie/link/genome files for bootstrap and rating/tag files as provenance-only (their divided batch slices are the event pipeline inputs).
- `landing/movielens/arrivals/rating/` and `.../tag/` contain event-time batches. `batch_0000_history` contains rows before `T`; subsequent batch numbers cover consecutive windows of `--window-days` days.
- Each event table includes a later batch with `late_arrivals.csv` (event timestamps from an earlier batch) and `duplicate_delivery.csv` (different exact rows redelivered from that earlier batch). The manifest records the source event batch for each injected file.
- `landing/movielens/arrivals/movie_cdc/` contains deterministic full-image CDC rows with `op_type`, `changed_at`, `is_deleted`, and scenario labels: Type 2 genre update, Type 1 whitespace cleanup, Type 3 synthetic display-title change, soft delete, and two inserts.
- `manifest.json` records the cutoff, window size, source and arrival paths, row counts, byte sizes, checksums, and the replay scenario. For an idempotency rerun, process the exact same batch path and `batch_id` twice; do not create a second copy of the batch.

The simulated event CSVs are newly serialized batch payloads derived from the immutable input snapshots. Their business fields are retained, but their bytes are not expected to match the complete source CSV because rows are divided among batches. Treat `source_snapshot/` as the byte-preserving source archive and `arrivals/` as the simulator-generated feed for Bronze/Silver processing.

Daily arrival frequency does not require daily storage partitions in Silver or Gold. Keep event-month partitioning as the initial storage layout unless measured query patterns justify a different layout, to avoid excessive small partitions/files.

The selected post-cutoff source event batch must contain at least two times `--scenario-rows` rows so the simulator can create distinct late and duplicate samples. If not, choose an earlier cutoff or lower the scenario row count.

## AWS handoff

Review the manifest and generated layout before uploading. Upload `landing/` to a unique S3 prefix for this run; preserve the paths and never overwrite an existing run. Bootstrap movie, link, genome score, and genome tag tables from their source snapshot files. For ratings and user tags, bootstrap from the generated `batch_0000_history` slices and process the later arrival batches; keep the full rating/tag snapshots as provenance and do not ingest them a second time as full event tables.
