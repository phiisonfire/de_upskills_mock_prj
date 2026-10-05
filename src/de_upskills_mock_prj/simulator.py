"""Create deterministic MovieLens micro-batches and synthetic movie CDC inputs."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

SOURCE_FILES = {
    "rating": "rating.csv",
    "tag": "tag.csv",
    "movie": "movie.csv",
    "link": "link.csv",
    "genome_scores": "genome_scores.csv",
    "genome_tags": "genome_tags.csv",
}
GENRES = [
    "Action", "Adventure", "Animation", "Children", "Comedy", "Crime",
    "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "IMAX",
    "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western",
]
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _csv_data_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return max(sum(1 for _ in csv.reader(handle)) - 1, 0)


def _file_manifest(path: Path, table: str, batch_id: str, scenario: str) -> dict[str, Any]:
    return {
        "table": table,
        "batch_id": batch_id,
        "scenario": scenario,
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "row_count": _csv_data_rows(path),
        "sha256": _sha256(path),
    }


def _event_batch_frame(lf: pl.LazyFrame, cutoff: date, window_days: int) -> pl.LazyFrame:
    cutoff_datetime = datetime.combine(cutoff, datetime.min.time())
    return (
        lf.with_columns(
            pl.col("timestamp")
            .str.to_datetime(format=TIMESTAMP_FORMAT, strict=False)
            .alias("_event_time")
        )
        .with_columns(
            (
                pl.col("_event_time").dt.date().cast(pl.Int32)
                - pl.lit(cutoff).cast(pl.Int32)
            ).alias("_days_since_cutoff")
        )
        .with_columns(
            ((pl.col("_days_since_cutoff") // window_days) + 1)
            .cast(pl.Int32)
            .alias("_period_number")
        )
        .with_columns(
            pl.when(pl.col("_event_time") < pl.lit(cutoff_datetime))
            .then(pl.lit("batch_0000_history"))
            .otherwise(
                pl.concat_str(
                    [
                        pl.lit("batch_"),
                        pl.col("_period_number").cast(pl.String).str.pad_start(4, "0"),
                    ]
                )
            )
            .alias("batch_id")
        )
        .drop("_event_time", "_days_since_cutoff", "_period_number")
    )


def _write_event_batches(
    source_path: Path,
    table: str,
    landing_root: Path,
    cutoff: date,
    window_days: int,
) -> tuple[list[dict[str, Any]], pl.LazyFrame, list[str]]:
    lf = pl.scan_csv(source_path, infer_schema_length=10_000, try_parse_dates=False)
    source_columns = lf.collect_schema().names()
    if "timestamp" not in source_columns:
        raise ValueError(f"Expected a timestamp column in {source_path}")

    batched = _event_batch_frame(lf, cutoff, window_days)
    counts = (
        batched.group_by("batch_id")
        .agg(pl.len().alias("rows"))
        .sort("batch_id")
        .collect()
    )
    if counts.filter(pl.col("batch_id").is_null()).height:
        raise ValueError(f"Could not parse a timestamp or assign a batch in {source_path}")
    table_root = landing_root / "movielens" / "arrivals" / table
    table_root.mkdir(parents=True, exist_ok=True)
    batched.select([*source_columns, "batch_id"]).sink_csv(
        pl.PartitionBy(table_root, key="batch_id", include_key=False),
        maintain_order=True,
    )

    batch_counts = {row["batch_id"]: row["rows"] for row in counts.to_dicts()}
    outputs = []
    for batch_id, row_count in batch_counts.items():
        batch_dir = table_root / f"batch_id={batch_id}"
        for path in sorted(batch_dir.glob("*.csv")):
            outputs.append({
                "table": table,
                "batch_id": batch_id,
                "scenario": "event_time_partition",
                "path": str(path),
                "size_bytes": path.stat().st_size,
                "row_count": _csv_data_rows(path),
                "expected_batch_rows": row_count,
                "sha256": _sha256(path),
            })
    return outputs, batched, source_columns


def _write_scenario_rows(
    batched: pl.LazyFrame,
    columns: list[str],
    source_batch_id: str,
    output_path: Path,
    row_offset: int,
    row_count: int,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    (
        batched.filter(pl.col("batch_id") == source_batch_id)
        .select(columns)
        .slice(row_offset, row_count)
        .sink_csv(output_path, maintain_order=True, mkdir=True)
    )
    return _csv_data_rows(output_path)


def _inject_arrival_scenarios(
    table: str,
    batched: pl.LazyFrame,
    columns: list[str],
    batch_counts: dict[str, int],
    landing_root: Path,
    requested_rows: int,
) -> list[dict[str, Any]]:
    numeric_batches = sorted(
        (int(batch_id.removeprefix("batch_")), batch_id)
        for batch_id in batch_counts
        if batch_id.startswith("batch_") and batch_id != "batch_0000_history"
    )
    if not numeric_batches:
        return []

    # Find a source event window with enough rows to create distinct late and duplicate samples.
    selected: tuple[int, str, int] | None = None
    for number, batch_id in numeric_batches:
        next_number = number + 1
        available = batch_counts[batch_id]
        take = min(requested_rows, available // 2)
        if take > 0:
            selected = (next_number, batch_id, take)
            break
    if selected is None:
        raise ValueError(
            f"No {table} event batch contains enough rows for late-arrival and duplicate scenarios. "
            "Choose a cutoff with more events or lower --scenario-rows."
        )

    arrival_number, source_batch_id, take = selected
    arrival_batch_id = f"batch_{arrival_number:04d}"
    table_root = landing_root / "movielens" / "arrivals" / table / f"batch_id={arrival_batch_id}"
    late_path = table_root / "late_arrivals.csv"
    duplicate_path = table_root / "duplicate_delivery.csv"
    late_count = _write_scenario_rows(
        batched, columns, source_batch_id, late_path, row_offset=0, row_count=take
    )
    duplicate_count = _write_scenario_rows(
        batched, columns, source_batch_id, duplicate_path, row_offset=take, row_count=take
    )
    if late_count != take or duplicate_count != take:
        raise RuntimeError(f"Could not write the requested {table} scenario rows")
    return [
        {**_file_manifest(late_path, table, arrival_batch_id, "late_arrival"), "source_event_batch_id": source_batch_id},
        {**_file_manifest(duplicate_path, table, arrival_batch_id, "duplicate_delivery"), "source_event_batch_id": source_batch_id},
    ]


def _movie_cdc_rows(movie_path: Path, cutoff: date, window_days: int) -> list[dict[str, Any]]:
    movies = pl.read_csv(movie_path, infer_schema_length=10_000)
    movie_by_id = {row["movieId"]: row for row in movies.to_dicts()}
    maximum_id = int(movies.get_column("movieId").max())

    def current(movie_id: int) -> dict[str, Any]:
        if movie_id not in movie_by_id:
            raise ValueError(f"Movie ID {movie_id} was not found in {movie_path}")
        return movie_by_id[movie_id]

    def cdc_row(
        movie_id: int,
        title: str,
        genres: str,
        op_type: str,
        period: int,
        scenario: str,
        is_deleted: bool = False,
    ) -> dict[str, Any]:
        changed_date = cutoff + timedelta(days=(period - 1) * window_days)
        changed_at = datetime.combine(changed_date, datetime.min.time()).replace(hour=12)
        return {
            "movieId": movie_id,
            "title": title,
            "genres": genres,
            "is_deleted": is_deleted,
            "op_type": op_type,
            "changed_at": changed_at.strftime(TIMESTAMP_FORMAT),
            "scenario": scenario,
            "batch_id": f"batch_{period:04d}",
        }

    # Pick a title with whitespace for a plausible Type 1 cleanup event.
    whitespace_titles = movies.filter(
        (~pl.col("movieId").is_in([1, 2, 3]))
        & (pl.col("title") != pl.col("title").str.strip_chars())
    )
    if whitespace_titles.height == 0:
        raise ValueError("No whitespace-padded title exists for the Type 1 correction scenario")
    type1_movie = whitespace_titles.row(0, named=True)
    type1_title = type1_movie["title"].strip()

    # Type 2 example: add a valid genre the movie did not previously have.
    genre_movie = current(1)
    existing_genres = set(str(genre_movie["genres"]).split("|"))
    added_genre = next((genre for genre in GENRES if genre not in existing_genres), None)
    if added_genre is None:
        raise ValueError("Could not find a genre to add for the Type 2 scenario")
    changed_genres = "|".join([*existing_genres, added_genre])
    # Keep a stable output order for deterministic generated data.
    changed_genres = "|".join([g for g in GENRES if g in set(changed_genres.split("|"))])

    # Type 3 example: synthetic deliberate display-title change, keeping a terminal year if present.
    type3_movie = current(2)
    original_title = type3_movie["title"]
    title_match = __import__("re").search(r"\s*\(\d{4}\)\s*$", original_title)
    if title_match:
        type3_title = original_title[: title_match.start()] + " - Alternate Cut" + original_title[title_match.start() :]
    else:
        type3_title = original_title + " - Alternate Cut"

    deleted_movie = current(3)
    inserted_movies = [
        {
            "movieId": maximum_id + 1,
            "title": "Synthetic Arrival Movie A (2020)",
            "genres": "Drama|Mystery",
        },
        {
            "movieId": maximum_id + 2,
            "title": "Synthetic Arrival Movie B (2021)",
            "genres": "Comedy|Sci-Fi",
        },
    ]
    rows = [
        cdc_row(1, genre_movie["title"], changed_genres, "U", 1, "scd_type_2_genre_change"),
        cdc_row(type1_movie["movieId"], type1_title, type1_movie["genres"], "U", 2, "scd_type_1_title_trim"),
        cdc_row(2, type3_title, type3_movie["genres"], "U", 3, "scd_type_3_display_title_change"),
        cdc_row(3, deleted_movie["title"], deleted_movie["genres"], "D", 4, "soft_delete", is_deleted=True),
    ]
    for inserted in inserted_movies:
        rows.append(cdc_row(
            inserted["movieId"], inserted["title"], inserted["genres"],
            "I", 4, "insert_new_movie",
        ))
    return rows


def create_simulation(
    data_dir: Path,
    output_dir: Path,
    run_id: str,
    cutoff: date,
    window_days: int = 1,
    scenario_rows: int = 5,
) -> Path:
    if window_days < 1:
        raise ValueError("window_days must be at least 1")
    if scenario_rows < 1:
        raise ValueError("scenario_rows must be at least 1")
    run_root = output_dir / run_id
    if run_root.exists():
        raise FileExistsError(
            f"Simulation output already exists at {run_root}; choose a new --run-id. "
            "Existing simulated landing data is never overwritten."
        )

    missing = [name for name in SOURCE_FILES.values() if not (data_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing MovieLens source files in {data_dir}: {', '.join(missing)}")

    landing_root = run_root / "landing"
    history_batch = "batch_0000_history"
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "cutoff": cutoff.isoformat(),
        "window_days": window_days,
        "timestamp_timezone": "unspecified_naive_source_time",
        "source_files": [],
        "arrival_files": [],
        "movie_cdc_files": [],
        "scenario_plan": {
            "late_arrival": "Rows keep their earlier event timestamp but are delivered in a later arrival batch.",
            "duplicate_delivery": "Exact event rows are redelivered in a later arrival batch.",
            "idempotency_replay": "Reprocess the same batch path and batch_id a second time; do not create another copy of its files.",
            "movie_cdc": [
                "Type 2 genre update", "Type 1 whitespace correction", "Type 3 display-title change",
                "soft delete", "two inserts",
            ],
        },
    }

    # Keep the six source snapshots byte-for-byte in Landing for provenance and replay.
    # The split rating/tag batches below are the simulator's event arrivals and are
    # the inputs to the incremental event pipeline.
    snapshot_root = landing_root / "movielens" / "source_snapshot"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    for table, file_name in SOURCE_FILES.items():
        source_path = data_dir / file_name
        target_path = snapshot_root / file_name
        shutil.copy2(source_path, target_path)
        ingest_role = (
            "provenance_only_split_into_event_batches"
            if table in ("rating", "tag")
            else "bootstrap_snapshot"
        )
        manifest["source_files"].append({
            **_file_manifest(target_path, table, history_batch, "immutable_source_snapshot"),
            "ingest_role": ingest_role,
        })

    for table in ("rating", "tag"):
        outputs, batched, columns = _write_event_batches(
            data_dir / SOURCE_FILES[table], table, landing_root, cutoff, window_days
        )
        manifest["arrival_files"].extend(outputs)
        batch_counts: dict[str, int] = {}
        for entry in outputs:
            if entry["scenario"] == "event_time_partition":
                batch_counts[entry["batch_id"]] = batch_counts.get(entry["batch_id"], 0) + entry["row_count"]

        scenario_outputs = _inject_arrival_scenarios(
            table,
            batched,
            columns,
            batch_counts,
            landing_root,
            scenario_rows,
        )
        manifest["arrival_files"].extend(scenario_outputs)

    movie_cdc = _movie_cdc_rows(data_dir / SOURCE_FILES["movie"], cutoff, window_days)
    movie_cdc_root = landing_root / "movielens" / "arrivals" / "movie_cdc"
    for row in movie_cdc:
        batch_id = row.pop("batch_id")
        target = movie_cdc_root / f"batch_id={batch_id}" / "movie_cdc.csv"
        target.parent.mkdir(parents=True, exist_ok=True)
        file_exists = target.exists()
        with target.open("a" if file_exists else "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

    for path in sorted(movie_cdc_root.glob("batch_id=*/*.csv")):
        batch_id = path.parent.name.removeprefix("batch_id=")
        manifest["movie_cdc_files"].append(
            _file_manifest(path, "movie_cdc", batch_id, "synthetic_cdc")
        )

    manifest_path = run_root / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return run_root


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split MovieLens events into deterministic batches and create synthetic movie CDC scenarios."
    )
    parser.add_argument("--data-dir", type=Path, default=Path("data/MovieLens"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/simulated"))
    parser.add_argument("--run-id", default="movielens_sim_v1")
    parser.add_argument(
        "--cutoff", required=True, type=date.fromisoformat,
        help="History cutoff date (YYYY-MM-DD); choose it from the event-volume profile.",
    )
    parser.add_argument("--window-days", type=int, default=1)
    parser.add_argument("--scenario-rows", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = create_simulation(
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        run_id=args.run_id,
        cutoff=args.cutoff,
        window_days=args.window_days,
        scenario_rows=args.scenario_rows,
    )
    print(f"Created simulated Landing at {result / 'landing'}")
    print(f"Manifest: {result / 'manifest.json'}")


if __name__ == "__main__":
    main()
