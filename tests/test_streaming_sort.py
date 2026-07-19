"""Unit tests for streaming external sort (Stage 2 of omop_streaming)."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from meds_etl.omop_streaming import (
    partition_to_sorted_runs,
    streaming_external_sort,
    streaming_merge_shard,
)


def _write_unsorted(unsorted_dir: Path) -> None:
    unsorted_dir.mkdir(parents=True, exist_ok=True)
    # Two files so partitioning creates multiple runs when chunk_rows is small
    pl.DataFrame(
        {
            "subject_id": [1, 3, 2, 1],
            "time": [
                "2020-01-02T00:00:00",
                "2020-01-01T00:00:00",
                "2020-01-03T00:00:00",
                "2020-01-01T00:00:00",
            ],
            "code": ["A", "B", "C", "D"],
            "numeric_value": [1.0, 2.0, 3.0, 4.0],
            "text_value": [None, None, None, None],
        }
    ).with_columns(pl.col("time").str.to_datetime()).write_parquet(unsorted_dir / "part0.parquet")

    pl.DataFrame(
        {
            "subject_id": [2, 3],
            "time": ["2020-01-04T00:00:00", "2020-01-05T00:00:00"],
            "code": ["E", "F"],
            "numeric_value": [5.0, 6.0],
            "text_value": [None, None],
        }
    ).with_columns(pl.col("time").str.to_datetime()).write_parquet(unsorted_dir / "part1.parquet")


def test_partition_to_sorted_runs_creates_multiple_runs(tmp_path: Path):
    unsorted_dir = tmp_path / "unsorted"
    runs_dir = tmp_path / "runs"
    _write_unsorted(unsorted_dir)

    partition_to_sorted_runs(
        unsorted_dir=unsorted_dir,
        runs_dir=runs_dir,
        num_shards=2,
        chunk_rows=2,
        compression="lz4",
        verbose=True,
        num_workers=1,
    )

    run_files = list(runs_dir.glob("shard_*/run_*.parquet"))
    assert len(run_files) >= 2

    # Each run should be sorted by subject_id, time
    for run_file in run_files:
        df = pl.read_parquet(run_file)
        assert len(df) > 0
        assert df.select(["subject_id", "time"]).equals(df.sort(["subject_id", "time"]).select(["subject_id", "time"]))


def test_partition_empty_unsorted_dir(tmp_path: Path):
    unsorted_dir = tmp_path / "unsorted"
    unsorted_dir.mkdir()
    runs_dir = tmp_path / "runs"

    partition_to_sorted_runs(
        unsorted_dir=unsorted_dir,
        runs_dir=runs_dir,
        num_shards=2,
        chunk_rows=100,
        num_workers=1,
    )
    assert list(runs_dir.glob("shard_*/run_*.parquet")) == []


def test_streaming_merge_shard_empty_returns_zero(tmp_path: Path):
    shard_dir = tmp_path / "shard_0"
    shard_dir.mkdir()
    output_file = tmp_path / "0.parquet"
    assert streaming_merge_shard(shard_dir, output_file) == 0
    assert not output_file.exists()


def test_streaming_external_sort_end_to_end(tmp_path: Path):
    unsorted_dir = tmp_path / "unsorted"
    output_dir = tmp_path / "out"
    _write_unsorted(unsorted_dir)

    streaming_external_sort(
        unsorted_dir=unsorted_dir,
        output_dir=output_dir,
        num_shards=2,
        chunk_rows=2,
        merge_workers=1,
        verbose=True,
        num_workers=1,
    )

    data_dir = output_dir / "data"
    shards = list(data_dir.glob("*.parquet"))
    assert shards

    all_rows = pl.concat([pl.read_parquet(f) for f in shards])
    assert len(all_rows) == 6
    assert set(all_rows["code"].to_list()) == {"A", "B", "C", "D", "E", "F"}

    for f in shards:
        df = pl.read_parquet(f)
        if len(df) > 1:
            assert df.select(["subject_id", "time"]).equals(
                df.sort(["subject_id", "time"]).select(["subject_id", "time"])
            )

    # Temp runs should be cleaned up after full partition+merge
    assert not (output_dir / "runs_temp").exists()


def test_streaming_external_sort_partition_only(tmp_path: Path):
    unsorted_dir = tmp_path / "unsorted"
    output_dir = tmp_path / "out"
    _write_unsorted(unsorted_dir)

    streaming_external_sort(
        unsorted_dir=unsorted_dir,
        output_dir=output_dir,
        num_shards=2,
        chunk_rows=2,
        merge_workers=1,
        run_partition=True,
        run_merge=False,
        num_workers=1,
    )

    assert (output_dir / "runs_temp").exists()
    assert list((output_dir / "runs_temp").glob("shard_*/run_*.parquet"))
