"""Unit tests for omop.py Stage 2 shard sort helpers."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from meds_etl.omop import parallel_shard_sort, process_single_shard, sequential_shard_sort


def _meds_schema() -> dict:
    return {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
    }


def _write_unsorted_shards(unsorted_dir: Path) -> None:
    unsorted_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "subject_id": [1, 2, 3, 4],
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
    ).with_columns(pl.col("time").str.to_datetime()).write_parquet(unsorted_dir / "events.parquet")


def test_process_single_shard_success(tmp_path: Path):
    unsorted_dir = tmp_path / "unsorted"
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    _write_unsorted_shards(unsorted_dir)
    files = list(unsorted_dir.glob("*.parquet"))
    schema = _meds_schema()

    result = process_single_shard((0, files, output_dir, 2, schema))
    assert result["success"] is True
    assert result["rows"] > 0
    assert (output_dir / "0.parquet").exists()

    df = pl.read_parquet(output_dir / "0.parquet")
    assert df.select(["subject_id", "time"]).equals(df.sort(["subject_id", "time"]).select(["subject_id", "time"]))
    # shard 0: subject_id % 2 == 0 → subjects 2, 4
    assert set(df["subject_id"].to_list()).issubset({2, 4})


def test_process_single_shard_empty(tmp_path: Path):
    unsorted_dir = tmp_path / "unsorted"
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    _write_unsorted_shards(unsorted_dir)
    files = list(unsorted_dir.glob("*.parquet"))

    # With 100 shards, shard 99 has no subjects in our tiny file
    result = process_single_shard((99, files, output_dir, 100, _meds_schema()))
    assert result["success"] is True
    assert result["rows"] == 0


def test_sequential_shard_sort(tmp_path: Path):
    unsorted_dir = tmp_path / "unsorted"
    output_dir = tmp_path / "out"
    _write_unsorted_shards(unsorted_dir)

    sequential_shard_sort(unsorted_dir, output_dir, num_shards=2, meds_schema=_meds_schema(), verbose=True)

    data_dir = output_dir / "result" / "data"
    shards = list(data_dir.glob("*.parquet"))
    assert shards
    all_rows = pl.concat([pl.read_parquet(f) for f in shards])
    assert len(all_rows) == 4


def test_sequential_shard_sort_no_files(tmp_path: Path):
    unsorted_dir = tmp_path / "unsorted"
    unsorted_dir.mkdir()
    output_dir = tmp_path / "out"
    sequential_shard_sort(unsorted_dir, output_dir, num_shards=2, meds_schema=_meds_schema())
    assert list((output_dir / "result" / "data").glob("*.parquet")) == []


def test_parallel_shard_sort_single_worker(tmp_path: Path):
    unsorted_dir = tmp_path / "unsorted"
    output_dir = tmp_path / "out"
    _write_unsorted_shards(unsorted_dir)

    parallel_shard_sort(
        unsorted_dir,
        output_dir,
        num_shards=2,
        num_workers=1,
        meds_schema=_meds_schema(),
        verbose=True,
    )

    data_dir = output_dir / "result" / "data"
    shards = list(data_dir.glob("*.parquet"))
    assert shards
    all_rows = pl.concat([pl.read_parquet(f) for f in shards])
    assert len(all_rows) == 4
