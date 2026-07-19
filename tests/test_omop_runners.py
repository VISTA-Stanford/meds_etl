"""End-to-end tests for OMOP runner entry points (omop.py and omop_streaming.py)."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from meds_etl.omop import run_omop_to_meds_etl
from meds_etl.omop_streaming import run_omop_to_meds_streaming
from tests.fixtures_omop import MINIMAL_CONFIG, write_config, write_minimal_omop


def _assert_meds_output(output_dir: Path, expected_min_rows: int = 1) -> pl.DataFrame:
    data_dir = output_dir / "data"
    assert data_dir.exists(), f"Missing data dir: {data_dir}"
    parquet_files = list(data_dir.glob("*.parquet"))
    assert parquet_files, "No shard parquet files written"

    frames = [pl.read_parquet(f) for f in parquet_files]
    df = pl.concat(frames, how="vertical_relaxed")
    assert len(df) >= expected_min_rows
    for col in ("subject_id", "time", "code"):
        assert col in df.columns

    # Within each shard, rows should be ordered by subject_id, time
    for f in parquet_files:
        shard = pl.read_parquet(f)
        if len(shard) > 1:
            sorted_shard = shard.sort(["subject_id", "time"])
            assert shard.select(["subject_id", "time"]).equals(sorted_shard.select(["subject_id", "time"]))

    metadata = output_dir / "metadata" / "dataset.json"
    assert metadata.exists()
    meta = json.loads(metadata.read_text())
    assert "etl_name" in meta
    assert "meds_version" in meta

    return df


def test_run_omop_to_meds_streaming_e2e(tmp_path: Path):
    omop_dir = tmp_path / "omop"
    omop_dir.mkdir()
    write_minimal_omop(omop_dir)
    config_path = write_config(tmp_path / "config.json", MINIMAL_CONFIG)
    output_dir = tmp_path / "out_stream"

    run_omop_to_meds_streaming(
        omop_dir=omop_dir,
        output_dir=output_dir,
        config_path=config_path,
        num_workers=1,
        num_shards=2,
        chunk_rows=2,
        merge_workers=1,
        verbose=False,
        process_method="spawn",
        force_refresh=True,
    )

    df = _assert_meds_output(output_dir, expected_min_rows=5)
    codes = set(df["code"].to_list())
    assert "MEDS_BIRTH" in codes
    assert any(c.startswith("MEAS/") for c in codes)
    assert set(df["subject_id"].unique().to_list()) == {1, 2, 3}


def test_run_omop_to_meds_etl_low_memory_e2e(tmp_path: Path):
    omop_dir = tmp_path / "omop"
    omop_dir.mkdir()
    write_minimal_omop(omop_dir)
    config_path = write_config(tmp_path / "config.json", MINIMAL_CONFIG)
    output_dir = tmp_path / "out_etl"

    run_omop_to_meds_etl(
        omop_dir=omop_dir,
        output_dir=output_dir,
        config_path=config_path,
        num_workers=1,
        num_shards=2,
        backend="polars",
        low_memory=True,
        verbose=False,
        process_method="spawn",
        force_refresh=True,
    )

    df = _assert_meds_output(output_dir, expected_min_rows=5)
    assert "MEDS_BIRTH" in set(df["code"].to_list())


def test_run_omop_to_meds_etl_parallel_shards_e2e(tmp_path: Path):
    omop_dir = tmp_path / "omop"
    omop_dir.mkdir()
    write_minimal_omop(omop_dir)
    config_path = write_config(tmp_path / "config.json", MINIMAL_CONFIG)
    output_dir = tmp_path / "out_parallel"

    run_omop_to_meds_etl(
        omop_dir=omop_dir,
        output_dir=output_dir,
        config_path=config_path,
        num_workers=1,
        num_shards=2,
        parallel_shards=True,
        verbose=False,
        process_method="spawn",
        force_refresh=True,
    )

    _assert_meds_output(output_dir, expected_min_rows=5)


def test_runner_rejects_existing_output_without_force(tmp_path: Path):
    omop_dir = tmp_path / "omop"
    omop_dir.mkdir()
    write_minimal_omop(omop_dir)
    config_path = write_config(tmp_path / "config.json", MINIMAL_CONFIG)
    output_dir = tmp_path / "out_exists"
    (output_dir / "data").mkdir(parents=True)

    with pytest.raises(SystemExit):
        run_omop_to_meds_etl(
            omop_dir=omop_dir,
            output_dir=output_dir,
            config_path=config_path,
            num_workers=1,
            num_shards=2,
            low_memory=True,
            force_refresh=False,
        )
