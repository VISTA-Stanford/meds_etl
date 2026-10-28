"""End-to-end tests for OMOP runner entry points (omop.py and omop_streaming.py)."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from meds_etl.omop import run_omop_to_meds_etl
from meds_etl.omop_streaming import run_omop_to_meds_streaming
from tests.fixtures_omop import MINIMAL_CONFIG, write_config, write_minimal_omop


@pytest.mark.parametrize("canonical", [False, True])
def test_fallback_notes_survive_concept_prescan(tmp_path: Path, canonical: bool):
    """A fallback absent from source IDs must survive vocabulary optimization."""
    from tests.fixtures_omop import write_concept_omop

    omop_dir = tmp_path / "omop"
    omop_dir.mkdir()
    write_concept_omop(omop_dir)
    note_dir = omop_dir / "note"
    note_dir.mkdir()
    texts = ["known class\nfull text", "unknown class é猫", "null class"]
    pl.DataFrame(
        {
            "person_id": [1, 1, 2],
            "note_id": [10, 11, 12],
            "note_class_concept_id": [3004410, 0, None],
            "note_datetime": ["2020-01-01T10:00:00"] * 3,
            "note_text": texts,
        }
    ).with_columns(pl.col("note_datetime").str.to_datetime()).write_parquet(note_dir / "data.parquet")
    note_config = {
        "time_start": "@note_datetime",
        "code": "$omop.lookup:@note_class_concept_id || $omop.lookup:3000963",
        "text_value": "@note_text",
        "properties": [{"name": "note_id", "value": "@note_id", "type": "int"}],
    }
    config = {
        "primary_key": "person_id",
        "vocabulary": {"$omop": {"sources": ["concept"], "standard_only": ["S", "C"]}},
        "tables": {"note": note_config},
    }
    if canonical:
        config["canonical_events"] = {"note": {"table": "note", **config["tables"].pop("note")}}
    from meds_etl.config_compiler import compile_config
    from meds_etl.omop_common import prescan_concept_ids

    used = prescan_concept_ids(omop_dir, compile_config(config), num_workers=1)
    assert {3004410, 3000963} <= used  # Ensure optimization actually ran.
    out = tmp_path / "meds"
    run_omop_to_meds_etl(
        omop_dir,
        out,
        write_config(tmp_path / "config.json", config),
        num_workers=1,
        num_shards=2,
        backend="polars",
        low_memory=True,
    )
    result = pl.read_parquet(str(out / "data/*.parquet")).sort("note_id")
    assert result["note_id"].to_list() == [10, 11, 12]
    assert result["text_value"].to_list() == texts
    assert result["code"].to_list() == ["LOINC/8310-5", "LOINC/8867-4", "LOINC/8867-4"]


def test_etl_fails_closed_on_table_error(tmp_path: Path, monkeypatch):
    import meds_etl.omop as omop

    source = tmp_path / "omop"
    source.mkdir()
    write_minimal_omop(source)
    monkeypatch.setattr(
        omop,
        "process_omop_file_worker",
        lambda task: {"success": False, "file": "data.parquet", "table": "note", "error": "bad mapping"},
    )
    output = tmp_path / "output"
    with pytest.raises(RuntimeError, match="refusing to publish a partial"):
        run_omop_to_meds_etl(
            source,
            output,
            write_config(tmp_path / "config.json", MINIMAL_CONFIG),
            num_workers=1,
            num_shards=1,
            backend="polars",
            low_memory=True,
        )
    assert not (output / "data").exists()


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
