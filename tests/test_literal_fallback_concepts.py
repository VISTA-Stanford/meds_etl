"""`$omop.lookup:<int>` fallbacks must survive the concept-table pre-scan.

The pre-scan keeps only concept ids found in data columns. A literal fallback
id is written in the config, not in the data, so it used to be filtered out
of the concept table whenever any column lookup was present. Rows that
needed the fallback then got a null code and were silently dropped.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from meds_etl.config_compiler import compile_config
from meds_etl.omop import run_omop_to_meds_etl
from meds_etl.omop_common import literal_fallback_concept_ids, prescan_concept_ids
from meds_etl.omop_streaming import run_omop_to_meds_streaming
from tests.fixtures_omop import write_config

VISIT_FALLBACK_CONFIG = {
    "primary_key": "person_id",
    "vocabulary": {"$omop": {"sources": ["concept"]}},
    "tables": {
        "visit_occurrence": {
            "time_start": "@visit_start_datetime",
            "code": "$omop.lookup:@visit_concept_id || $omop.lookup:8",
            "properties": [
                {"name": "table", "value": "$literal:visit_occurrence", "type": "string"},
                {"name": "visit_occurrence_id", "value": "@visit_occurrence_id", "type": "int"},
            ],
        }
    },
}


def _write_omop(omop_dir: Path) -> None:
    visit_dir = omop_dir / "visit_occurrence"
    visit_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "visit_occurrence_id": [10, 20, 30],
            "visit_start_datetime": ["2020-01-01T10:00:00", "2020-02-01T10:00:00", "2020-03-01T10:00:00"],
            # 10 has a real concept; 20 and 30 are unmapped (0 / null) and need the fallback.
            "visit_concept_id": [9201, 0, None],
        },
        schema_overrides={"visit_concept_id": pl.Int64},
    ).with_columns(pl.col("visit_start_datetime").str.to_datetime()).write_parquet(
        visit_dir / "visit_occurrence.parquet"
    )

    concept_dir = omop_dir / "concept"
    concept_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "concept_id": [9201, 8, 9202],
            "concept_code": ["IP", "OMOP generated", "OP"],
            "concept_name": ["Inpatient Visit", "Visit", "Outpatient Visit"],
            "vocabulary_id": ["Visit", "Domain", "Visit"],
            "domain_id": ["Visit", "Metadata", "Visit"],
            "concept_class_id": ["Visit", "Domain", "Visit"],
            "standard_concept": ["S", None, "S"],
        }
    ).write_parquet(concept_dir / "concept.parquet")


def _read_visits(output_dir: Path) -> pl.DataFrame:
    frames = [pl.read_parquet(f) for f in (output_dir / "data").glob("*.parquet")]
    return pl.concat(frames, how="vertical_relaxed").filter(pl.col("table") == "visit_occurrence")


def _assert_all_visits_kept(output_dir: Path) -> None:
    codes = dict(_read_visits(output_dir).select("visit_occurrence_id", "code").iter_rows())
    assert codes == {10: "Visit/IP", 20: "Domain/OMOP generated", 30: "Domain/OMOP generated"}


def test_literal_fallback_concept_ids_collects_all_literal_forms():
    config = compile_config(
        {
            "primary_key": "person_id",
            "vocabulary": {"$omop": {"sources": ["concept", "concept_relationship"]}},
            "tables": {
                "visit_occurrence": {"time_start": "@t", "code": "$omop.lookup:@visit_concept_id || $omop.lookup:8"},
                "note": {"time_start": "@t", "code": "$omop.resolve:@note_class_concept_id || $omop.resolve:44814637"},
                "visit_detail": {"time_start": "@t", "code": "$omop:@visit_detail_concept_id || $omop:4203722"},
                "measurement": {"time_start": "@t", "code": "$omop.lookup:@measurement_concept_id"},
            },
        }
    )
    assert literal_fallback_concept_ids(config) == {8, 44814637, 4203722}


def test_prescan_keeps_literal_fallback_ids(tmp_path: Path):
    _write_omop(tmp_path)
    used = prescan_concept_ids(tmp_path, compile_config(VISIT_FALLBACK_CONFIG), num_workers=1)
    assert used == {9201, 8}


def test_prescan_with_nothing_to_scan_still_means_no_filtering(tmp_path: Path):
    """An empty pre-scan result means "use the full concept table"; literals must not change that."""
    config = compile_config(
        {
            "primary_key": "person_id",
            "vocabulary": {"$omop": {"sources": ["concept"]}},
            "tables": {"visit_occurrence": {"time_start": "@visit_start_datetime", "code": "$omop.lookup:8"}},
        }
    )
    _write_omop(tmp_path)
    assert prescan_concept_ids(tmp_path, config, num_workers=1) == set()


@pytest.mark.parametrize("runner", ["etl", "streaming"])
def test_rows_needing_literal_fallback_are_not_dropped(tmp_path: Path, runner: str):
    omop_dir = tmp_path / "omop"
    _write_omop(omop_dir)
    config_path = write_config(tmp_path / "config.json", VISIT_FALLBACK_CONFIG)
    output_dir = tmp_path / "out"
    common = {
        "omop_dir": omop_dir,
        "output_dir": output_dir,
        "config_path": config_path,
        "num_workers": 1,
        "num_shards": 1,
        "verbose": False,
        "process_method": "spawn",
        "force_refresh": True,
    }
    if runner == "etl":
        run_omop_to_meds_etl(backend="polars", **common)
    else:
        run_omop_to_meds_streaming(chunk_rows=2, merge_workers=1, **common)
    _assert_all_visits_kept(output_dir)
