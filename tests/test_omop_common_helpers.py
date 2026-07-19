"""Tests for omop_common orchestration helpers (prescan, tasks, predicates)."""

from __future__ import annotations

import pickle
from pathlib import Path

import polars as pl
import pytest

from meds_etl.config_compiler import compile_config
from meds_etl.omop_common import (
    build_relationship_resolution_map,
    collect_stage1_tasks,
    config_requires_concept_lookup,
    config_requires_relationship_mapping,
    config_requires_standard_only,
    describe_code_mapping,
    find_concept_id_columns_for_prescan,
    get_meds_schema_from_config,
    prescan_concept_ids,
    prescan_worker,
    table_requires_concept_lookup,
    validate_parquet_file,
)
from tests.fixtures_omop import (
    CONCEPT_LOOKUP_CONFIG,
    MINIMAL_CONFIG,
    write_concept_omop,
    write_minimal_omop,
)


def test_table_and_config_requires_concept_lookup():
    no_lookup = {"code_mappings": {"source_value": {"source_value_field": "x", "code_prefix": "P"}}}
    assert table_requires_concept_lookup(no_lookup) is False

    with_lookup = {
        "code_mappings": {
            "concept_id": {"concept_id_field": "measurement_concept_id"},
        }
    }
    assert table_requires_concept_lookup(with_lookup) is True

    compiled = compile_config(CONCEPT_LOOKUP_CONFIG)
    assert config_requires_concept_lookup(compiled) is True

    compiled_minimal = compile_config(MINIMAL_CONFIG)
    assert config_requires_concept_lookup(compiled_minimal) is False


def test_config_requires_relationship_and_standard_only():
    cfg = {
        "vocabulary": {"$omop": {"sources": ["concept", "concept_relationship"], "standard_only": True}},
        "tables": {},
    }
    assert config_requires_relationship_mapping(cfg) is True
    assert config_requires_standard_only(cfg) == ["S"]

    cfg_list = {"vocabulary": {"$omop": {"sources": ["concept"], "standard_only": ["S", "C"]}}}
    assert config_requires_relationship_mapping(cfg_list) is False
    assert config_requires_standard_only(cfg_list) == ["S", "C"]

    cfg_list_form = {"vocabulary": {"$omop": ["concept", "concept_relationship"]}}
    assert config_requires_relationship_mapping(cfg_list_form) is True
    assert config_requires_standard_only(cfg_list_form) == []


def test_describe_code_mapping():
    assert describe_code_mapping({}, is_canonical=True, fixed_code="MEDS_BIRTH") == "fixed_code"
    assert describe_code_mapping({}, is_canonical=False, fixed_code=None) == "unknown"
    assert (
        describe_code_mapping(
            {"code_mappings": {"concept_id": {}, "source_value": {}}},
            is_canonical=False,
            fixed_code=None,
        )
        == "concept_id+source_value"
    )


def test_find_concept_id_columns_for_prescan():
    compiled = compile_config(CONCEPT_LOOKUP_CONFIG)
    cols = find_concept_id_columns_for_prescan("measurement", compiled)
    assert "measurement_concept_id" in cols


def test_validate_parquet_file(tmp_path: Path):
    good = tmp_path / "good.parquet"
    pl.DataFrame({"a": [1]}).write_parquet(good)
    assert validate_parquet_file(good) is True

    bad = tmp_path / "bad.parquet"
    bad.write_text("not a parquet file")
    assert validate_parquet_file(bad) is False


def test_prescan_worker_and_prescan_concept_ids(tmp_path: Path):
    omop_dir = tmp_path / "omop"
    omop_dir.mkdir()
    write_concept_omop(omop_dir)
    compiled = compile_config(CONCEPT_LOOKUP_CONFIG)

    meas_file = omop_dir / "measurement" / "measurement.parquet"
    cols = find_concept_id_columns_for_prescan("measurement", compiled)
    result = prescan_worker((0, [(meas_file, cols)]))
    assert result["files_processed"] == 1
    assert 3004410 in result["concept_ids"]
    assert 3000963 in result["concept_ids"]

    used = prescan_concept_ids(omop_dir, compiled, num_workers=1, verbose=True, process_method="spawn")
    assert 3004410 in used
    assert 3000963 in used


def test_build_relationship_resolution_map(tmp_path: Path):
    omop_dir = tmp_path / "omop"
    omop_dir.mkdir()
    write_concept_omop(omop_dir)

    concept_df = pl.DataFrame(
        {
            "concept_id": [3004410, 3000963, 2000000001],
            "code": ["LOINC/8310-5", "LOINC/8867-4", "Custom/C001"],
            "standard_concept": ["S", "S", None],
        }
    )
    resolved = build_relationship_resolution_map(omop_dir, concept_df, verbose=True)
    assert resolved is not None
    assert len(resolved) == 1
    row = resolved.to_dicts()[0]
    assert row["source_concept_id"] == 2000000001
    assert row["resolved_code"] == "LOINC/8310-5"

    empty = build_relationship_resolution_map(tmp_path / "empty", concept_df, verbose=True)
    assert empty is None


def test_collect_stage1_tasks_basic(tmp_path: Path):
    omop_dir = tmp_path / "omop"
    omop_dir.mkdir()
    write_minimal_omop(omop_dir)
    compiled = compile_config(MINIMAL_CONFIG)
    unsorted_dir = tmp_path / "unsorted"
    unsorted_dir.mkdir()
    schema = get_meds_schema_from_config(compiled)

    tasks = collect_stage1_tasks(
        config=compiled,
        omop_dir=omop_dir,
        primary_key="person_id",
        unsorted_dir=unsorted_dir,
        meds_schema=schema,
        concept_df_data=None,
        compression="zstd",
    )
    # person (canonical) + measurement
    assert len(tasks) == 2
    tables = {t[1] for t in tasks}
    assert tables == {"person", "measurement"}
    # Canonical birth should be marked is_canonical with fixed code
    person_task = next(t for t in tasks if t[1] == "person")
    assert person_task[7] is True
    assert person_task[8] == "MEDS_BIRTH"


def test_collect_stage1_tasks_with_pre_join(tmp_path: Path):
    omop_dir = tmp_path / "omop"
    omop_dir.mkdir()

    visit_dir = omop_dir / "visit_occurrence"
    visit_dir.mkdir()
    pl.DataFrame(
        {
            "person_id": [1],
            "visit_occurrence_id": [10],
            "visit_start_datetime": ["2020-01-01T00:00:00"],
            "care_site_id": [100],
        }
    ).with_columns(pl.col("visit_start_datetime").str.to_datetime()).write_parquet(visit_dir / "visit.parquet")

    care_dir = omop_dir / "care_site"
    care_dir.mkdir()
    pl.DataFrame({"care_site_id": [100], "care_site_name": ["Main Hospital"]}).write_parquet(
        care_dir / "care_site.parquet"
    )

    config = {
        "primary_key": "person_id",
        "tables": {
            "visit_occurrence": {
                "time_start": "@visit_start_datetime",
                "code": "VISIT/{@care_site_name}",
                "pre_join": [
                    {
                        "table": "care_site",
                        "on": "care_site_id",
                        "select": ["care_site_name"],
                    }
                ],
            }
        },
    }
    compiled = compile_config(config)
    unsorted_dir = tmp_path / "unsorted"
    unsorted_dir.mkdir()
    schema = get_meds_schema_from_config(compiled)

    tasks = collect_stage1_tasks(
        config=compiled,
        omop_dir=omop_dir,
        primary_key="person_id",
        unsorted_dir=unsorted_dir,
        meds_schema=schema,
        concept_df_data=None,
    )
    assert len(tasks) == 1
    table_config = tasks[0][2]
    assert "_pre_join_data" in table_config
    join_df = pickle.loads(table_config["_pre_join_data"][0]["df_bytes"])
    assert "care_site_name" in join_df.columns
    assert join_df["care_site_name"].to_list() == ["Main Hospital"]


def test_collect_stage1_tasks_canonical_requires_code(tmp_path: Path):
    omop_dir = tmp_path / "omop"
    omop_dir.mkdir()
    write_minimal_omop(omop_dir)
    bad_config = {
        "primary_key": "person_id",
        "canonical_events": {
            "birth": {
                "table": "person",
                "time_start": "@birth_datetime",
                # missing code and code_mappings
            }
        },
        "tables": {},
    }
    # Bypass compile validation by using already-shaped incomplete config
    with pytest.raises(ValueError, match="must define either"):
        collect_stage1_tasks(
            config=bad_config,
            omop_dir=omop_dir,
            primary_key="person_id",
            unsorted_dir=tmp_path / "u",
            meds_schema={"subject_id": pl.Int64, "time": pl.Datetime("us"), "code": pl.Utf8},
            concept_df_data=None,
        )
