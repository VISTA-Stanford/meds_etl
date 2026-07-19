"""Shared helpers for generating tiny OMOP Parquet fixtures in tests."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

MINIMAL_CONFIG = {
    "primary_key": "person_id",
    "canonical_events": {
        "birth": {
            "table": "person",
            "code": "MEDS_BIRTH",
            "time_start": "@birth_datetime",
            "properties": [
                {"name": "table", "value": "$literal:person", "type": "string"},
            ],
        }
    },
    "tables": {
        "measurement": {
            "time_start": "@measurement_datetime",
            "code": "MEAS/{@measurement_source_value}",
            "numeric_value": "@value_as_number",
            "properties": [
                {"name": "table", "value": "$literal:measurement", "type": "string"},
            ],
        }
    },
}


CONCEPT_LOOKUP_CONFIG = {
    "primary_key": "person_id",
    "vocabulary": {
        "$omop": {
            "sources": ["concept", "concept_relationship"],
            "standard_only": ["S", "C"],
        }
    },
    "tables": {
        "measurement": {
            "time_start": "@measurement_datetime",
            "code": "$omop.lookup:@measurement_concept_id",
            "numeric_value": "@value_as_number",
            "properties": [
                {"name": "table", "value": "$literal:measurement", "type": "string"},
            ],
        }
    },
}


def write_minimal_omop(omop_dir: Path) -> None:
    """Write person + measurement tables with 3 subjects."""
    person_dir = omop_dir / "person"
    person_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "birth_datetime": [
                "2000-01-01T00:00:00",
                "2001-02-02T00:00:00",
                "2002-03-03T00:00:00",
            ],
        }
    ).with_columns(pl.col("birth_datetime").str.to_datetime()).write_parquet(person_dir / "person.parquet")

    meas_dir = omop_dir / "measurement"
    meas_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "person_id": [1, 1, 2, 3, 3],
            "measurement_datetime": [
                "2020-01-01T10:00:00",
                "2020-01-02T11:00:00",
                "2020-02-01T09:00:00",
                "2020-03-01T08:00:00",
                "2020-03-02T12:00:00",
            ],
            "measurement_source_value": ["HR", "TEMP", "HR", "TEMP", "HR"],
            "value_as_number": [72.0, 98.6, 80.0, 97.0, 75.0],
        }
    ).with_columns(pl.col("measurement_datetime").str.to_datetime()).write_parquet(meas_dir / "measurement.parquet")


def write_concept_omop(omop_dir: Path) -> None:
    """Write measurement + concept (+ relationship) for lookup/resolve tests."""
    meas_dir = omop_dir / "measurement"
    meas_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "person_id": [1, 2],
            "measurement_datetime": ["2020-01-01T10:00:00", "2020-01-02T11:00:00"],
            "measurement_concept_id": [3004410, 3000963],
            "measurement_source_concept_id": [2000000001, 0],
            "value_as_number": [98.6, 72.0],
        }
    ).with_columns(pl.col("measurement_datetime").str.to_datetime()).write_parquet(meas_dir / "measurement.parquet")

    concept_dir = omop_dir / "concept"
    concept_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "concept_id": [3004410, 3000963, 2000000001],
            "concept_code": ["8310-5", "8867-4", "C001"],
            "concept_name": ["Body temperature", "Heart rate", "Custom temp"],
            "vocabulary_id": ["LOINC", "LOINC", "Custom"],
            "domain_id": ["Measurement", "Measurement", "Measurement"],
            "concept_class_id": ["Clinical Observation", "Clinical Observation", "Clinical Observation"],
            "standard_concept": ["S", "S", None],
        }
    ).write_parquet(concept_dir / "concept.parquet")

    rel_dir = omop_dir / "concept_relationship"
    rel_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "concept_id_1": [2000000001],
            "concept_id_2": [3004410],
            "relationship_id": ["Maps to"],
        }
    ).write_parquet(rel_dir / "concept_relationship.parquet")


def write_config(path: Path, config: dict) -> Path:
    path.write_text(json.dumps(config, indent=2))
    return path
