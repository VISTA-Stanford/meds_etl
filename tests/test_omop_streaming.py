"""
Unit tests for omop_streaming.py

Tests the config-driven OMOP to MEDS ETL with streaming external sort.
"""

import datetime
import tempfile
from pathlib import Path

import polars as pl
import pytest

from meds_etl.config_compiler import compile_config
from meds_etl.omop_streaming import (
    apply_transforms,
    build_concept_map,
    config_type_to_polars,
    find_omop_table_files,
    get_meds_schema_from_config,
    get_metadata_column_info,
    process_omop_file_worker,
    transform_to_meds_unsorted,
    validate_config_against_data,
)

# ============================================================================
# SCHEMA UTILITIES TESTS
# ============================================================================


def test_config_type_to_polars():
    """Test type string to Polars dtype conversion."""
    assert config_type_to_polars("int") == pl.Int64
    assert config_type_to_polars("integer") == pl.Int64
    assert config_type_to_polars("float") == pl.Float64
    assert config_type_to_polars("string") == pl.Utf8
    assert config_type_to_polars("str") == pl.Utf8
    assert config_type_to_polars("datetime") == pl.Datetime("us")
    assert config_type_to_polars("boolean") == pl.Boolean

    # Case insensitive
    assert config_type_to_polars("INT") == pl.Int64
    assert config_type_to_polars("STRING") == pl.Utf8

    # Unknown type defaults to Utf8
    assert config_type_to_polars("unknown_type") == pl.Utf8


def test_get_metadata_column_info():
    """Test extracting metadata column types from config."""
    config = {
        "canonical_events": {
            "birth": {
                "table": "person",
                "metadata": [
                    {"name": "gender_concept_id", "type": "int"},
                    {"name": "race_concept_id", "type": "int"},
                ],
            }
        },
        "tables": {
            "measurement": {
                "metadata": [
                    {"name": "unit_concept_id", "type": "int"},
                    {"name": "range_low", "type": "float"},
                ]
            }
        },
    }

    col_types = get_metadata_column_info(config)

    assert col_types["gender_concept_id"] == "int"
    assert col_types["race_concept_id"] == "int"
    assert col_types["unit_concept_id"] == "int"
    assert col_types["range_low"] == "float"


def test_get_metadata_column_info_type_mismatch():
    """Test that inconsistent types for same column raise error."""
    config = {
        "canonical_events": {
            "birth": {
                "table": "person",
                "metadata": [
                    {"name": "provider_id", "type": "int"},
                ],
            }
        },
        "tables": {
            "measurement": {
                "metadata": [
                    {"name": "provider_id", "type": "string"},  # Different type!
                ]
            }
        },
    }

    with pytest.raises(ValueError, match="has inconsistent types"):
        get_metadata_column_info(config)


def test_get_meds_schema_from_config():
    """Test building global MEDS schema."""
    config = {
        "tables": {
            "measurement": {
                "metadata": [
                    {"name": "unit_concept_id", "type": "int"},
                    {"name": "range_low", "type": "float"},
                ]
            }
        }
    }

    schema = get_meds_schema_from_config(config)

    # Core MEDS columns
    assert schema["subject_id"] == pl.Int64
    assert schema["time"] == pl.Datetime("us")
    assert schema["code"] == pl.Utf8
    assert schema["numeric_value"] == pl.Float64
    assert schema["text_value"] == pl.Utf8
    assert schema["end"] == pl.Datetime("us")

    # Metadata columns
    assert schema["unit_concept_id"] == pl.Int64
    assert schema["range_low"] == pl.Float64


# ============================================================================
# TRANSFORM UTILITIES TESTS
# ============================================================================


def test_apply_transforms_replace():
    """Test simple string replacement transform."""
    expr = pl.lit("hello world")
    transforms = [{"type": "replace", "pattern": "world", "replacement": "polars"}]

    result_expr = apply_transforms(expr, transforms)
    df = pl.DataFrame({"col": [1]}).select(result_expr.alias("result"))

    assert df["result"][0] == "hello polars"


def test_apply_transforms_regex_replace():
    """Test regex replacement transform."""
    expr = pl.lit("test  123  spaces")
    transforms = [{"type": "regex_replace", "pattern": "\\s+", "replacement": "-"}]

    result_expr = apply_transforms(expr, transforms)
    df = pl.DataFrame({"col": [1]}).select(result_expr.alias("result"))

    assert df["result"][0] == "test-123-spaces"


def test_apply_transforms_case_conversion():
    """Test case conversion transforms."""
    # Lowercase
    expr = pl.lit("HELLO WORLD")
    transforms = [{"type": "lower"}]
    result_expr = apply_transforms(expr, transforms)
    df = pl.DataFrame({"col": [1]}).select(result_expr.alias("result"))
    assert df["result"][0] == "hello world"

    # Uppercase
    expr = pl.lit("hello world")
    transforms = [{"type": "upper"}]
    result_expr = apply_transforms(expr, transforms)
    df = pl.DataFrame({"col": [1]}).select(result_expr.alias("result"))
    assert df["result"][0] == "HELLO WORLD"


def test_apply_transforms_strip():
    """Test strip transforms."""
    # Strip whitespace
    expr = pl.lit("  hello  ")
    transforms = [{"type": "strip"}]
    result_expr = apply_transforms(expr, transforms)
    df = pl.DataFrame({"col": [1]}).select(result_expr.alias("result"))
    assert df["result"][0] == "hello"

    # Strip specific characters
    expr = pl.lit("___hello___")
    transforms = [{"type": "strip_chars", "characters": "_"}]
    result_expr = apply_transforms(expr, transforms)
    df = pl.DataFrame({"col": [1]}).select(result_expr.alias("result"))
    assert df["result"][0] == "hello"


def test_apply_transforms_chained():
    """Test multiple transforms in sequence."""
    expr = pl.lit("  Hello  World  ")
    transforms = [
        {"type": "strip"},
        {"type": "regex_replace", "pattern": "\\s+", "replacement": "-"},
        {"type": "upper"},
    ]

    result_expr = apply_transforms(expr, transforms)
    df = pl.DataFrame({"col": [1]}).select(result_expr.alias("result"))

    assert df["result"][0] == "HELLO-WORLD"


# ============================================================================
# TRANSFORM TO MEDS TESTS
# ============================================================================


def test_transform_to_meds_fixed_code():
    """Test transformation with fixed code (canonical event)."""
    df = pl.DataFrame(
        {
            "person_id": [12345],
            "birth_datetime": [datetime.datetime(1990, 1, 1)],
            "gender_concept_id": [8507],
        }
    )

    table_config = {
        "subject_id_field": "person_id",
        "time_start": "birth_datetime",
        "code_mappings": {},
        "metadata": [{"name": "gender_concept_id", "type": "int"}],
    }

    meds_schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
        "end": pl.Datetime("us"),
        "gender_concept_id": pl.Int64,
    }

    result = transform_to_meds_unsorted(
        df=df,
        table_config=table_config,
        primary_key="person_id",
        meds_schema=meds_schema,
        fixed_code="MEDS_BIRTH",
    )

    assert len(result) == 1
    assert result["subject_id"][0] == 12345
    assert result["code"][0] == "MEDS_BIRTH"
    assert result["gender_concept_id"][0] == 8507


def test_transform_to_meds_source_value_field():
    """Test transformation with source_value field mapping."""
    df = pl.DataFrame(
        {
            "person_id": [12345, 12345],
            "condition_start_datetime": [datetime.datetime(2020, 1, 1), datetime.datetime(2020, 2, 1)],
            "condition_source_value": ["ICD10/E11.9", "ICD10/I10"],
        }
    )

    table_config = {
        "subject_id_field": "person_id",
        "time_start": "condition_start_datetime",
        "code_mappings": {"source_value": {"field": "condition_source_value"}},
        "metadata": [],
    }

    meds_schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
        "end": pl.Datetime("us"),
    }

    result = transform_to_meds_unsorted(
        df=df,
        table_config=table_config,
        primary_key="person_id",
        meds_schema=meds_schema,
    )

    assert len(result) == 2
    assert result["code"][0] == "ICD10/E11.9"
    assert result["code"][1] == "ICD10/I10"


def test_transform_to_meds_template():
    """Test transformation with template-based code generation."""
    df = pl.DataFrame(
        {
            "person_id": [12345, 12345],
            "note_datetime": [datetime.datetime(2020, 1, 1), datetime.datetime(2020, 2, 1)],
            "note_title": ["Progress Notes", "Discharge Summary"],
        }
    )

    table_config = {
        "subject_id_field": "person_id",
        "time_start": "note_datetime",
        "code_mappings": {
            "concept_id": {
                "template": "NOTE/{note_title}",
                "transforms": [{"type": "regex_replace", "pattern": "\\s+", "replacement": "-"}, {"type": "upper"}],
            }
        },
        "metadata": [],
    }

    meds_schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
        "end": pl.Datetime("us"),
    }

    result = transform_to_meds_unsorted(
        df=df,
        table_config=table_config,
        primary_key="person_id",
        meds_schema=meds_schema,
    )

    assert len(result) == 2
    assert result["code"][0] == "NOTE/PROGRESS-NOTES"
    assert result["code"][1] == "NOTE/DISCHARGE-SUMMARY"


def test_transform_to_meds_concept_join():
    """Test transformation with concept table join."""
    df = pl.DataFrame(
        {
            "person_id": [12345, 12345],
            "measurement_datetime": [datetime.datetime(2020, 1, 1), datetime.datetime(2020, 2, 1)],
            "measurement_concept_id": [3004410, 3000963],
            "value_as_number": [98.6, 120.0],
        }
    )

    concept_df = pl.DataFrame(
        {
            "concept_id": [3004410, 3000963],
            "code": ["LOINC/8310-5", "LOINC/8867-4"],
        }
    )

    table_config = {
        "subject_id_field": "person_id",
        "time_start": "measurement_datetime",
        "code_mappings": {"concept_id": {"concept_id_field": "measurement_concept_id"}},
        "numeric_value_field": "value_as_number",
        "metadata": [],
    }

    meds_schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
        "end": pl.Datetime("us"),
    }

    result = transform_to_meds_unsorted(
        df=df,
        table_config=table_config,
        primary_key="person_id",
        meds_schema=meds_schema,
        concept_df=concept_df,
    )

    assert len(result) == 2
    assert result["code"][0] == "LOINC/8310-5"
    assert result["code"][1] == "LOINC/8867-4"
    assert result["numeric_value"][0] == 98.6
    assert result["numeric_value"][1] == 120.0


def test_transform_to_meds_with_end_time():
    """Test transformation with end time."""
    df = pl.DataFrame(
        {
            "person_id": [12345],
            "visit_start_datetime": [datetime.datetime(2020, 1, 1)],
            "visit_end_datetime": [datetime.datetime(2020, 1, 5)],
            "visit_concept_id": [9201],
        }
    )

    concept_df = pl.DataFrame(
        {
            "concept_id": [9201],
            "code": ["Visit/IP"],
        }
    )

    table_config = {
        "subject_id_field": "person_id",
        "time_start": "visit_start_datetime",
        "time_end": "visit_end_datetime",
        "code_mappings": {"concept_id": {"concept_id_field": "visit_concept_id"}},
        "metadata": [],
    }

    meds_schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
        "end": pl.Datetime("us"),
    }

    result = transform_to_meds_unsorted(
        df=df,
        table_config=table_config,
        primary_key="person_id",
        meds_schema=meds_schema,
        concept_df=concept_df,
    )

    assert len(result) == 1
    assert result["code"][0] == "Visit/IP"
    assert result["end"][0] == datetime.datetime(2020, 1, 5)


def test_transform_to_meds_filters_null_codes():
    """Test that rows with null codes are filtered out."""
    df = pl.DataFrame(
        {
            "person_id": [12345, 12345, 12345],
            "measurement_datetime": [
                datetime.datetime(2020, 1, 1),
                datetime.datetime(2020, 2, 1),
                datetime.datetime(2020, 3, 1),
            ],
            "measurement_concept_id": [3004410, None, 3000963],
        }
    )

    concept_df = pl.DataFrame(
        {
            "concept_id": [3004410, 3000963],
            "code": ["LOINC/8310-5", "LOINC/8867-4"],
        }
    )

    table_config = {
        "subject_id_field": "person_id",
        "time_start": "measurement_datetime",
        "code_mappings": {"concept_id": {"concept_id_field": "measurement_concept_id"}},
        "metadata": [],
    }

    meds_schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
        "end": pl.Datetime("us"),
    }

    result = transform_to_meds_unsorted(
        df=df,
        table_config=table_config,
        primary_key="person_id",
        meds_schema=meds_schema,
        concept_df=concept_df,
    )

    # Should only have 2 rows (null concept_id filtered out)
    assert len(result) == 2
    assert result["code"][0] == "LOINC/8310-5"
    assert result["code"][1] == "LOINC/8867-4"


# ============================================================================
# FILE DISCOVERY TESTS
# ============================================================================


def test_find_omop_table_files_directory():
    """Test finding parquet files in a directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create table directory with parquet files
        table_dir = tmpdir_path / "measurement"
        table_dir.mkdir()

        (table_dir / "file1.parquet").touch()
        (table_dir / "file2.parquet").touch()
        (table_dir / "file3.parquet").touch()

        files = find_omop_table_files(tmpdir_path, "measurement")

        assert len(files) == 3
        assert all(f.suffix == ".parquet" for f in files)


def test_find_omop_table_files_single_file():
    """Test finding a single parquet file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create single parquet file
        (tmpdir_path / "measurement.parquet").touch()

        files = find_omop_table_files(tmpdir_path, "measurement")

        assert len(files) == 1
        assert files[0].name == "measurement.parquet"


def test_find_omop_table_files_not_found():
    """Test when table files don't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        files = find_omop_table_files(tmpdir_path, "nonexistent_table")

        assert len(files) == 0


# ============================================================================
# CONFIG VALIDATION TESTS
# ============================================================================


def test_validate_config_against_data_success():
    """Test successful config validation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create sample parquet file
        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "measurement_datetime": [
                    datetime.datetime(2020, 1, 1),
                    datetime.datetime(2020, 1, 2),
                    datetime.datetime(2020, 1, 3),
                ],
                "measurement_concept_id": [100, 101, 102],
                "value_as_number": [1.0, 2.0, 3.0],
            }
        )

        measurement_dir = tmpdir_path / "measurement"
        measurement_dir.mkdir()
        df.write_parquet(measurement_dir / "data.parquet")

        config = {
            "primary_key": "person_id",
            "tables": {
                "measurement": {
                    "subject_id_field": "person_id",
                    "time_start": "measurement_datetime",
                    "code_mappings": {"concept_id": {"concept_id_field": "measurement_concept_id"}},
                    "numeric_value_field": "value_as_number",
                    "metadata": [],
                }
            },
        }

        # Should not raise
        validate_config_against_data(tmpdir_path, config, verbose=False)


def test_validate_config_against_data_missing_field():
    """Test config validation with missing field."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create sample parquet file WITHOUT measurement_concept_id
        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "measurement_datetime": [
                    datetime.datetime(2020, 1, 1),
                    datetime.datetime(2020, 1, 2),
                    datetime.datetime(2020, 1, 3),
                ],
                # Missing measurement_concept_id!
            }
        )

        measurement_dir = tmpdir_path / "measurement"
        measurement_dir.mkdir()
        df.write_parquet(measurement_dir / "data.parquet")

        config = {
            "primary_key": "person_id",
            "tables": {
                "measurement": {
                    "subject_id_field": "person_id",
                    "time_start": "measurement_datetime",
                    "code_mappings": {
                        "concept_id": {"concept_id_field": "measurement_concept_id"}  # This doesn't exist!
                    },
                    "metadata": [],
                }
            },
        }

        with pytest.raises(SystemExit):
            validate_config_against_data(tmpdir_path, config, verbose=False)


# ============================================================================
# CONCEPT MAP TESTS
# ============================================================================


def test_build_concept_map_basic():
    """Test building concept map from concept table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create sample concept table (includes all columns expected by build_concept_map)
        concept_df = pl.DataFrame(
            {
                "concept_id": [3004410, 3000963, 2000000001],
                "vocabulary_id": ["LOINC", "LOINC", "Custom"],
                "concept_code": ["8310-5", "8867-4", "C001"],
                "concept_name": ["Body temperature", "Heart rate", "Custom concept"],
                "domain_id": ["Measurement", "Measurement", "Measurement"],
                "concept_class_id": ["Clinical Observation", "Clinical Observation", "Clinical Observation"],
            }
        )

        concept_dir = tmpdir_path / "concept"
        concept_dir.mkdir()
        concept_df.write_parquet(concept_dir / "concept.parquet")

        result_df, code_metadata = build_concept_map(tmpdir_path, verbose=False)

        assert len(result_df) == 3
        assert "concept_id" in result_df.columns
        assert "code" in result_df.columns

        # Check code format
        codes = result_df["code"].to_list()
        assert "LOINC/8310-5" in codes
        assert "LOINC/8867-4" in codes
        assert "Custom/C001" in codes

        # Check metadata for custom concepts (concept_id > 2B)
        assert len(code_metadata) == 1
        assert "Custom/C001" in code_metadata


def test_build_concept_map_no_files():
    """Test building concept map when no concept files exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        result_df, code_metadata = build_concept_map(tmpdir_path, verbose=False)

        # Should return empty DataFrame with correct schema
        assert len(result_df) == 0
        assert "concept_id" in result_df.columns
        assert "code" in result_df.columns
        assert len(code_metadata) == 0


# ============================================================================
# WORKER FUNCTION TESTS
# ============================================================================


def test_process_omop_file_worker_success():
    """Test OMOP file worker processes data correctly."""
    import pickle

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create sample OMOP file
        df = pl.DataFrame(
            {
                "person_id": [12345, 12345],
                "measurement_datetime": [datetime.datetime(2020, 1, 1), datetime.datetime(2020, 2, 1)],
                "measurement_concept_id": [3004410, 3000963],
                "value_as_number": [98.6, 120.0],
            }
        )

        input_file = tmpdir_path / "measurement_001.parquet"
        df.write_parquet(input_file)

        # Create concept map
        concept_df = pl.DataFrame(
            {
                "concept_id": [3004410, 3000963],
                "code": ["LOINC/8310-5", "LOINC/8867-4"],
            }
        )

        table_config = {
            "subject_id_field": "person_id",
            "time_start": "measurement_datetime",
            "code_mappings": {"concept_id": {"concept_id_field": "measurement_concept_id"}},
            "numeric_value_field": "value_as_number",
            "metadata": [],
        }

        meds_schema = {
            "subject_id": pl.Int64,
            "time": pl.Datetime("us"),
            "code": pl.Utf8,
            "numeric_value": pl.Float64,
            "text_value": pl.Utf8,
            "end": pl.Datetime("us"),
        }

        output_dir = tmpdir_path / "output"
        output_dir.mkdir()

        args = (
            input_file,
            "measurement",
            table_config,
            "person_id",
            output_dir,
            meds_schema,
            pickle.dumps(concept_df),
            False,  # is_canonical
            None,  # fixed_code
        )

        result = process_omop_file_worker(args)

        assert result["success"] is True
        assert result["input_rows"] == 2
        assert result["output_rows"] == 2
        assert result["filtered_rows"] == 0

        # Check output file was created
        output_files = list(output_dir.glob("*.parquet"))
        assert len(output_files) == 1


def test_process_omop_file_worker_empty_file():
    """Test worker handles empty files correctly."""
    import pickle

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create empty OMOP file
        df = pl.DataFrame(
            {
                "person_id": pl.Series([], dtype=pl.Int64),
                "measurement_datetime": pl.Series([], dtype=pl.Datetime("us")),
                "measurement_concept_id": pl.Series([], dtype=pl.Int64),
            }
        )

        input_file = tmpdir_path / "measurement_empty.parquet"
        df.write_parquet(input_file)

        table_config = {
            "subject_id_field": "person_id",
            "time_start": "measurement_datetime",
            "code_mappings": {"concept_id": {"concept_id_field": "measurement_concept_id"}},
            "metadata": [],
        }

        meds_schema = {
            "subject_id": pl.Int64,
            "time": pl.Datetime("us"),
            "code": pl.Utf8,
            "numeric_value": pl.Float64,
            "text_value": pl.Utf8,
            "end": pl.Datetime("us"),
        }

        output_dir = tmpdir_path / "output"
        output_dir.mkdir()

        args = (
            input_file,
            "measurement",
            table_config,
            "person_id",
            output_dir,
            meds_schema,
            pickle.dumps(None),
            False,
            None,
        )

        result = process_omop_file_worker(args)

        assert result["success"] is True
        assert result["input_rows"] == 0
        assert result["output_rows"] == 0


# ============================================================================
# INTEGRATION TESTS
# ============================================================================


def test_end_to_end_simple_measurement():
    """Test end-to-end transformation of measurement data."""
    # This tests the full pipeline from OMOP to MEDS

    # Create sample measurement data
    df = pl.DataFrame(
        {
            "person_id": [12345, 12345, 67890],
            "measurement_datetime": [
                datetime.datetime(2020, 1, 1, 10, 0),
                datetime.datetime(2020, 1, 2, 11, 0),
                datetime.datetime(2020, 1, 3, 12, 0),
            ],
            "measurement_concept_id": [3004410, 3000963, 3004410],
            "value_as_number": [98.6, 72.0, 99.1],
            "unit_concept_id": [586323, 8541, 586323],
        }
    )

    concept_df = pl.DataFrame(
        {
            "concept_id": [3004410, 3000963],
            "code": ["LOINC/8310-5", "LOINC/8867-4"],
        }
    )

    table_config = {
        "subject_id_field": "person_id",
        "time_start": "measurement_datetime",
        "code_mappings": {"concept_id": {"concept_id_field": "measurement_concept_id"}},
        "numeric_value_field": "value_as_number",
        "metadata": [{"name": "unit_concept_id", "type": "int"}],
    }

    meds_schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
        "end": pl.Datetime("us"),
        "unit_concept_id": pl.Int64,
    }

    result = transform_to_meds_unsorted(
        df=df,
        table_config=table_config,
        primary_key="person_id",
        meds_schema=meds_schema,
        concept_df=concept_df,
    )

    # Verify results
    assert len(result) == 3
    assert set(result["subject_id"].unique().to_list()) == {12345, 67890}
    assert set(result["code"].to_list()) == {"LOINC/8310-5", "LOINC/8867-4"}
    assert all(result["numeric_value"].is_not_null())
    assert all(result["unit_concept_id"].is_not_null())

    # Check sorting
    assert result["time"][0] <= result["time"][1] <= result["time"][2]


# ============================================================================
# NEW FEATURES TESTS: Properties, Literals, Aliasing
# ============================================================================


def test_properties_backwards_compat():
    """Test that both 'properties' and 'metadata' keys work."""
    # Test with 'metadata' (old)
    config_old = {"tables": {"visit_occurrence": {"metadata": [{"name": "visit_id", "type": "int"}]}}}

    # Test with 'properties' (new)
    config_new = {"tables": {"visit_occurrence": {"properties": [{"name": "visit_id", "type": "int"}]}}}

    result_old = get_metadata_column_info(config_old)
    result_new = get_metadata_column_info(config_new)

    assert result_old == result_new == {"visit_id": "int"}


def test_literal_property():
    """Test literal values in properties."""
    df = pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "visit_start_datetime": [
                datetime.datetime(2020, 1, 1),
                datetime.datetime(2020, 1, 2),
                datetime.datetime(2020, 1, 3),
            ],
            "visit_concept_id": [1, 2, 3],
        }
    )

    concept_df = pl.DataFrame(
        {
            "concept_id": [1, 2, 3],
            "code": ["VISIT/ER", "VISIT/IP", "VISIT/OP"],
        }
    )

    table_config = {
        "time_start": "visit_start_datetime",
        "code_mappings": {"concept_id": {"concept_id_field": "visit_concept_id"}},
        "properties": [
            {"name": "table_name", "literal": "visit_occurrence", "type": "string"},
            {"name": "row_number", "literal": 42, "type": "int"},
        ],
    }

    meds_schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
        "end": pl.Datetime("us"),
        "table_name": pl.Utf8,
        "row_number": pl.Int64,
    }

    result = transform_to_meds_unsorted(
        df=df,
        table_config=table_config,
        primary_key="person_id",
        meds_schema=meds_schema,
        concept_df=concept_df,
    )

    # Check that literal values are applied to all rows
    assert result["table_name"].unique().to_list() == ["visit_occurrence"]
    assert result["row_number"].unique().to_list() == [42]
    assert len(result) == 3


def test_property_concept_lookup():
    """Test properties that require concept lookups."""
    config = {
        "primary_key": "person_id",
        "tables": {
            "observation": {
                "time_start": "@observation_datetime",
                "code": "@observation_source_value",
                "properties": [{"name": "drug_type", "value": "$omop:@drug_type_concept_id", "type": "string"}],
            }
        },
    }

    compiled = compile_config(config)
    table_config = compiled["tables"]["observation"]
    meds_schema = get_meds_schema_from_config(compiled)

    df = pl.DataFrame(
        {
            "person_id": [1, 2],
            "observation_datetime": [
                datetime.datetime(2020, 1, 1),
                datetime.datetime(2020, 1, 2),
            ],
            "observation_source_value": ["X", "Y"],
            "drug_type_concept_id": [111, None],
        }
    )

    concept_df = pl.DataFrame({"concept_id": [111], "code": ["DRUG_TYPE_A"]})

    result = transform_to_meds_unsorted(
        df=df,
        table_config=table_config,
        primary_key="person_id",
        meds_schema=meds_schema,
        concept_df=concept_df,
    )

    assert result["drug_type"].to_list() == ["DRUG_TYPE_A", None]


def test_aliased_property():
    """Test column aliasing/renaming in properties."""
    df = pl.DataFrame(
        {
            "person_id": [1, 2],
            "measurement_datetime": [
                datetime.datetime(2020, 1, 1),
                datetime.datetime(2020, 1, 2),
            ],
            "measurement_concept_id": [100, 200],
            "measurement_id": [9999, 8888],  # Source column
            "unit_concept_id": [5, 6],
        }
    )

    concept_df = pl.DataFrame(
        {
            "concept_id": [100, 200],
            "code": ["LOINC/1234-5", "LOINC/9876-1"],
        }
    )

    table_config = {
        "time_start": "measurement_datetime",
        "code_mappings": {"concept_id": {"concept_id_field": "measurement_concept_id"}},
        "properties": [
            # Alias: measurement_id → meas_id
            {"name": "measurement_id", "alias": "meas_id", "type": "int"},
            # No alias: unit_concept_id stays the same
            {"name": "unit_concept_id", "type": "int"},
        ],
    }

    meds_schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
        "end": pl.Datetime("us"),
        "meas_id": pl.Int64,  # Aliased output name
        "unit_concept_id": pl.Int64,
    }

    result = transform_to_meds_unsorted(
        df=df,
        table_config=table_config,
        primary_key="person_id",
        meds_schema=meds_schema,
        concept_df=concept_df,
    )

    # Check that aliasing worked
    assert "meas_id" in result.columns
    assert "measurement_id" not in result.columns  # Source column should not appear
    assert result["meas_id"].to_list() == [9999, 8888]
    assert result["unit_concept_id"].to_list() == [5, 6]


def test_concept_mapped_template():
    """Test template with concept_fields for concept table lookup."""
    df = pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "birth_datetime": [
                datetime.datetime(1990, 1, 1),
                datetime.datetime(1991, 2, 2),
                datetime.datetime(1992, 3, 3),
            ],
            "gender_concept_id": [8507, 8532, 8507],  # MALE, FEMALE, MALE
        }
    )

    # Concept table with gender concepts
    concept_df = pl.DataFrame(
        {
            "concept_id": [8507, 8532],
            "code": ["MALE", "FEMALE"],
        }
    )

    table_config = {
        "time_start": "birth_datetime",
        "code_mappings": {
            "concept_id": {
                "template": "Gender/{gender_concept_id}",
                "concept_fields": ["gender_concept_id"],  # Map this field via concept table
            }
        },
    }

    meds_schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
        "end": pl.Datetime("us"),
    }

    result = transform_to_meds_unsorted(
        df=df,
        table_config=table_config,
        primary_key="person_id",
        meds_schema=meds_schema,
        concept_df=concept_df,
    )

    # Check that concept lookup happened before template substitution
    assert result["code"].to_list() == ["Gender/MALE", "Gender/FEMALE", "Gender/MALE"]
    assert len(result) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
