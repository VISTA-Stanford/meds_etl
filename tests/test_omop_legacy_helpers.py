"""Focused helper tests for omop_legacy.py (no full main() integration)."""

from __future__ import annotations

import gzip
from pathlib import Path

import polars as pl
import pytest

from meds_etl.omop_legacy import (
    cast_to_datetime,
    extract_metadata,
    get_table_files,
    load_file,
    read_polars_df,
    split_list,
)


def test_split_list():
    assert split_list([1, 2, 3, 4, 5], 2) == [[1, 2, 3], [4, 5]]
    assert split_list([], 3) == []
    assert split_list([1], 5) == [[1]]


def test_get_table_files_directory_and_single(tmp_path: Path):
    # Directory with mixed shards
    meas = tmp_path / "measurement"
    meas.mkdir()
    (meas / "a.csv").write_text("person_id\n1\n")
    (meas / "b.parquet").touch()
    pl.DataFrame({"person_id": [1]}).write_parquet(meas / "b.parquet")
    (meas / "c.csv.gz").write_bytes(gzip.compress(b"person_id\n2\n"))

    csvs, parquets = get_table_files(str(tmp_path), "measurement")
    assert len(csvs) == 2
    assert len(parquets) == 1

    # Single parquet file
    pl.DataFrame({"person_id": [1]}).write_parquet(tmp_path / "person.parquet")
    csvs, parquets = get_table_files(str(tmp_path), "person")
    assert csvs == []
    assert len(parquets) == 1

    # Missing table
    assert get_table_files(str(tmp_path), "observation") == ([], [])


def test_read_polars_df_csv_and_parquet(tmp_path: Path):
    csv_path = tmp_path / "t.csv"
    csv_path.write_text("Person_ID,Value\n1,x\n")
    df = read_polars_df(str(csv_path))
    assert "person_id" in df.columns
    assert df["person_id"].to_list() == ["1"]

    pq_path = tmp_path / "t.parquet"
    pl.DataFrame({"Person_ID": [2]}).write_parquet(pq_path)
    df_pq = read_polars_df(str(pq_path))
    assert "person_id" in df_pq.columns

    with pytest.raises(RuntimeError, match="unknown type"):
        read_polars_df(str(tmp_path / "t.txt"))


def test_load_file_plain_and_gz(tmp_path: Path):
    plain = tmp_path / "a.csv"
    plain.write_text("hello")
    with load_file(str(tmp_path), str(plain)) as f:
        assert f.read() == "hello"

    gz = tmp_path / "b.csv.gz"
    gz.write_bytes(gzip.compress(b"world"))
    decompressed_dir = tmp_path / "decomp"
    decompressed_dir.mkdir()
    with load_file(str(decompressed_dir), str(gz)) as f:
        assert Path(f.name).read_text() == "world"


def test_cast_to_datetime_variants():
    from datetime import date

    df = pl.DataFrame(
        {
            "as_str": ["2020-01-01 12:00:00", "2020-01-02"],
            "as_date": [date(2020, 1, 1), date(2020, 1, 2)],
        }
    )
    schema = df.schema

    expr = cast_to_datetime(schema, "as_str", move_to_end_of_day=False)
    out = df.select(t=expr)
    assert out["t"].dtype == pl.Datetime("us")

    expr_eod = cast_to_datetime(schema, "as_str", move_to_end_of_day=True)
    out_eod = df.select(t=expr_eod)
    assert out_eod["t"].dtype == pl.Datetime("us")

    date_expr = cast_to_datetime(schema, "as_date", move_to_end_of_day=True)
    date_out = df.select(t=date_expr)
    assert date_out["t"].dtype == pl.Datetime("us")

    # Missing column → null datetime literal
    missing = cast_to_datetime(schema, "missing_col")
    nulls = df.with_columns(t=missing)
    assert nulls["t"].dtype == pl.Datetime("us")
    assert nulls["t"].null_count() == len(df)


def test_extract_metadata(tmp_path: Path):
    omop_dir = tmp_path / "omop"
    omop_dir.mkdir()
    decomp = tmp_path / "decomp"
    decomp.mkdir()

    concept_dir = omop_dir / "concept"
    concept_dir.mkdir()
    pl.DataFrame(
        {
            "concept_id": [3004410, 2000000001],
            "vocabulary_id": ["LOINC", "Custom"],
            "concept_code": ["8310-5", "C001"],
            "concept_name": ["Body temperature", "Custom temp"],
        }
    ).write_parquet(concept_dir / "concept.parquet")

    rel_dir = omop_dir / "concept_relationship"
    rel_dir.mkdir()
    pl.DataFrame(
        {
            "concept_id_1": [2000000001],
            "concept_id_2": [3004410],
            "relationship_id": ["Maps to"],
        }
    ).write_parquet(rel_dir / "rel.parquet")

    cdm_dir = omop_dir / "cdm_source"
    cdm_dir.mkdir()
    pl.DataFrame(
        {
            "cdm_source_name": ["Test OMOP"],
            "cdm_release_date": ["2024-01-01"],
        }
    ).write_parquet(cdm_dir / "cdm.parquet")

    metadata, code_metadata, concept_id_map, concept_name_map = extract_metadata(str(omop_dir), str(decomp), verbose=0)

    assert metadata["dataset_name"] == "Test OMOP"
    assert 3004410 in concept_id_map
    assert concept_id_map[3004410] == "LOINC/8310-5"
    assert concept_name_map[3004410] == "Body temperature"
    assert "LOINC/8310-5" in code_metadata
    assert "Custom/C001" in code_metadata
    assert "LOINC/8310-5" in code_metadata["Custom/C001"]["parent_codes"]
