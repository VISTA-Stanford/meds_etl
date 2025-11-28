"""
Test the new templated config format with the example config file.
"""

import json
from pathlib import Path

import polars as pl

from meds_etl.config_parser import parse_config_value


def test_parse_new_config_examples():
    """Test parsing examples from the new config format."""

    # Load the new config
    config_path = Path(__file__).parent.parent / "examples" / "omop_etl_vista_config_templated.json"
    with open(config_path) as f:
        config = json.load(f)

    # Test birth event
    birth = config["canonical_events"]["birth"]
    assert birth["code"] == "MEDS_BIRTH"
    time_expr = parse_config_value(birth["time_start"])
    # Should be a simple column reference
    assert time_expr is not None

    # Test gender event with vocab lookup
    gender = config["canonical_events"]["gender"]
    code_expr = parse_config_value(gender["code"])
    assert code_expr is not None

    # Test visit_occurrence with fallbacks
    visit = config["tables"]["visit_occurrence"]
    time_expr = parse_config_value(visit["time_start"])
    assert time_expr is not None

    code_expr = parse_config_value(visit["code"])
    assert code_expr is not None

    # Test drug_exposure
    drug = config["tables"]["drug_exposure"]
    time_expr = parse_config_value(drug["time_start"])
    code_expr = parse_config_value(drug["code"])
    assert time_expr is not None
    assert code_expr is not None

    # Test properties with vocab lookups
    drug_type_prop = next((p for p in drug["properties"] if p["name"] == "drug_type"), None)
    assert drug_type_prop is not None
    expr = parse_config_value(drug_type_prop["value"])
    assert expr is not None

    # Test note with transform
    note = config["tables"]["note"]
    code_expr = parse_config_value(note["code"])
    assert code_expr is not None

    # Test with actual data
    df = pl.DataFrame({"note_title": ["Progress Note", "Discharge Summary", "Procedure Note"]})
    result = df.select(code_expr.alias("code"))
    expected = ["NOTE/Progress-Note", "NOTE/Discharge-Summary", "NOTE/Procedure-Note"]
    assert result["code"].to_list() == expected, f"Got {result['code'].to_list()}, expected {expected}"

    # Test image_occurrence with complex template
    image = config["tables"]["image_occurrence"]
    code_expr = parse_config_value(image["code"])
    assert code_expr is not None

    df = pl.DataFrame({"modality_source_value": ["CT", "MRI"], "anatomic_site_source_value": ["CHEST", "BRAIN"]})
    result = df.select(code_expr.alias("code"))
    expected = ["IMAGE/CT|CHEST", "IMAGE/MRI|BRAIN"]
    assert result["code"].to_list() == expected


def test_parse_all_time_fields():
    """Test that all time fields can be parsed."""
    config_path = Path(__file__).parent.parent / "examples" / "omop_etl_vista_config_templated.json"
    with open(config_path) as f:
        config = json.load(f)

    # Test canonical events
    for event_name, event_config in config["canonical_events"].items():
        if "time_start" in event_config:
            expr = parse_config_value(event_config["time_start"])
            assert expr is not None, f"Failed to parse time_start for {event_name}"

    # Test tables
    for table_name, table_config in config["tables"].items():
        if "time_start" in table_config:
            expr = parse_config_value(table_config["time_start"])
            assert expr is not None, f"Failed to parse time_start for {table_name}"

        if "time_end" in table_config:
            expr = parse_config_value(table_config["time_end"])
            assert expr is not None, f"Failed to parse time_end for {table_name}"


def test_parse_all_code_fields():
    """Test that all code fields can be parsed."""
    config_path = Path(__file__).parent.parent / "examples" / "omop_etl_vista_config_templated.json"
    with open(config_path) as f:
        config = json.load(f)

    # Test canonical events
    for event_name, event_config in config["canonical_events"].items():
        if "code" in event_config:
            expr = parse_config_value(event_config["code"])
            assert expr is not None, f"Failed to parse code for {event_name}"

    # Test tables
    for table_name, table_config in config["tables"].items():
        if "code" in table_config:
            expr = parse_config_value(table_config["code"])
            assert expr is not None, f"Failed to parse code for {table_name}"


def test_parse_all_properties():
    """Test that all property values can be parsed."""
    config_path = Path(__file__).parent.parent / "examples" / "omop_etl_vista_config_templated.json"
    with open(config_path) as f:
        config = json.load(f)

    # Test canonical events
    for event_name, event_config in config["canonical_events"].items():
        for prop in event_config.get("properties", []):
            if "value" in prop:
                expr = parse_config_value(prop["value"])
                assert expr is not None, f"Failed to parse property {prop['name']} for {event_name}"

    # Test tables
    for table_name, table_config in config["tables"].items():
        for prop in table_config.get("properties", []):
            if "value" in prop:
                expr = parse_config_value(prop["value"])
                assert expr is not None, f"Failed to parse property {prop['name']} for {table_name}"


if __name__ == "__main__":
    import pytest

    pytest.main([__file__, "-v"])
