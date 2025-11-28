"""
Test the config compiler.
"""

import pytest

from meds_etl.config_compiler import compile_config


def test_compile_time_fallbacks():
    """Test compiling time_start with fallbacks."""
    config = {"tables": {"drug_exposure": {"time_start": "@drug_exposure_start_datetime || @drug_exposure_start_date"}}}

    compiled = compile_config(config)

    assert compiled["tables"]["drug_exposure"]["time_start"] == "drug_exposure_start_datetime"
    assert compiled["tables"]["drug_exposure"]["time_start_fallbacks"] == ["drug_exposure_start_date"]


def test_compile_simple_property():
    """Test compiling simple property with @column."""
    config = {
        "tables": {
            "drug_exposure": {"properties": [{"name": "visit_id", "value": "@visit_occurrence_id", "type": "int"}]}
        }
    }

    compiled = compile_config(config)

    prop = compiled["tables"]["drug_exposure"]["properties"][0]
    assert "value" not in prop
    assert prop["name"] == "visit_occurrence_id"
    assert prop["alias"] == "visit_id"


def test_compile_code_fallback_chain():
    """Test compiling fallback code expression."""
    config = {
        "tables": {
            "procedure_occurrence": {
                "code": "$omop:@procedure_source_concept_id || $omop:@procedure_concept_id || @procedure_source_value"
            }
        }
    }

    compiled = compile_config(config)
    code_mappings = compiled["tables"]["procedure_occurrence"]["code_mappings"]

    assert "concept_id" in code_mappings
    concept_cfg = code_mappings["concept_id"]
    assert concept_cfg["source_concept_id_field"] == "procedure_source_concept_id"
    assert concept_cfg["concept_id_field"] == "procedure_concept_id"

    assert "source_value" in code_mappings
    assert code_mappings["source_value"]["field"] == "procedure_source_value"


def test_compile_literal_property():
    """Test compiling property with literal value."""
    config = {
        "tables": {"drug_exposure": {"properties": [{"name": "table", "value": "drug_exposure", "type": "string"}]}}
    }

    compiled = compile_config(config)

    prop = compiled["tables"]["drug_exposure"]["properties"][0]
    assert "value" not in prop
    assert prop["literal"] == "drug_exposure"
    assert prop["name"] == "table"


def test_compile_preserves_old_format():
    """Test that old format is preserved."""
    config = {
        "tables": {
            "drug_exposure": {
                "time_start": "drug_exposure_start_datetime",
                "time_start_fallbacks": ["drug_exposure_start_date"],
                "properties": [{"name": "visit_occurrence_id", "alias": "visit_id", "type": "int"}],
            }
        }
    }

    compiled = compile_config(config)

    # Should be unchanged
    assert compiled["tables"]["drug_exposure"]["time_start"] == "drug_exposure_start_datetime"
    assert compiled["tables"]["drug_exposure"]["time_start_fallbacks"] == ["drug_exposure_start_date"]
    assert compiled["tables"]["drug_exposure"]["properties"][0]["name"] == "visit_occurrence_id"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
