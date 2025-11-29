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
    assert prop["name"] == "visit_id"
    assert prop.get("source") == "visit_occurrence_id"


def test_compile_property_concept_lookup():
    """Test compiling property with $omop lookup."""
    config = {
        "tables": {
            "drug_exposure": {
                "properties": [{"name": "drug_type", "value": "$omop:@drug_type_concept_id", "type": "string"}]
            }
        }
    }

    compiled = compile_config(config)

    prop = compiled["tables"]["drug_exposure"]["properties"][0]
    assert prop["name"] == "drug_type"
    assert prop["concept_lookup_field"] == "drug_type_concept_id"


def test_compile_property_alias_source():
    """Test compiling property with alias preserves output name."""
    config = {
        "tables": {
            "visit_occurrence": {
                "properties": [{"name": "visit_type_label", "value": "@visit_type_concept_id", "type": "string"}]
            }
        }
    }

    compiled = compile_config(config)

    prop = compiled["tables"]["visit_occurrence"]["properties"][0]
    assert prop["name"] == "visit_type_label"
    assert prop.get("source") == "visit_type_concept_id"


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


def test_compile_simple_omop_field_reference():
    """Test that simple $omop:@field references don't create templates."""
    config = {"tables": {"observation": {"code": "$omop:@observation_source_concept_id"}}}

    compiled = compile_config(config)
    code_mappings = compiled["tables"]["observation"]["code_mappings"]

    # Should create concept_id mapping without template
    assert "concept_id" in code_mappings
    concept_cfg = code_mappings["concept_id"]
    assert concept_cfg["source_concept_id_field"] == "observation_source_concept_id"
    # Crucially: no template key should be present
    assert "template" not in concept_cfg


def test_compile_simple_source_value_reference():
    """Test that simple @field references don't create templates."""
    config = {"tables": {"note": {"code": "@note_title"}}}

    compiled = compile_config(config)
    code_mappings = compiled["tables"]["note"]["code_mappings"]

    # Should create source_value mapping without template
    assert "source_value" in code_mappings
    source_cfg = code_mappings["source_value"]
    assert source_cfg["field"] == "note_title"
    # Crucially: no template key should be present
    assert "template" not in source_cfg


def test_compile_template_with_omop_field():
    """Test that actual templates with $omop:@field DO create template key."""
    config = {"tables": {"person": {"code": "Gender/{$omop:@gender_concept_id}"}}}

    compiled = compile_config(config)
    code_mappings = compiled["tables"]["person"]["code_mappings"]

    # Should create concept_id mapping WITH template
    assert "concept_id" in code_mappings
    concept_cfg = code_mappings["concept_id"]
    assert concept_cfg["concept_id_field"] == "gender_concept_id"
    # Template key SHOULD be present for actual templates
    assert "template" in concept_cfg
    assert concept_cfg["template"] == "Gender/{gender_concept_id}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
