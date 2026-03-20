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
    """Test compiling property with $literal: value."""
    config = {
        "tables": {
            "drug_exposure": {"properties": [{"name": "table", "value": "$literal:drug_exposure", "type": "string"}]}
        }
    }

    compiled = compile_config(config)

    prop = compiled["tables"]["drug_exposure"]["properties"][0]
    assert "value" not in prop
    assert prop["literal"] == "drug_exposure"
    assert prop["name"] == "table"


def test_compile_bare_string_property_errors():
    """Test that bare string property values raise an error."""
    config = {
        "tables": {"drug_exposure": {"properties": [{"name": "table", "value": "drug_exposure", "type": "string"}]}}
    }

    with pytest.raises(ValueError, match="Ambiguous property value"):
        compile_config(config)


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


def test_property_omop_lookup_compiles():
    """$omop.lookup: in property value compiles to concept_lookup_field."""
    config = {
        "tables": {
            "condition_occurrence": {
                "code": "$omop.lookup:@condition_concept_id",
                "time_start": "@condition_start_datetime",
                "properties": [
                    {"name": "source_code", "value": "$omop.lookup:@condition_source_concept_id", "type": "string"},
                    {"name": "standard_code", "value": "$omop.lookup:@condition_concept_id", "type": "string"},
                ],
            }
        }
    }
    compiled = compile_config(config)
    props = compiled["tables"]["condition_occurrence"]["properties"]
    for prop in props:
        assert "concept_lookup_field" in prop, f"Property '{prop['name']}' was not compiled"
        assert "value" not in prop, f"Property '{prop['name']}' still has raw 'value'"


def test_property_omop_resolve_compiles():
    """$omop.resolve: in property value compiles to concept_lookup_field."""
    config = {
        "tables": {
            "condition_occurrence": {
                "code": "$omop.resolve:@condition_source_concept_id",
                "time_start": "@condition_start_datetime",
                "properties": [
                    {"name": "source_code", "value": "$omop.resolve:@condition_source_concept_id", "type": "string"},
                ],
            }
        }
    }
    compiled = compile_config(config)
    props = compiled["tables"]["condition_occurrence"]["properties"]
    assert props[0].get("concept_lookup_field") == "condition_source_concept_id"


def test_text_value_omop_lookup_compiles():
    """$omop.lookup: in text_value compiles to text_value_field_concept_lookup."""
    config = {
        "canonical_events": {
            "gender": {
                "table": "person",
                "code": "MEDS_BIRTH",
                "time_start": "@birth_datetime",
                "text_value": "$omop.lookup:@gender_concept_id",
            }
        }
    }
    compiled = compile_config(config)
    event = compiled["canonical_events"]["gender"]
    assert "text_value_field_concept_lookup" in event
    assert event["text_value_field_concept_lookup"] == "gender_concept_id"
    assert "text_value" not in event


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
