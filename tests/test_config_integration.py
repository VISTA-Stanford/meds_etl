"""
Tests for config integration layer.
"""

import polars as pl
import pytest

from meds_etl.config_integration import is_new_config_format, parse_code_field, parse_property_value, parse_time_field


class TestParseTimeField:
    """Test parsing time fields in both old and new formats."""

    def test_old_format_single_field(self):
        """Test old format with single time field."""
        config = {"time_start": "drug_exposure_start_datetime"}
        expr = parse_time_field(config)

        df = pl.DataFrame({"drug_exposure_start_datetime": ["2024-01-01", "2024-01-02"]})
        df = df.with_columns(pl.col("drug_exposure_start_datetime").str.to_datetime())
        result = df.select(expr.alias("time"))

        assert len(result) == 2

    def test_old_format_with_fallbacks(self):
        """Test old format with fallback fields."""
        config = {"time_start": "drug_exposure_start_datetime", "time_start_fallbacks": ["drug_exposure_start_date"]}
        expr = parse_time_field(config)

        df = pl.DataFrame(
            {"drug_exposure_start_datetime": [None, "2024-01-02"], "drug_exposure_start_date": ["2024-01-01", None]}
        )
        df = df.with_columns(
            [
                pl.col("drug_exposure_start_datetime").str.to_datetime(),
                pl.col("drug_exposure_start_date").str.to_datetime(),
            ]
        )
        result = df.select(expr.alias("time"))

        assert result["time"][0] is not None
        assert result["time"][1] is not None

    def test_new_format_with_fallbacks(self):
        """Test new format with || fallbacks."""
        config = {"time_start": "@drug_exposure_start_datetime || @drug_exposure_start_date"}
        expr = parse_time_field(config)

        df = pl.DataFrame(
            {"drug_exposure_start_datetime": [None, "2024-01-02"], "drug_exposure_start_date": ["2024-01-01", None]}
        )
        df = df.with_columns(
            [
                pl.col("drug_exposure_start_datetime").str.to_datetime(),
                pl.col("drug_exposure_start_date").str.to_datetime(),
            ]
        )
        result = df.select(expr.alias("time"))

        assert result["time"][0] is not None
        assert result["time"][1] is not None


class TestParseCodeField:
    """Test parsing code fields in both old and new formats."""

    def test_literal_code(self):
        """Test literal code value."""
        config = {"code": "MEDS_BIRTH"}
        expr = parse_code_field(config)

        df = pl.DataFrame({"dummy": [1, 2]})
        result = df.select(expr.alias("code"))

        # Literals return a single value, not broadcasted
        assert len(result) == 1
        assert result["code"][0] == "MEDS_BIRTH"

    def test_new_format_simple(self):
        """Test new format with simple column reference."""
        config = {"code": "@drug_source_value"}
        expr = parse_code_field(config)

        df = pl.DataFrame({"drug_source_value": ["aspirin", "ibuprofen"]})
        result = df.select(expr.alias("code"))

        assert result["code"].to_list() == ["aspirin", "ibuprofen"]

    def test_new_format_with_fallbacks(self):
        """Test new format with fallback chain."""
        config = {"code": "@source_concept_id || @concept_id || @source_value"}
        expr = parse_code_field(config)

        df = pl.DataFrame(
            {
                "source_concept_id": [None, None, 3],
                "concept_id": [None, 2, None],
                "source_value": ["value1", None, None],
            }
        )
        result = df.select(expr.alias("code"))

        assert result["code"][0] == "value1"
        # Note: coalesce maintains the type of the first non-null column, so this will be a string
        assert result["code"][1] == "2"
        assert result["code"][2] == "3"

    def test_new_format_template(self):
        """Test new format with template."""
        config = {"code": "Gender/{@gender_value}"}
        expr = parse_code_field(config)

        df = pl.DataFrame({"gender_value": ["M", "F", "O"]})
        result = df.select(expr.alias("code"))

        assert result["code"].to_list() == ["Gender/M", "Gender/F", "Gender/O"]

    def test_old_format_returns_none(self):
        """Test that old code_mappings format returns None."""
        config = {"code_mappings": {"source_value": {"field": "drug_source_value"}}}
        expr = parse_code_field(config)

        # Should return None to indicate old format
        assert expr is None


class TestParsePropertyValue:
    """Test parsing property values in both old and new formats."""

    def test_new_format_column_ref(self):
        """Test new format with column reference."""
        prop_config = {"name": "visit_id", "value": "@visit_occurrence_id", "type": "int"}
        expr = parse_property_value(prop_config)

        df = pl.DataFrame({"visit_occurrence_id": [1, 2, 3]})
        result = df.select(expr.alias("visit_id"))

        assert result["visit_id"].to_list() == [1, 2, 3]

    def test_new_format_literal(self):
        """Test new format with literal value."""
        prop_config = {"name": "table", "value": "drug_exposure", "type": "string"}
        expr = parse_property_value(prop_config)

        df = pl.DataFrame({"dummy": [1, 2]})
        result = df.select(expr.alias("table"))

        # Literals return a single value, not broadcasted
        assert len(result) == 1
        assert result["table"][0] == "drug_exposure"

    def test_old_format_with_literal(self):
        """Test old format with literal field."""
        prop_config = {"name": "table", "literal": "drug_exposure", "type": "string"}
        expr = parse_property_value(prop_config)

        df = pl.DataFrame({"dummy": [1, 2]})
        result = df.select(expr.alias("table"))

        # Literals return a single value, not broadcasted
        assert len(result) == 1
        assert result["table"][0] == "drug_exposure"

    def test_old_format_with_alias(self):
        """Test old format with alias (name is column, alias is output)."""
        prop_config = {"name": "visit_occurrence_id", "alias": "visit_id", "type": "int"}
        expr = parse_property_value(prop_config)

        df = pl.DataFrame({"visit_occurrence_id": [1, 2, 3]})
        result = df.select(expr.alias("visit_id"))

        assert result["visit_id"].to_list() == [1, 2, 3]

    def test_old_format_without_alias(self):
        """Test old format without alias (name is both column and output)."""
        prop_config = {"name": "provider_id", "type": "int"}
        expr = parse_property_value(prop_config)

        df = pl.DataFrame({"provider_id": [10, 20, 30]})
        result = df.select(expr.alias("provider_id"))

        assert result["provider_id"].to_list() == [10, 20, 30]


class TestIsNewConfigFormat:
    """Test detection of new vs old config format."""

    def test_detects_new_format_with_code(self):
        """Test detection via new 'code' field."""
        config = {"tables": {"drug_exposure": {"code": "@drug_source_value"}}}
        assert is_new_config_format(config) is True

    def test_detects_new_format_with_time_template(self):
        """Test detection via templated time_start."""
        config = {"tables": {"drug_exposure": {"time_start": "@start_datetime || @start_date"}}}
        assert is_new_config_format(config) is True

    def test_detects_new_format_with_property_value(self):
        """Test detection via property 'value' field."""
        config = {"tables": {"drug_exposure": {"properties": [{"name": "visit_id", "value": "@visit_occurrence_id"}]}}}
        assert is_new_config_format(config) is True

    def test_detects_old_format(self):
        """Test detection of old format."""
        config = {
            "tables": {
                "drug_exposure": {
                    "time_start": "drug_exposure_start_datetime",
                    "code_mappings": {"source_value": {"field": "drug_source_value"}},
                    "properties": [{"name": "visit_occurrence_id", "alias": "visit_id"}],
                }
            }
        }
        assert is_new_config_format(config) is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
