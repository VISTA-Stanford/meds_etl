"""
Test extracting column names from template syntax.
"""

import pytest

from meds_etl.config_integration import extract_column_names_from_template, is_template_syntax


class TestExtractColumnNames:
    """Test extracting column names from template strings."""

    def test_simple_column_ref(self):
        """Test simple column reference."""
        result = extract_column_names_from_template("@birth_datetime")
        assert result == {"birth_datetime"}

    def test_fallback_chain(self):
        """Test fallback chain with multiple columns."""
        result = extract_column_names_from_template("@drug_exposure_start_datetime || @drug_exposure_start_date")
        assert result == {"drug_exposure_start_datetime", "drug_exposure_start_date"}

    def test_vocab_lookup(self):
        """Test vocabulary lookup - should extract the column."""
        result = extract_column_names_from_template("$omop:@drug_concept_id")
        assert result == {"drug_concept_id"}

    def test_template_string(self):
        """Test template string with multiple columns."""
        result = extract_column_names_from_template("IMAGE/{@modality_source_value}|{@anatomic_site_source_value}")
        assert result == {"modality_source_value", "anatomic_site_source_value"}

    def test_complex_expression(self):
        """Test complex expression."""
        result = extract_column_names_from_template(
            "$omop:@drug_source_concept_id || $omop:@drug_concept_id || @drug_source_value"
        )
        assert result == {"drug_source_concept_id", "drug_concept_id", "drug_source_value"}

    def test_no_columns(self):
        """Test string with no column references."""
        result = extract_column_names_from_template("MEDS_BIRTH")
        assert result == set()

    def test_literal_number(self):
        """Test with literal number in vocab lookup."""
        result = extract_column_names_from_template("$omop:1235243")
        assert result == set()


class TestIsTemplateSyntax:
    """Test detecting template syntax."""

    def test_column_ref(self):
        """Test @ marker."""
        assert is_template_syntax("@column_name") is True

    def test_vocab_lookup(self):
        """Test $ marker."""
        assert is_template_syntax("$omop:@column") is True

    def test_template_braces(self):
        """Test { marker."""
        assert is_template_syntax("PREFIX/{@column}") is True

    def test_fallback(self):
        """Test || marker."""
        assert is_template_syntax("@col1 || @col2") is True

    def test_plain_string(self):
        """Test plain string without markers."""
        assert is_template_syntax("plain_column_name") is False

    def test_literal(self):
        """Test literal value."""
        assert is_template_syntax("MEDS_BIRTH") is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
