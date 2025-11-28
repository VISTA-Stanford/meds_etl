"""
Unit tests for configuration template parser.
"""

import polars as pl
import pytest

from meds_etl.config_parser import (
    ColumnRef,
    Expression,
    FallbackChain,
    LiteralValue,
    PolarsExpressionBuilder,
    TemplateParser,
    TemplateString,
    VocabLookup,
    parse_config_value,
)


class TestTemplateParser:
    """Test the template parser."""

    def test_parse_simple_column_reference(self):
        """Test parsing simple column reference: @column_name"""
        parser = TemplateParser()
        result = parser.parse("@drug_exposure_start_datetime")

        assert isinstance(result, Expression)
        assert isinstance(result.expr, ColumnRef)
        assert result.expr.column_name == "drug_exposure_start_datetime"
        assert len(result.transforms) == 0

    def test_parse_simple_literal(self):
        """Test parsing simple literal value."""
        parser = TemplateParser()

        # String literal
        result = parser.parse("MEDS_BIRTH")
        assert isinstance(result, LiteralValue)
        assert result.value == "MEDS_BIRTH"

        # Integer literal
        result = parser.parse("123")
        assert isinstance(result, LiteralValue)
        assert result.value == 123

        # Float literal
        result = parser.parse("123.45")
        assert isinstance(result, LiteralValue)
        assert result.value == 123.45

    def test_parse_vocab_lookup_from_column(self):
        """Test parsing vocabulary lookup from column."""
        parser = TemplateParser()
        result = parser.parse("$omop:@drug_type_concept_id")

        assert isinstance(result, Expression)
        assert isinstance(result.expr, VocabLookup)
        assert result.expr.vocab_name == "omop"
        assert isinstance(result.expr.source, ColumnRef)
        assert result.expr.source.column_name == "drug_type_concept_id"
        assert result.expr.attribute == "concept_name"

    def test_parse_vocab_lookup_from_literal(self):
        """Test parsing vocabulary lookup from literal concept ID."""
        parser = TemplateParser()
        result = parser.parse("$omop:1235243")

        assert isinstance(result, Expression)
        assert isinstance(result.expr, VocabLookup)
        assert result.expr.vocab_name == "omop"
        assert isinstance(result.expr.source, LiteralValue)
        assert result.expr.source.value == 1235243

    def test_parse_vocab_lookup_with_attribute(self):
        """Test parsing vocabulary lookup with explicit attribute."""
        parser = TemplateParser()
        result = parser.parse("$omop:@drug_concept_id:concept_code")

        assert isinstance(result, Expression)
        assert isinstance(result.expr, VocabLookup)
        assert result.expr.vocab_name == "omop"
        assert result.expr.attribute == "concept_code"

    def test_parse_simple_template(self):
        """Test parsing simple template string."""
        parser = TemplateParser()
        result = parser.parse("Gender/{$omop:@gender_concept_id}")

        assert isinstance(result, TemplateString)
        assert len(result.parts) == 2
        assert result.parts[0] == "Gender/"
        assert isinstance(result.parts[1], Expression)

    def test_parse_complex_template(self):
        """Test parsing complex template with multiple parts."""
        parser = TemplateParser()
        result = parser.parse("IMAGE/{@modality_source_value}|{@anatomic_site_source_value}")

        assert isinstance(result, TemplateString)
        assert len(result.parts) == 4
        assert result.parts[0] == "IMAGE/"
        assert isinstance(result.parts[1], Expression)
        assert result.parts[2] == "|"
        assert isinstance(result.parts[3], Expression)

    def test_parse_fallback_chain(self):
        """Test parsing fallback chain with ||."""
        parser = TemplateParser()
        result = parser.parse("@drug_exposure_start_datetime || @drug_exposure_start_date")

        assert isinstance(result, FallbackChain)
        assert len(result.expressions) == 2
        assert all(isinstance(e, Expression) for e in result.expressions)

    def test_parse_complex_fallback_chain(self):
        """Test parsing complex fallback chain."""
        parser = TemplateParser()
        result = parser.parse("$omop:@source_concept_id || $omop:@concept_id || @source_value || $omop:8")

        assert isinstance(result, FallbackChain)
        assert len(result.expressions) == 4

    def test_parse_transform_upper(self):
        """Test parsing upper() transform."""
        parser = TemplateParser()
        result = parser.parse("@column_name | upper()")

        assert isinstance(result, Expression)
        assert len(result.transforms) == 1
        assert result.transforms[0].function_name == "upper"
        assert result.transforms[0].args == []

    def test_parse_transform_regex_replace(self):
        """Test parsing regex_replace() transform."""
        parser = TemplateParser()
        result = parser.parse("@note_title | regex_replace('\\s+', '-')")

        assert isinstance(result, Expression)
        assert len(result.transforms) == 1
        assert result.transforms[0].function_name == "regex_replace"
        assert result.transforms[0].args == ["\\s+", "-"]

    def test_parse_chained_transforms(self):
        """Test parsing multiple chained transforms."""
        parser = TemplateParser()
        result = parser.parse("@column | strip() | upper() | regex_replace(' ', '_')")

        assert isinstance(result, Expression)
        assert len(result.transforms) == 3
        assert result.transforms[0].function_name == "strip"
        assert result.transforms[1].function_name == "upper"
        assert result.transforms[2].function_name == "regex_replace"

    def test_parse_template_with_transforms(self):
        """Test parsing template with transforms inside braces."""
        parser = TemplateParser()
        result = parser.parse("NOTE/{@note_title | regex_replace('\\s+', '-')}")

        assert isinstance(result, TemplateString)
        assert len(result.parts) == 2
        assert result.parts[0] == "NOTE/"
        assert isinstance(result.parts[1], Expression)
        assert len(result.parts[1].transforms) == 1

    def test_parse_empty_string(self):
        """Test parsing empty string."""
        parser = TemplateParser()
        result = parser.parse("")

        assert isinstance(result, LiteralValue)
        assert result.value == ""

    def test_parse_whitespace_only(self):
        """Test parsing whitespace-only string."""
        parser = TemplateParser()
        result = parser.parse("   ")

        assert isinstance(result, LiteralValue)
        assert result.value == ""


class TestPolarsExpressionBuilder:
    """Test the Polars expression builder."""

    def test_build_column_reference(self):
        """Test building expression for column reference."""
        parser = TemplateParser()
        parsed = parser.parse("@person_id")

        builder = PolarsExpressionBuilder()
        expr = builder.build(parsed)

        # Test with sample data
        df = pl.DataFrame({"person_id": [1, 2, 3]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == [1, 2, 3]

    def test_build_literal(self):
        """Test building expression for literal value."""
        parser = TemplateParser()
        parsed = parser.parse("MEDS_BIRTH")

        builder = PolarsExpressionBuilder()
        expr = builder.build(parsed)

        # Test with sample data
        df = pl.DataFrame({"dummy": [1, 2, 3]})
        result = df.select(expr.alias("result"))

        # Literal values don't broadcast - they just return a single value
        # To get multiple rows, we need to use lit() which broadcasts
        assert result["result"].to_list() == ["MEDS_BIRTH"]

    def test_build_template_string(self):
        """Test building expression for template string."""
        parser = TemplateParser()
        parsed = parser.parse("Gender/{@gender_value}")

        builder = PolarsExpressionBuilder()
        expr = builder.build(parsed)

        # Test with sample data
        df = pl.DataFrame({"gender_value": ["M", "F", "O"]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == ["Gender/M", "Gender/F", "Gender/O"]

    def test_build_fallback_chain(self):
        """Test building expression for fallback chain."""
        parser = TemplateParser()
        parsed = parser.parse("@primary || @secondary || @tertiary")

        builder = PolarsExpressionBuilder()
        expr = builder.build(parsed)

        # Test with sample data
        df = pl.DataFrame({"primary": [1, None, None], "secondary": [None, 2, None], "tertiary": [None, None, 3]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == [1, 2, 3]

    def test_build_transform_upper(self):
        """Test building expression with upper() transform."""
        parser = TemplateParser()
        parsed = parser.parse("@name | upper()")

        builder = PolarsExpressionBuilder()
        expr = builder.build(parsed)

        # Test with sample data
        df = pl.DataFrame({"name": ["alice", "bob", "charlie"]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == ["ALICE", "BOB", "CHARLIE"]

    def test_build_transform_lower(self):
        """Test building expression with lower() transform."""
        parser = TemplateParser()
        parsed = parser.parse("@name | lower()")

        builder = PolarsExpressionBuilder()
        expr = builder.build(parsed)

        # Test with sample data
        df = pl.DataFrame({"name": ["ALICE", "BOB", "CHARLIE"]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == ["alice", "bob", "charlie"]

    def test_build_transform_strip(self):
        """Test building expression with strip() transform."""
        parser = TemplateParser()
        parsed = parser.parse("@name | strip()")

        builder = PolarsExpressionBuilder()
        expr = builder.build(parsed)

        # Test with sample data
        df = pl.DataFrame({"name": ["  alice  ", "  bob  ", "  charlie  "]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == ["alice", "bob", "charlie"]

    def test_build_transform_regex_replace(self):
        """Test building expression with regex_replace() transform."""
        parser = TemplateParser()
        parsed = parser.parse("@text | regex_replace('\\s+', '-')")

        builder = PolarsExpressionBuilder()
        expr = builder.build(parsed)

        # Test with sample data
        df = pl.DataFrame({"text": ["hello world", "foo  bar", "test   data"]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == ["hello-world", "foo-bar", "test-data"]

    def test_build_transform_replace(self):
        """Test building expression with replace() transform."""
        parser = TemplateParser()
        parsed = parser.parse("@text | replace(' ', '_')")

        builder = PolarsExpressionBuilder()
        expr = builder.build(parsed)

        # Test with sample data
        df = pl.DataFrame({"text": ["hello world", "foo bar", "test data"]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == ["hello_world", "foo_bar", "test_data"]

    def test_build_chained_transforms(self):
        """Test building expression with chained transforms."""
        parser = TemplateParser()
        parsed = parser.parse("@text | strip() | upper() | regex_replace(' ', '_')")

        builder = PolarsExpressionBuilder()
        expr = builder.build(parsed)

        # Test with sample data
        df = pl.DataFrame({"text": ["  hello world  ", "  foo bar  "]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == ["HELLO_WORLD", "FOO_BAR"]

    def test_build_complex_template(self):
        """Test building complex template with multiple parts."""
        parser = TemplateParser()
        parsed = parser.parse("IMAGE/{@modality}|{@site}")

        builder = PolarsExpressionBuilder()
        expr = builder.build(parsed)

        # Test with sample data
        df = pl.DataFrame({"modality": ["CT", "MRI", "XRAY"], "site": ["CHEST", "BRAIN", "HAND"]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == ["IMAGE/CT|CHEST", "IMAGE/MRI|BRAIN", "IMAGE/XRAY|HAND"]

    def test_build_template_with_null_values(self):
        """Test building template with null values in columns."""
        parser = TemplateParser()
        parsed = parser.parse("PREFIX/{@col1}_{@col2}")

        builder = PolarsExpressionBuilder()
        expr = builder.build(parsed)

        # Test with sample data including nulls
        df = pl.DataFrame({"col1": ["A", None, "C"], "col2": ["X", "Y", None]})
        result = df.select(expr.alias("result"))

        # Nulls should be replaced with empty string
        assert result["result"].to_list() == ["PREFIX/A_X", "PREFIX/_Y", "PREFIX/C_"]


class TestParseConfigValue:
    """Test the main parse_config_value function."""

    def test_parse_simple_column(self):
        """Test parsing simple column reference."""
        expr = parse_config_value("@person_id")

        df = pl.DataFrame({"person_id": [1, 2, 3]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == [1, 2, 3]

    def test_parse_fallback_chain(self):
        """Test parsing fallback chain."""
        expr = parse_config_value("@start_datetime || @start_date")

        df = pl.DataFrame({"start_datetime": [1, None, 3], "start_date": [None, 2, None]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == [1, 2, 3]

    def test_parse_template(self):
        """Test parsing template string."""
        expr = parse_config_value("CODE/{@code_value}")

        df = pl.DataFrame({"code_value": ["A", "B", "C"]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == ["CODE/A", "CODE/B", "CODE/C"]

    def test_parse_with_transforms(self):
        """Test parsing with transforms."""
        expr = parse_config_value("@name | upper()")

        df = pl.DataFrame({"name": ["alice", "bob"]})
        result = df.select(expr.alias("result"))

        assert result["result"].to_list() == ["ALICE", "BOB"]


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_invalid_transform_syntax(self):
        """Test that invalid transform syntax raises error."""
        parser = TemplateParser()

        with pytest.raises(ValueError, match="Invalid transform syntax"):
            parsed = parser.parse("@col | invalid syntax")
            builder = PolarsExpressionBuilder()
            builder.build(parsed)

    def test_unknown_transform_function(self):
        """Test that unknown transform function raises error."""
        parser = TemplateParser()
        parsed = parser.parse("@col | unknown_func()")

        builder = PolarsExpressionBuilder()
        with pytest.raises(ValueError, match="Unknown transform function"):
            builder.build(parsed)

    def test_regex_replace_wrong_args(self):
        """Test that regex_replace with wrong number of args raises error."""
        parser = TemplateParser()
        parsed = parser.parse("@col | regex_replace('pattern')")

        builder = PolarsExpressionBuilder()
        with pytest.raises(ValueError, match="regex_replace requires 2 arguments"):
            builder.build(parsed)

    def test_vocab_lookup_without_concept_df(self):
        """Test that vocab lookup without concept_df still works (returns source)."""
        parser = TemplateParser()
        parsed = parser.parse("$omop:@drug_concept_id")

        builder = PolarsExpressionBuilder(concept_df=None)
        # Should not raise error, but return the source column
        expr = builder.build(parsed)

        # Test that it returns the source column value
        df = pl.DataFrame({"drug_concept_id": [123, 456, 789]})
        result = df.select(expr.alias("result"))
        assert result["result"].to_list() == [123, 456, 789]

    def test_complex_real_world_example(self):
        """Test a complex real-world example from the config."""
        expr = parse_config_value("$omop:@drug_source_concept_id || $omop:@drug_concept_id || @drug_source_value")

        # This should create a fallback chain
        # First try source_concept_id, then concept_id, then source_value
        df = pl.DataFrame(
            {
                # Keep everything Utf8 so pl.coalesce can evaluate the fallback chain
                "drug_source_concept_id": [None, None, "3"],
                "drug_concept_id": [None, "2", None],
                "drug_source_value": ["aspirin", None, None],
            }
        )

        result = df.select(expr.alias("result"))
        assert result["result"].to_list() == ["aspirin", "2", "3"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
