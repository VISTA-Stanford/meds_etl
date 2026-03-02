"""
Tests for DSL enhancements:
- >> transform pipe operator
- split() 3-arg support in parser
- $literal: syntax
- filter compilation and application
- config schema validation
- vocabulary provider abstraction
"""

import datetime

import polars as pl
import pytest

from meds_etl.config_compiler import compile_config, compile_filter_expression
from meds_etl.config_parser import Expression, LiteralValue, TemplateParser, parse_config_value
from meds_etl.config_schema import validate_config_schema
from meds_etl.omop_common import apply_filter_conditions, transform_to_meds_unsorted
from meds_etl.vocabulary import OMOPVocabularyProvider, VocabularyRegistry

# ============================================================================
# >> PIPE OPERATOR
# ============================================================================


class TestDoubleArrowPipe:
    """Test the >> transform pipe operator."""

    def test_parse_double_arrow_pipe(self):
        parser = TemplateParser()
        result = parser.parse("@name >> upper()")

        assert isinstance(result, Expression)
        assert len(result.transforms) == 1
        assert result.transforms[0].function_name == "upper"

    def test_parse_double_arrow_chained(self):
        parser = TemplateParser()
        result = parser.parse("@col >> strip() >> upper() >> regex_replace(' ', '_')")

        assert isinstance(result, Expression)
        assert len(result.transforms) == 3
        assert result.transforms[0].function_name == "strip"
        assert result.transforms[1].function_name == "upper"
        assert result.transforms[2].function_name == "regex_replace"

    def test_build_double_arrow_pipe(self):
        expr = parse_config_value("@name >> upper()")

        df = pl.DataFrame({"name": ["alice", "bob"]})
        result = df.select(expr.alias("result"))
        assert result["result"].to_list() == ["ALICE", "BOB"]

    def test_template_with_double_arrow(self):
        expr = parse_config_value("NOTE/{@note_title >> regex_replace('\\s+', '-')}")

        df = pl.DataFrame({"note_title": ["Progress Notes", "Discharge Summary"]})
        result = df.select(expr.alias("result"))
        assert result["result"].to_list() == ["NOTE/Progress-Notes", "NOTE/Discharge-Summary"]

    def test_backwards_compat_single_pipe_still_works(self):
        """Ensure | still works as a pipe operator."""
        expr = parse_config_value("@name | upper()")

        df = pl.DataFrame({"name": ["alice", "bob"]})
        result = df.select(expr.alias("result"))
        assert result["result"].to_list() == ["ALICE", "BOB"]

    def test_compile_double_arrow_template(self):
        config = {
            "tables": {
                "note": {
                    "code": "NOTE/{@note_title >> regex_replace('\\s+', '-')}",
                }
            }
        }
        compiled = compile_config(config)
        code_mappings = compiled["tables"]["note"]["code_mappings"]
        assert "source_value" in code_mappings
        source_cfg = code_mappings["source_value"]
        assert "template" in source_cfg
        assert source_cfg["column_transforms"]["note_title"][0]["type"] == "regex_replace"


# ============================================================================
# SPLIT() 3-ARG IN PARSER
# ============================================================================


class TestSplit3Arg:
    """Test split() with 3 arguments (delimiter, index, default) in parser."""

    def test_parse_split_3_args(self):
        parser = TemplateParser()
        result = parser.parse("@col >> split('|', 3, 'Unknown')")

        assert isinstance(result, Expression)
        assert len(result.transforms) == 1
        assert result.transforms[0].function_name == "split"
        assert result.transforms[0].args == ["|", "3", "Unknown"]

    def test_build_split_3_args_with_value(self):
        expr = parse_config_value("@col >> split('|', 1)")

        df = pl.DataFrame({"col": ["a|b|c", "x|y|z"]})
        result = df.select(expr.alias("result"))
        assert result["result"].to_list() == ["b", "y"]

    def test_build_split_3_args_with_default(self):
        expr = parse_config_value("@col >> split('|', 5, 'DEFAULT')")

        df = pl.DataFrame({"col": ["a|b|c", "x|y"]})
        result = df.select(expr.alias("result"))
        assert result["result"].to_list() == ["DEFAULT", "DEFAULT"]

    def test_build_split_3_args_mixed(self):
        expr = parse_config_value("@col >> split('|', 2, 'N/A')")

        df = pl.DataFrame({"col": ["a|b|c", "x|y"]})
        result = df.select(expr.alias("result"))
        assert result["result"].to_list() == ["c", "N/A"]


# ============================================================================
# $literal: SYNTAX
# ============================================================================


class TestLiteralSyntax:
    """Test $literal: syntax for explicit literals."""

    def test_parse_literal_prefix(self):
        parser = TemplateParser()
        result = parser.parse("$literal:person")

        # $literal:person is a simple literal with no transforms, so the parser
        # unwraps it to a bare LiteralValue rather than Expression(LiteralValue).
        assert isinstance(result, LiteralValue)
        assert result.value == "person"

    def test_build_literal_prefix(self):
        expr = parse_config_value("$literal:hello world")

        df = pl.DataFrame({"dummy": [1, 2]})
        result = df.select(expr.alias("result"))
        assert result["result"][0] == "hello world"

    def test_compile_literal_prefix_in_property(self):
        config = {
            "tables": {
                "visit": {
                    "properties": [
                        {"name": "table", "value": "$literal:visit_occurrence", "type": "string"},
                    ]
                }
            }
        }
        compiled = compile_config(config)
        prop = compiled["tables"]["visit"]["properties"][0]
        assert prop["literal"] == "visit_occurrence"
        assert "value" not in prop

    def test_bare_string_property_raises_error(self):
        config = {
            "tables": {
                "visit": {
                    "properties": [
                        {"name": "table", "value": "visit_occurrence", "type": "string"},
                    ]
                }
            }
        }
        with pytest.raises(ValueError, match="Ambiguous property value"):
            compile_config(config)

    def test_non_string_literal_still_works(self):
        """Numeric literals in property values should pass through unchanged."""
        config = {
            "tables": {
                "visit": {
                    "properties": [
                        {"name": "count", "value": 42, "type": "int"},
                    ]
                }
            }
        }
        compiled = compile_config(config)
        prop = compiled["tables"]["visit"]["properties"][0]
        assert prop["literal"] == 42


# ============================================================================
# FILTER COMPILATION & APPLICATION
# ============================================================================


class TestFilterCompilation:
    """Test filter expression compilation."""

    def test_compile_not_equal(self):
        conditions = compile_filter_expression("@concept_id != 0")
        assert len(conditions) == 1
        assert conditions[0] == {"field": "concept_id", "op": "!=", "value": 0}

    def test_compile_equal(self):
        conditions = compile_filter_expression("@status == 'active'")
        assert len(conditions) == 1
        assert conditions[0] == {"field": "status", "op": "==", "value": "active"}

    def test_compile_is_not_null(self):
        conditions = compile_filter_expression("@value_as_number IS NOT NULL")
        assert len(conditions) == 1
        assert conditions[0] == {"field": "value_as_number", "op": "is_not_null"}

    def test_compile_is_null(self):
        conditions = compile_filter_expression("@death_date IS NULL")
        assert len(conditions) == 1
        assert conditions[0] == {"field": "death_date", "op": "is_null"}

    def test_compile_in(self):
        conditions = compile_filter_expression("@domain_id IN ('Measurement', 'Observation')")
        assert len(conditions) == 1
        assert conditions[0]["field"] == "domain_id"
        assert conditions[0]["op"] == "in"
        assert conditions[0]["value"] == ["Measurement", "Observation"]

    def test_compile_comparison_operators(self):
        for op in [">", "<", ">=", "<="]:
            conditions = compile_filter_expression(f"@value {op} 5")
            assert conditions[0]["op"] == op
            assert conditions[0]["value"] == 5

    def test_compile_and_conditions(self):
        conditions = compile_filter_expression("@concept_id != 0 AND @value_as_number IS NOT NULL")
        assert len(conditions) == 2
        assert conditions[0]["field"] == "concept_id"
        assert conditions[1]["field"] == "value_as_number"

    def test_compile_filter_in_config(self):
        config = {
            "tables": {
                "measurement": {
                    "filter": "@measurement_concept_id != 0",
                    "time_start": "@measurement_datetime",
                    "code": "$omop:@measurement_concept_id",
                }
            }
        }
        compiled = compile_config(config)
        assert "_compiled_filter" in compiled["tables"]["measurement"]
        conditions = compiled["tables"]["measurement"]["_compiled_filter"]
        assert conditions[0]["field"] == "measurement_concept_id"
        assert conditions[0]["op"] == "!="


class TestFilterApplication:
    """Test runtime filter application."""

    def test_apply_not_equal(self):
        df = pl.DataFrame({"concept_id": [0, 1, 2, 0, 3]})
        conditions = [{"field": "concept_id", "op": "!=", "value": 0}]
        result = apply_filter_conditions(df, conditions)
        assert result["concept_id"].to_list() == [1, 2, 3]

    def test_apply_is_not_null(self):
        df = pl.DataFrame({"value": [1.0, None, 3.0, None]})
        conditions = [{"field": "value", "op": "is_not_null"}]
        result = apply_filter_conditions(df, conditions)
        assert len(result) == 2

    def test_apply_in(self):
        df = pl.DataFrame({"domain": ["Measurement", "Drug", "Observation"]})
        conditions = [{"field": "domain", "op": "in", "value": ["Measurement", "Observation"]}]
        result = apply_filter_conditions(df, conditions)
        assert result["domain"].to_list() == ["Measurement", "Observation"]

    def test_apply_multiple_conditions(self):
        df = pl.DataFrame(
            {
                "concept_id": [0, 1, 2, 3],
                "value": [1.0, None, 3.0, 4.0],
            }
        )
        conditions = [
            {"field": "concept_id", "op": "!=", "value": 0},
            {"field": "value", "op": "is_not_null"},
        ]
        result = apply_filter_conditions(df, conditions)
        assert len(result) == 2
        assert result["concept_id"].to_list() == [2, 3]

    def test_apply_filter_missing_column_is_skipped(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        conditions = [{"field": "nonexistent", "op": "!=", "value": 0}]
        result = apply_filter_conditions(df, conditions)
        assert len(result) == 3

    def test_filter_in_transform_to_meds(self):
        """Integration test: filter applied before MEDS transformation."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3, 4],
                "measurement_datetime": [
                    datetime.datetime(2020, 1, 1),
                    datetime.datetime(2020, 1, 2),
                    datetime.datetime(2020, 1, 3),
                    datetime.datetime(2020, 1, 4),
                ],
                "measurement_concept_id": [100, 0, 200, 0],
            }
        )

        concept_df = pl.DataFrame(
            {
                "concept_id": [100, 200],
                "code": ["LOINC/A", "LOINC/B"],
            }
        )

        table_config = {
            "time_start": "measurement_datetime",
            "code_mappings": {"concept_id": {"concept_id_field": "measurement_concept_id"}},
            "_compiled_filter": [{"field": "measurement_concept_id", "op": "!=", "value": 0}],
        }

        meds_schema = {
            "subject_id": pl.Int64,
            "time": pl.Datetime("us"),
            "code": pl.Utf8,
            "numeric_value": pl.Float32,
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
        assert set(result["code"].to_list()) == {"LOINC/A", "LOINC/B"}


# ============================================================================
# CONFIG SCHEMA VALIDATION
# ============================================================================


class TestConfigSchemaValidation:
    """Test config schema validation."""

    def test_valid_config_passes(self):
        config = {
            "primary_key": "person_id",
            "canonical_events": {
                "birth": {
                    "table": "person",
                    "code": "MEDS_BIRTH",
                    "time_start": "@birth_datetime",
                }
            },
            "tables": {
                "measurement": {
                    "time_start": "@measurement_datetime",
                    "code": "$omop:@measurement_concept_id",
                    "properties": [
                        {"name": "table", "value": "$literal:measurement", "type": "string"},
                    ],
                }
            },
        }
        errors = validate_config_schema(config)
        assert errors == []

    def test_unknown_top_level_key(self):
        config = {
            "tables": {"m": {"time_start": "@t", "code": "@c"}},
            "unknown_key": "value",
        }
        errors = validate_config_schema(config)
        assert any("Unknown top-level key" in e for e in errors)

    def test_missing_time_start(self):
        config = {"tables": {"m": {"code": "@c"}}}
        errors = validate_config_schema(config)
        assert any("missing required key 'time_start'" in e for e in errors)

    def test_missing_code_and_code_mappings(self):
        config = {"tables": {"m": {"time_start": "@t"}}}
        errors = validate_config_schema(config)
        assert any("must define either 'code' or 'code_mappings'" in e for e in errors)

    def test_canonical_missing_table(self):
        config = {
            "canonical_events": {
                "birth": {
                    "code": "MEDS_BIRTH",
                    "time_start": "@birth_datetime",
                }
            }
        }
        errors = validate_config_schema(config)
        assert any("missing required key 'table'" in e for e in errors)

    def test_unknown_table_key(self):
        config = {"tables": {"m": {"time_start": "@t", "code": "@c", "unknown_key": "val"}}}
        errors = validate_config_schema(config)
        assert any("unknown key 'unknown_key'" in e for e in errors)

    def test_invalid_property_type(self):
        config = {
            "tables": {
                "m": {
                    "time_start": "@t",
                    "code": "@c",
                    "properties": [{"name": "x", "value": "@x", "type": "invalid_type"}],
                }
            }
        }
        errors = validate_config_schema(config)
        assert any("invalid type 'invalid_type'" in e for e in errors)

    def test_bare_string_property_value_flagged(self):
        config = {
            "tables": {
                "m": {
                    "time_start": "@t",
                    "code": "@c",
                    "properties": [{"name": "table", "value": "measurement", "type": "string"}],
                }
            }
        }
        errors = validate_config_schema(config)
        assert any("ambiguous bare string" in e.lower() for e in errors)

    def test_unbalanced_braces(self):
        config = {"tables": {"m": {"time_start": "@t", "code": "FOO/{@col"}}}
        errors = validate_config_schema(config)
        assert any("unclosed" in e for e in errors)

    def test_unknown_vocab_flagged(self):
        config = {"tables": {"m": {"time_start": "@t", "code": "$fhir:@concept"}}}
        errors = validate_config_schema(config)
        assert any("unknown vocabulary" in e.lower() for e in errors)

    def test_filter_validation_string(self):
        config = {
            "tables": {
                "m": {
                    "time_start": "@t",
                    "code": "@c",
                    "filter": "@concept_id != 0",
                }
            }
        }
        errors = validate_config_schema(config)
        assert errors == []

    def test_filter_validation_missing_column_ref(self):
        config = {
            "tables": {
                "m": {
                    "time_start": "@t",
                    "code": "@c",
                    "filter": "concept_id != 0",
                }
            }
        }
        errors = validate_config_schema(config)
        assert any("must reference at least one column" in e for e in errors)

    def test_filter_validation_no_operator(self):
        config = {
            "tables": {
                "m": {
                    "time_start": "@t",
                    "code": "@c",
                    "filter": "@concept_id",
                }
            }
        }
        errors = validate_config_schema(config)
        assert any("no recognized operator" in e for e in errors)

    def test_property_type_consistency(self):
        config = {
            "tables": {
                "a": {
                    "time_start": "@t",
                    "code": "@c",
                    "properties": [{"name": "visit_id", "value": "@vid", "type": "int"}],
                },
                "b": {
                    "time_start": "@t",
                    "code": "@c",
                    "properties": [{"name": "visit_id", "value": "@vid", "type": "string"}],
                },
            }
        }
        errors = validate_config_schema(config)
        assert any("inconsistent types" in e for e in errors)


# ============================================================================
# VOCABULARY PROVIDER
# ============================================================================


class TestVocabularyProvider:
    """Test the vocabulary provider abstraction."""

    def test_omop_provider_basic(self):
        concept_df = pl.DataFrame(
            {
                "concept_id": [100, 200],
                "code": ["LOINC/A", "LOINC/B"],
                "concept_name": ["Concept A", "Concept B"],
            }
        )
        provider = OMOPVocabularyProvider(concept_df)

        assert provider.name == "omop"
        assert len(provider.get_concept_df()) == 2

    def test_omop_provider_build_join(self):
        concept_df = pl.DataFrame(
            {
                "concept_id": [100, 200, 300],
                "code": ["LOINC/A", "LOINC/B", "LOINC/C"],
            }
        )
        provider = OMOPVocabularyProvider(concept_df)

        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "my_concept_id": [100, 200, 999],
            }
        )

        result = provider.build_join(df, "my_concept_id", "resolved_code")
        assert "resolved_code" in result.columns
        assert result["resolved_code"][0] == "LOINC/A"
        assert result["resolved_code"][1] == "LOINC/B"
        assert result["resolved_code"][2] is None

    def test_omop_provider_filter_to_used_ids(self):
        concept_df = pl.DataFrame(
            {
                "concept_id": [100, 200, 300],
                "code": ["A", "B", "C"],
            }
        )
        provider = OMOPVocabularyProvider(concept_df)
        provider.filter_to_used_ids({100, 300})
        assert len(provider.get_concept_df()) == 2

    def test_registry(self):
        concept_df = pl.DataFrame(
            {
                "concept_id": [1],
                "code": ["X"],
            }
        )
        provider = OMOPVocabularyProvider(concept_df)

        registry = VocabularyRegistry()
        registry.register(provider)

        assert "omop" in registry
        assert registry.get("omop") is provider
        assert registry.get("fhir") is None
        assert registry.names == ["omop"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
