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
        groups = compile_filter_expression("@concept_id != 0")
        assert len(groups) == 1
        assert groups[0] == [{"field": "concept_id", "op": "!=", "value": 0}]

    def test_compile_equal(self):
        groups = compile_filter_expression("@status == 'active'")
        assert len(groups) == 1
        assert groups[0] == [{"field": "status", "op": "==", "value": "active"}]

    def test_compile_is_not_null(self):
        groups = compile_filter_expression("@value_as_number IS NOT NULL")
        assert len(groups) == 1
        assert groups[0] == [{"field": "value_as_number", "op": "is_not_null"}]

    def test_compile_is_null(self):
        groups = compile_filter_expression("@death_date IS NULL")
        assert len(groups) == 1
        assert groups[0] == [{"field": "death_date", "op": "is_null"}]

    def test_compile_in(self):
        groups = compile_filter_expression("@domain_id IN ('Measurement', 'Observation')")
        assert len(groups) == 1
        assert groups[0][0]["field"] == "domain_id"
        assert groups[0][0]["op"] == "in"
        assert groups[0][0]["value"] == ["Measurement", "Observation"]

    def test_compile_comparison_operators(self):
        for op in [">", "<", ">=", "<="]:
            groups = compile_filter_expression(f"@value {op} 5")
            assert groups[0][0]["op"] == op
            assert groups[0][0]["value"] == 5

    def test_compile_and_conditions(self):
        groups = compile_filter_expression("@concept_id != 0 AND @value_as_number IS NOT NULL")
        assert len(groups) == 1
        assert len(groups[0]) == 2
        assert groups[0][0]["field"] == "concept_id"
        assert groups[0][1]["field"] == "value_as_number"

    def test_compile_or_conditions(self):
        groups = compile_filter_expression("@value_as_number IS NOT NULL OR @value_as_string IS NOT NULL")
        assert len(groups) == 2
        assert groups[0][0]["field"] == "value_as_number"
        assert groups[1][0]["field"] == "value_as_string"

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
        groups = compiled["tables"]["measurement"]["_compiled_filter"]
        assert groups[0][0]["field"] == "measurement_concept_id"
        assert groups[0][0]["op"] == "!="

    def test_compile_filter_list_in_config(self):
        """List of strings in config = each predicate ORed."""
        config = {
            "tables": {
                "observation": {
                    "filter": [
                        "@observation_concept_id != 2000006253",
                        "@value_as_number IS NOT NULL OR @value_as_string IS NOT NULL",
                    ],
                    "time_start": "@observation_datetime",
                    "code": "$omop:@observation_concept_id",
                }
            }
        }
        compiled = compile_config(config)
        groups = compiled["tables"]["observation"]["_compiled_filter"]
        # Three OR groups: the != predicate, value_as_number not null, value_as_string not null
        assert len(groups) == 3


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

    def test_apply_or_filter(self):
        """OR-of-AND groups: keep rows matching ANY group."""
        df = pl.DataFrame(
            {
                "concept_id": [100, 200, 300, 400],
                "value_as_number": [1.0, None, None, None],
                "value_as_string": [None, "abc", None, None],
            }
        )
        # OR groups: row kept if value_as_number is not null OR value_as_string is not null
        groups = [
            [{"field": "value_as_number", "op": "is_not_null"}],
            [{"field": "value_as_string", "op": "is_not_null"}],
        ]
        result = apply_filter_conditions(df, groups)
        assert result["concept_id"].to_list() == [100, 200]

    def test_apply_or_with_and_group(self):
        """OR between an AND group and a simple condition."""
        df = pl.DataFrame(
            {
                "concept_id": [0, 100, 200, 300],
                "value": [5.0, None, 3.0, None],
            }
        )
        # Group 1: concept_id != 0 AND value IS NOT NULL (rows 2)
        # Group 2: concept_id == 0 (rows 0)
        # Result: rows 0 and 2
        groups = [
            [{"field": "concept_id", "op": "!=", "value": 0}, {"field": "value", "op": "is_not_null"}],
            [{"field": "concept_id", "op": "==", "value": 0}],
        ]
        result = apply_filter_conditions(df, groups)
        assert result["concept_id"].to_list() == [0, 200]


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


# ============================================================================
# RELATIONSHIP RESOLUTION (resolve_source_concepts)
# ============================================================================


class TestRelationshipResolution:
    """Test concept_relationship 'Maps to' resolution in transform pipeline."""

    @pytest.fixture
    def concept_df(self):
        return pl.DataFrame(
            {
                "concept_id": [1001, 2001, 3001, 4001, 5001, 6001],
                "code": [
                    "STANFORD_MEAS/GLUCOSE",
                    "SNOMED/166900001",
                    "LOINC/2339-0",
                    "CPT4/99213",
                    "STANFORD_PROC/XR_CHEST",
                    "LOINC/IO_OUT",
                ],
                "concept_code": ["GLUCOSE", "166900001", "2339-0", "99213", "XR_CHEST", "IO_OUT"],
                "concept_name": [
                    "Glucose by Meter",
                    "Glucometer blood glucose",
                    "Glucose",
                    "Office visit",
                    "Chest X-Ray",
                    "Output",
                ],
                "vocabulary_id": ["STANFORD_MEAS", "SNOMED", "LOINC", "CPT4", "STANFORD_PROC", "LOINC"],
                "domain_id": ["Measurement", "Measurement", "Measurement", "Procedure", "Procedure", "Measurement"],
                "concept_class_id": [
                    "Lab Test",
                    "Clinical Finding",
                    "Lab Test",
                    "CPT4",
                    "Procedure",
                    "LOINC Hierarchy",
                ],
                "standard_concept": [None, "S", "S", "S", None, "C"],
            }
        )

    @pytest.fixture
    def relationship_map_df(self):
        """Source concept → resolved standard code via 'Maps to'."""
        return pl.DataFrame(
            {
                "source_concept_id": pl.Series([1001, 5001], dtype=pl.Int64),
                "resolved_code": ["SNOMED/166900001", "SNOMED/399208008"],
            }
        )

    @pytest.fixture
    def meds_schema(self):
        return {
            "subject_id": pl.Int64,
            "time": pl.Datetime("us"),
            "code": pl.Utf8,
            "numeric_value": pl.Float32,
            "text_value": pl.Utf8,
            "end": pl.Datetime("us"),
        }

    def test_concept_id_takes_priority_over_source(self, concept_df, relationship_map_df, meds_schema):
        """When concept_id resolves to a standard concept, it wins over source_concept_id."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "measurement_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                ],
                "measurement_concept_id": [3001, 4001],
                "measurement_source_concept_id": pl.Series([1001, None], dtype=pl.Int64),
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                    "source_concept_id_field": "measurement_source_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
        )

        codes = result["code"].to_list()
        # Row 1: concept_id 3001 → LOINC/2339-0 (standard concept wins)
        assert codes[0] == "LOINC/2339-0"
        # Row 2: concept_id 4001 → CPT4/99213, source is null
        assert codes[1] == "CPT4/99213"

    def test_relationship_fallback_when_concept_id_is_zero(self, concept_df, relationship_map_df, meds_schema):
        """When concept_id is 0 (unmapped), relationship resolution wins over raw source code."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_concept_id": [0],
                "measurement_source_concept_id": pl.Series([1001], dtype=pl.Int64),
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                    "source_concept_id_field": "measurement_source_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
        )

        codes = result["code"].to_list()
        # concept_id 0 → null, relationship resolves source 1001 → SNOMED/166900001
        # (relationship-resolved standard code wins over raw source STANFORD_MEAS/GLUCOSE)
        assert codes[0] == "SNOMED/166900001"

    def test_relationship_fallback_when_source_is_only_option(self, concept_df, relationship_map_df, meds_schema):
        """When only source_concept_id_field is configured (no concept_id_field),
        concept table lookup happens on source_concept_id, with relationship as fallback."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_source_concept_id": pl.Series([1001], dtype=pl.Int64),
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "source_concept_id_field": "measurement_source_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
        )

        codes = result["code"].to_list()
        # source_concept_id 1001 → STANFORD_MEAS/GLUCOSE via concept table
        # (concept table lookup succeeds, relationship fallback not needed)
        assert codes[0] == "STANFORD_MEAS/GLUCOSE"

    def test_no_relationship_map_concept_id_wins(self, concept_df, meds_schema):
        """Without relationship map, concept_id takes priority over source_concept_id."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_concept_id": [3001],
                "measurement_source_concept_id": [1001],
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                    "source_concept_id_field": "measurement_source_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=None,
        )

        codes = result["code"].to_list()
        # concept_id 3001 → LOINC/2339-0 (standard concept wins over source)
        assert codes[0] == "LOINC/2339-0"

    def test_standard_anchor_never_falls_back_to_source_code(self, concept_df, meds_schema):
        """When concept_id_field is the anchor and concept_id=0, raw source code is NOT used."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_concept_id": [0],
                "measurement_source_concept_id": [1001],
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                    "source_concept_id_field": "measurement_source_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=None,
        )

        # concept_id 0 → null, no relationship map, raw source code NOT allowed
        # → row filtered out (null code)
        assert len(result) == 0

    def test_source_anchor_allows_source_code(self, concept_df, meds_schema):
        """When source_concept_id_field is the anchor, source codes are acceptable."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_source_concept_id": [1001],
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "source_concept_id_field": "measurement_source_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=None,
        )

        codes = result["code"].to_list()
        # source anchor → source code STANFORD_MEAS/GLUCOSE is acceptable
        assert codes[0] == "STANFORD_MEAS/GLUCOSE"

    def test_auto_detect_source_concept_id_for_relationship(self, concept_df, relationship_map_df, meds_schema):
        """When only concept_id_field is configured and relationship_map_df is provided,
        the companion _source_concept_id column is auto-detected for relationship resolution."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "measurement_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                    datetime.datetime(2024, 1, 3),
                ],
                # Row 1: non-standard concept_id, source has Maps to chain
                # Row 2: standard concept_id, source doesn't matter
                # Row 3: concept_id 0, source has Maps to chain
                "measurement_concept_id": [1001, 3001, 0],
                "measurement_source_concept_id": pl.Series([1001, 1001, 5001], dtype=pl.Int64),
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        # With standard_only=["S"]: concept 1001 is non-standard → filtered out by
        # concept table join, but source 1001 → SNOMED/166900001 via relationship
        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
            standard_only=["S"],
        )

        codes = result["code"].to_list()
        assert len(result) == 3
        # Row 1: concept_id 1001 not standard → null, relationship resolves source 1001 → SNOMED/166900001
        assert codes[0] == "SNOMED/166900001"
        # Row 2: concept_id 3001 standard → LOINC/2339-0
        assert codes[1] == "LOINC/2339-0"
        # Row 3: concept_id 0 → null, relationship resolves source 5001 → SNOMED/399208008
        assert codes[2] == "SNOMED/399208008"

    def test_auto_detect_no_companion_column(self, concept_df, relationship_map_df, meds_schema):
        """When the companion _source_concept_id column doesn't exist in the DataFrame,
        auto-detection doesn't fire and relationship resolution is skipped."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_concept_id": [1001],
                # No measurement_source_concept_id column
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
            standard_only=["S"],
        )

        # concept_id 1001 is non-standard, no companion column for relationship → dropped
        assert len(result) == 0

    def test_auto_detect_standard_only_false(self, concept_df, relationship_map_df, meds_schema):
        """With standard_only=None, auto-detected companion still enables relationship
        resolution as a fallback for concept_ids not in concept table."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "measurement_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                ],
                # Row 1: concept_id 1001 exists in concept table (non-standard but allowed)
                # Row 2: concept_id 0 not in concept table → relationship fallback
                "measurement_concept_id": [1001, 0],
                "measurement_source_concept_id": pl.Series([1001, 5001], dtype=pl.Int64),
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
            standard_only=None,
        )

        codes = result["code"].to_list()
        assert len(result) == 2
        # Row 1: concept_id 1001 → STANFORD_MEAS/GLUCOSE (non-standard allowed)
        assert codes[0] == "STANFORD_MEAS/GLUCOSE"
        # Row 2: concept_id 0 → null, relationship resolves source 5001 → SNOMED/399208008
        assert codes[1] == "SNOMED/399208008"

    def test_standard_only_filters_non_standard_concepts(self, concept_df, meds_schema):
        """With standard_only=["S"], non-standard concepts produce null codes."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "measurement_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                ],
                "measurement_concept_id": [1001, 3001],
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            standard_only=["S"],
        )

        codes = result["code"].to_list()
        # concept 1001 (STANFORD_MEAS, standard_concept=NULL) → filtered out
        # concept 3001 (LOINC, standard_concept='S') → kept
        assert len(result) == 1
        assert codes[0] == "LOINC/2339-0"

    def test_standard_only_false_keeps_non_standard(self, concept_df, meds_schema):
        """With standard_only=None (default), non-standard concepts are kept."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "measurement_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                ],
                "measurement_concept_id": [1001, 3001],
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            standard_only=None,
        )

        assert len(result) == 2

    def test_config_schema_validates_omop_concept_tables(self):
        """The schema validator accepts valid vocabulary config (list and object form)."""
        for omop_val in [
            ["concept", "concept_relationship"],
            {"sources": ["concept", "concept_relationship"], "standard_only": True},
        ]:
            config = {
                "vocabulary": {"$omop": omop_val},
                "tables": {
                    "measurement": {
                        "time_start": "@measurement_datetime",
                        "code": "$omop:@measurement_concept_id",
                    }
                },
            }
            errors = validate_config_schema(config)
            assert not any("vocabulary" in e for e in errors), f"Failed for {omop_val}: {errors}"

    def test_config_schema_rejects_invalid_omop_concept_tables(self):
        """The schema validator rejects invalid entries in vocabulary.$omop."""
        config = {
            "vocabulary": {
                "$omop": ["concept", "foobar"],
            },
            "tables": {
                "measurement": {
                    "time_start": "@measurement_datetime",
                    "code": "$omop:@measurement_concept_id",
                }
            },
        }
        errors = validate_config_schema(config)
        assert any("foobar" in e for e in errors)

    def test_fallback_concept_id_when_column_is_zero(self, concept_df, meds_schema):
        """fallback_concept_id kicks in when concept_id column is 0 (OMOP unmapped)."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "note_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                ],
                # Row 1: concept_id=0 (unmapped) → should fall back to 3001 (LOINC/2339-0)
                # Row 2: concept_id=4001 (valid) → should use directly
                "note_class_concept_id": [0, 4001],
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "note_class_concept_id",
                    "fallback_concept_id": 3001,
                }
            },
            "time_start": "note_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
        )

        codes = result["code"].to_list()
        assert len(result) == 2
        # Row 1: concept_id 0 → fallback 3001 → LOINC/2339-0
        assert codes[0] == "LOINC/2339-0"
        # Row 2: concept_id 4001 → CPT4/99213
        assert codes[1] == "CPT4/99213"

    def test_fallback_concept_id_when_column_is_null(self, concept_df, meds_schema):
        """fallback_concept_id also kicks in when concept_id column is null."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "note_datetime": [datetime.datetime(2024, 1, 1)],
                "note_class_concept_id": pl.Series([None], dtype=pl.Int64),
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "note_class_concept_id",
                    "fallback_concept_id": 3001,
                }
            },
            "time_start": "note_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
        )

        codes = result["code"].to_list()
        assert len(result) == 1
        assert codes[0] == "LOINC/2339-0"

    def test_fallback_concept_id_source_only_zero(self, concept_df, meds_schema):
        """fallback_concept_id works when source_concept_id_field is 0."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_source_concept_id": pl.Series([0], dtype=pl.Int64),
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "source_concept_id_field": "measurement_source_concept_id",
                    "fallback_concept_id": 3001,
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
        )

        codes = result["code"].to_list()
        assert len(result) == 1
        assert codes[0] == "LOINC/2339-0"

    def test_fallback_concept_id_dual_column_zero(self, concept_df, relationship_map_df, meds_schema):
        """fallback_concept_id works in dual-column path when concept_id is 0."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_concept_id": [0],
                "measurement_source_concept_id": pl.Series([0], dtype=pl.Int64),
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                    "source_concept_id_field": "measurement_source_concept_id",
                    "fallback_concept_id": 3001,
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
        )

        codes = result["code"].to_list()
        assert len(result) == 1
        # concept_id=0 → fallback 3001 → LOINC/2339-0
        assert codes[0] == "LOINC/2339-0"

    def test_exempt_codes_rescues_non_standard(self, concept_df, meds_schema):
        """exempt_codes allows specific non-standard codes through the standard_only filter."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "measurement_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                    datetime.datetime(2024, 1, 3),
                ],
                # 1001: STANFORD_MEAS/GLUCOSE (non-standard) — exempt
                # 3001: LOINC/2339-0 (standard) — passes normally
                # 5001: STANFORD_PROC/XR_CHEST (non-standard) — NOT exempt
                "measurement_concept_id": [1001, 3001, 5001],
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                }
            },
            "time_start": "measurement_datetime",
            "exempt_codes": ["STANFORD_MEAS/GLUCOSE"],
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            standard_only=["S"],
        )

        codes = result["code"].to_list()
        assert len(result) == 2
        # Row 1: concept 1001 non-standard but exempt → STANFORD_MEAS/GLUCOSE
        assert codes[0] == "STANFORD_MEAS/GLUCOSE"
        # Row 2: concept 3001 standard → LOINC/2339-0
        assert codes[1] == "LOINC/2339-0"
        # Row 3: concept 5001 non-standard, NOT exempt → dropped

    def test_exempt_codes_not_needed_without_standard_only(self, concept_df, meds_schema):
        """Without standard_only, exempt_codes has no effect (all concepts pass)."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_concept_id": [1001],
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                }
            },
            "time_start": "measurement_datetime",
            "exempt_codes": ["STANFORD_MEAS/GLUCOSE"],
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            standard_only=None,
        )

        codes = result["code"].to_list()
        assert len(result) == 1
        assert codes[0] == "STANFORD_MEAS/GLUCOSE"

    def test_exempt_codes_with_relationship_resolution(self, concept_df, relationship_map_df, meds_schema):
        """Relationship resolution takes priority over exempt_codes rescue."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "measurement_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                ],
                # Row 1: concept 1001 non-standard, has Maps to → relationship wins
                # Row 2: concept 5001 non-standard, has Maps to → relationship wins
                "measurement_concept_id": [1001, 5001],
                "measurement_source_concept_id": pl.Series([1001, 5001], dtype=pl.Int64),
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                }
            },
            "time_start": "measurement_datetime",
            "exempt_codes": ["STANFORD_MEAS/GLUCOSE", "STANFORD_PROC/XR_CHEST"],
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
            standard_only=["S"],
        )

        codes = result["code"].to_list()
        assert len(result) == 2
        # Relationship resolution produces standard codes, which beat exempt rescue
        assert codes[0] == "SNOMED/166900001"
        assert codes[1] == "SNOMED/399208008"

    def test_config_schema_validates_exempt_codes(self):
        """The schema validator accepts valid exempt_codes."""
        config = {
            "tables": {
                "observation": {
                    "time_start": "@observation_datetime",
                    "code": "$omop:@observation_concept_id",
                    "exempt_codes": ["LOINC/LP21258-6"],
                }
            },
        }
        errors = validate_config_schema(config)
        assert not any("exempt_codes" in e for e in errors)

    def test_config_schema_rejects_invalid_exempt_codes(self):
        """The schema validator rejects non-list exempt_codes."""
        config = {
            "tables": {
                "observation": {
                    "time_start": "@observation_datetime",
                    "code": "$omop:@observation_concept_id",
                    "exempt_codes": "LOINC/LP21258-6",
                }
            },
        }
        errors = validate_config_schema(config)
        assert any("exempt_codes" in e for e in errors)

    def test_standard_only_list_includes_classification(self, concept_df, meds_schema):
        """standard_only=["S", "C"] includes both Standard and Classification concepts."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "measurement_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                    datetime.datetime(2024, 1, 3),
                ],
                # 3001: LOINC/2339-0 (S) → kept
                # 6001: LOINC/IO_OUT (C) → kept with ["S", "C"]
                # 1001: STANFORD_MEAS/GLUCOSE (None) → dropped
                "measurement_concept_id": [3001, 6001, 1001],
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            standard_only=["S", "C"],
        )

        codes = result["code"].to_list()
        assert len(result) == 2
        assert codes[0] == "LOINC/2339-0"
        assert codes[1] == "LOINC/IO_OUT"

    def test_standard_only_s_drops_classification(self, concept_df, meds_schema):
        """standard_only=["S"] drops Classification concepts."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "measurement_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                ],
                # 3001: LOINC/2339-0 (S) → kept
                # 6001: LOINC/IO_OUT (C) → dropped
                "measurement_concept_id": [3001, 6001],
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            standard_only=["S"],
        )

        codes = result["code"].to_list()
        assert len(result) == 1
        assert codes[0] == "LOINC/2339-0"

    def test_config_schema_validates_standard_only_list(self):
        """The schema validator accepts standard_only as a list of strings."""
        config = {
            "vocabulary": {"$omop": {"sources": ["concept"], "standard_only": ["S", "C"]}},
            "tables": {
                "measurement": {
                    "time_start": "@measurement_datetime",
                    "code": "$omop:@measurement_concept_id",
                }
            },
        }
        errors = validate_config_schema(config)
        assert not any("standard_only" in e for e in errors)


class TestPreJoin:
    """Test pre_join reference table enrichment."""

    @pytest.fixture
    def meds_schema(self):
        return {
            "subject_id": pl.Int64,
            "time": pl.Datetime("us"),
            "code": pl.Utf8,
            "numeric_value": pl.Float32,
            "text_value": pl.Utf8,
            "end": pl.Datetime("us"),
        }

    def test_pre_join_enriches_dataframe(self, meds_schema):
        """pre_join adds columns from a reference table before transform."""
        import pickle

        care_site_df = pl.DataFrame(
            {
                "care_site_id": [100, 200, 300],
                "care_site_name": ["Emergency Dept", "ICU North", "Outpatient Clinic"],
            }
        )

        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "visit_detail_start_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                    datetime.datetime(2024, 1, 3),
                ],
                "care_site_id": [100, 200, 999],
            }
        )

        table_config = {
            "time_start": "visit_detail_start_datetime",
            "code_mappings": {
                "source_value": {"template": "CARE_SITE/{care_site_name}"},
            },
            "_pre_join_data": [
                {
                    "on": "care_site_id",
                    "select": ["care_site_name"],
                    "df_bytes": pickle.dumps(care_site_df),
                }
            ],
        }

        # Simulate worker: apply pre_join then transform
        pre_join_data = table_config.get("_pre_join_data", [])
        for join_spec in pre_join_data:
            join_df = pickle.loads(join_spec["df_bytes"])
            on_col = join_spec["on"]
            if on_col in df.columns:
                df = df.join(join_df, on=on_col, how="left")

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
        )

        codes = result["code"].to_list()
        # Row 1: care_site_id 100 → "Emergency Dept"
        assert codes[0] == "CARE_SITE/Emergency Dept"
        # Row 2: care_site_id 200 → "ICU North"
        assert codes[1] == "CARE_SITE/ICU North"
        # Row 3: care_site_id 999 → null join → "CARE_SITE/" (null name becomes empty string in concat)
        assert len(result) == 3

    def test_pre_join_schema_validation(self):
        """The schema validator accepts valid pre_join configs."""
        config = {
            "tables": {
                "visit_detail": {
                    "time_start": "@visit_detail_start_datetime",
                    "code": "CARE_SITE/{@care_site_name}",
                    "pre_join": [
                        {
                            "table": "care_site",
                            "on": "care_site_id",
                            "select": ["care_site_name"],
                        }
                    ],
                }
            },
        }
        errors = validate_config_schema(config)
        assert not any("pre_join" in e for e in errors)

    def test_pre_join_schema_rejects_invalid(self):
        """The schema validator rejects pre_join with missing required keys."""
        config = {
            "tables": {
                "visit_detail": {
                    "time_start": "@visit_detail_start_datetime",
                    "code": "CARE_SITE/{@care_site_name}",
                    "pre_join": [{"table": "care_site"}],
                }
            },
        }
        errors = validate_config_schema(config)
        assert any("on" in e for e in errors)


class TestSourceFirstOrdering:
    """Test that || ordering in code fields is respected for $omop: fields."""

    @pytest.fixture
    def concept_df(self):
        return pl.DataFrame(
            {
                "concept_id": [1001, 2001, 3001],
                "code": ["SOURCE/Glucose", "SNOMED/Standard_Glucose", "LOINC/2339-0"],
                "concept_code": ["Glucose", "Standard_Glucose", "2339-0"],
                "concept_name": ["Source Glucose", "Standard Glucose", "Glucose"],
                "vocabulary_id": ["SOURCE", "SNOMED", "LOINC"],
                "domain_id": ["Measurement", "Measurement", "Measurement"],
                "concept_class_id": ["Lab Test", "Clinical Finding", "Lab Test"],
                "standard_concept": [None, "S", "S"],
            }
        )

    @pytest.fixture
    def relationship_map_df(self):
        return pl.DataFrame(
            {
                "source_concept_id": pl.Series([1001], dtype=pl.Int64),
                "resolved_code": ["SNOMED/Standard_Glucose"],
            }
        )

    @pytest.fixture
    def meds_schema(self):
        return {
            "subject_id": pl.Int64,
            "time": pl.Datetime("us"),
            "code": pl.Utf8,
            "numeric_value": pl.Float32,
            "text_value": pl.Utf8,
            "end": pl.Datetime("us"),
        }

    def test_compiler_emits_source_first_flag(self):
        """$omop:@source || $omop:@concept sets _source_first in compiled config."""
        config = {
            "tables": {
                "measurement": {
                    "time_start": "@measurement_datetime",
                    "code": "$omop:@measurement_source_concept_id || $omop:@measurement_concept_id",
                }
            }
        }
        compiled = compile_config(config)
        concept_id_config = compiled["tables"]["measurement"]["code_mappings"]["concept_id"]
        assert concept_id_config.get("_source_first") is True

    def test_compiler_no_flag_when_concept_first(self):
        """$omop:@concept || $omop:@source does NOT set _source_first."""
        config = {
            "tables": {
                "measurement": {
                    "time_start": "@measurement_datetime",
                    "code": "$omop:@measurement_concept_id || $omop:@measurement_source_concept_id",
                }
            }
        }
        compiled = compile_config(config)
        concept_id_config = compiled["tables"]["measurement"]["code_mappings"]["concept_id"]
        assert "_source_first" not in concept_id_config

    def test_concept_first_uses_concept_id_priority(self, concept_df, relationship_map_df, meds_schema):
        """Default ordering: concept_id lookup wins over relationship-resolved source."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_concept_id": [3001],
                "measurement_source_concept_id": pl.Series([1001], dtype=pl.Int64),
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                    "source_concept_id_field": "measurement_source_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
        )

        codes = result["code"].to_list()
        assert codes[0] == "LOINC/2339-0"

    def test_source_first_uses_relationship_priority(self, concept_df, relationship_map_df, meds_schema):
        """When _source_first is set, relationship-resolved code wins over concept_id."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_concept_id": [3001],
                "measurement_source_concept_id": pl.Series([1001], dtype=pl.Int64),
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                    "source_concept_id_field": "measurement_source_concept_id",
                    "_source_first": True,
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
        )

        codes = result["code"].to_list()
        # source 1001 → relationship resolves to SNOMED/Standard_Glucose
        # This should beat concept_id 3001 → LOINC/2339-0
        assert codes[0] == "SNOMED/Standard_Glucose"

    def test_source_first_falls_through_when_source_is_zero(self, concept_df, relationship_map_df, meds_schema):
        """When _source_first but source_concept_id=0, falls through to concept_id."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_concept_id": [3001],
                "measurement_source_concept_id": pl.Series([0], dtype=pl.Int64),
            }
        )

        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                    "source_concept_id_field": "measurement_source_concept_id",
                    "_source_first": True,
                }
            },
            "time_start": "measurement_datetime",
        }

        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
        )

        codes = result["code"].to_list()
        # source_concept_id=0 → no relationship match → falls through to concept_id 3001
        assert codes[0] == "LOINC/2339-0"


class TestExplicitVocabOperations:
    """Test $omop.lookup and $omop.resolve explicit vocabulary operations."""

    @pytest.fixture
    def concept_df(self):
        return pl.DataFrame(
            {
                "concept_id": [1001, 2001, 3001],
                "code": ["SOURCE/Glucose", "SNOMED/Standard_Glucose", "LOINC/2339-0"],
                "concept_code": ["Glucose", "Standard_Glucose", "2339-0"],
                "concept_name": ["Source Glucose", "Standard Glucose", "Glucose"],
                "vocabulary_id": ["SOURCE", "SNOMED", "LOINC"],
                "domain_id": ["Measurement", "Measurement", "Measurement"],
                "concept_class_id": ["Lab Test", "Clinical Finding", "Lab Test"],
                "standard_concept": [None, "S", "S"],
            }
        )

    @pytest.fixture
    def relationship_map_df(self):
        return pl.DataFrame(
            {
                "source_concept_id": pl.Series([1001], dtype=pl.Int64),
                "resolved_code": ["SNOMED/Standard_Glucose"],
            }
        )

    @pytest.fixture
    def meds_schema(self):
        return {
            "subject_id": pl.Int64,
            "time": pl.Datetime("us"),
            "code": pl.Utf8,
            "numeric_value": pl.Float32,
            "text_value": pl.Utf8,
            "end": pl.Datetime("us"),
        }

    # --- Compiler tests ---

    def test_compile_omop_lookup_single(self):
        """$omop.lookup:@field compiles with _operation=lookup."""
        config = {
            "tables": {
                "measurement": {
                    "time_start": "@measurement_datetime",
                    "code": "$omop.lookup:@measurement_concept_id",
                }
            }
        }
        compiled = compile_config(config)
        cid = compiled["tables"]["measurement"]["code_mappings"]["concept_id"]
        assert cid["concept_id_field"] == "measurement_concept_id"
        assert cid["_concept_id_field_operation"] == "lookup"

    def test_compile_omop_resolve_single(self):
        """$omop.resolve:@field compiles with _operation=resolve."""
        config = {
            "tables": {
                "measurement": {
                    "time_start": "@measurement_datetime",
                    "code": "$omop.resolve:@measurement_source_concept_id",
                }
            }
        }
        compiled = compile_config(config)
        cid = compiled["tables"]["measurement"]["code_mappings"]["concept_id"]
        assert cid["source_concept_id_field"] == "measurement_source_concept_id"
        assert cid["_source_concept_id_field_operation"] == "resolve"

    def test_compile_mixed_chain(self):
        """$omop.resolve || $omop.lookup compiles with per-field operations."""
        config = {
            "tables": {
                "measurement": {
                    "time_start": "@measurement_datetime",
                    "code": "$omop.resolve:@measurement_source_concept_id || $omop.lookup:@measurement_concept_id",
                }
            }
        }
        compiled = compile_config(config)
        cid = compiled["tables"]["measurement"]["code_mappings"]["concept_id"]
        assert cid["source_concept_id_field"] == "measurement_source_concept_id"
        assert cid["_source_concept_id_field_operation"] == "resolve"
        assert cid["concept_id_field"] == "measurement_concept_id"
        assert cid["_concept_id_field_operation"] == "lookup"
        assert cid.get("_source_first") is True

    def test_compile_legacy_no_operation(self):
        """Legacy $omop:@field compiles without _operation annotations."""
        config = {
            "tables": {
                "measurement": {
                    "time_start": "@measurement_datetime",
                    "code": "$omop:@measurement_concept_id",
                }
            }
        }
        compiled = compile_config(config)
        cid = compiled["tables"]["measurement"]["code_mappings"]["concept_id"]
        assert cid["concept_id_field"] == "measurement_concept_id"
        assert "_concept_id_field_operation" not in cid

    # --- Runtime tests: $omop.lookup ---

    def test_lookup_returns_concept_table_code(self, concept_df, relationship_map_df, meds_schema):
        """$omop.lookup returns the concept table code, no relationship resolution."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_source_concept_id": pl.Series([1001], dtype=pl.Int64),
            }
        )
        table_config = {
            "code_mappings": {
                "concept_id": {
                    "source_concept_id_field": "measurement_source_concept_id",
                    "_source_concept_id_field_operation": "lookup",
                }
            },
            "time_start": "measurement_datetime",
        }
        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
        )
        codes = result["code"].to_list()
        # lookup returns the raw concept table entry, NOT the relationship-resolved code
        assert codes[0] == "SOURCE/Glucose"

    def test_lookup_ignores_relationship_map(self, concept_df, relationship_map_df, meds_schema):
        """$omop.lookup never triggers concept_relationship resolution."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_concept_id": [3001],
                "measurement_source_concept_id": pl.Series([1001], dtype=pl.Int64),
            }
        )
        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                    "_concept_id_field_operation": "lookup",
                    "source_concept_id_field": "measurement_source_concept_id",
                    "_source_concept_id_field_operation": "lookup",
                    "_source_first": True,
                }
            },
            "time_start": "measurement_datetime",
        }
        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
        )
        codes = result["code"].to_list()
        # Both are lookups, source is first → raw source concept code
        assert codes[0] == "SOURCE/Glucose"

    # --- Runtime tests: $omop.resolve ---

    def test_resolve_returns_relationship_code(self, concept_df, relationship_map_df, meds_schema):
        """$omop.resolve returns the relationship-resolved code."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_source_concept_id": pl.Series([1001], dtype=pl.Int64),
            }
        )
        table_config = {
            "code_mappings": {
                "concept_id": {
                    "source_concept_id_field": "measurement_source_concept_id",
                    "_source_concept_id_field_operation": "resolve",
                }
            },
            "time_start": "measurement_datetime",
        }
        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
        )
        codes = result["code"].to_list()
        assert codes[0] == "SNOMED/Standard_Glucose"

    def test_resolve_returns_null_when_no_mapping(self, concept_df, relationship_map_df, meds_schema):
        """$omop.resolve returns null when no 'Maps to' exists (row dropped)."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_source_concept_id": pl.Series([3001], dtype=pl.Int64),
            }
        )
        table_config = {
            "code_mappings": {
                "concept_id": {
                    "source_concept_id_field": "measurement_source_concept_id",
                    "_source_concept_id_field_operation": "resolve",
                }
            },
            "time_start": "measurement_datetime",
        }
        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
        )
        # concept 3001 has no "Maps to" → null code → row filtered
        assert len(result) == 0

    # --- Runtime tests: mixed || chain ---

    def test_resolve_then_lookup_fallback(self, concept_df, relationship_map_df, meds_schema):
        """$omop.resolve:@source || $omop.lookup:@concept uses resolve first, falls back to lookup."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "measurement_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                ],
                "measurement_concept_id": [3001, 3001],
                "measurement_source_concept_id": pl.Series([1001, 0], dtype=pl.Int64),
            }
        )
        table_config = {
            "code_mappings": {
                "concept_id": {
                    "source_concept_id_field": "measurement_source_concept_id",
                    "_source_concept_id_field_operation": "resolve",
                    "concept_id_field": "measurement_concept_id",
                    "_concept_id_field_operation": "lookup",
                    "_source_first": True,
                }
            },
            "time_start": "measurement_datetime",
        }
        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=relationship_map_df,
        )
        codes = result.sort("subject_id")["code"].to_list()
        # Row 1: source 1001 resolves → SNOMED/Standard_Glucose
        assert codes[0] == "SNOMED/Standard_Glucose"
        # Row 2: source 0 → no resolve → falls back to lookup concept_id 3001 → LOINC/2339-0
        assert codes[1] == "LOINC/2339-0"

    def test_lookup_then_lookup_chain(self, concept_df, meds_schema):
        """$omop.lookup:@source || $omop.lookup:@concept respects left-to-right order."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "measurement_datetime": [
                    datetime.datetime(2024, 1, 1),
                    datetime.datetime(2024, 1, 2),
                ],
                "measurement_concept_id": [3001, 3001],
                "measurement_source_concept_id": pl.Series([1001, 0], dtype=pl.Int64),
            }
        )
        table_config = {
            "code_mappings": {
                "concept_id": {
                    "source_concept_id_field": "measurement_source_concept_id",
                    "_source_concept_id_field_operation": "lookup",
                    "concept_id_field": "measurement_concept_id",
                    "_concept_id_field_operation": "lookup",
                    "_source_first": True,
                }
            },
            "time_start": "measurement_datetime",
        }
        result = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key="person_id",
            meds_schema=meds_schema,
            concept_df=concept_df,
            relationship_map_df=None,
        )
        codes = result.sort("subject_id")["code"].to_list()
        # Row 1: source 1001 → SOURCE/Glucose (raw lookup, source first)
        assert codes[0] == "SOURCE/Glucose"
        # Row 2: source 0 → null → falls through to concept_id 3001 → LOINC/2339-0
        assert codes[1] == "LOINC/2339-0"

    # --- Config validation tests ---

    def test_schema_rejects_resolve_without_concept_relationship(self):
        """$omop.resolve without concept_relationship in sources fails validation."""
        config = {
            "vocabulary": {"$omop": {"sources": ["concept"]}},
            "tables": {
                "measurement": {
                    "time_start": "@measurement_datetime",
                    "code": "$omop.resolve:@measurement_source_concept_id",
                }
            },
        }
        errors = validate_config_schema(config)
        assert any("$omop.resolve" in e and "concept_relationship" in e for e in errors)

    def test_schema_accepts_resolve_with_concept_relationship(self):
        """$omop.resolve with concept_relationship in sources passes validation."""
        config = {
            "vocabulary": {"$omop": {"sources": ["concept", "concept_relationship"]}},
            "tables": {
                "measurement": {
                    "time_start": "@measurement_datetime",
                    "code": "$omop.resolve:@measurement_source_concept_id",
                }
            },
        }
        errors = validate_config_schema(config)
        assert not any("$omop.resolve" in e for e in errors)

    def test_schema_accepts_lookup_without_concept_relationship(self):
        """$omop.lookup works without concept_relationship in sources."""
        config = {
            "vocabulary": {"$omop": {"sources": ["concept"]}},
            "tables": {
                "measurement": {
                    "time_start": "@measurement_datetime",
                    "code": "$omop.lookup:@measurement_concept_id",
                }
            },
        }
        errors = validate_config_schema(config)
        assert not any("$omop.resolve" in e for e in errors)

    # --- Backward compatibility ---

    def test_legacy_omop_still_works(self, concept_df, relationship_map_df, meds_schema):
        """Legacy $omop:@field syntax continues to work identically."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_concept_id": [3001],
                "measurement_source_concept_id": pl.Series([1001], dtype=pl.Int64),
            }
        )
        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                    "source_concept_id_field": "measurement_source_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = transform_to_meds_unsorted(
                df=df,
                table_config=table_config,
                primary_key="person_id",
                meds_schema=meds_schema,
                concept_df=concept_df,
                relationship_map_df=relationship_map_df,
            )
        codes = result["code"].to_list()
        # Legacy: concept_id 3001 → LOINC/2339-0 takes priority (concept first)
        assert codes[0] == "LOINC/2339-0"

    def test_legacy_omop_emits_deprecation_warning(self, concept_df, relationship_map_df, meds_schema):
        """Legacy $omop:@field emits DeprecationWarning when implicit resolution fires."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "measurement_datetime": [datetime.datetime(2024, 1, 1)],
                "measurement_concept_id": [0],
                "measurement_source_concept_id": pl.Series([1001], dtype=pl.Int64),
            }
        )
        table_config = {
            "code_mappings": {
                "concept_id": {
                    "concept_id_field": "measurement_concept_id",
                    "source_concept_id_field": "measurement_source_concept_id",
                }
            },
            "time_start": "measurement_datetime",
        }
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            transform_to_meds_unsorted(
                df=df,
                table_config=table_config,
                primary_key="person_id",
                meds_schema=meds_schema,
                concept_df=concept_df,
                relationship_map_df=relationship_map_df,
            )
        deprecations = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecations) >= 1
        assert "$omop.resolve" in str(deprecations[0].message)
