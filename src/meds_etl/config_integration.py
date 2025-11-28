"""
Integration layer for using the new config parser with existing ETL code.

This module provides backward-compatible functions that can handle both
the old and new configuration formats.
"""

import re
from typing import Any, Dict, Optional, Set

import polars as pl

from meds_etl.config_parser import parse_config_value


def extract_column_names_from_template(template: str) -> Set[str]:
    """
    Extract column names referenced in a template string.

    This is useful for validation - extracting what columns are actually needed.

    Args:
        template: Template string (e.g., "@col1 || @col2" or "{@col3}")

    Returns:
        Set of column names referenced in the template
    """
    column_names = set()

    # Find all @column_name references
    # Match @word_chars but not inside $vocab: lookups
    for match in re.finditer(r"@([a-zA-Z_][a-zA-Z0-9_]*)", template):
        column_names.add(match.group(1))

    return column_names


def is_template_syntax(value: str) -> bool:
    """
    Check if a string uses template syntax.

    Args:
        value: String to check

    Returns:
        True if it contains template syntax markers
    """
    return any(marker in value for marker in ["@", "$", "{", "||"])


def parse_time_field(
    config: Dict[str, Any],
    df_schema: Optional[Dict[str, pl.DataType]] = None,
    concept_df: Optional[pl.DataFrame] = None,
) -> pl.Expr:
    """
    Parse time_start field from config, supporting both old and new formats.

    Old format:
        "time_start": "column_name",
        "time_start_fallbacks": ["fallback1", "fallback2"]

    New format:
        "time_start": "@column_name || @fallback1 || @fallback2"

    Args:
        config: Table or event configuration
        df_schema: DataFrame schema for validation
        concept_df: Concept DataFrame for vocabulary lookups

    Returns:
        Polars expression for time field
    """
    time_start = config.get("time_start")

    if not time_start:
        raise ValueError("No time_start field in config")

    # Check if it's new format (contains @ or ||)
    if is_template_syntax(time_start):
        # New templated format
        return parse_config_value(time_start, concept_df, df_schema).cast(pl.Datetime("us"))

    # Old format - build expression from time_start and fallbacks
    time_options = []
    time_options.append(pl.col(time_start).cast(pl.Datetime("us")))

    for fallback in config.get("time_start_fallbacks", []):
        time_options.append(pl.col(fallback).cast(pl.Datetime("us")))

    if len(time_options) > 1:
        return pl.coalesce(time_options)
    return time_options[0]


def parse_code_field(
    config: Dict[str, Any],
    df_schema: Optional[Dict[str, pl.DataType]] = None,
    concept_df: Optional[pl.DataFrame] = None,
    code_mapping_choice: str = "auto",
) -> Optional[pl.Expr]:
    """
    Parse code field from config, supporting both old and new formats.

    Old format:
        "code_mappings": {
            "source_value": {"field": "drug_source_value"},
            "concept_id": {
                "concept_id_field": "drug_concept_id",
                "source_concept_id_field": "drug_source_concept_id"
            }
        }

    New format:
        "code": "$omop:@drug_source_concept_id || $omop:@drug_concept_id || @drug_source_value"

    Args:
        config: Table or event configuration
        df_schema: DataFrame schema for validation
        concept_df: Concept DataFrame for vocabulary lookups
        code_mapping_choice: Which mapping to use ("auto", "source_value", "concept_id", "template")

    Returns:
        Polars expression for code field, or None if no code mapping
    """
    # Check for new format first
    if "code" in config:
        code = config["code"]
        # Check if it's new templated format
        if isinstance(code, str) and is_template_syntax(code):
            # New templated format
            return parse_config_value(code, concept_df, df_schema)
        # Otherwise it's a literal string (like "MEDS_BIRTH")
        return pl.lit(code)

    # Fall back to old code_mappings format
    code_mappings = config.get("code_mappings", {})
    if not code_mappings:
        return None

    # Handle old format (existing transform_to_meds_unsorted logic would go here)
    # For now, just return None to indicate old format should be handled by existing code
    return None


def parse_property_value(
    prop_config: Dict[str, Any],
    df_schema: Optional[Dict[str, pl.DataType]] = None,
    concept_df: Optional[pl.DataFrame] = None,
) -> pl.Expr:
    """
    Parse property value from config, supporting both old and new formats.

    Old format:
        {"name": "visit_id", "alias": "visit_occurrence_id", "type": "int"}

    New format:
        {"name": "visit_id", "value": "@visit_occurrence_id", "type": "int"}

    Args:
        prop_config: Property configuration dict
        df_schema: DataFrame schema for validation
        concept_df: Concept DataFrame for vocabulary lookups

    Returns:
        Polars expression for property value
    """
    # Check for new "value" field
    if "value" in prop_config:
        value = prop_config["value"]
        # Check if it's new templated format
        if isinstance(value, str) and is_template_syntax(value):
            return parse_config_value(value, concept_df, df_schema)
        # Literal value
        return pl.lit(value)

    # Check for old "literal" field
    if "literal" in prop_config:
        return pl.lit(prop_config["literal"])

    # Fall back to old format with "name" or "alias"
    # The old format uses the "name" field as both the column name and output name
    # unless "alias" is specified, in which case "alias" is the output name and "name" is the column
    if "alias" in prop_config:
        # Old format: name is column, alias is output
        return pl.col(prop_config["name"])
    else:
        # Old format: name is both column and output
        return pl.col(prop_config["name"])


def is_new_config_format(config: Dict[str, Any]) -> bool:
    """
    Detect if config uses new templated format.

    Args:
        config: Configuration dict

    Returns:
        True if new format, False if old format
    """
    # Check for new format indicators in canonical_events or tables
    for section in ["canonical_events", "tables"]:
        for event_config in config.get(section, {}).values():
            # Check for new "code" field with templates
            if "code" in event_config:
                code = event_config["code"]
                if isinstance(code, str) and ("@" in code or "$" in code):
                    return True

            # Check for new "time_start" with templates
            if "time_start" in event_config:
                time_start = event_config["time_start"]
                if isinstance(time_start, str) and ("@" in time_start or "||" in time_start):
                    return True

            # Check for new "value" in properties
            for prop in event_config.get("properties", []):
                if "value" in prop:
                    return True

    return False
