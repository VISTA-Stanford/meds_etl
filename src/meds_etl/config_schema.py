"""
Structural validation for ETL configuration files.

Validates config structure, required fields, allowed keys, and DSL syntax
**without** requiring access to the actual OMOP data.  Data-level validation
(column existence, type compatibility) lives in
``omop_common.validate_config_against_data``.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

# ============================================================================
# KNOWN KEYS
# ============================================================================

_TOP_LEVEL_KEYS = {
    "omop_cdm_version",
    "primary_key",
    "canonical_events",
    "tables",
    "num_shards",
    "description",
    "version",
    "vocabulary",
}

_EVENT_TABLE_KEYS = {
    "table",
    "code",
    "code_mappings",
    "time_start",
    "time_start_fallbacks",
    "time_end",
    "time_end_fallbacks",
    "time_field",
    "datetime_field",
    "time_fallbacks",
    "time_end_field",
    "subject_id_field",
    "numeric_value",
    "numeric_value_field",
    "numeric_value_field_fallbacks",
    "numeric_value_field_concept_lookup",
    "text_value",
    "text_value_field",
    "text_value_field_fallbacks",
    "text_value_field_concept_lookup",
    "properties",
    "metadata",
    "filter",
    # Compiled keys (produced by config_compiler, not user-written)
    "_compiled_filter",
}

_PROPERTY_KEYS = {
    "name",
    "value",
    "literal",
    "type",
    "alias",
    "source",
    "concept_lookup_field",
}

_VALID_PROPERTY_TYPES = {
    "int",
    "integer",
    "int64",
    "float",
    "float64",
    "double",
    "string",
    "str",
    "utf8",
    "datetime",
    "timestamp",
    "date",
    "boolean",
    "bool",
}

# DSL markers that indicate a value is a column reference or expression
_DSL_MARKERS = re.compile(r"[@${\|>]")

# Patterns that look like column names (lowercase with underscores)
_COLUMN_LIKE = re.compile(r"^[a-z][a-z0-9_]*$")


# ============================================================================
# PUBLIC API
# ============================================================================


def validate_config_schema(config: Dict[str, Any]) -> List[str]:
    """Validate configuration structure without requiring OMOP data.

    Returns a list of human-readable error/warning strings.  An empty list
    means the config passes structural validation.
    """
    errors: List[str] = []

    # Top-level keys
    for key in config:
        if key not in _TOP_LEVEL_KEYS:
            errors.append(f"Unknown top-level key '{key}'")

    # Must have at least one of canonical_events or tables
    if "canonical_events" not in config and "tables" not in config:
        errors.append("Config must define at least 'canonical_events' or 'tables'")

    # Validate vocabulary
    _VALID_OMOP_SOURCES = {"concept", "concept_relationship"}
    if "vocabulary" in config:
        vocab = config["vocabulary"]
        if not isinstance(vocab, dict):
            errors.append('\'vocabulary\' must be a dict (e.g., {"$omop": ["concept"]})')
        else:
            for prefix, sources in vocab.items():
                if prefix != "$omop":
                    errors.append(
                        f"'vocabulary' contains unknown prefix '{prefix}'; " f"currently only '$omop' is supported"
                    )
                    continue
                if not isinstance(sources, list):
                    errors.append(
                        f"'vocabulary[\"{prefix}\"]' must be a list " f'(e.g., ["concept", "concept_relationship"])'
                    )
                else:
                    for entry in sources:
                        if entry not in _VALID_OMOP_SOURCES:
                            errors.append(
                                f"'vocabulary[\"{prefix}\"]' contains unknown source '{entry}'; "
                                f"valid values: {sorted(_VALID_OMOP_SOURCES)}"
                            )

    # Validate canonical events
    for event_name, event_config in config.get("canonical_events", {}).items():
        errors.extend(_validate_event_or_table(event_name, event_config, is_canonical=True))

    # Validate tables
    for table_name, table_config in config.get("tables", {}).items():
        errors.extend(_validate_event_or_table(table_name, table_config, is_canonical=False))

    # Cross-table property type consistency
    errors.extend(_validate_property_type_consistency(config))

    return errors


# ============================================================================
# INTERNAL VALIDATORS
# ============================================================================


def _validate_event_or_table(
    name: str,
    config: Dict[str, Any],
    is_canonical: bool,
) -> List[str]:
    """Validate a single canonical event or table config."""
    errors: List[str] = []
    section = "canonical_event" if is_canonical else "table"

    if not isinstance(config, dict):
        errors.append(f"{section} '{name}': must be a dict, got {type(config).__name__}")
        return errors

    # Unknown keys
    for key in config:
        if key not in _EVENT_TABLE_KEYS:
            errors.append(f"{section} '{name}': unknown key '{key}'")

    # Required: table (for canonical events)
    if is_canonical and "table" not in config:
        errors.append(f"{section} '{name}': missing required key 'table'")

    # Required: time_start (or legacy time_field / datetime_field)
    has_time = any(config.get(k) for k in ("time_start", "time_field", "datetime_field"))
    if not has_time:
        errors.append(f"{section} '{name}': missing required key 'time_start'")

    # Required: code or code_mappings
    has_code = "code" in config or "code_mappings" in config
    if not has_code:
        errors.append(f"{section} '{name}': must define either 'code' or 'code_mappings'")

    # Validate code DSL syntax
    if "code" in config and isinstance(config["code"], str):
        errors.extend(_validate_dsl_expression(config["code"], f"{section} '{name}' code"))

    # Validate time_start DSL syntax
    if "time_start" in config and isinstance(config["time_start"], str):
        errors.extend(_validate_dsl_expression(config["time_start"], f"{section} '{name}' time_start"))

    # Validate time_end DSL syntax
    if "time_end" in config and isinstance(config["time_end"], str):
        errors.extend(_validate_dsl_expression(config["time_end"], f"{section} '{name}' time_end"))

    # Validate filter syntax
    if "filter" in config:
        errors.extend(_validate_filter(config["filter"], f"{section} '{name}'"))

    # Validate numeric_value / text_value
    for field_key in ("numeric_value", "text_value"):
        if field_key in config and isinstance(config[field_key], str):
            errors.extend(_validate_dsl_expression(config[field_key], f"{section} '{name}' {field_key}"))

    # Validate properties
    properties = config.get("properties") or config.get("metadata", [])
    if not isinstance(properties, list):
        errors.append(f"{section} '{name}': 'properties' must be a list")
    else:
        for i, prop in enumerate(properties):
            errors.extend(_validate_property(prop, f"{section} '{name}' property[{i}]"))

    return errors


def _validate_property(prop: Dict[str, Any], context: str) -> List[str]:
    """Validate a single property spec."""
    errors: List[str] = []

    if not isinstance(prop, dict):
        errors.append(f"{context}: must be a dict")
        return errors

    # Unknown keys
    for key in prop:
        if key not in _PROPERTY_KEYS:
            errors.append(f"{context}: unknown key '{key}'")

    # Required: name
    if "name" not in prop:
        errors.append(f"{context}: missing required key 'name'")

    # Validate type
    prop_type = prop.get("type", "string")
    if prop_type not in _VALID_PROPERTY_TYPES:
        errors.append(f"{context}: invalid type '{prop_type}'. " f"Valid types: {sorted(_VALID_PROPERTY_TYPES)}")

    # Validate value field (bare string check)
    if "value" in prop:
        value = prop["value"]
        if isinstance(value, str):
            if not _DSL_MARKERS.search(value) and not value.startswith("$literal:"):
                errors.append(
                    f"{context}: ambiguous bare string value '{value}'. "
                    f"Use '@{value}' for a column reference or "
                    f"'$literal:{value}' for a literal string."
                )

    return errors


def _validate_dsl_expression(expr: str, context: str) -> List[str]:
    """Basic structural validation of a DSL expression."""
    errors: List[str] = []

    # Check balanced braces
    depth = 0
    for ch in expr:
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        if depth < 0:
            errors.append(f"{context}: unbalanced '}}' in expression '{expr}'")
            break
    if depth > 0:
        errors.append(f"{context}: unclosed '{{' in expression '{expr}'")

    # Check that @column references have valid identifiers
    for match in re.finditer(r"@([^\s|>}{,)]+)", expr):
        ident = match.group(1)
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", ident):
            errors.append(f"{context}: invalid column reference '@{ident}'")

    # Check $vocab: syntax has a valid vocab name
    for match in re.finditer(r"\$([a-zA-Z_][a-zA-Z0-9_]*):", expr):
        vocab = match.group(1)
        if vocab not in ("omop", "literal"):
            errors.append(f"{context}: unknown vocabulary '${vocab}:'. " f"Supported: $omop:, $literal:")

    return errors


def _validate_filter(filter_val: Any, context: str) -> List[str]:
    """Validate a filter expression."""
    errors: List[str] = []

    if isinstance(filter_val, str):
        # Must reference at least one column
        if "@" not in filter_val:
            errors.append(f"{context} filter: must reference at least one column with '@'")

        # Basic operator check
        has_operator = any(
            op in filter_val.upper() for op in ["!=", "==", ">=", "<=", ">", "<", "IS NOT NULL", "IS NULL", " IN "]
        )
        if not has_operator:
            errors.append(
                f"{context} filter: no recognized operator found. "
                f"Supported: !=, ==, >, <, >=, <=, IS NULL, IS NOT NULL, IN"
            )

    elif isinstance(filter_val, list):
        for i, cond in enumerate(filter_val):
            if not isinstance(cond, dict):
                errors.append(f"{context} filter[{i}]: must be a dict")
                continue
            if "field" not in cond:
                errors.append(f"{context} filter[{i}]: missing 'field'")
            if "op" not in cond:
                errors.append(f"{context} filter[{i}]: missing 'op'")

    else:
        errors.append(f"{context} filter: must be a string or list, got {type(filter_val).__name__}")

    return errors


def _validate_property_type_consistency(config: Dict[str, Any]) -> List[str]:
    """Check that properties with the same output name have consistent types."""
    errors: List[str] = []
    seen: Dict[str, tuple] = {}  # col_name -> (type, source_description)

    for section in ("canonical_events", "tables"):
        for name, cfg in config.get(section, {}).items():
            properties = cfg.get("properties") or cfg.get("metadata", [])
            if not isinstance(properties, list):
                continue
            for prop in properties:
                if not isinstance(prop, dict):
                    continue
                col_name = prop.get("alias", prop.get("name"))
                col_type = prop.get("type", "string")
                source = f"{section}.{name}"

                if col_name in seen:
                    prev_type, prev_source = seen[col_name]
                    if prev_type != col_type:
                        errors.append(
                            f"Property '{col_name}' has inconsistent types: "
                            f"'{prev_type}' in {prev_source} vs '{col_type}' in {source}"
                        )
                else:
                    seen[col_name] = (col_type, source)

    return errors
