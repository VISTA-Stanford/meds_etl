# Configuration Template Parser - Implementation Summary

## Overview

Implemented a comprehensive template parser for OMOP ETL configuration files that provides a cleaner, more expressive syntax while maintaining backward compatibility with the existing format.

## New Syntax Features

### 1. Column References: `@column_name`
Direct reference to columns in the source table:
```json
"time_start": "@drug_exposure_start_datetime"
```

### 2. Vocabulary Lookups: `$vocab:source:attribute`
Look up values from OMOP vocabulary tables:
```json
"code": "$omop:@drug_type_concept_id"
"code": "$omop:@drug_concept_id:concept_code"
"code": "$omop:1235243"
```

### 3. Template Strings: `{expression}`
Compose values from multiple parts:
```json
"code": "Gender/{$omop:@gender_concept_id}"
"code": "IMAGE/{@modality_source_value}|{@anatomic_site_source_value}"
```

### 4. Fallback Chains: `expr1 || expr2 || expr3`
Specify fallback values:
```json
"time_start": "@drug_exposure_start_datetime || @drug_exposure_start_date"
"code": "$omop:@source_concept_id || $omop:@concept_id || @source_value"
```

### 5. Pipe-Style Transforms: `expression | function(args)`
Apply transformations to values:
```json
"code": "NOTE/{@note_title | regex_replace('\\s+', '-')}"
"name": "@text | upper() | regex_replace(' ', '_')"
```

**Supported transforms:**
- `upper()` - Convert to uppercase
- `lower()` - Convert to lowercase
- `strip()` / `trim()` - Remove whitespace
- `regex_replace(pattern, replacement)` - Regex substitution
- `replace(old, new)` - Simple string replacement
- `substring(start, length)` - String slicing

## Modules Implemented

### 1. `src/meds_etl/config_parser.py`
Core parser implementation with two main classes:

**`TemplateParser`**
- Parses template strings into structured objects
- Handles all syntax features (column refs, vocab lookups, templates, fallbacks, transforms)
- Returns parsed representation as dataclasses

**`PolarsExpressionBuilder`**
- Converts parsed objects into Polars expressions
- Handles vocabulary lookups via DataFrame joins
- Applies transforms efficiently using Polars string operations

**`parse_config_value(value, concept_df, df_schema)`**
- Main entry point for parsing config values
- Returns ready-to-execute Polars expressions

### 2. `src/meds_etl/config_integration.py`
Integration layer for backward compatibility:

**Functions:**
- `parse_time_field(config)` - Parse time_start fields (old or new format)
- `parse_code_field(config)` - Parse code fields (old or new format)
- `parse_property_value(prop_config)` - Parse property values (old or new format)
- `is_new_config_format(config)` - Detect which format is being used

## Example Config Comparison

### Old Format
```json
{
    "time_start": "drug_exposure_start_datetime",
    "time_start_fallbacks": ["drug_exposure_start_date"],
    "code_mappings": {
        "source_value": {"field": "drug_source_value"},
        "concept_id": {
            "concept_id_field": "drug_concept_id",
            "source_concept_id_field": "drug_source_concept_id"
        }
    },
    "properties": [
        {
            "name": "visit_occurrence_id",
            "alias": "visit_id",
            "type": "int"
        },
        {
            "name": "table",
            "literal": "drug_exposure",
            "type": "string"
        }
    ]
}
```

### New Format
```json
{
    "time_start": "@drug_exposure_start_datetime || @drug_exposure_start_date",
    "code": "$omop:@drug_source_concept_id || $omop:@drug_concept_id || @drug_source_value",
    "properties": [
        {
            "name": "visit_id",
            "value": "@visit_occurrence_id",
            "type": "int"
        },
        {
            "name": "table",
            "value": "drug_exposure",
            "type": "string"
        }
    ]
}
```

## Test Coverage

### Core Parser Tests (`tests/test_config_parser.py`)
- 36 tests covering all parsing and expression building functionality
- Tests for each syntax feature
- Edge case and error handling tests

### Integration Tests (`tests/test_config_integration.py`)
- 17 tests for backward compatibility layer
- Tests for both old and new format handling
- Format detection tests

### Config Format Tests (`tests/test_new_config_format.py`)
- 4 tests validating the example config file
- Tests parsing all time, code, and property fields
- Integration test with actual data

**Total: 57 tests, all passing**

## Performance Characteristics

- **Zero performance delta**: The parser generates the same Polars expressions as hand-written code
- **Parsing is done once at load time**: Templates are parsed when config is loaded, not during data processing
- **Efficient execution**: Generated expressions use Polars' optimized operations
- **No runtime overhead**: Once parsed, execution is identical to manually constructed expressions

## Backward Compatibility

- Existing configs continue to work without changes
- Integration layer auto-detects format and handles appropriately
- New and old formats can be mixed in the same config
- No breaking changes to existing ETL code

## Files Created/Modified

**New Files:**
1. `src/meds_etl/config_parser.py` (550 lines)
2. `src/meds_etl/config_integration.py` (230 lines)
3. `examples/omop_etl_vista_config_templated.json` (378 lines)
4. `tests/test_config_parser.py` (456 lines)
5. `tests/test_config_integration.py` (222 lines)
6. `tests/test_new_config_format.py` (115 lines)

**Total: ~2,000 lines of implementation and tests**

## Usage Example

```python
from meds_etl.config_parser import parse_config_value
import polars as pl

# Parse a template
expr = parse_config_value(
    "@start_datetime || @start_date",
    concept_df=None,
    df_schema=None
)

# Use with Polars DataFrame
df = pl.DataFrame({
    "start_datetime": [None, "2024-01-02"],
    "start_date": ["2024-01-01", None]
})

result = df.select(expr.alias("time"))
# Both rows have non-null values via fallback
```

## Future Enhancements

Potential additions for future versions:
1. Additional transform functions (split, join, etc.)
2. Conditional expressions (if/then/else)
3. Math operations for numeric fields
4. Custom function plugins
5. Config validation and linting tools
6. Migration tool to convert old configs to new format

## Documentation Needs

To complete integration:
1. Update ETL documentation with new syntax examples
2. Add migration guide for users wanting to adopt new format
3. Document vocabulary lookup behavior and requirements
4. Add cookbook with common patterns

