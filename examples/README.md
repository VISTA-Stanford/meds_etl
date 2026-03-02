# MEDS ETL Example Configurations

This directory contains example JSON configs for converting OMOP CDM data to MEDS format. Each config demonstrates a different code resolution strategy.

## Available Configs

| Config | Strategy | Output Codes |
|--------|----------|--------------|
| [`omop_etl_vista_std_concepts.json`](omop_etl_vista_std_concepts.json) | Standard concept lookup via `$omop:` | `LOINC/12345-6`, `SNOMED/123456` |
| [`omop_etl_vista_raw_codes.json`](omop_etl_vista_raw_codes.json) | Source concept lookup via `$omop:` on `*_source_concept_id` columns | `ICD10CM/E11.9`, `CPT4/99213` |
| [`omop_etl_vista_omop_concepts.json`](omop_etl_vista_omop_concepts.json) | Raw OMOP concept IDs (no vocab lookup) | `4012345`, `8507` |

**Recommended starting point:** `omop_etl_vista_std_concepts.json`

## Config Structure

```json
{
    "omop_cdm_version": "5.3.1",
    "primary_key": "person_id",
    "canonical_events": { ... },
    "tables": { ... }
}
```

### Top-level Keys

| Key | Required | Description |
|-----|----------|-------------|
| `omop_cdm_version` | No | OMOP CDM version string |
| `primary_key` | No | Patient identifier column (default: `person_id`) |
| `canonical_events` | No | Demographic/lifecycle events (birth, death, gender, race, ethnicity) |
| `tables` | Yes | Map of OMOP table names to event extraction configs |

### Table Config Keys

| Key | Required | Description |
|-----|----------|-------------|
| `time_start` | Yes | Timestamp expression for event start |
| `time_end` | No | Timestamp expression for event end |
| `code` | Yes | Code expression (literal, column ref, or vocab lookup) |
| `numeric_value` | No | Column reference for numeric values |
| `text_value` | No | Column reference for text values |
| `filter` | No | Row-level filter expression (e.g., `@concept_id != 0`) |
| `properties` | No | Array of additional properties to extract |

### Property Objects

```json
{
    "name": "visit_id",
    "value": "@visit_occurrence_id",
    "type": "int"
}
```

| Key | Required | Description |
|-----|----------|-------------|
| `name` | Yes | Output property name |
| `value` | Yes | Column reference (`@col`) or literal (`$literal:value`) |
| `type` | Yes | Data type: `string`, `int`, `float` |

## DSL Expression Syntax

### Column References

Prefix a column name with `@` to reference it:

```json
"time_start": "@measurement_datetime"
```

### Fallback Chains

Use `||` to fall back to alternative columns when the first is null:

```json
"time_start": "@measurement_datetime || @measurement_date"
```

### Vocabulary Lookups

Use `$omop:` to resolve an OMOP concept ID to `vocabulary_id/concept_code` format:

```json
"code": "$omop:@measurement_concept_id"
```

This joins the source column against the OMOP `concept` table and produces codes like `LOINC/12345-6`.

### Templates

Use `{...}` braces within a string to embed column references and transforms:

```json
"code": "STANFORD_NOTE/{@note_title >> regex_replace('\\s+', '-')}"
```

### Transform Pipe (`>>`)

Inside `{...}` braces, use `>>` to pipe a column through a transform function:

```json
"code": "PREFIX/{@column >> split('|', 3, 'Unknown')}"
```

Available transforms:
- `split(delimiter, index[, default])` — split string and take element at index
- `regex_replace(pattern, replacement)` — regex substitution
- `upper()` — uppercase
- `lower()` — lowercase
- `strip()` — trim whitespace

> **Note:** `|` is still accepted as a pipe operator for backward compatibility, but `>>` is preferred to avoid ambiguity with the `||` fallback operator.

### Literal Values in Properties

Use `$literal:` for literal string values in property `value` fields:

```json
{
    "name": "table",
    "value": "$literal:measurement",
    "type": "string"
}
```

Bare strings without `@` or `$literal:` are treated as errors to prevent accidental mistakes where a column name is intended but the `@` prefix is forgotten.

### Row-level Filters

Use the `filter` key on a table config to restrict which rows are processed:

```json
"measurement": {
    "filter": "@measurement_concept_id != 0 AND @value_as_number IS NOT NULL",
    "time_start": "@measurement_datetime",
    "code": "$omop:@measurement_concept_id",
    "numeric_value": "@value_as_number"
}
```

Supported filter operators: `!=`, `==`, `>`, `<`, `>=`, `<=`, `IS NULL`, `IS NOT NULL`, `IN (...)`. Combine conditions with `AND`.

## Choosing a Code Strategy

- **Standard concepts** (`omop_etl_vista_std_concepts.json`): Best for interoperability. Resolves `*_concept_id` columns via `$omop:` to get standard vocabulary codes (SNOMED, LOINC, RxNorm, etc.). This is the recommended approach.

- **Raw/source codes** (`omop_etl_vista_raw_codes.json`): Uses `*_source_concept_id` columns with `$omop:` to preserve the original source system codes (ICD-10, CPT, etc.). Useful when you need source-level granularity.

- **OMOP concept IDs** (`omop_etl_vista_omop_concepts.json`): Passes through raw integer concept IDs without vocabulary lookup. Fastest processing, but codes are opaque integer identifiers.
