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
| `vocabulary` | No | Configures `$prefix:` operators; e.g., `{"$omop": {"sources": ["concept"], "standard_only": ["S", "C"]}}` |

### Table Config Keys

| Key | Required | Description |
|-----|----------|-------------|
| `time_start` | Yes | Timestamp expression for event start |
| `time_end` | No | Timestamp expression for event end |
| `code` | Yes | Code expression (literal, column ref, or vocab lookup) |
| `numeric_value` | No | Column reference for numeric values |
| `text_value` | No | Column reference for text values |
| `filter` | No | Row-level filter expression (e.g., `@concept_id != 0`) |
| `exempt_codes` | No | List of code strings that bypass the `standard_only` filter |
| `pre_join` | No | Enrich rows by joining a reference table before transform |
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

For multiple predicates combined with OR, use a list of strings:

```json
"observation": {
    "filter": [
        "@observation_concept_id != 2000006253",
        "@value_as_number IS NOT NULL OR @value_as_string IS NOT NULL"
    ],
    "time_start": "@observation_datetime",
    "code": "$omop:@observation_concept_id"
}
```

| Format | Semantics |
|--------|-----------|
| String | Single predicate (can contain `AND` and `OR` internally) |
| List of strings | Each string is a predicate; all ORed together (keep row if ANY matches) |

Supported filter operators: `!=`, `==`, `>`, `<`, `>=`, `<=`, `IS NULL`, `IS NOT NULL`, `IN (...)`. Combine conditions with `AND` or `OR`.

### Exempt Codes

When `standard_only` is set in the vocabulary config, concepts not matching the allowed values are dropped. Use `exempt_codes` on a table to allow specific non-matching codes through:

```json
"observation": {
    "code": "$omop:@observation_concept_id",
    "exempt_codes": ["LOINC/LP21258-6"],
    "filter": [
        "@value_as_number IS NOT NULL OR @value_as_string IS NOT NULL"
    ]
}
```

This is useful when upstream OMOP mapping assigns a non-standard concept (e.g., a LOINC hierarchy node) that is still clinically meaningful and needed for downstream compatibility (e.g., tokenizer vocabularies). Exempt codes bypass the `standard_only` filter but are still resolved through the concept table — they are a last resort after standard concept lookup and relationship resolution both fail.

### Pre-Join Reference Tables

Use `pre_join` to enrich source rows with columns from an OMOP reference table before the transform runs. This is useful for resolving IDs to human-readable names (e.g., `care_site_id` → `care_site_name`):

```json
"visit_detail": {
    "pre_join": [
        {
            "table": "care_site",
            "on": "care_site_id",
            "select": ["care_site_name"]
        }
    ],
    "code": "CARE_SITE/{@care_site_name}",
    ...
}
```

| Key | Required | Description |
|-----|----------|-------------|
| `table` | Yes | OMOP table name to join (e.g., `care_site`, `provider`, `location`) |
| `on` | Yes | Column name to join on (must exist in both source and reference table) |
| `select` | No | List of columns to bring in from the reference table (default: all) |

The reference table is loaded once at pipeline startup and reused across all file shards. Multiple `pre_join` entries can be specified per table. The join is a left join — rows with no match in the reference table get null values for the joined columns.

## Choosing a Code Strategy

- **Standard concepts** (`omop_etl_vista_std_concepts.json`): Best for interoperability. Resolves `*_concept_id` columns via `$omop:` to get standard vocabulary codes (SNOMED, LOINC, RxNorm, etc.). This is the recommended approach.

- **Raw/source codes** (`omop_etl_vista_raw_codes.json`): Uses `*_source_concept_id` columns with `$omop:` to preserve the original source system codes (ICD-10, CPT, etc.). Useful when you need source-level granularity.

- **OMOP concept IDs** (`omop_etl_vista_omop_concepts.json`): Passes through raw integer concept IDs without vocabulary lookup. Fastest processing, but codes are opaque integer identifiers.

## Resolving Source Concepts via concept_relationship

Some OMOP datasets include non-standard concepts — both site-specific custom concepts (e.g., Stanford's `STANFORD_MEAS`, `STANFORD_PROC` vocabularies) and standard vocabulary concepts that aren't marked as standard (e.g., ICD10CM codes). These concepts often have "Maps to" relationships in the OMOP `concept_relationship` table that resolve to standard concepts (SNOMED, LOINC, RxNorm, etc.).

### Explicit Operations: `$omop.lookup` and `$omop.resolve`

The ETL provides two explicit vocabulary operations:

- **`$omop.lookup:@field`** — direct concept table join. Returns `vocabulary_id/concept_code` for the given concept ID. Never consults `concept_relationship`.
- **`$omop.resolve:@field`** — resolves through `concept_relationship` "Maps to" chains to reach a standard concept. Returns null if no mapping exists.

These can be combined with `||` fallback:

```json
{
    "vocabulary": {
        "$omop": {
            "sources": ["concept", "concept_relationship"],
            "standard_only": ["S", "C"]
        }
    },
    "tables": {
        "measurement": {
            "time_start": "@measurement_datetime",
            "code": "$omop.resolve:@measurement_source_concept_id || $omop.lookup:@measurement_concept_id",
            "numeric_value": "@value_as_number"
        }
    }
}
```

This reads: "resolve the source concept through 'Maps to' mappings. If that fails (source = 0 or no mapping), fall back to looking up the standard concept_id directly."

### Legacy `$omop:` Syntax

The `$omop:` prefix (without `.lookup` or `.resolve`) continues to work as a backward-compatible alias for `$omop.lookup:`. When `concept_relationship` is in `vocabulary.sources`, `$omop:` also triggers implicit relationship resolution as a fallback — but a deprecation warning is logged. New configs should use `$omop.lookup:` and `$omop.resolve:` explicitly.

The `vocabulary.$omop` config accepts either a shorthand list (`["concept"]`) or an object with:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sources` | list | `["concept"]` | OMOP tables used for resolution |
| `standard_only` | bool or list | `false` | Filter concepts by `standard_concept` value. `true` = `["S"]`, `["S", "C"]` = Standard + Classification |

`$omop.resolve:` requires `concept_relationship` in `vocabulary.$omop.sources`. The config validator will reject configs that use `$omop.resolve:` without it.

| `vocabulary.$omop` config | Behavior |
|--------------------------|----------|
| `["concept"]` (default when omitted) | Direct concept table lookup only, all concepts |
| `{"sources": ["concept"], "standard_only": true}` | Direct lookup, standard concepts (`S`) only |
| `{"sources": ["concept"], "standard_only": ["S", "C"]}` | Direct lookup, standard + classification concepts |
| `{"sources": ["concept", "concept_relationship"], "standard_only": ["S", "C"]}` | Standard + classification concepts + "Maps to" resolution via `$omop.resolve` |
