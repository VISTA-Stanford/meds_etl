# MEDS ETL Configuration Examples

This directory contains example configuration files for the MEDS ETL pipeline.

## Available Examples

### 1. `omop_etl_base_config.json` - Minimal Configuration
A minimal configuration covering the core OMOP tables:
- person (demographics, birth, death)
- visit_occurrence
- condition_occurrence
- procedure_occurrence
- drug_exposure
- measurement
- observation

**Use this for**: Standard OMOP CDM implementations, getting started

### 2. `omop_etl_standard_5.4.json` - Standard OMOP CDM 5.4
Standard configuration for OMOP CDM v5.4 with:
- All core clinical tables
- Concept ID mappings via concept table
- Property preservation
- Standard OMOP field names

**Use this for**: OMOP CDM 5.4 compliant databases

### 3. `omop_etl_extended_config.json` - Extended Configuration
Comprehensive configuration including:
- All core OMOP tables
- Extended tables (note, imaging, specimen)
- Template-based code generation
- Custom property fields
- Advanced transforms

**Use this for**: Custom OMOP implementations with additional tables

### 4. `omop_etl_with_properties.json` - New Features Demo
Demonstrates new property features (v0.2+):
- **Literal values**: Add constant columns (e.g., table names)
- **Column aliasing**: Rename columns (e.g., `visit_occurrence_id` → `visit_id`)
- **"properties" key**: Modern terminology for additional columns

**Use this for**: Learning new features, custom column requirements

## How to Use

1. **Copy an example**:
   ```bash
   cp examples/omop_etl_base_config.json my_config.json
   ```

2. **Customize for your schema**:
   - Update field names to match your database
   - Add/remove tables as needed
   - Configure code mappings
   - Add metadata columns

3. **Run the ETL**:
   ```bash
   python -m meds_etl.omop_streaming \
     --omop_dir /path/to/omop/data \
     --output_dir /path/to/output \
     --config my_config.json \
     --workers 8 \
     --shards 8
   ```

## Configuration Structure

All configs follow this structure:

```json
{
  "primary_key": "person_id",
  "canonical_events": {
    "birth": { "table": "person", "code": "MEDS_BIRTH", ... },
    "death": { "table": "person", "code": "MEDS_DEATH", ... },
    ...
  },
  "tables": {
    "table_name": {
      "subject_id_field": "person_id",
      "time_start": "datetime_field",
      "code_mappings": { ... },
      "numeric_value_field": "value_field",
      "properties": [ ... ]  // or "metadata" for backwards compat
    }
  }
}
```

## Code Mapping Strategies

### 1. Concept ID (via concept table)
```json
"code_mappings": {
  "concept_id": {
    "concept_id_field": "measurement_concept_id"
  }
}
```

### 2. Source Value
```json
"code_mappings": {
  "source_value": {
    "field": "measurement_source_value"
  }
}
```

### 3. Template-Based
```json
"code_mappings": {
  "concept_id": {
    "template": "NOTE/{note_title}",
    "transforms": [
      {"type": "regex_replace", "pattern": "\\s+", "replacement": "-"},
      {"type": "upper"}
    ]
  }
}
```

### 4. Template with Concept Mapping (NEW! ✨)
Map concept_ids to codes via concept table BEFORE substituting into template:

```json
"code_mappings": {
  "concept_id": {
    "template": "Gender/{gender_concept_id}",
    "concept_fields": ["gender_concept_id"]
  }
}
```

**Without `concept_fields`:** `gender_concept_id=8507` → `"Gender/8507"`  
**With `concept_fields`:** `gender_concept_id=8507` → lookup in concept table → `"Gender/MALE"`

**Use case:** Canonical events like gender/race/ethnicity where you want human-readable codes.

## Properties (Additional Columns)

**Note:** Both `"properties"` and `"metadata"` keys are supported for backwards compatibility. We recommend using `"properties"` (matches MEDS terminology for non-core columns).

### 1. Standard Properties
Add OMOP columns to MEDS output (same name in input and output):

```json
"properties": [
  {
    "name": "visit_occurrence_id",
    "type": "int"
  },
  {
    "name": "provider_id",
    "type": "int"
  }
]
```

**Output columns:** `visit_occurrence_id`, `provider_id`

### 2. Aliased Properties
Rename OMOP columns in MEDS output:

```json
"properties": [
  {
    "name": "visit_occurrence_id",  // OMOP column name
    "alias": "visit_id",            // MEDS output name
    "type": "int"
  },
  {
    "name": "measurement_id",
    "alias": "meas_id",
    "type": "int"
  }
]
```

**Output columns:** `visit_id`, `meas_id` (original names are replaced)

### 3. Literal Properties
Add constant values (same for all rows in a table):

```json
"properties": [
  {
    "name": "table_name",
    "literal": "visit_occurrence",
    "type": "string"
  },
  {
    "name": "dataset_version",
    "literal": 2,
    "type": "int"
  }
]
```

**Use cases:**
- Track source table for downstream analysis
- Add version/batch identifiers
- Flag data subsets

### 4. Combining Property Types
Mix and match in the same table:

```json
"properties": [
  {
    "name": "table_name",
    "literal": "measurement",
    "type": "string"
  },
  {
    "name": "measurement_id",
    "alias": "meas_id",
    "type": "int"
  },
  {
    "name": "unit_concept_id",
    "type": "int"
  }
]
```

**Output columns:** `table_name` (literal), `meas_id` (aliased), `unit_concept_id` (standard)

### Supported Types
- `"int"` or `"integer"` → Int64
- `"float"` → Float64
- `"string"` or `"str"` → Utf8
- `"boolean"` or `"bool"` → Boolean
- `"datetime"` → Datetime(us)

## Need Help?

- See the [main README](../README.md) for detailed documentation
- Check [COMPARISON_SUMMARY.md](../COMPARISON_SUMMARY.md) for pipeline comparisons
- Review [DEVELOPMENT.md](../DEVELOPMENT.md) for development setup

## Contributing

Have a useful configuration? Submit a PR to add it to this directory!

