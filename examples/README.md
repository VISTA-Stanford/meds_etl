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
- Metadata preservation
- Standard OMOP field names

**Use this for**: OMOP CDM 5.4 compliant databases

### 3. `omop_etl_extended_config.json` - Extended Configuration
Comprehensive configuration including:
- All core OMOP tables
- Extended tables (note, imaging, specimen)
- Template-based code generation
- Custom metadata fields
- Advanced transforms

**Use this for**: Custom OMOP implementations with additional tables

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
      "metadata": [ ... ]
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

## Need Help?

- See the [main README](../README.md) for detailed documentation
- Check [COMPARISON_SUMMARY.md](../COMPARISON_SUMMARY.md) for pipeline comparisons
- Review [DEVELOPMENT.md](../DEVELOPMENT.md) for development setup

## Contributing

Have a useful configuration? Submit a PR to add it to this directory!

