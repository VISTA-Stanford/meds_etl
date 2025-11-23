# MEDS ETL Test Suite for omop_streaming.py

## Overview

Comprehensive unit tests for the config-driven OMOP to MEDS ETL pipeline with streaming external sort.

## Test Coverage

### ✅ **25 Tests - All Passing**

### Test Categories:

#### 1. **Schema Utilities** (4 tests)
- `test_config_type_to_polars` - Type string to Polars dtype conversion
- `test_get_metadata_column_info` - Extract metadata from config
- `test_get_metadata_column_info_type_mismatch` - Validate type consistency
- `test_get_meds_schema_from_config` - Build global MEDS schema

#### 2. **Transform Utilities** (5 tests)
- `test_apply_transforms_replace` - Simple string replacement
- `test_apply_transforms_regex_replace` - Regex-based replacement
- `test_apply_transforms_case_conversion` - Upper/lowercase conversion
- `test_apply_transforms_strip` - Whitespace and character stripping
- `test_apply_transforms_chained` - Multiple transforms in sequence

#### 3. **MEDS Transformation** (6 tests)
- `test_transform_to_meds_fixed_code` - Canonical events with fixed codes
- `test_transform_to_meds_source_value_field` - Source value field mapping
- `test_transform_to_meds_template` - Template-based code generation
- `test_transform_to_meds_concept_join` - Concept table joins
- `test_transform_to_meds_with_end_time` - End time handling
- `test_transform_to_meds_filters_null_codes` - Null code filtering

#### 4. **File Discovery** (3 tests)
- `test_find_omop_table_files_directory` - Find files in directory
- `test_find_omop_table_files_single_file` - Single file discovery
- `test_find_omop_table_files_not_found` - Handle missing tables

#### 5. **Config Validation** (2 tests)
- `test_validate_config_against_data_success` - Valid config
- `test_validate_config_against_data_missing_field` - Missing field detection

#### 6. **Concept Mapping** (2 tests)
- `test_build_concept_map_basic` - Build concept map from files
- `test_build_concept_map_no_files` - Handle missing concept files

#### 7. **Worker Functions** (2 tests)
- `test_process_omop_file_worker_success` - Successful file processing
- `test_process_omop_file_worker_empty_file` - Empty file handling

#### 8. **Integration Tests** (1 test)
- `test_end_to_end_simple_measurement` - Complete pipeline test

## Running the Tests

### Run All Tests
```bash
cd /Users/jfries/code/sandbox/meds_etl
uv run python -m pytest tests/test_omop_streaming.py -v
```

### Run Specific Test
```bash
uv run python -m pytest tests/test_omop_streaming.py::test_transform_to_meds_template -v
```

### Run with Coverage
```bash
uv pip install pytest-cov
uv run python -m pytest tests/test_omop_streaming.py --cov=meds_etl.omop_streaming --cov-report=html
```

### Run Tests by Category
```bash
# Schema tests only
uv run python -m pytest tests/test_omop_streaming.py -k "schema" -v

# Transform tests only
uv run python -m pytest tests/test_omop_streaming.py -k "transform" -v

# Worker tests only
uv run python -m pytest tests/test_omop_streaming.py -k "worker" -v
```

## Test Features

### ✅ **Comprehensive Coverage**
- Tests all major functions in `omop_streaming.py`
- Covers happy path and error cases
- Includes integration tests

### ✅ **Real-World Scenarios**
- Template-based code generation (e.g., `NOTE/{note_title}`)
- Concept table joins
- Transform chains (regex, case, strip)
- Null filtering and error handling

### ✅ **Fast Execution**
- All 25 tests run in **0.24 seconds**
- Uses temporary directories for file operations
- Minimal external dependencies

### ✅ **Clear Assertions**
- Each test validates specific behavior
- Easy to understand and maintain
- Good error messages on failure

## Example Test Cases

### Template-Based Code Generation
```python
def test_transform_to_meds_template():
    """Test transformation with template-based code generation."""
    # Creates codes like: NOTE/PROGRESS-NOTES, NOTE/DISCHARGE-SUMMARY
    # From template: "NOTE/{note_title}" with regex and uppercase transforms
```

### Concept Table Joins
```python
def test_transform_to_meds_concept_join():
    """Test transformation with concept table join."""
    # Maps concept_id → code via concept DataFrame
    # Example: 3004410 → LOINC/8310-5
```

### Chained Transforms
```python
def test_apply_transforms_chained():
    """Test multiple transforms in sequence."""
    # "  Hello  World  " → "HELLO-WORLD"
    # Via: strip → regex_replace → upper
```

## Key Testing Patterns

### 1. **Temporary Directories**
Tests use `tempfile.TemporaryDirectory()` for isolated file operations.

### 2. **Sample Data**
Uses small, focused Polars DataFrames with realistic OMOP data.

### 3. **Schema Validation**
Tests verify correct Polars dtypes and MEDS schema compliance.

### 4. **Error Handling**
Tests use `pytest.raises()` to verify proper error detection.

## Adding New Tests

### Test Template
```python
def test_new_feature():
    """Test description."""
    # Arrange - Set up test data
    df = pl.DataFrame({...})
    config = {...}
    
    # Act - Call function
    result = function_under_test(df, config)
    
    # Assert - Verify results
    assert result["expected_column"][0] == expected_value
```

### Best Practices
1. **One concept per test** - Test one thing at a time
2. **Clear naming** - Use descriptive test function names
3. **Minimal setup** - Only create what's needed
4. **Good assertions** - Check specific expected values
5. **Clean resources** - Use temp directories that auto-cleanup

## CI/CD Integration

### GitHub Actions Example
```yaml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install -e .[dev]
      - run: pytest tests/test_omop_streaming.py -v
```

## Test Dependencies

Required packages (already in `pyproject.toml`):
- `polars >= 1.22`
- `pyarrow >= 8`
- `pytest >= 9.0` (install with `uv pip install pytest`)

## Test Results Summary

```
============================= test session starts ==============================
platform darwin -- Python 3.12.4, pytest-9.0.1, pluggy-1.6.0
rootdir: /Users/jfries/code/sandbox/meds_etl
collected 25 items

tests/test_omop_streaming.py::test_config_type_to_polars PASSED          [  4%]
tests/test_omop_streaming.py::test_get_metadata_column_info PASSED       [  8%]
tests/test_omop_streaming.py::test_get_metadata_column_info_type_mismatch PASSED [ 12%]
tests/test_omop_streaming.py::test_get_meds_schema_from_config PASSED    [ 16%]
tests/test_omop_streaming.py::test_apply_transforms_replace PASSED       [ 20%]
tests/test_omop_streaming.py::test_apply_transforms_regex_replace PASSED [ 24%]
tests/test_omop_streaming.py::test_apply_transforms_case_conversion PASSED [ 28%]
tests/test_omop_streaming.py::test_apply_transforms_strip PASSED         [ 32%]
tests/test_omop_streaming.py::test_apply_transforms_chained PASSED       [ 36%]
tests/test_omop_streaming.py::test_transform_to_meds_fixed_code PASSED   [ 40%]
tests/test_omop_streaming.py::test_transform_to_meds_source_value_field PASSED [ 44%]
tests/test_omop_streaming.py::test_transform_to_meds_template PASSED     [ 48%]
tests/test_omop_streaming.py::test_transform_to_meds_concept_join PASSED [ 52%]
tests/test_omop_streaming.py::test_transform_to_meds_with_end_time PASSED [ 56%]
tests/test_omop_streaming.py::test_transform_to_meds_filters_null_codes PASSED [ 60%]
tests/test_omop_streaming.py::test_find_omop_table_files_directory PASSED [ 64%]
tests/test_omop_streaming.py::test_find_omop_table_files_single_file PASSED [ 68%]
tests/test_omop_streaming.py::test_find_omop_table_files_not_found PASSED [ 72%]
tests/test_omop_streaming.py::test_validate_config_against_data_success PASSED [ 76%]
tests/test_omop_streaming.py::test_validate_config_against_data_missing_field PASSED [ 80%]
tests/test_omop_streaming.py::test_build_concept_map_basic PASSED        [ 84%]
tests/test_omop_streaming.py::test_build_concept_map_no_files PASSED     [ 88%]
tests/test_omop_streaming.py::test_process_omop_file_worker_success PASSED [ 92%]
tests/test_omop_streaming.py::test_process_omop_file_worker_empty_file PASSED [ 96%]
tests/test_omop_streaming.py::test_end_to_end_simple_measurement PASSED  [100%]

============================== 25 passed in 0.24s ==============================
```

## Future Test Additions

Potential areas for expanded test coverage:

1. **Streaming Sort Tests** - Test `partition_to_sorted_runs`, `streaming_merge_shard`
2. **Parallel Processing** - Test worker parallelization
3. **Large Dataset Tests** - Performance tests with larger data
4. **Error Recovery** - Test handling of malformed data
5. **Config Edge Cases** - More complex config scenarios

## Contributing

When adding features to `omop_streaming.py`, please:
1. Add corresponding unit tests
2. Ensure all existing tests still pass
3. Follow the established test patterns
4. Update this README with new test descriptions

