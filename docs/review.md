# MEDS ETL Code Review & Design Document

## Executive Summary

This document captures findings from a full review of the `meds_etl` codebase, covering critical bugs, code duplication hazards, DSL design frictions, and missing features. The most severe issues are around the config compilation pipeline where template-based canonical events are silently mishandled in `omop_streaming.py`, vocab-lookup expressions are dropped from `text_value`/`numeric_value` fields, and the `config_parser.py` vocab lookup is unimplemented. The DSL itself is well-conceived but has several sharp edges around ambiguity, incomplete feature parity across code paths, and the lack of formal schema validation.

---

## 1. Critical Bugs

### 1.1 `omop_streaming.py` breaks canonical events with template-based codes

**Severity: Data-corrupting**

After `compile_config()` converts a template-based `"code"` field (e.g. `"Gender/{@gender_concept_id}"`) into `code_mappings` format and deletes the `"code"` key, `omop_streaming.py` line 1970 falls through to the default:

```python
fixed_code = event_config.get("code", f"MEDS_{event_name.upper()}")
```

This means the "gender" canonical event produces `"MEDS_GENDER"` as a literal code instead of resolving the template. The `omop.py` pipeline handles this correctly by checking for both `"code"` and `"code_mappings"`:

```python
fixed_code = event_config.get("code")  # None if not specified
has_code_mappings = "code_mappings" in event_config
```

**Impact:** All canonical events with template/concept-lookup codes (`gender`, `race`, `ethnicity` in the example configs) produce wrong fixed-string codes in the streaming pipeline.

### 1.2 `text_value` and `numeric_value` with `$omop:` syntax are silently dropped

**Severity: Silent data loss**

The example config `omop_etl_vista_std_concepts.json` uses:
```json
"text_value": "$omop:@gender_concept_id"
```

In `_compile_table_config`, the `compile_value_field` helper only recognizes plain `@column` references:

```python
def compile_value_field(key: str, target_key: str):
    if key in compiled:
        value = compiled.pop(key)
        if isinstance(value, str) and value.startswith("@") and not any(c in value for c in ["||", "$", "{", "|"]):
            compiled[target_key] = value[1:]
```

Since `"$omop:@gender_concept_id"` starts with `$`, the condition fails. The value is `pop`-ed from the dict but never assigned to `text_value_field`, so the text_value is silently lost. The same applies to any `numeric_value` using `$omop:`, `||` fallbacks, or template syntax.

### 1.3 `numeric_value` type mismatch between pipelines

**Severity: Schema incompatibility**

`omop.py` defines `numeric_value` as `Float32` (matching the MEDS standard), while `omop_streaming.py` defines it as `Float64`. Outputs from the two pipelines have incompatible schemas and cannot be concatenated or mixed.

### 1.4 `PolarsExpressionBuilder._build_vocab_lookup` is a no-op

**Severity: Feature incomplete**

In `config_parser.py` lines 490-497, the vocabulary lookup path always returns `source_expr` unchanged:

```python
if self.concept_df is None:
    return source_expr
# TODO: Implement actual vocabulary lookup with join
return source_expr
```

This means `parse_config_value("$omop:@drug_concept_id")` evaluates to `pl.col("drug_concept_id")` (the raw integer) rather than the resolved `vocabulary_id/concept_code` string. The actual concept resolution only works through the compiled/old-format path in `transform_to_meds_unsorted`.

### 1.5 `omop_streaming.py` prescan uses unsafe `mp.Pool()` 

**Severity: Potential deadlock on Linux**

`omop_streaming.py` line 767 uses `mp.Pool()` in `prescan_concept_ids`, while `omop.py` correctly uses `mp.get_context(process_method).Pool()`. Despite `omop_streaming.py` carefully setting `mp.set_start_method()` globally, using bare `mp.Pool()` after a `set_start_method("spawn")` should work, but mixing patterns is fragile and inconsistent with the rest of the codebase's approach.

### 1.6 Concept join leaks extra columns into result

In `transform_to_meds_unsorted`, the concept join (`result.join(join_df, ...)`) brings in all columns from `concept_df` (concept_code, concept_name, vocabulary_id, domain_id, concept_class_id) but only drops `concept_id`. The extra columns persist until the final schema enforcement step at the end, which silently drops them. This is wasteful (memory, CPU) and could cause name collisions if a source table has columns named `vocabulary_id` or `domain_id`.

---

## 2. Code Duplication Hazards

### 2.1 `omop.py` and `omop_streaming.py` share ~800 lines of near-identical code

The following functions are fully duplicated with minor divergences:

| Function | Lines in each file |
|---|---|
| `config_type_to_polars` | ~18 |
| `get_meds_schema_from_config` / `get_metadata_column_info` | ~60 |
| `validate_config_against_data` | ~200 |
| `build_concept_map` | ~120 |
| `find_omop_table_files` | ~10 |
| `apply_transforms` | ~70 |
| `build_template_expression` | ~40 |
| `transform_to_meds_unsorted` | ~300 |
| `process_omop_file_worker` | ~60 |
| Prescan functions (`prescan_worker`, `prescan_concept_ids`, etc.) | ~150 |

The divergences are already causing bugs (Float32 vs Float64, canonical event handling, process pool creation). Every bug fix or feature addition must be made in two places, and the drift will only grow.

**Recommendation:** Extract all shared Stage 1 logic into a common module (e.g., `meds_etl/omop_common.py` or expand the existing `omop.py` imports). `omop_streaming.py` should import from `omop.py` for Stage 1 and only implement Stage 2 (streaming sort).

### 2.2 `_smart_split_pipes` is duplicated

`config_parser.py` and `config_compiler.py` each have their own `_smart_split_pipes` implementation with subtly different escape handling. The `config_compiler.py` version handles `\\` escapes while the `config_parser.py` version does not.

---

## 3. DSL Design Frictions

### 3.1 `|` is overloaded: pipe transform vs. literal separator

The template `"STANFORD_IMAGE/{@modality_source_value}-{@anatomic_site_source_value}"` uses `-` as a separator. But in the older version of this config, `|` was used as a literal separator between template parts. This creates ambiguity: `"A/{@col1}|{@col2}"` — is `|` a template-literal separator or a pipe into a transform? The parser resolves this because `|` outside `{}` braces is literal text, but this is subtle and undocumented.

**Recommendation:** Document the scoping rules clearly. Consider supporting an explicit escape (`\|`) for literal pipes inside braces, or use a different separator for transforms (e.g., `->` or `>>`) to avoid confusion.

### 3.2 `split()` transform with 3 args (default value) only works in compiled path

The example config uses:
```json
"code": "STANFORD_VISIT_DETAIL/{@visit_detail_source_value | split('|', 3, 'Unknown')}"
```

The `PolarsExpressionBuilder._apply_transform` in `config_parser.py` only handles 1-2 args for `split()`:

```python
elif func_name == "split":
    if len(args) == 2:
        # ...
    elif len(args) == 1:
        # ...
```

The 3-arg form (with default value) is only supported in the compiled/old-format `apply_transforms` function. If the new parser path is ever used directly (without compilation), this will silently produce nulls instead of `"Unknown"`.

### 3.3 Literal string values in properties are ambiguous

```json
{"name": "table", "value": "person", "type": "string"}
```

Is `"person"` a literal string or a column reference? Answer: literal (no `@` prefix). But this is subtle. A user writing `"value": "visit_occurrence_id"` intending a column reference would silently get a literal string. The `@` sigil is the only discriminator and it's easy to forget.

**Recommendation:** Consider requiring explicit literal syntax like `"value": "'person'"` (quoted literal) or `"value": {"literal": "person"}` — but this trades verbosity for clarity. At minimum, add a warning when a property value looks like a column name but lacks `@`.

### 3.4 No formal JSON schema for config validation

Config files have no schema. A typo like `"time_sart"` instead of `"time_start"` is silently ignored. Invalid keys, missing required fields, and type mismatches are only caught at runtime (if at all).

**Recommendation:** Create a JSON Schema (or Pydantic model) for config validation. The `validate_config_against_data` function partially does this, but it requires actual OMOP data to be present. A standalone `validate_config(config)` that checks structure without data would catch errors earlier.

### 3.5 `$omop:` is hardcoded as the only vocabulary

The parser supports a generic `$vocab:source` syntax but only `$omop:` has any real implementation. The `default_vocab` parameter and `vocab_name` field exist but are unused. Users of non-OMOP vocabularies (SNOMED standalone, ICD mappings, custom ontologies) have no extension point.

**Recommendation:** If multi-vocabulary support is intended, define a vocabulary registry interface. If not, simplify the syntax to just `$:@column` or `concept(@column)` to avoid implying generality that doesn't exist.

### 3.6 No row-level filtering in the DSL

A common ETL need is filtering rows: "only include measurements where value_as_number is not null" or "exclude concept_id = 0". The current DSL has no syntax for this — all rows from a configured table are included (rows with null codes are filtered, but that's it).

**Recommendation:** Add a `"filter"` key to table configs:
```json
{
    "filter": "@measurement_concept_id != 0",
    "time_start": "...",
    "code": "..."
}
```

### 3.7 No way to express multi-event emission per row

Some OMOP tables encode multiple events per row (e.g., a measurement row has both the measurement code and a unit code, or a visit row should emit both a visit-start and visit-end event). The DSL only supports one event per row per table config.

**Recommendation:** Allow a table config to be a list of event specs, each producing one event per row:
```json
"measurement": [
    {"code": "$omop:@measurement_concept_id", "numeric_value": "@value_as_number"},
    {"code": "$omop:@unit_concept_id", "filter": "@unit_concept_id IS NOT NULL"}
]
```

### 3.8 `text_value` / `numeric_value` don't support the full DSL

As noted in bug 1.2, only plain `@column` references work for `text_value` and `numeric_value` after compilation. Fallbacks (`||`), transforms (`| upper()`), and concept lookups (`$omop:`) are all silently broken for these fields.

---

## 4. Missing Features

### 4.1 Config validation CLI

There is no standalone way to validate a config file. A command like:
```bash
meds_etl validate --config examples/omop_etl_vista_std_concepts.json --omop_dir /path/to/omop
```
would catch errors without running the full ETL (potentially hours of computation).

### 4.2 Dry-run / preview mode

Users cannot preview the output schema, sample rows, or per-table event counts without running the full pipeline.

### 4.3 Config migration tool

The docs mention a migration tool from old to new config format as future work. This would reduce friction for existing users.

### 4.4 CSV/other input support in new pipelines

`omop_legacy.py` supports CSV input but `omop.py` and `omop_streaming.py` only support Parquet. Users with CSV exports must convert first.

### ~~4.5 Missing `examples/README.md`~~ **Fixed**

~~Referenced in `README.md` but does not exist in the repository.~~ Created with full DSL reference documentation.

### 4.6 Incremental / resume support

If the ETL fails mid-pipeline (e.g., during Stage 2 after Stage 1 completes), there's no way to resume. The entire pipeline must be re-run. The `--pipeline` flag in `omop_streaming.py` partially addresses this but the temp directory is cleaned up on failure.

### 4.7 Output schema preview

No way to inspect the output MEDS schema that a config will produce without running the ETL:
```bash
meds_etl schema --config examples/omop_etl_vista_std_concepts.json
```

---

## 5. Test Coverage Gaps

### Untested modules

| Module | Status |
|---|---|
| `meds_etl.utils` | No tests for `parse_time` or `convert_generic_value_to_specific` |
| `meds_etl.omop` | No tests for the full pipeline or any of its unique sort paths |
| `meds_etl.omop_legacy` | Only `write_event_data` tested; `process_table_csv/parquet`, `extract_metadata`, `main` untested |
| `meds_etl.mimic` | `TestMimicETL` requires demo data (`tests/data/mimic-iv-demo`) that isn't included |

### Critical untested paths

1. **Config compilation round-trip**: No test verifies that `compile_config` + `transform_to_meds_unsorted` produces the same output as direct `parse_config_value` evaluation for the same config
2. **`omop_streaming.py` canonical events**: The template-code bug (Section 1.1) would be caught by a test running the streaming pipeline with the example configs
3. **`text_value` / `numeric_value` with DSL syntax**: No test covers `$omop:` or `||` in these fields
4. **End-to-end integration test with example configs**: The three example configs in `examples/` are never used in tests
5. **CLI entry points**: None of the 5 CLI commands have any test coverage

---

## 6. Architecture Recommendations

### 6.1 Extract shared code from `omop.py` / `omop_streaming.py`

Create `meds_etl/omop_common.py` containing all shared Stage 1 logic:
- Schema utilities (`config_type_to_polars`, `get_meds_schema_from_config`, `get_property_column_info`)
- Validation (`validate_config_against_data`)
- Concept mapping (`build_concept_map`, `prescan_concept_ids`, etc.)
- Transformation (`apply_transforms`, `build_template_expression`, `transform_to_meds_unsorted`)
- Worker functions (`process_omop_file_worker`)
- File discovery (`find_omop_table_files`)

Then `omop.py` focuses on the cpp/polars sort backend, and `omop_streaming.py` focuses on the Polars streaming sort.

### 6.2 Complete the `config_parser.py` → Polars pipeline

The `PolarsExpressionBuilder` vocab lookup is a TODO. Either:
- **(a)** Finish the implementation so `parse_config_value` can handle `$omop:` end-to-end, replacing the compile-to-old-format indirection, or
- **(b)** Remove the `PolarsExpressionBuilder` and commit to the compilation approach, documenting that `config_parser.py` is only used for parsing (AST generation), not execution

Option (a) is cleaner long-term but more work. Option (b) avoids dead code.

### 6.3 Fix the compilation pipeline for `text_value` / `numeric_value`

`compile_value_field` should handle the full DSL, not just `@column` references. At minimum, it needs to support `$omop:`, `||` fallbacks, and template syntax — routing complex expressions through the same `convert_code_expression` / template machinery used for `code`.

### 6.4 Add config schema validation

A JSON Schema or Pydantic model would catch:
- Typos in field names
- Missing required fields (`time_start`)
- Invalid types in `properties`
- Unknown keys (warning)

This could be validated at config load time (zero cost, high value).

---

## 7. DSL Improvement Proposals

### 7.1 Short-term (non-breaking)

| Proposal | Effort | Impact | Status |
|---|---|---|---|
| Document `|` scoping rules (literal outside `{}`, pipe inside) | Low | Reduces user confusion | **Done** — `>>` introduced as preferred pipe, documented in README and examples/README.md |
| Add `split(delim, idx, default)` to `config_parser.py` | Low | Feature parity with compiled path | **Done** — 3-arg `split()` supported in both parser and compiler |
| Support `text_value` / `numeric_value` with full DSL | Medium | Fixes silent data loss | Open |
| Add JSON Schema for config validation | Medium | Catches config errors early | **Done** — `config_schema.py` validates at load time |
| Warn when property `"value"` looks like a column name without `@` | Low | Prevents common mistake | **Done** — bare strings now raise errors; `$literal:` required |

### 7.2 Medium-term (minor DSL evolution)

| Proposal | Effort | Impact | Status |
|---|---|---|---|
| Add `"filter"` key for row-level filtering | Medium | Addresses common ETL need | **Done** — `filter` key on table configs, compiled and applied at runtime |
| Add `"subject_id"` as a table-level DSL key (currently only `"subject_id_field"`) | Low | Consistency with other fields | Open |
| Support list-of-events per table for multi-event emission | Medium | Handles complex OMOP patterns | Deferred |
| Add `$literal:value` syntax for explicit literals in ambiguous contexts | Low | Eliminates literal vs. column ambiguity | **Done** — all example configs updated |

### 7.3 Long-term (breaking changes, major version)

| Proposal | Effort | Impact | Status |
|---|---|---|---|
| Replace `||` fallback with `coalesce(@a, @b)` function syntax | High | More explicit, avoids overloading `\|\|` | Open |
| Unify `code`, `text_value`, `numeric_value` under a single expression grammar | High | Eliminates per-field compilation quirks | Open |
| Replace the compile-to-old-format pipeline with direct AST → Polars execution | High | Eliminates entire class of round-trip bugs | Open |
| YAML config support (alongside JSON) for better readability | Medium | Developer ergonomics | Open |

---

## 8. Summary of Priority Fixes

### P0 — Fix before next release
1. Fix canonical event template codes in `omop_streaming.py` (Section 1.1)
2. Fix `text_value` / `numeric_value` `$omop:` and `||` compilation (Section 1.2)
3. Fix `numeric_value` Float32 vs Float64 inconsistency (Section 1.3)

### P1 — Fix soon
4. Extract shared code from `omop.py` / `omop_streaming.py` (Section 6.1)
5. Select only needed columns in concept joins (Section 1.6)
6. ~~Add `split(delim, idx, default)` to `config_parser.py` (Section 3.2)~~ **Done**
7. ~~Create `examples/README.md` (Section 4.5)~~ **Done**

### P2 — Improve
8. ~~Add JSON Schema for config validation (Section 6.4)~~ **Done** — `config_schema.py`
9. Implement config validation CLI (Section 4.1)
10. Add end-to-end tests using example configs (Section 5)
11. ~~Resolve or remove the `PolarsExpressionBuilder` vocab lookup TODO (Section 6.2)~~ **Done** — documented as intentionally unimplemented; production uses compile path

### Implemented since initial review
- **`>>` pipe operator**: Preferred transform pipe, replacing `|` inside `{...}` to avoid ambiguity with `||` fallback
- **`$literal:` syntax**: Required for literal string values in properties; bare strings raise errors
- **`filter` key**: Row-level filtering on table configs with `AND`-combined conditions
- **Config schema validation**: Structural validation at config load time (`config_schema.py`)
- **Vocabulary abstraction**: `VocabularyProvider` ABC + `OMOPVocabularyProvider` in `vocabulary.py`
- **`split()` 3-arg form**: Feature parity between parser and compiler
- **Example configs**: All three configs updated to new syntax (`>>`, `$literal:`)
- **`examples/README.md`**: Comprehensive config documentation created
