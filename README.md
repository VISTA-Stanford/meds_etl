<h1 align="center">
  <img src="assets/logo.png" alt="pipelines" width="175">
  <br>
  MEDS ETL
  <br>
</h1>

<div align="center">

<a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-%3E3.10-blue" alt="Python > 3.10"></a><a href="https://github.com/VISTA-Stanford/meds_etl/actions/workflows/python-test.yml"><img src="https://github.com/VISTA-Stanford/meds_etl/actions/workflows/python-test.yml/badge.svg?branch=main" alt="Tests"></a><img src="https://img.shields.io/badge/MEDS-0.3.3-blue" alt="MEDS 0.3.3"><a href="https://github.com/VISTA-Stanford/meds_etl/graphs/commit-activity"><img src="https://img.shields.io/badge/Maintained%3F-yes-green.svg" alt="Maintained"></a><a href="https://github.com/psf/black"><img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Code style: black"></a><a href="https://github.com/VISTA-Stanford/meds_etl/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License: Apache 2.0"></a>

</div>

<br>

High-performance ETL for transforming **OMOP CDM** and **MIMIC-IV** into [MEDS](https://github.com/Medical-Event-Data-Standard/meds) format.

## Installation

```bash
# Install uv (recommended package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install
git clone https://github.com/VISTA-Stanford/meds_etl
cd meds_etl
uv sync

# With C++ backend for 7x faster processing (Linux/macOS)
uv sync --extra cpp
```

**macOS users:** Run `xcode-select --install` first for C++ backend.

## Usage

```bash
uv run python -m meds_etl.omop \
  --omop_dir /path/to/omop \
  --output_dir /path/to/meds \
  --config examples/omop_etl_vista_std_concepts.json \
  --workers 10 \
  --shards 10 \
  --backend cpp \
  --verbose
```

On a 128-core Linux server, the C++ backend processes **3 billion events in ~5 minutes** (vs ~40 min with pure Python).

### Pure Python Alternative

For environments without C++ compilation, `omop_streaming.py` uses Polars' streaming engine for memory-efficient sorting:

```bash
uv run python -m meds_etl.omop_streaming \
  --omop_dir /path/to/omop \
  --output_dir /path/to/meds \
  --config examples/omop_etl_vista_std_concepts.json \
  --workers 10 \
  --shards 10 \
  --verbose
```

This uses a two-phase external sort (partition → k-way merge) that stays memory-bounded and can be faster than the C++ backend for very large datasets.

## 📚 Example Configs

| Config | Code Format | Example Output |
|--------|-------------|----------------|
| [`omop_etl_vista_std_concepts.json`](examples/omop_etl_vista_std_concepts.json) | Standard vocabulary codes | `LOINC/12345-6`, `SNOMED/123456` |
| [`omop_etl_vista_raw_codes.json`](examples/omop_etl_vista_raw_codes.json) | Source system codes | `ICD10CM/E11.9`, `CPT4/99213` |
| [`omop_etl_vista_omop_concepts.json`](examples/omop_etl_vista_omop_concepts.json) | Raw OMOP concept IDs | `4012345`, `8507` |

**Recommended:** Start with `omop_etl_vista_std_concepts.json` for standardized vocabulary codes.

📖 See [`examples/README.md`](examples/README.md) for detailed config documentation.

## Configuration

The JSON config file tells the ETL **which OMOP tables to process** and **how to transform each row into a MEDS event**.

**Example** — process the `measurement` table, look up concept IDs in the OMOP concept table to get standardized codes like `LOINC/12345-6`:

```json
{
  "tables": {
    "measurement": {
      "time_start": "@measurement_datetime",
      "code": "$omop:@measurement_concept_id",
      "numeric_value": "@value_as_number"
    }
  }
}
```

### DSL Quick Reference

| Syntax | Meaning | Example |
|--------|---------|---------|
| `@column` | Column reference | `@measurement_datetime` |
| `$omop:@col` | OMOP concept lookup | `$omop:@measurement_concept_id` |
| `$literal:value` | Explicit literal string | `$literal:measurement` |
| `@col1 \|\| @col2` | Fallback (first non-null) | `@measurement_datetime \|\| @measurement_date` |
| `{@col >> transform()}` | Transform pipe | `{@note_title >> regex_replace('\\s+', '-')}` |
| `filter` | Row-level filtering | `"@concept_id != 0"` or `["pred1", "pred2"]` (ORed) |
| `exempt_codes` | Bypass `standard_only` for specific codes | `["LOINC/LP21258-6"]` |
| `vocabulary` | Concept resolution config (top-level) | `{"$omop": {"sources": [...], "standard_only": ["S", "C"]}}` |

- **`$omop:` prefix** triggers a join with the OMOP `concept` table, producing codes in `vocabulary_id/concept_code` format.
- **`$literal:`** must be used for literal string values in properties. Bare strings (without `@` or `$literal:`) are treated as errors.
- **`>>`** is the preferred transform pipe operator inside `{...}` braces. (`|` is still supported for backward compatibility but `>>` avoids ambiguity with the `||` fallback operator.)
- **Transforms:** `split(delim, index[, default])`, `regex_replace(pattern, replacement)`, `upper()`, `lower()`, `strip()`
- **Config validation** runs automatically at load time, catching typos in field names, missing required fields, and invalid syntax before the ETL starts.

### Config Validation

Configs are automatically validated at load time. The validator checks:

- Unknown or misspelled keys (e.g., `time_sart` instead of `time_start`)
- Missing required fields (`time_start`, `code`)
- Invalid property types
- DSL syntax errors (unbalanced braces, unknown vocabulary prefixes)
- Ambiguous bare string literals in properties (must use `$literal:`)

### Source Concept Resolution

For OMOP datasets with site-specific custom concepts (e.g., Stanford), configure the `vocabulary` key to have `$omop:` also walk "Maps to" chains for custom source concepts:

```json
"vocabulary": {
    "$omop": {
        "sources": ["concept", "concept_relationship"],
        "standard_only": ["S", "C"]
    }
}
```

When `concept_relationship` is included, the ETL **auto-detects** the companion `*_source_concept_id` column for each `$omop:@*_concept_id` lookup (e.g., `observation_concept_id` → `observation_source_concept_id`). If the primary concept lookup fails (or is non-standard when `standard_only` is set), the companion source concept is walked through "Maps to" chains to find a standard code.

`standard_only` accepts `true` (shorthand for `["S"]`), a list like `["S", "C"]` to include Classification concepts, or `false`/omitted for no filtering. OMOP's `standard_concept` values are `"S"` (Standard), `"C"` (Classification), and null (non-standard).

See [`examples/README.md`](examples/README.md#resolving-source-concepts-via-concept_relationship) for details.

## Testing

```bash
uv sync --extra tests
uv run pytest
```

## Citation

```bibtex
@software{meds_etl,
  title = {MEDS ETL: Scalable pipelines for OMOP to MEDS conversion},
  author = {{MEDS Development Team}},
  year = {2024},
  url = {https://github.com/VISTA-Stanford/meds_etl}
}
```

## Links

[MEDS Standard](https://github.com/Medical-Event-Data-Standard/meds) · [Issues](https://github.com/VISTA-Stanford/meds_etl/issues) · [License](LICENSE)
