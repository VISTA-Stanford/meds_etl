<h1 align="center">
  <br>
  MEDS ETL
  <br>
</h1>

<p align="center">
  <a href="https://www.python.org/downloads/">
    <img src="https://img.shields.io/badge/python-%3E3.10-blue" alt="Python > 3.10">
  </a>
  <a href="https://github.com/Medical-Event-Data-Standard/meds_etl/actions/workflows/test.yml">
    <img src="https://github.com/Medical-Event-Data-Standard/meds_etl/actions/workflows/test.yml/badge.svg?branch=main" alt="Tests">
  </a>
  <img src="https://img.shields.io/badge/MEDS-0.3.3-blue" alt="MEDS 0.3.3">
  <a href="https://github.com/Medical-Event-Data-Standard/meds_etl/graphs/commit-activity">
    <img src="https://img.shields.io/badge/Maintained%3F-yes-green.svg" alt="Maintained">
  </a>
  <a href="https://github.com/psf/black">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Code style: black">
  </a>
  <a href="https://github.com/Medical-Event-Data-Standard/meds_etl/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License: Apache 2.0">
  </a>
</p>

<p align="center">
  Scalable, config-driven ETL pipelines for converting OMOP CDM and MIMIC-IV to Medical Event Data Standard (MEDS)
</p>

---

## 🚀 Overview

**MEDS ETL** provides high-performance, production-ready pipelines for transforming clinical data from OMOP CDM v5.x and MIMIC-IV into the [Medical Event Data Standard (MEDS)](https://github.com/Medical-Event-Data-Standard/meds) format. Built with Polars for speed and configured via JSON for flexibility.

### Key Features

✅ **Config-Driven Architecture** - Define complex transformations in JSON, no code changes needed  
✅ **High Performance** - Parallelized processing with streaming external sort  
✅ **Production Ready** - Comprehensive test suite (30+ tests), validated on large datasets  
✅ **Flexible Code Mapping** - Template-based code generation, concept table joins, source values  
✅ **Memory Efficient** - Streaming operations, configurable memory limits, low-memory modes  
✅ **Multiple Backends** - Pure Python (Polars) or C++ acceleration (optional)  

### Supported Data Sources

- **OMOP CDM v5.3 / v5.4** (PostgreSQL, BigQuery, Parquet)
- **MIMIC-IV** (with concept mapping)
- **Custom OMOP Variants** (flexible JSON configuration)

---

## 📦 Installation

### Basic Installation

```bash
pip install meds_etl
```

### Development Installation

```bash
git clone https://github.com/Medical-Event-Data-Standard/meds_etl.git
cd meds_etl

# Install with uv (recommended)
pip install uv
uv pip install -e ".[tests]"

# Or with pip
pip install -e ".[tests]"
```

### Optional Dependencies

```bash
# C++ backend (faster for large datasets)
pip install meds_etl[cpp]

# Development tools (pytest, pytest-cov)
pip install meds_etl[dev]
```

---

## 🏃 Quick Start

### OMOP → MEDS (Config-Driven)

The **recommended** approach for production pipelines:

```bash
# 1. Create your config (see examples below)
cat > my_omop_config.json << 'EOF'
{
  "primary_key": "person_id",
  "canonical_events": {
    "birth": {
      "table": "person",
      "code": "MEDS_BIRTH"
    }
  },
  "tables": {
    "measurement": {
      "subject_id_field": "person_id",
      "time_start": "measurement_datetime",
      "code_mappings": {
        "concept_id": {
          "concept_id_field": "measurement_concept_id"
        }
      },
      "numeric_value_field": "value_as_number",
      "metadata": [
        {"name": "unit_concept_id", "type": "int"}
      ]
    }
  }
}
EOF

# 2. Run the streaming ETL
python -m meds_etl.omop_streaming \
  --omop_dir /path/to/omop/data \
  --output_dir /path/to/meds/output \
  --config my_omop_config.json \
  --workers 8 \
  --shards 10 \
  --verbose
```

### OMOP → MEDS (Legacy)

```bash
# Original hardcoded OMOP ETL (for standard OMOP CDM only)
python -m meds_etl.omop \
  /path/to/omop/data \
  /path/to/meds/output \
  --num_shards 10 \
  --num_proc 8 \
  --backend polars \
  --omop_version 5.4
```

### MIMIC-IV → MEDS

```bash
python -m meds_etl.mimic \
  /path/to/mimic-iv \
  /path/to/meds/output
```

---

## 📝 Configuration Guide

### Template-Based Code Generation

Create hierarchical, structured codes using templates:

```json
{
  "tables": {
    "note": {
      "code_mappings": {
        "concept_id": {
          "template": "NOTE/{note_title}",
          "transforms": [
            {"type": "regex_replace", "pattern": "\\s+", "replacement": "-"},
            {"type": "upper"}
          ]
        }
      }
    }
  }
}
```

**Result:** `NOTE/PROGRESS-NOTES`, `NOTE/DISCHARGE-SUMMARY`

### Image/Imaging Codes

```json
{
  "tables": {
    "image_occurrence": {
      "code_mappings": {
        "concept_id": {
          "template": "IMAGE/{modality_source_value}|{anatomic_site_source_value}"
        }
      }
    }
  }
}
```

**Result:** `IMAGE/CT|CHEST`, `IMAGE/MR|BRAIN`

### Concept Table Mapping

```json
{
  "tables": {
    "measurement": {
      "code_mappings": {
        "concept_id": {
          "concept_id_field": "measurement_concept_id",
          "source_concept_id_field": "measurement_source_concept_id",
          "fallback_concept_id": 0
        }
      }
    }
  }
}
```

### Multiple Code Strategies

```json
{
  "tables": {
    "condition_occurrence": {
      "code_mappings": {
        "concept_id": {
          "concept_id_field": "condition_concept_id"
        },
        "source_value": {
          "field": "condition_source_value"
        }
      }
    }
  }
}
```

Run with `--code_mapping concept_id` or `--code_mapping source_value`

---

## ⚙️ Command-Line Reference

### `omop_streaming` (Recommended)

```bash
python -m meds_etl.omop_streaming \
  --omop_dir PATH              # OMOP data directory (Parquet files)
  --output_dir PATH            # Output directory for MEDS
  --config PATH                # Config JSON file (required)
  --workers 8                  # Parallel workers for Stage 1 & 2.1
  --shards 10                  # Number of output shards
  --merge_workers 4            # Workers for Stage 2.2 merge
  --chunk_rows 10000000        # Rows per sorted run
  --code_mapping auto          # Code mapping strategy (auto/concept_id/source_value)
  --run_compression lz4        # Intermediate compression (lz4/zstd/snappy)
  --final_compression zstd     # Final compression (zstd/snappy/lz4)
  --low_memory                 # Enable low-memory mode
  --row_group_size 100000      # Parquet row group size
  --polars_threads 1           # Polars threads per worker
  --verbose                    # Detailed logging
```

### `omop_refactor`

```bash
python -m meds_etl.omop_refactor \
  --omop_dir PATH \
  --output_dir PATH \
  --config PATH \
  --workers 8 \
  --shards 10 \
  --backend auto               # auto/polars/cpp
  --code_mapping auto \
  --verbose
```

### `omop` (Legacy)

```bash
python -m meds_etl.omop \
  OMOP_DIR \
  OUTPUT_DIR \
  --num_shards 10 \
  --num_proc 8 \
  --backend polars \
  --omop_version 5.4           # 5.3 or 5.4
```

---

## 🏗️ Architecture

### Pipeline Stages

#### **Stage 1: OMOP → MEDS Unsorted**
- Parallel processing of OMOP tables
- Fast DataFrame joins for concept mapping
- No sorting (fast!)
- Configurable via JSON

#### **Stage 2.1: Partitioning** (Parallelized ⚡)
- Reads unsorted files
- Partitions by `subject_id % num_shards`
- Creates sorted runs per shard
- **NEW:** Parallel file processing with `--workers`

#### **Stage 2.2: Merging**
- K-way merge of sorted runs per shard
- Streaming operations (bounded memory)
- Polars native merge (optimized)
- Configurable with `--merge_workers`

### Memory Optimization

```bash
# Low memory mode (32-64GB machines)
--low_memory \
--row_group_size 50000 \
--polars_threads 1 \
--merge_workers 1

# High performance mode (128GB+ machines)
--row_group_size 200000 \
--polars_threads 4 \
--merge_workers 8 \
--workers 16
```

---

## 📊 Performance

### Benchmarks (Example Dataset - 9K patients, 190M events)

| Metric | omop.py | omop_streaming.py | Improvement |
|--------|---------|-------------------|-------------|
| Output size | 8.0 GB | 3.7 GB | **54% smaller** |
| Unique codes | 46,913 | 48,260 | +1,347 codes |
| Metadata columns | 22 | 51 | +29 columns |
| Compression | Default | zstd | Better |
| Stage 2.1 | Sequential | **Parallel (8 workers)** | ~8x faster |

### Throughput

- **Stage 1:** ~1-2M rows/sec (depends on workers)
- **Stage 2:** ~500K-1M rows/sec (streaming merge)
- **Overall:** Can process 100M+ rows in minutes

---

## 🧪 Testing

### Run Tests

```bash
# Install with test dependencies
uv pip install -e ".[tests]"

# Run all tests
uv run python -m pytest -v

# Run specific test file
uv run python -m pytest tests/test_omop_streaming.py -v

# Run with coverage
uv pip install -e ".[dev]"
uv run python -m pytest --cov=meds_etl --cov-report=html
```

### Test Coverage

- **30 passing tests** (25 for omop_streaming.py)
- Schema validation
- Transform chains (regex, case conversion, templates)
- Concept mapping
- File discovery
- Worker functions
- End-to-end integration tests

---

## 📚 Examples

### Example Configs

See the `examples/` directory for:
- **OMOP CDM 5.4** - Standard OMOP configuration
- **Custom OMOP** - Extended schema with imaging/notes
- **Minimal Config** - Basic configuration template

### Example Output

```python
import polars as pl

# Read MEDS data
df = pl.read_parquet("output/data/0.parquet")

# MEDS schema
print(df.columns)
# ['subject_id', 'time', 'code', 'numeric_value', 'text_value', 
#  'end', 'unit_concept_id', 'provider_id', ...]

# Sample patient
patient = df.filter(pl.col("subject_id") == 12345)
print(patient.head())
```

---

## 🔧 Development

### Setup

```bash
# Clone repo
git clone https://github.com/Medical-Event-Data-Standard/meds_etl.git
cd meds_etl

# Install with dev dependencies
uv pip install -e ".[dev]"

# Run tests
uv run python -m pytest -v

# Format code
black src/ tests/
isort src/ tests/
```

### Project Structure

```
meds_etl/
├── src/meds_etl/
│   ├── omop.py              # Legacy OMOP ETL
│   ├── omop_refactor.py     # Refactored version
│   ├── omop_streaming.py    # Config-driven streaming ETL ⭐
│   ├── mimic/               # MIMIC-IV ETL
│   └── unsorted.py          # Sorting utilities
├── tests/
│   ├── test_omop_streaming.py  # 25 tests for streaming ETL
│   └── test_*.py            # Other test files
├── examples/
│   └── omop_*.json          # Example configurations
└── pyproject.toml           # Package configuration
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `uv run python -m pytest -v`
5. Format code: `black src/ tests/`
6. Submit a pull request

---

## 📖 Documentation

### Detailed Guides

- [DEVELOPMENT.md](DEVELOPMENT.md) - Development environment setup
- [COMPARISON_SUMMARY.md](COMPARISON_SUMMARY.md) - Comparison of different ETL approaches
- [tests/README_TESTS.md](tests/README_TESTS.md) - Testing guide

### Configuration Reference

See example configs in the `examples/` directory:
- `omop_etl_base_config.json` - Minimal OMOP config
- `omop_etl_extended_config.json` - Full config with imaging/notes

---

## 🤝 Citation

If you use MEDS ETL in your research, please cite:

```bibtex
@software{meds_etl,
  title = {MEDS ETL: Scalable pipelines for OMOP to MEDS conversion},
  author = {{MEDS Development Team}},
  year = {2024},
  url = {https://github.com/Medical-Event-Data-Standard/meds_etl}
}
```

---

## 📄 License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

- **MEDS Standard** - [Medical Event Data Standard](https://github.com/Medical-Event-Data-Standard/meds)
- **OMOP CDM** - [Observational Medical Outcomes Partnership](https://ohdsi.github.io/CommonDataModel/)
- **Polars** - [High-performance DataFrame library](https://pola.rs/)
- **MIMIC-IV** - [MIT-LCP MIMIC Critical Care Database](https://mimic.mit.edu/)

---

## 📞 Support

- **Issues:** [GitHub Issues](https://github.com/Medical-Event-Data-Standard/meds_etl/issues)
- **Discussions:** [GitHub Discussions](https://github.com/Medical-Event-Data-Standard/meds_etl/discussions)
- **MEDS Slack:** [Join the community](https://meds-standard.slack.com)

---

<p align="center">
  <strong>Built with ❤️ for the clinical ML community</strong>
</p>
