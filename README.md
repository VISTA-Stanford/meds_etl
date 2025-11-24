<h1 align="center">
  <img src="assets/logo.png" alt="pipelines" width="175">
  <br>
  MEDS ETL
  <br>
</h1>

<div align="center">

  <a href="https://www.python.org/downloads/">
    <img src="https://img.shields.io/badge/python-%3E3.10-blue" alt="Python > 3.10">
  </a>
  <a href="https://github.com/Medical-Event-Data-Standard/meds_etl/actions/workflows/python-test.yml">
    <img src="https://github.com/Medical-Event-Data-Standard/meds_etl/actions/workflows/python-test.yml/badge.svg?branch=main" alt="Tests">
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

</div>

## 🚀 Overview

**MEDS ETL** provides high-performance, production-ready pipelines for transforming clinical data from OMOP CDM v5.x and MIMIC-IV into the [Medical Event Data Standard (MEDS)](https://github.com/Medical-Event-Data-Standard/meds) format. Built with Polars for speed and configured via JSON for flexibility.

### Key Features

✅ **Config-Driven Architecture** - Define complex transformations in JSON, no code changes needed  
✅ **Ultra-Fast C++ Backend** - Optional native acceleration for production workloads (Linux only)  
✅ **Production Ready** - Comprehensive test suite (30+ tests), validated on large datasets  
✅ **Flexible Code Mapping** - Template-based code generation, concept table joins, source values  
✅ **Memory Efficient** - Streaming operations, configurable memory limits, low-memory modes  
✅ **Multiple Deployment Options** - Laptop-friendly Python or production C++ backend  

### Supported Data Sources

- **OMOP CDM v5.3 / v5.4** (PostgreSQL, BigQuery, Parquet)
- **MIMIC-IV** (with concept mapping)
- **Custom OMOP Variants** (flexible JSON configuration)

---

## 📦 Installation

### Install `uv` (Recommended Package Manager)

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or with pip
pip install uv
```

### Basic Installation (Python Only)

```bash
uv pip install meds_etl
```

### Production Installation (with C++ Backend - Linux Only)

```bash
uv pip install meds_etl[cpp]
```

**Note:** The C++ backend (`meds_etl_cpp`) is currently only available for Linux. macOS and Windows users should use the pure Python implementation.

### Development Installation

```bash
git clone https://github.com/Medical-Event-Data-Standard/meds_etl.git
cd meds_etl

# Install with uv (recommended)
uv pip install -e ".[tests,cpp]"
```

---

## 🏃 Quick Start

### Which Script Should I Use?

| Script | Use Case | Backend | Speed | Platform |
|--------|----------|---------|-------|----------|
| **`omop_refactor.py`** | **Production pipelines** | C++ (auto-fallback to Python) | ⚡ **Fastest** | Linux (C++), Any (Python) |
| **`omop_streaming.py`** | **Development, laptops, small machines** | Pure Python (Polars) | 🚀 Fast, memory-efficient | Any |
| `omop.py` | Legacy (hardcoded schema) | Python or C++ | Medium | Any |

---

## 🚀 Production: `omop_refactor.py` (Recommended)

**Best for:** Large datasets, production servers, Linux machines

### Features
- ✅ **Ultra-fast C++ backend** (~6x faster than Python)
- ✅ **Config-driven** - No code changes needed
- ✅ **Auto-fallback** - Uses Python if C++ unavailable
- ✅ **Simple interface** - Single command for entire pipeline

### Usage

```bash
# With C++ backend (Linux only)
uv run python -m meds_etl.omop_refactor \
  --omop_dir /path/to/omop/parquet \
  --output_dir /path/to/meds/output \
  --config examples/omop_etl_vista_config.json \
  --workers 8 \
  --shards 100 \
  --backend auto \
  --verbose

# Options:
#   --backend auto    # Try C++, fallback to Python (default)
#   --backend cpp     # Force C++ (error if unavailable)
#   --backend polars  # Force Python (always available)
```

### Performance (211K patients, 3.08B events)

**Hardware:** 128 CPU cores, 1TB RAM (Linux server)

| Backend | Total Time | Stage 1 | Stage 2 | Speedup vs Legacy |
|---------|------------|---------|---------|-------------------|
| **C++ (omop_refactor)** | **5m 26s** | ~3m | **2m 38s** | **6x faster** |
| Python (omop_streaming) | 39m 28s | 1m 20s | 38m 7s | 2.1x faster |
| Legacy omop.py (C++) | 29m 57s | N/A | N/A | Baseline (old) |
| Legacy omop.py (Python) | 82m 58s | N/A | N/A | Baseline (old) |

**Key Insights:** 

- C++ backend (`omop_refactor`) is **7.3x faster** than pure Python (`omop_streaming`)
- Both new implementations are **significantly faster** than legacy code
- `omop_streaming` throughput: **1.30M rows/sec** on 128 workers

---

## 💻 Development: `omop_streaming.py`

**Best for:** Laptops, small machines, development, debugging

### Features
- ✅ **Pure Python** - No C++ compilation needed
- ✅ **Memory-efficient** - Streaming operations, low-memory mode
- ✅ **Cross-platform** - Works on macOS, Windows, Linux
- ✅ **Detailed control** - Fine-tune memory usage, chunk sizes

### Usage

```bash
uv run python -m meds_etl.omop_streaming \
  --omop_dir /path/to/omop/parquet \
  --output_dir /path/to/meds/output \
  --config examples/omop_etl_vista_config.json \
  --workers 4 \
  --shards 10 \
  --low_memory \
  --verbose

# Memory optimization options:
#   --low_memory          # Enable low-memory mode
#   --chunk_rows 5000000  # Rows per sorted run (lower = less memory)
#   --merge_workers 2     # Parallel merge workers
#   --polars_threads 1    # Threads per worker
```

### When to Use `omop_streaming.py`

- 🖥️ **Developing on a laptop** (limited RAM)
- 🐛 **Debugging transformations** (easier to trace Python code)
- 🪟 **Non-Linux platforms** (macOS, Windows)
- 📊 **Small datasets** (<10GB) where speed isn't critical

---

## 📝 Configuration Guide

Both `omop_refactor.py` and `omop_streaming.py` use the same JSON configuration format.

### Basic Config Structure

```json
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
      "text_value_field": "value_source_value",
      "properties": [
        {"name": "unit_concept_id", "type": "int"},
        {"name": "provider_id", "type": "int"}
      ]
    }
  }
}
```

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

### `omop_refactor` (Production)

```bash
uv run python -m meds_etl.omop_refactor \
  --omop_dir PATH              # OMOP data directory (Parquet files)
  --output_dir PATH            # Output directory for MEDS
  --config PATH                # Config JSON file (required)
  --workers 8                  # Parallel workers
  --shards 100                 # Number of output shards
  --backend auto               # auto/cpp/polars (default: auto)
  --code_mapping auto          # auto/concept_id/source_value
  --verbose                    # Detailed logging
```

### `omop_streaming` (Development)

```bash
uv run python -m meds_etl.omop_streaming \
  --omop_dir PATH              # OMOP data directory (Parquet files)
  --output_dir PATH            # Output directory for MEDS
  --config PATH                # Config JSON file (required)
  --workers 4                  # Parallel workers for Stage 1 & 2.1
  --shards 10                  # Number of output shards
  --merge_workers 2            # Workers for Stage 2.2 merge
  --chunk_rows 10000000        # Rows per sorted run
  --code_mapping auto          # Code mapping strategy
  --run_compression lz4        # Intermediate compression (lz4/zstd/snappy)
  --final_compression zstd     # Final compression (zstd/snappy/lz4)
  --low_memory                 # Enable low-memory mode
  --row_group_size 100000      # Parquet row group size
  --polars_threads 1           # Polars threads per worker
  --verbose                    # Detailed logging
```

---

## 🏗️ Architecture

### Pipeline Stages

Both `omop_refactor.py` and `omop_streaming.py` use a two-stage pipeline:

#### **Stage 1: OMOP → MEDS Unsorted**
- Parallel processing of OMOP tables
- Fast DataFrame joins for concept mapping
- Template-based code generation
- No sorting (fast!)
- Configurable via JSON

#### **Stage 2: External Sort**
- **C++ Backend (omop_refactor):** Memory-bounded k-way merge with multi-threading
- **Python Backend (omop_streaming):** Polars streaming with configurable chunking
- Partitions by `subject_id % num_shards`
- Produces sorted MEDS output

### Memory Optimization

```bash
# Low memory mode (32-64GB machines) - omop_streaming
uv run python -m meds_etl.omop_streaming \
  --low_memory \
  --chunk_rows 5000000 \
  --row_group_size 50000 \
  --polars_threads 1 \
  --merge_workers 1

# High performance mode (128GB+ machines) - omop_refactor
uv run python -m meds_etl.omop_refactor \
  --workers 16 \
  --shards 100 \
  --backend cpp
```

---

## 📊 Performance Benchmarks

### Large Dataset (211K patients, 3.08B events)

**Hardware:** 128 CPU cores, 1TB RAM (Linux server)

| Implementation | Stage 1 | Stage 2 | Total Time | Throughput | Notes |
|----------------|---------|---------|------------|------------|-------|
| **omop_refactor (C++)** | ~3m | **2m 38s** | **5m 26s** | N/A | ⚡ **Fastest** |
| **omop_streaming (Python)** | **1m 20s** | **38m 7s** | **39m 28s** | **1.30M rows/s** | Pure Python, 128 workers |
| omop.py (C++) | N/A | N/A | 29m 57s | N/A | Legacy |
| omop.py (Python) | N/A | N/A | 82m 58s | N/A | Legacy |

**Key Insight:** 
- `omop_refactor` with C++ backend is **7.3x faster** than `omop_streaming`
- `omop_streaming` is still **2.1x faster** than legacy `omop.py` (Python)
- Both new implementations are **significantly faster** than legacy code

### Configuration Details

**Test Environment:** 128 CPU cores, 1TB RAM, Linux server

**omop_streaming (39m 28s):**
```bash
--workers 128 --shards 128 --chunk_rows 10_000_000 
--merge_workers 48 --run_compression lz4 --final_compression zstd
```

**omop_refactor (5m 26s):**
```bash
--workers 8 --shards 100 --backend cpp
```

**Note:** Performance will scale with available CPU cores and RAM. These benchmarks represent optimal performance on a high-end server.

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

- **30+ passing tests**
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
- **`omop_etl_vista_config.json`** - Full OMOP CDM 5.4 with imaging/notes
- **`omop_etl_config.json`** - Standard OMOP configuration
- **`omop_etl_simple_config.json`** - Minimal configuration template

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
uv pip install -e ".[dev,cpp]"

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
│   ├── omop_refactor.py    # Production ETL (C++ backend) ⭐
│   ├── omop_streaming.py   # Development ETL (Pure Python) ⭐
│   ├── omop.py             # Legacy OMOP ETL
│   ├── mimic/              # MIMIC-IV ETL
│   └── unsorted.py         # Sorting utilities
├── tests/
│   ├── test_omop_streaming.py  # 25+ tests for streaming ETL
│   └── test_*.py           # Other test files
├── examples/
│   └── omop_*.json         # Example configurations
└── pyproject.toml          # Package configuration
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `uv run python -m pytest -v`
5. Format code: `black src/ tests/`
6. Submit a pull request

---

## 🔄 Legacy: `omop.py`

The original `omop.py` script is still available for backward compatibility but is **not recommended** for new projects. It uses a hardcoded schema (no config file) and is significantly slower than the refactored versions.

```bash
# Legacy usage (not recommended)
uv run python -m meds_etl.omop \
  /path/to/omop/data \
  /path/to/meds/output \
  --num_shards 10 \
  --num_proc 8 \
  --backend cpp \
  --omop_version 5.4
```

**Migration:** Use `omop_refactor.py` (production) or `omop_streaming.py` (development) instead.

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
