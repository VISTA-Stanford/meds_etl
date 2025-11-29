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
✅ **Ultra-Fast C++ Backend** - Optional native acceleration for production workloads (Linux, macOS)  
✅ **Production Ready** - Comprehensive test suite (30+ tests), validated on large datasets  
✅ **Flexible Code Mapping** - Template-based code generation, concept table joins, source values  
✅ **Memory Efficient** - Streaming operations, configurable memory limits, low-memory modes  
✅ **Auto-Fallback** - Automatically uses Python if C++ backend unavailable  

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

### Production Installation (with C++ Backend - Linux & macOS)

```bash
# Install with C++ backend for maximum performance
uv pip install meds_etl[cpp]
```

**Note:** The C++ backend (`meds_etl_cpp`) is available for Linux and macOS (including Apple Silicon). It provides optimized parallel processing using C++17 with Arrow-based columnar data handling. Windows users will automatically use the pure Python fallback.

### Development Installation

```bash
git clone https://github.com/Medical-Event-Data-Standard/meds_etl.git
cd meds_etl

# Install with uv (recommended)
uv pip install -e ".[tests]"

# Optional: Install C++ backend for maximum performance (Linux & macOS)
uv pip install -e ".[cpp]"
```

---

## 🏃 Quick Start

### Which Script Should I Use?

| Script | Status | Backend | Speed | Platform |
|--------|--------|---------|-------|----------|
| **`omop_refactor.py`** | ✅ **Recommended** | C++ with Python fallback | ⚡ **Fastest** | Linux, macOS (C++), Windows (Python) |
| `omop_streaming.py` | ⚠️ **Deprecated** | Pure Python (Polars) | Slow | Any |
| `omop.py` | ⚠️ **Legacy** | Python or C++ | Medium | Any |

**Use `omop_refactor.py` for all new projects.** It automatically uses the C++ backend when available and falls back to Python otherwise.

---

## 🚀 `omop_refactor.py` (Recommended)

**This is the preferred script for all OMOP → MEDS ETL pipelines.**

### Features
- ✅ **Ultra-fast C++ backend** (~7x faster than deprecated scripts)
- ✅ **Config-driven** - No code changes needed
- ✅ **Auto-fallback** - Uses Python if C++ unavailable
- ✅ **Cross-platform** - Works on Linux, macOS, Windows
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

| Script | Backend | Total Time | Stage 1 | Stage 2 | Status |
|--------|---------|------------|---------|---------|--------|
| **omop_refactor.py** | **C++** | **5m 26s** | ~3m | **2m 38s** | ✅ **Recommended** |
| omop_refactor.py | Python | ~39m | ~1m | ~38m | ✅ Auto-fallback |
| omop_streaming.py | Python | 39m 28s | 1m 20s | 38m 7s | ⚠️ Deprecated |
| omop.py | C++ | 29m 57s | N/A | N/A | ⚠️ Legacy |
| omop.py | Python | 82m 58s | N/A | N/A | ⚠️ Legacy |

**Key Insights:** 

- **omop_refactor.py with C++ backend is 7.3x faster** than deprecated Python-only scripts
- C++ backend available on Linux and macOS
- Automatic fallback to Python ensures cross-platform compatibility

---

## 📝 Configuration Guide

`omop_refactor.py` uses a flexible JSON configuration format.

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

The config controls the fallback order automatically for each table and fallback chain.

---

## ⚙️ Command-Line Reference

### `omop_refactor` (Recommended)

```bash
uv run python -m meds_etl.omop_refactor \
  --omop_dir PATH              # OMOP data directory (Parquet files)
  --output_dir PATH            # Output directory for MEDS
  --config PATH                # Config JSON file (required)
  --workers 8                  # Parallel workers
  --shards 100                 # Number of output shards
  --backend auto               # auto/cpp/polars (default: auto)
  --verbose                    # Detailed logging
```

---

## 🏗️ Architecture

### Pipeline Stages

`omop_refactor.py` uses a two-stage pipeline:

#### **Stage 1: OMOP → MEDS Unsorted**
- Parallel processing of OMOP tables
- Fast DataFrame joins for concept mapping
- Template-based code generation
- No sorting (fast!)
- Configurable via JSON

#### **Stage 2: External Sort**
- **C++ Backend (Linux, macOS):** Memory-bounded k-way merge with multi-threading
- **Python Backend (Fallback):** Polars streaming with configurable chunking
- Partitions by `subject_id % num_shards`
- Produces sorted MEDS output

### Performance Tuning

```bash
# Standard configuration (works on most systems)
uv run python -m meds_etl.omop_refactor \
  --workers 8 \
  --shards 100 \
  --backend auto

# High-performance server (128GB+ RAM)
uv run python -m meds_etl.omop_refactor \
  --workers 16 \
  --shards 100 \
  --backend cpp
```

---

## 📊 Performance Benchmarks

### Large Dataset (211K patients, 3.08B events)

**Hardware:** 128 CPU cores, 1TB RAM (Linux server)

| Script | Backend | Total Time | Notes |
|--------|---------|------------|-------|
| **omop_refactor.py** | **C++** | **5m 26s** | ⚡ **Recommended** |
| omop_refactor.py | Python | ~39m | Auto-fallback |
| omop_streaming.py | Python | 39m 28s | ⚠️ Deprecated |
| omop.py | C++ | 29m 57s | ⚠️ Legacy |
| omop.py | Python | 82m 58s | ⚠️ Legacy |

**Configuration:** `--workers 8 --shards 100 --backend auto`

**Note:** Use `omop_refactor.py` for all new projects. It's 7.3x faster with C++ and automatically falls back to Python when needed.

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
uv pip install -e ".[dev]"

# Optional: Install C++ backend for performance
uv pip install -e ".[cpp]"

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
│   ├── omop_refactor.py    # ⭐ Recommended ETL (C++ + Python)
│   ├── omop_streaming.py   # ⚠️ Deprecated (Pure Python)
│   ├── omop.py             # ⚠️ Legacy
│   ├── mimic/              # MIMIC-IV ETL
│   └── unsorted.py         # Sorting utilities
├── tests/
│   ├── test_omop_streaming.py  # 25+ tests
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

## ⚠️ Deprecated Scripts

### `omop_streaming.py` and `omop.py`

These scripts are **deprecated** and maintained only for backward compatibility. 

**Please use `omop_refactor.py` for all new projects.**

#### Why deprecated?

- **7.3x slower** than `omop_refactor.py` with C++ backend
- **Limited optimization** compared to modern implementation
- **No longer actively developed**

#### Migration

```bash
# Old (deprecated)
python -m meds_etl.omop_streaming --omop_dir ... --config ...

# New (recommended)
python -m meds_etl.omop_refactor --omop_dir ... --config ...
```

The configuration format is the same, making migration straightforward.

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
