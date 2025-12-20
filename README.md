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
  --config examples/omop_etl_vista_std_concepts.json\
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

The `$omop:` prefix triggers a join with the OMOP `concept` table, producing codes in `vocabulary_id/concept_code` format.

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
  url = {https://github.com/Medical-Event-Data-Standard/meds_etl}
}
```

## Links

[MEDS Standard](https://github.com/Medical-Event-Data-Standard/meds) · [Issues](https://github.com/Medical-Event-Data-Standard/meds_etl/issues) · [License](LICENSE)
