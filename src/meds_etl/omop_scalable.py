#!/usr/bin/env python3
"""
Scalable OMOP to MEDS ETL Pipeline

MEDS-compliant implementation with:
- subject_id (not person_id)
- Datetime type for timestamps
- Expanded metadata columns (not JSON strings)
- Dynamic schema from config file
- Schema-agnostic design (works for any EHR format)
- DataFrame-based processing (no dict serialization)

Code Mapping Strategies:
1. Fixed code (canonical events): "code": "MEDS_BIRTH"
2. Template (vectorized string construction): "template": "IMAGE/{modality}|{site}"
3. Source value (direct field): "source_value": {"field": "condition_source_value"}
4. Concept ID (lookup via join): "concept_id": {"concept_id_field": "...", "source_concept_id_field": "..."}

Usage:
    python omop_scalable.py \\
        --omop_dir /path/to/omop \\
        --output_dir /path/to/meds \\
        --config omop_etl_base_config.json \\
        --code_mapping source_value \\
        --shards 100 \\
        --workers 8
"""

import argparse
import hashlib
import heapq
import json
import multiprocessing as mp
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import polars as pl
except ImportError:
    print("ERROR: polars required. Install with: pip install polars")
    sys.exit(1)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("WARNING: pyarrow not available, some features may be slower")
    pq = None

try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

    # Fallback: tqdm is just a passthrough
    def tqdm(iterable, **kwargs):
        return iterable


# ============================================================================
# LOGGING UTILITIES
# ============================================================================

SEPARATOR = "━" * 60


def log_section(title: str):
    """Print a section header with separator lines."""
    #print(f"\n{SEPARATOR}")
    print(f"{title}")
    print(SEPARATOR)


def log_kv(prefix: str, key: str, value: Any, width: int = 20):
    """Print a key-value pair with consistent formatting."""
    print(f"[{prefix}] {key:<{width}}: {value}")


def log_info(prefix: str, message: str):
    """Print an info message with prefix."""
    print(f"[{prefix}] {message}")


# ============================================================================
# MEDS SCHEMA UTILITIES
# ============================================================================

# Column name mappings: OMOP → MEDS
COLUMN_RENAME_MAP = {
    "visit_occurrence_id": "visit_id",
    "unit_source_value": "unit",
    # Add more as needed
}


def get_meds_schema_from_config(config: Dict) -> Dict[str, type]:
    """
    Build MEDS schema dynamically from config file.

    Core MEDS columns:
    - subject_id: Int64
    - time: Datetime(us)
    - code: String
    - numeric_value: Float32 (nullable)
    - text_value: String (nullable)

    Metadata columns (all nullable):
    - Collected from all table/event metadata fields in config
    - Special columns: 'table' (always present), 'end' (from time_end_field)

    Args:
        config: ETL configuration dictionary

    Returns:
        Dictionary mapping column name → Polars dtype
    """
    # Core MEDS schema (required)
    schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float32,
        "text_value": pl.Utf8,
    }

    # Collect all possible metadata column names from config
    metadata_cols = set()

    # From canonical events
    for event_name, event_config in config.get("canonical_events", {}).items():
        for meta_spec in event_config.get("metadata", []):
            col_name = meta_spec["name"]
            # Apply rename mapping
            col_name = COLUMN_RENAME_MAP.get(col_name, col_name)
            metadata_cols.add(col_name)

    # From tables
    for table_name, table_config in config.get("tables", {}).items():
        for meta_spec in table_config.get("metadata", []):
            col_name = meta_spec["name"]
            # Apply rename mapping
            col_name = COLUMN_RENAME_MAP.get(col_name, col_name)
            metadata_cols.add(col_name)

    # Always include these special metadata columns
    metadata_cols.add("table")  # Table name (always present)
    metadata_cols.add("end")  # End timestamp (from time_end_field, often null)

    # Get metadata column type info from config
    col_type_info = get_metadata_column_info(config)

    # Add metadata columns to schema with proper types from config
    for col_name in sorted(metadata_cols):
        if col_name == "table":
            schema[col_name] = pl.Utf8
        elif col_name == "end":
            schema[col_name] = pl.Datetime("us")
        elif col_name in col_type_info:
            # Use type from config
            schema[col_name] = config_type_to_polars(col_type_info[col_name])
        else:
            # Default to String if not specified
            schema[col_name] = pl.Utf8

    return schema


def get_metadata_column_info(config: Dict) -> Dict[str, str]:
    """
    Extract metadata column types from config.

    Returns mapping: column_name → type_string (from config)
    """
    col_types = {}

    # From canonical events
    for event_name, event_config in config.get("canonical_events", {}).items():
        for meta_spec in event_config.get("metadata", []):
            col_name = meta_spec["name"]
            col_name = COLUMN_RENAME_MAP.get(col_name, col_name)
            col_types[col_name] = meta_spec.get("type", "string")

    # From tables
    for table_name, table_config in config.get("tables", {}).items():
        for meta_spec in table_config.get("metadata", []):
            col_name = meta_spec["name"]
            col_name = COLUMN_RENAME_MAP.get(col_name, col_name)
            col_types[col_name] = meta_spec.get("type", "string")

    return col_types


def config_type_to_polars(type_str: str) -> type:
    """Convert config type string to Polars dtype."""
    type_map = {
        "int": pl.Int64,
        "float": pl.Float64,
        "string": pl.Utf8,
        "datetime": pl.Datetime("us"),
    }
    return type_map.get(type_str.lower(), pl.Utf8)


def validate_config_against_data(omop_dir: Path, config: Dict, verbose: bool = False) -> None:
    """
    Validate that the ETL config matches the actual Parquet data schema.
    
    Reads one Parquet file per table and checks:
    - All referenced columns exist in the data
    - Metadata column types are compatible
    
    Raises SystemExit if validation fails.
    """
    all_tables = {}
    
    # Collect all tables from canonical_events
    for event_name, event_config in config.get("canonical_events", {}).items():
        table_name = event_config.get("table")
        if table_name:
            if table_name not in all_tables:
                all_tables[table_name] = {
                    "config": event_config,
                    "type": "canonical_event",
                    "name": event_name
                }
    
    # Collect all tables from tables section
    for table_name, table_config in config.get("tables", {}).items():
        if table_name not in all_tables:
            all_tables[table_name] = {
                "config": table_config,
                "type": "table",
                "name": table_name
            }
    
    if not all_tables:
        log_info("WARN", "No tables found in config to validate")
        return
    
    errors = []
    
    for table_name, table_info in all_tables.items():
        table_config = table_info["config"]
        config_type = table_info["type"]
        display_name = table_info["name"]
        
        # Find the table directory/files
        table_dir = omop_dir / table_name
        parquet_files = []
        
        if table_dir.exists() and table_dir.is_dir():
            parquet_files = list(table_dir.glob("*.parquet"))
        
        if not parquet_files:
            errors.append(f"  ✗ Table '{table_name}': No Parquet files found in {table_dir}")
            continue
        
        # Read schema from first parquet file
        try:
            sample_file = parquet_files[0]
            df_schema = pl.scan_parquet(sample_file).collect_schema()
            actual_columns = set(df_schema.keys())
            
            # Check subject_id field (canonical events use global primary_key)
            if config_type == "canonical_event":
                subject_id_field = config.get("primary_key")
            else:
                subject_id_field = table_config.get("subject_id_field")
            
            if subject_id_field and subject_id_field not in actual_columns:
                errors.append(
                    f"  ✗ Table '{table_name}': subject_id_field '{subject_id_field}' not found in data\n"
                    f"    Available columns: {sorted(actual_columns)}"
                )
            
            # Check datetime/time field (canonical events use "time_field", regular tables use "datetime_field")
            if config_type == "canonical_event":
                datetime_field = table_config.get("time_field")
                time_fallbacks = table_config.get("time_fallbacks", [])
            else:
                datetime_field = table_config.get("datetime_field")
                time_fallbacks = table_config.get("time_fallbacks", [])
            
            if datetime_field and datetime_field not in actual_columns:
                errors.append(
                    f"  ✗ Table '{table_name}': time/datetime field '{datetime_field}' not found in data\n"
                    f"    Available columns: {sorted(actual_columns)}"
                )
            
            # Check time_fallbacks fields (optional)
            for fallback_field in time_fallbacks:
                if fallback_field and fallback_field not in actual_columns:
                    errors.append(
                        f"  ✗ Table '{table_name}': time_fallback field '{fallback_field}' not found in data\n"
                        f"    Available columns: {sorted(actual_columns)}"
                    )
            
            # Check code mappings (for regular tables)
            if config_type != "canonical_event":
                code_mappings = table_config.get("code_mappings", {})
                
                # Check template mapping
                if "template" in code_mappings:
                    import re
                    template = code_mappings["template"]
                    if isinstance(template, dict):
                        template_str = template.get("format", "")
                    else:
                        template_str = template
                    
                    # Extract field references from template
                    field_refs = re.findall(r'\{([^}]+)\}', template_str)
                    for field_ref in field_refs:
                        if field_ref not in actual_columns:
                            errors.append(
                                f"  ✗ Table '{table_name}': template field '{field_ref}' not found in data\n"
                                f"    Template: {template_str}\n"
                                f"    Available columns: {sorted(actual_columns)}"
                            )
                
                # Check source_value mapping
                if "source_value" in code_mappings:
                    source_value_field = code_mappings["source_value"].get("field")
                    if source_value_field and source_value_field not in actual_columns:
                        errors.append(
                            f"  ✗ Table '{table_name}': source_value field '{source_value_field}' not found in data\n"
                            f"    Available columns: {sorted(actual_columns)}"
                        )
                
                # Check concept_id mapping
                if "concept_id" in code_mappings:
                    concept_id_config = code_mappings["concept_id"]
                    concept_id_field = concept_id_config.get("concept_id_field")
                    source_concept_id_field = concept_id_config.get("source_concept_id_field")
                    
                    if concept_id_field and concept_id_field not in actual_columns:
                        errors.append(
                            f"  ✗ Table '{table_name}': concept_id_field '{concept_id_field}' not found in data\n"
                            f"    Available columns: {sorted(actual_columns)}"
                        )
                    
                    if source_concept_id_field and source_concept_id_field not in actual_columns:
                        errors.append(
                            f"  ✗ Table '{table_name}': source_concept_id_field '{source_concept_id_field}' not found in data\n"
                            f"    Available columns: {sorted(actual_columns)}"
                        )
            
            # Check primary_key field
            primary_key = table_config.get("primary_key")
            if primary_key and primary_key not in actual_columns:
                errors.append(
                    f"  ✗ Table '{table_name}': primary_key '{primary_key}' not found in data\n"
                    f"    Available columns: {sorted(actual_columns)}"
                )
            
            # Check metadata fields
            metadata_specs = table_config.get("metadata", [])
            for meta_spec in metadata_specs:
                meta_col = meta_spec["name"]
                meta_type = meta_spec.get("type", "string").lower()
                
                if meta_col not in actual_columns:
                    errors.append(
                        f"  ✗ Table '{table_name}': metadata column '{meta_col}' not found in data\n"
                        f"    Available columns: {sorted(actual_columns)}"
                    )
                    continue
                
                # Check type compatibility
                actual_dtype = df_schema[meta_col]
                expected_polars_type = config_type_to_polars(meta_type)
                
                # Type compatibility check (allow nullability, but check base type)
                compatible = False
                if meta_type in ["int", "integer", "int64"]:
                    compatible = actual_dtype in [pl.Int64, pl.Int32, pl.Int16, pl.Int8, 
                                                   pl.UInt64, pl.UInt32, pl.UInt16, pl.UInt8]
                elif meta_type in ["float", "float64", "double"]:
                    compatible = actual_dtype in [pl.Float64, pl.Float32]
                elif meta_type in ["string", "str", "utf8"]:
                    compatible = actual_dtype in [pl.Utf8, pl.Categorical]
                elif meta_type in ["datetime", "timestamp"]:
                    compatible = isinstance(actual_dtype, pl.Datetime) or actual_dtype == pl.Date
                elif meta_type in ["date"]:
                    compatible = actual_dtype == pl.Date or isinstance(actual_dtype, pl.Datetime)
                elif meta_type in ["boolean", "bool"]:
                    compatible = actual_dtype == pl.Boolean
                else:
                    # Unknown type, be permissive
                    compatible = True
                
                if not compatible:
                    errors.append(
                        f"  ✗ Table '{table_name}': metadata column '{meta_col}' has type mismatch\n"
                        f"    Expected: {meta_type} (Polars: {expected_polars_type})\n"
                        f"    Actual: {actual_dtype}"
                    )
        
        except Exception as e:
            errors.append(f"  ✗ Table '{table_name}': Failed to read schema from {sample_file}\n    Error: {e}")
    
    if errors:
        log_info("VALIDATION", "FAILED")
        print()
        for error in errors:
            print(error)
        print()
        log_info("ERROR", "Please fix the config file and try again.")
        sys.exit(1)
    else:
        if verbose:
            log_kv("VALIDATION", "status", "PASSED")


# ============================================================================
# STAGE 0: CONCEPT MAPPING (OPTIONAL)
# ============================================================================


def build_concept_map(omop_dir: Path, verbose: bool = False, include_custom: bool = True) -> Dict[int, str]:
    """
    Build a mapping from concept_id to concept_code.

    Optionally includes custom/site-specific concepts via concept_relationship.
    """
    start_time = time.time()
    concept_map = {}

    # Look for concept table files
    concept_dir = omop_dir / "concept"
    concept_files = []

    if concept_dir.exists() and concept_dir.is_dir():
        concept_files = list(concept_dir.glob("*.csv")) + list(concept_dir.glob("*.csv.gz"))
        concept_files += list(concept_dir.glob("*.parquet"))
    else:
        for ext in [".csv", ".csv.gz", ".parquet"]:
            if (omop_dir / f"concept{ext}").exists():
                concept_files.append(omop_dir / f"concept{ext}")

    if not concept_files:
        return {}

    total_concepts = 0

    for file_path in concept_files:
        try:
            if str(file_path).endswith(".parquet"):
                df = pl.read_parquet(file_path)
            else:
                df = pl.read_csv(file_path, infer_schema_length=0)

            df = df.rename({c: c.lower() for c in df.columns})

            if "concept_id" in df.columns and "vocabulary_id" in df.columns and "concept_code" in df.columns:
                df = df.select(
                    [
                        pl.col("concept_id").cast(pl.Int64),
                        (pl.col("vocabulary_id") + pl.lit("/") + pl.col("concept_code")).alias("full_code"),
                    ]
                )

                for row in df.iter_rows():
                    concept_id, full_code = row
                    if concept_id is not None and full_code:
                        concept_map[concept_id] = full_code
                        total_concepts += 1

        except Exception as e:
            # Silently skip problematic files
            continue

    # ========== Add custom concept mappings (optional) ==========
    standard_count = len(concept_map)
    if include_custom:
        _add_custom_concept_mappings(omop_dir, concept_map, verbose)
    custom_count = len(concept_map) - standard_count

    return concept_map


def _add_custom_concept_mappings(omop_dir: Path, concept_map: Dict[int, str], verbose: bool) -> None:
    """
    Add custom/site-specific concept mappings via concept_relationship (flat, no hierarchy).

    Reads concept_relationship table and finds "Maps to" relationships where:
    - concept_id_1 > 2,000,000,000 (custom/site-specific)
    - concept_id_2 is in concept_map (standard concept)

    Adds flat mapping: concept_map[custom_id] = concept_map[standard_id]
    
    Modifies concept_map in-place.
    """
    CUSTOM_CONCEPT_ID_START = 2_000_000_000

    # Find concept_relationship files
    relationship_dir = omop_dir / "concept_relationship"
    relationship_files = []

    if relationship_dir.exists() and relationship_dir.is_dir():
        relationship_files = list(relationship_dir.glob("*.csv")) + list(relationship_dir.glob("*.csv.gz"))
        relationship_files += list(relationship_dir.glob("*.parquet"))
    else:
        for ext in [".csv", ".csv.gz", ".parquet"]:
            if (omop_dir / f"concept_relationship{ext}").exists():
                relationship_files.append(omop_dir / f"concept_relationship{ext}")

    if not relationship_files:
        return

    for file_path in relationship_files:
        try:
            if str(file_path).endswith(".parquet"):
                df = pl.read_parquet(file_path)
            else:
                df = pl.read_csv(file_path, infer_schema_length=0)

            df = df.rename({c: c.lower() for c in df.columns})

            # Filter for custom concept "Maps to" relationships
            if "concept_id_1" in df.columns and "concept_id_2" in df.columns and "relationship_id" in df.columns:
                custom_mappings = df.filter(
                    pl.col("concept_id_1").cast(pl.Int64) > CUSTOM_CONCEPT_ID_START,
                    pl.col("relationship_id") == "Maps to",
                    pl.col("concept_id_1") != pl.col("concept_id_2"),
                ).select([pl.col("concept_id_1").cast(pl.Int64), pl.col("concept_id_2").cast(pl.Int64)])

                # Add flat mappings: custom_id → standard_code
                for custom_id, standard_id in custom_mappings.iter_rows():
                    if standard_id in concept_map:
                        concept_map[custom_id] = concept_map[standard_id]

        except Exception as e:
            # Silently skip problematic files
            continue


def find_concept_id_columns_for_prescan(table_name: str, config: Dict) -> List[str]:
    """
    Find all concept_id columns for a table based on config (for pre-scanning).

    Returns list of column names to scan.
    """
    columns = []

    if table_name not in config.get("tables", {}):
        return columns

    table_config = config["tables"][table_name]
    code_mappings = table_config.get("code_mappings", {})
    concept_id_config = code_mappings.get("concept_id", {})

    if not concept_id_config:
        return columns

    # Get concept_id field
    concept_id_field = concept_id_config.get("concept_id_field", f"{table_name}_concept_id")
    columns.append(concept_id_field)

    # Get source_concept_id field
    source_concept_id_field = concept_id_config.get("source_concept_id_field")
    if source_concept_id_field:
        columns.append(source_concept_id_field)
    else:
        # Try standard pattern
        inferred_source = concept_id_field.replace("_concept_id", "_source_concept_id")
        columns.append(inferred_source)

    return columns


def fast_scan_file_for_concept_ids(file_path: Path, concept_id_columns: List[str]) -> Set[int]:
    """
    Fast scan of a single file to extract unique concept_ids.

    Uses Polars lazy evaluation to only read specified columns.
    """
    concept_ids = set()

    try:
        # Use lazy scan (doesn't load full file into memory)
        if str(file_path).endswith(".parquet"):
            lazy_df = pl.scan_parquet(file_path)
        else:
            lazy_df = pl.scan_csv(file_path, infer_schema_length=0)

        # Normalize column names
        schema = lazy_df.collect_schema()
        col_mapping = {c: c.lower() for c in schema.names()}
        lazy_df = lazy_df.rename(col_mapping)

        # Check which concept_id columns exist
        existing_cols = [col.lower() for col in concept_id_columns if col.lower() in lazy_df.collect_schema().names()]

        if not existing_cols:
            return concept_ids

        # Select only concept_id columns and get unique values
        for col in existing_cols:
            unique_values = lazy_df.select(pl.col(col).cast(pl.Int64)).unique().collect()

            # Add to set (filter out nulls)
            for row in unique_values.iter_rows():
                concept_id = row[0]
                if concept_id is not None and concept_id > 0:
                    concept_ids.add(concept_id)

    except Exception as e:
        pass  # Silently skip problematic files

    return concept_ids


def prescan_worker(args: Tuple) -> Set[int]:
    """
    Worker process to scan a batch of files for concept_ids.

    Returns:
        Set of all concept_ids found by this worker
    """
    worker_id, file_batch = args

    worker_concept_ids = set()

    for file_path, concept_id_columns in file_batch:
        concept_ids = fast_scan_file_for_concept_ids(file_path, concept_id_columns)
        worker_concept_ids.update(concept_ids)

    return worker_concept_ids


def prescan_concept_ids(omop_dir: Path, config: Dict, num_workers: int, verbose: bool = True) -> Set[int]:
    """
    Fast parallel pre-scan to collect all unique concept_ids used in OMOP data.

    Uses same greedy load balancing as main ETL.
    
    Returns:
        Set of unique concept_ids found in the data
    """
    start_time = time.time()

    # Collect files to scan
    files_to_scan = []

    for table_name, table_config in config.get("tables", {}).items():
        code_mappings = table_config.get("code_mappings", {})
        if "concept_id" not in code_mappings:
            continue

        concept_id_columns = find_concept_id_columns_for_prescan(table_name, config)
        if not concept_id_columns:
            continue

        # Find table files
        table_dir = omop_dir / table_name
        table_files = []

        if table_dir.exists() and table_dir.is_dir():
            table_files.extend(table_dir.glob("*.csv"))
            table_files.extend(table_dir.glob("*.csv.gz"))
            table_files.extend(table_dir.glob("*.parquet"))
        else:
            for ext in [".csv", ".csv.gz", ".parquet"]:
                file_path = omop_dir / f"{table_name}{ext}"
                if file_path.exists():
                    table_files.append(file_path)

        for file_path in table_files:
            files_to_scan.append((file_path, concept_id_columns))

    if not files_to_scan:
        if verbose:
            log_info("STAGE 0", "No files to scan")
        return set()

    # Greedy load balancing
    file_info = []
    for file_tuple in files_to_scan:
        file_path = file_tuple[0]
        try:
            file_size = file_path.stat().st_size
        except OSError:
            file_size = 0
        file_info.append((file_tuple, file_size))

    file_info.sort(key=lambda x: x[1], reverse=True)

    worker_loads = [[] for _ in range(num_workers)]
    worker_sizes = [0] * num_workers

    for file_tuple, size in file_info:
        min_worker = worker_sizes.index(min(worker_sizes))
        worker_loads[min_worker].append(file_tuple)
        worker_sizes[min_worker] += size

    # Create worker arguments
    worker_args = [(i, batch) for i, batch in enumerate(worker_loads)]

    # Run workers in parallel
    import multiprocessing as mp

    with mp.Pool(processes=num_workers) as pool:
        worker_results = pool.map(prescan_worker, worker_args)

    # Merge results
    all_concept_ids = set()
    for worker_set in worker_results:
        all_concept_ids.update(worker_set)

    return all_concept_ids


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def parse_compression(compression_str: str) -> Tuple[str, Optional[int]]:
    """
    Parse compression string like 'zstd:3' into (algorithm, level).
    
    Returns:
        (algorithm, level) where level is None if not specified
    
    Examples:
        'zstd' -> ('zstd', None)
        'zstd:3' -> ('zstd', 3)
        'lz4' -> ('lz4', None)
    """
    if ':' in compression_str:
        algo, level = compression_str.split(':', 1)
        return algo, int(level)
    return compression_str, None


def hash_subject_id(subject_id: str, num_shards: int) -> int:
    """Hash a subject_id to determine shard assignment."""
    hash_val = int(hashlib.md5(str(subject_id).encode()).hexdigest(), 16)
    return hash_val % num_shards


def find_omop_table_files(omop_dir: Path, table_name: str) -> List[Path]:
    """Find all files for an OMOP table (sharded or single file)."""
    files = []

    table_dir = omop_dir / table_name
    if table_dir.exists() and table_dir.is_dir():
        files.extend(table_dir.glob("*.csv"))
        files.extend(table_dir.glob("*.csv.gz"))
        files.extend(table_dir.glob("*.parquet"))
        return sorted(files)

    for ext in [".csv", ".csv.gz", ".parquet"]:
        file_path = omop_dir / f"{table_name}{ext}"
        if file_path.exists():
            files.append(file_path)

    return files


# ============================================================================
# STAGE 1: TRANSFORMATION - Generic Table → MEDS (Schema-Agnostic)
# ============================================================================


def transform_to_meds(
    df: pl.DataFrame,
    table_config: Dict,
    primary_key: str,
    meds_schema: Dict[str, type],
    concept_df: Optional[pl.DataFrame] = None,
    fixed_code: Optional[str] = None,
    table_name: Optional[str] = None,
) -> pl.DataFrame:
    """
    Transform ANY tabular data to MEDS format (schema-agnostic).
    
    Assumptions:
    - df is Parquet-backed (types are already correct)
    - Column names are lowercase
    - Config specifies extraction rules
    
    Args:
        df: Input DataFrame
        table_config: Table configuration from config file
        primary_key: Primary key column name (e.g., "person_id")
        meds_schema: Target MEDS schema (all columns with types)
        concept_df: Optional concept lookup DataFrame (concept_id → concept_code)
        fixed_code: Optional fixed code for canonical events (e.g., "MEDS_BIRTH")
        table_name: Source table name (for metadata)
    
    Returns:
        MEDS DataFrame
    """
    pk_lower = primary_key.lower()
    
    # Early return if primary key missing
    if pk_lower not in df.columns:
        return pl.DataFrame(schema=meds_schema)
    
    # Build all extraction expressions
    select_exprs = []
    
    # 1. subject_id (rename from primary key)
    select_exprs.append(pl.col(pk_lower).cast(pl.Int64).alias("subject_id"))
    
    # 2. time (with fallbacks)
    time_field = table_config.get("time_field", "").lower()
    time_fallbacks = [f.lower() for f in table_config.get("time_fallbacks", [])]
    time_candidates = [c for c in [time_field] + time_fallbacks if c and c in df.columns]
    
    if not time_candidates:
        return pl.DataFrame(schema=meds_schema)
    
    # Coalesce time fields (cast to datetime if needed)
    time_exprs = [pl.col(c).cast(pl.Datetime("us")) for c in time_candidates]
    select_exprs.append(pl.coalesce(time_exprs).alias("time"))
    
    # 3. code (four strategies)
    if fixed_code:
        # Strategy A: Fixed code for canonical events
        select_exprs.append(pl.lit(fixed_code).alias("code"))
    else:
        code_mappings = table_config.get("code_mappings", {})
        
        if "template" in code_mappings:
            # Strategy B: Template-based code construction (vectorized string operations)
            template = code_mappings["template"]
            if isinstance(template, dict):
                template_str = template.get("format", "")
            else:
                template_str = template
            
            # Parse template to extract field references: {field_name}
            import re
            field_refs = re.findall(r'\{([^}]+)\}', template_str)
            
            # Build code using Polars concat_str for performance
            if field_refs:
                # Split template into parts (literals and field references)
                parts = re.split(r'(\{[^}]+\})', template_str)
                
                # Build list of expressions
                concat_parts = []
                for part in parts:
                    if part.startswith('{') and part.endswith('}'):
                        field_name = part[1:-1].lower()
                        if field_name in df.columns:
                            # Cast to string and handle nulls
                            concat_parts.append(pl.col(field_name).cast(pl.Utf8).fill_null(""))
                        else:
                            # Missing field - use empty string
                            concat_parts.append(pl.lit(""))
                    elif part:
                        # Literal string
                        concat_parts.append(pl.lit(part))
                
                if concat_parts:
                    code_expr = pl.concat_str(concat_parts)
                    select_exprs.append(code_expr.alias("code"))
                else:
                    select_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("code"))
            else:
                # No field references, just use literal
                select_exprs.append(pl.lit(template_str).alias("code"))
        
        elif "source_value" in code_mappings:
            # Strategy C: Direct field mapping
            code_field = code_mappings["source_value"].get("field", "").lower()
            if code_field and code_field in df.columns:
                select_exprs.append(pl.col(code_field).cast(pl.Utf8).alias("code"))
            else:
                select_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("code"))
                
        elif "concept_id" in code_mappings and concept_df is not None:
            # Strategy D: Concept lookup via LEFT JOIN
            concept_config = code_mappings["concept_id"]
            source_cid_field = concept_config.get("source_concept_id_field", "").lower()
            cid_field = concept_config.get("concept_id_field", "").lower()
            
            # Coalesce: prefer source_concept_id, fallback to concept_id
            join_candidates = [c for c in [source_cid_field, cid_field] if c and c in df.columns]
            
            if join_candidates:
                cid_exprs = [pl.col(c).cast(pl.Int64) for c in join_candidates]
                df = df.with_columns(pl.coalesce(cid_exprs).alias("_cid"))
                
                # LEFT JOIN concept lookup
                df = df.join(concept_df, left_on="_cid", right_on="concept_id", how="left")
                select_exprs.append(pl.col("concept_code").alias("code"))
            else:
                select_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("code"))
        else:
            select_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("code"))

    # 4. numeric_value
    numeric_field = table_config.get("numeric_value_field", "").lower()
    if numeric_field and numeric_field in df.columns:
        select_exprs.append(pl.col(numeric_field).cast(pl.Float32).alias("numeric_value"))
    else:
        select_exprs.append(pl.lit(None, dtype=pl.Float32).alias("numeric_value"))
    
    # 5. text_value
    text_field = table_config.get("text_value_field", "").lower()
    if text_field and text_field in df.columns:
        select_exprs.append(pl.col(text_field).cast(pl.Utf8).alias("text_value"))
    else:
        select_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("text_value"))
    
    # 6. Metadata columns
    # Always add 'table'
    select_exprs.append(pl.lit(table_name or "unknown").alias("table"))
    
    # Add 'end' (time_end_field)
    time_end_field = table_config.get("time_end_field", "").lower()
    if time_end_field and time_end_field in df.columns:
        select_exprs.append(pl.col(time_end_field).cast(pl.Datetime("us")).alias("end"))
    else:
        select_exprs.append(pl.lit(None, dtype=pl.Datetime("us")).alias("end"))
    
    # Add configured metadata fields
    for meta_spec in table_config.get("metadata", []):
        meta_col = meta_spec["name"].lower()
        meta_name = COLUMN_RENAME_MAP.get(meta_spec["name"], meta_spec["name"])
        
        if meta_col in df.columns:
            meta_type = config_type_to_polars(meta_spec.get("type", "string"))
            select_exprs.append(pl.col(meta_col).cast(meta_type).alias(meta_name))
    
    # Execute transformation
    result = df.select(select_exprs)
    
    # Build final schema in single pass: add missing columns, reorder, and enforce types
    existing_cols = set(result.columns)
    final_select = []
    for col_name, col_type in meds_schema.items():
        if col_name in existing_cols:
            # Column exists - cast to correct type
            final_select.append(pl.col(col_name).cast(col_type))
        else:
            # Column missing - add as null with correct type
            final_select.append(pl.lit(None).cast(col_type).alias(col_name))
    
    result = result.select(final_select)
    
    # Filter: require subject_id, time, and code
    result = result.filter(
        pl.col("subject_id").is_not_null() 
        & pl.col("time").is_not_null() 
        & pl.col("code").is_not_null()
    )
    
    return result


def hash_subject_id_vectorized(subject_ids: pl.Series, num_shards: int) -> pl.Series:
    """
    Vectorized hash partitioning using Polars.
    
    Much faster than row-by-row Python hashing.
    """
    return subject_ids.hash(seed=0) % num_shards


def process_file(
    file_path: Path,
    table_config: Dict,
    primary_key: str,
    meds_schema: Dict[str, type],
    concept_df: Optional[pl.DataFrame] = None,
    fixed_code: Optional[str] = None,
    table_name: Optional[str] = None,
) -> pl.DataFrame:
    """
    Process a Parquet file and transform to MEDS format.
    
    Simplified: No CSV support, no chunking, no dict serialization.
    Just: read Parquet → transform → return DataFrame.
    
    Args:
        file_path: Path to Parquet file
        table_config: Table configuration
        primary_key: Primary key column name
        meds_schema: Target MEDS schema
        concept_df: Optional concept lookup DataFrame
        fixed_code: Optional fixed code for canonical events
        table_name: Source table name
    
    Returns:
        MEDS DataFrame
    """
    try:
        # Read Parquet file (lazy scan)
        df = pl.scan_parquet(file_path).collect(streaming=True)
        
        # Normalize column names to lowercase
        df = df.rename({c: c.lower() for c in df.columns})
        
        # Transform to MEDS format
        result = transform_to_meds(
            df=df,
            table_config=table_config,
            primary_key=primary_key,
            meds_schema=meds_schema,
            concept_df=concept_df,
            fixed_code=fixed_code,
            table_name=table_name,
        )
        
        return result
        
    except Exception as e:
        print(f"\nERROR processing {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame(schema=meds_schema)


# ============================================================================
# STAGE 1: PARTITION
# ============================================================================


def partition_worker(args: Tuple) -> Dict:
    """
    Worker for Stage 1 - partition into shards using DataFrame accumulation.
    
    Key improvements:
    - No dict serialization (keep DataFrames)
    - Vectorized hash partitioning
    - Memory-conscious buffering with total row tracking
    """
    (
        worker_id,
        file_batch,
        config,
        num_shards,
        rows_per_run,
        temp_dir,
        compression,
        meds_schema,
        concept_df,
        progress_counter,
    ) = args

    start_time = time.time()
    
    # Buffers: Dict[shard_id, List[DataFrame]]
    shard_buffers = {i: [] for i in range(num_shards)}
    shard_row_counts = {i: 0 for i in range(num_shards)}
    
    run_sequence = 0
    rows_processed = 0
    files_processed = 0
    total_buffered_rows = 0  # Track total across all shards

    primary_key = config["primary_key"]

    for table_name, file_path, table_config, is_canonical, event_name in file_batch:
        file_start_time = time.time()
        
        try:
            # Determine fixed_code for canonical events
            fixed_code = None
            if is_canonical:
                fixed_code = table_config.get("code", f"MEDS_{event_name.upper()}")
            
            # Process file
            df = process_file(
                file_path=file_path,
                table_config=table_config,
                primary_key=primary_key,
                meds_schema=meds_schema,
                concept_df=concept_df,
                fixed_code=fixed_code,
                table_name=table_name,
            )
            
            if len(df) == 0:
                continue
            
            rows_processed += len(df)
            
            # Hash partitioning (efficient single-pass)
            df = df.with_columns(
                hash_subject_id_vectorized(pl.col("subject_id"), num_shards).alias("_shard_id")
            )
            
            # Efficient partitioning: group by shard_id in ONE pass (not N filters!)
            for shard_id, shard_df in df.group_by("_shard_id", maintain_order=False):
                shard_id_value = shard_id[0]  # Extract scalar from tuple
                shard_df = shard_df.drop("_shard_id")
                
                shard_buffers[shard_id_value].append(shard_df)
                shard_row_counts[shard_id_value] += len(shard_df)
                total_buffered_rows += len(shard_df)
            
            # Flush if TOTAL buffered rows exceed threshold
            if total_buffered_rows >= rows_per_run:
                # Only flush shards with data (avoid iterating empty shards)
                active_shards = [sid for sid, buf in shard_buffers.items() if buf]
                
                for shard_id in active_shards:
                    flush_shard_buffer(
                        shard_id, 
                        shard_buffers[shard_id], 
                        temp_dir, 
                        worker_id, 
                        run_sequence, 
                        meds_schema, 
                        compression
                    )
                    shard_buffers[shard_id] = []
                    shard_row_counts[shard_id] = 0
                
                run_sequence += 1
                total_buffered_rows = 0
            
            files_processed += 1
                
        except Exception as e:
            if TQDM_AVAILABLE:
                tqdm.write(f"\nERROR in worker {worker_id} processing {file_path}: {e}")
            else:
                print(f"\nERROR in worker {worker_id} processing {file_path}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # ALWAYS increment progress counter
            if progress_counter is not None:
                try:
                    progress_counter.value += 1
                except:
                    pass  # Ignore errors from progress counter

    # Flush remaining buffers (only shards with data)
    active_shards = [sid for sid, buf in shard_buffers.items() if buf]
    
    for shard_id in active_shards:
        flush_shard_buffer(
            shard_id, 
            shard_buffers[shard_id], 
            temp_dir, 
            worker_id, 
            run_sequence, 
            meds_schema, 
            compression
        )

    elapsed = time.time() - start_time

    return {
        "worker_id": worker_id,
        "files_processed": files_processed,
        "rows_processed": rows_processed,
        "elapsed_sec": elapsed,
    }


def flush_shard_buffer(
    shard_id: int,
    dataframes: List[pl.DataFrame],
    temp_dir: Path,
    worker_id: int,
    run_seq: int,
    meds_schema: Dict[str, type],
    compression: str = "lz4",
):
    """
    Flush shard buffer: concat DataFrames, sort, and write to disk.
    
    Memory-efficient: concatenates and immediately writes, no dict overhead.
    """
    if not dataframes:
        return

    shard_dir = temp_dir / f"shard={shard_id}"
    shard_dir.mkdir(parents=True, exist_ok=True)

    # Concatenate all DataFrames in buffer
    df = pl.concat(dataframes, rechunk=True)

    # Sort by (subject_id, time) - required for Stage 2 k-way merge
    df = df.sort(["subject_id", "time"])

    # Write to Parquet (skip statistics for temp files - only read once by Stage 2)
    output_file = shard_dir / f"run-worker{worker_id}-{run_seq}.parquet"
    
    # Parse compression (e.g., "zstd:3" -> algo="zstd", level=3)
    algo, level = parse_compression(compression)
    if level is not None:
        df.write_parquet(output_file, compression=algo, compression_level=level, statistics=False)
    else:
        df.write_parquet(output_file, compression=algo, statistics=False)


# ============================================================================
# STAGE 2: MERGE (same k-way merge logic as v1, new schema)
# ============================================================================


class ParquetRunIterator:
    """Streaming iterator for k-way merge."""

    def __init__(self, file_path: Path, batch_size: int = 50000):
        self.file_path = file_path
        self.batch_size = batch_size

        self.lazy_df = pl.scan_parquet(file_path)
        self.total_rows = self.lazy_df.select(pl.count()).collect().item()

        self.offset = 0
        self.buffer = []
        self.buffer_index = 0

        self._load_next_batch()

    def _load_next_batch(self):
        if self.offset >= self.total_rows:
            self.buffer = []
            self.buffer_index = 0
            return

        batch_df = self.lazy_df.slice(self.offset, self.batch_size).collect()
        self.buffer = batch_df.to_dicts()
        self.buffer_index = 0
        self.offset += len(self.buffer)

    def __iter__(self):
        return self

    def __next__(self):
        if self.buffer_index >= len(self.buffer):
            self._load_next_batch()

        if not self.buffer:
            raise StopIteration

        row = self.buffer[self.buffer_index]
        self.buffer_index += 1
        return row


def kway_merge_shard(
    run_files: List[Path],
    output_file: Path,
    batch_size: int,
    meds_schema: Dict[str, type],
    compression: str,
) -> int:
    """K-way merge with MEDS schema."""
    if not run_files:
        return 0

    writer = None
    schema_pa = None
    
    # Parse compression (e.g., "zstd:3" -> algo="zstd", level=3)
    compression_algo, compression_level = parse_compression(compression)

    heap = []
    for i, file_path in enumerate(run_files):
        try:
            it = ParquetRunIterator(file_path, batch_size=50000)
            first_row = next(it)
            # Sort by (subject_id, time) - both are comparable
            sort_key = (first_row["subject_id"], first_row["time"])
            heapq.heappush(heap, (sort_key, i, first_row, it))
        except StopIteration:
            pass

    batch = []
    total_rows = 0

    while heap:
        sort_key, file_idx, row, iterator = heapq.heappop(heap)

        batch.append(row)
        total_rows += 1

        if len(batch) >= batch_size:
            if writer is None:
                # Create DataFrame WITH explicit schema_overrides
                df = pl.DataFrame(batch, schema_overrides=meds_schema)
                schema_pa = df.to_arrow().schema
                # Keep statistics=True for final output (helps downstream queries)
                if compression_level is not None:
                    writer = pq.ParquetWriter(
                        output_file, schema_pa, 
                        compression=compression_algo, 
                        compression_level=compression_level,
                        write_statistics=True
                    )
                else:
                    writer = pq.ParquetWriter(
                        output_file, schema_pa, 
                        compression=compression_algo,
                        write_statistics=True
                    )

            # Create DataFrame WITH explicit schema_overrides (fast - all dicts have same keys)
            df = pl.DataFrame(batch, schema_overrides=meds_schema)

            table = df.to_arrow()
            writer.write_table(table)
            batch = []

        try:
            next_row = next(iterator)
            next_key = (next_row["subject_id"], next_row["time"])
            heapq.heappush(heap, (next_key, file_idx, next_row, iterator))
        except StopIteration:
            pass

    if batch:
        # Create DataFrame for final batch WITH explicit schema_overrides
        df = pl.DataFrame(batch, schema_overrides=meds_schema)

        if writer is None:
            # Keep statistics for final output (no writer means this is the only batch)
            if compression_level is not None:
                df.write_parquet(
                    output_file, 
                    compression=compression_algo, 
                    compression_level=compression_level,
                    statistics=True
                )
            else:
                df.write_parquet(output_file, compression=compression_algo, statistics=True)
        else:
            table = df.to_arrow()
            writer.write_table(table)

    if writer is not None:
        writer.close()

    return total_rows


def merge_worker(args: Tuple) -> Dict:
    """Worker for Stage 2 - merge runs for a shard."""
    shard_id, temp_dir, output_dir, batch_size, meds_schema, compression = args

    start_time = time.time()

    shard_dir = temp_dir / f"shard={shard_id}"
    if not shard_dir.exists():
        return {"shard_id": shard_id, "rows": 0, "elapsed_sec": 0}

    run_files = list(shard_dir.glob("run-*.parquet"))
    if not run_files:
        return {"shard_id": shard_id, "rows": 0, "elapsed_sec": 0}

    output_shard_dir = output_dir / f"shard={shard_id}"
    output_shard_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_shard_dir / "part-0000.parquet"

    row_count = kway_merge_shard(run_files, output_file, batch_size, meds_schema, compression)

    elapsed = time.time() - start_time

    return {"shard_id": shard_id, "rows": row_count, "files_merged": len(run_files), "elapsed_sec": elapsed}


# ============================================================================
# PIPELINE ORCHESTRATION
# ============================================================================


def discover_omop_files(omop_dir: Path, config: Dict, code_mapping_type: str, strict: bool = True) -> List[Tuple]:
    """
    Discover OMOP files to process.
    
    Args:
        omop_dir: Root directory containing OMOP data
        config: ETL configuration
        code_mapping_type: Preferred code mapping type (source_value or concept_id)
        strict: If True, error on missing tables or incompatible mappings; if False, warn and skip
    
    Returns:
        List of tuples: (table_name, file_path, table_config, is_canonical, event_name)
    """
    files = []
    errors = []

    for event_name, event_config in config.get("canonical_events", {}).items():
        table_name = event_config["table"]
        table_files = find_omop_table_files(omop_dir, table_name)
        
        if not table_files:
            msg = f"Table '{table_name}' (canonical event '{event_name}'): No Parquet files found"
            if strict:
                errors.append(msg)
            else:
                print(f"WARNING: {msg}, skipping")
            continue

        for file_path in table_files:
            files.append((table_name, file_path, event_config, True, event_name))

    for table_name, table_config in config.get("tables", {}).items():
        code_mappings = table_config.get("code_mappings", {})
        
        # Check if table has ANY valid code mapping
        has_template = "template" in code_mappings
        has_source_value = "source_value" in code_mappings
        has_concept_id = "concept_id" in code_mappings
        has_requested_mapping = code_mapping_type in code_mappings
        
        # If table has no mappings at all, skip/error
        if not (has_template or has_source_value or has_concept_id):
            msg = f"Table '{table_name}': No code mappings defined (template, source_value, or concept_id)"
            if strict:
                errors.append(msg)
            else:
                print(f"WARNING: {msg}, skipping")
            continue
        
        # If concept_id mapping requested but not available, check if we have alternatives
        if code_mapping_type == "concept_id" and not has_concept_id:
            # Template or source_value can work without concept_id mapping
            if not (has_template or has_source_value):
                msg = f"Table '{table_name}': Does not have '{code_mapping_type}' mapping and no alternative (template/source_value)"
                if strict:
                    errors.append(msg)
                else:
                    print(f"WARNING: {msg}, skipping")
                continue
            else:
                # Has alternative mapping, use it
                if not strict:
                    print(f"INFO: Table '{table_name}' using alternative mapping (no concept_id available)")
        
        # Check if files exist
        table_files = find_omop_table_files(omop_dir, table_name)
        if not table_files:
            msg = f"Table '{table_name}': No Parquet files found"
            if strict:
                errors.append(msg)
            else:
                print(f"WARNING: {msg}, skipping")
            continue

        for file_path in table_files:
            files.append((table_name, file_path, table_config, False, None))
    
    # If strict mode and errors found, exit
    if strict and errors:
        log_info("ERROR", "STRICT MODE: PIPELINE CANNOT PROCEED")
        print()
        for error in errors:
            print(f"  ✗ {error}")
        print()
        log_info("ERROR", "Fix the config or data directory, or run with --no-strict to skip problematic tables.")
        sys.exit(1)

    return files


def run_pipeline(
    omop_dir: Path,
    output_dir: Path,
    config: Dict,
    code_mapping_type: str,
    num_shards: int,
    num_workers: int,
    rows_per_run: int,
    batch_size: int,
    verbose: bool,
    polars_threads: Optional[int],
    memory_limit_mb: Optional[int],
    compression: str,
    optimize_concepts: bool = True,
    skip_merge: bool = False,
    strict: bool = True,
):
    """
    Run ETL pipeline with refactored DataFrame-based processing.
    
    Key improvements:
    - DataFrame accumulation (no dict serialization)
    - Vectorized hash partitioning
    - Simplified code (schema-agnostic)
    
    Args:
        strict: If True (default), exit on missing tables or invalid config; if False, warn and skip
    """
    # Print header
    log_section("OMOP → MEDS ETL")
    
    if verbose:
        # Runtime info
        start_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        log_kv("RUN", "start_time", start_timestamp)
        log_kv("RUN", "strict_mode", "ON" if strict else "OFF")
        log_kv("RUN", "verbose", "ON" if verbose else "OFF")
        log_kv("RUN", "code_mapping", code_mapping_type)
        
        # Configuration
        log_section("[CONFIG]")
        log_kv("CONFIG", "omop_dir", omop_dir)
        log_kv("CONFIG", "output_dir", output_dir)
        log_kv("CONFIG", "config_file", config.get("_config_file", "unknown"))
        log_kv("CONFIG", "shards", f"{num_shards:,}")
        log_kv("CONFIG", "workers", num_workers)
        log_kv("CONFIG", "rows_per_run", f"{rows_per_run:,}")
        log_kv("CONFIG", "polars_thr", polars_threads if polars_threads else "auto")
        log_kv("CONFIG", "compression", compression)
    
    # Validate config against actual data schema
    if verbose:
        log_section("[VALIDATION] ETL config vs OMOP data")
    validate_config_against_data(omop_dir, config, verbose)
    
    # Build MEDS schema from config
    meds_schema = get_meds_schema_from_config(config)

    if verbose:
        log_section("[SCHEMA] MEDS")
        log_info("SCHEMA", "core_columns      : subject_id, time, code, numeric_value, text_value")
        log_kv("SCHEMA", "metadata_columns", len(meds_schema) - 5)
        log_kv("SCHEMA", "total_columns", len(meds_schema))

    # Configure threading
    cpu_count = mp.cpu_count()
    if polars_threads is None:
        polars_threads = max(1, cpu_count // num_workers)

    os.environ["POLARS_MAX_THREADS"] = str(polars_threads)

    # Memory auto-tuning
    if memory_limit_mb:
        # Calculate per-worker memory budget
        per_worker_mb = memory_limit_mb / num_workers * 0.7  # 70% usable per worker
        bytes_per_row = 1000  # Estimate: ~1KB per row with metadata
        per_worker_rows_budget = int((per_worker_mb * 1024 * 1024) / bytes_per_row)

        # rows_per_run controls TOTAL buffered rows per worker before flush
        auto_rows_per_run = int(per_worker_rows_budget * 0.8)  # 80% of per-worker budget
        auto_rows_per_run = max(10_000, min(auto_rows_per_run, 1_000_000))

        if verbose:
            log_info("MEMORY", f"Auto-tune calculation: {memory_limit_mb} MB limit / {num_workers} workers * 0.7 = {per_worker_mb:.0f} MB/worker")
            log_info("MEMORY", f"Auto-tuned rows_per_run: {auto_rows_per_run:,} (capped at 1M)")
        
        if rows_per_run != auto_rows_per_run:
            if verbose:
                log_info("MEMORY", f"Overriding --rows_per_run: {rows_per_run:,} → {auto_rows_per_run:,}")
            rows_per_run = auto_rows_per_run
    
    if verbose:
        log_section("[MEMORY]")
        log_kv("MEMORY", "memory_limit", f"{memory_limit_mb} MB" if memory_limit_mb else "none")
        log_kv("MEMORY", "rows_per_run", f"{rows_per_run:,} (buffered rows per worker before flush)")
        log_kv("MEMORY", "est_mem/worker", f"~{(rows_per_run * 1000) / 1024 / 1024:.0f} MB")
        log_kv("MEMORY", "est_mem_total", f"~{(rows_per_run * 1000 * num_workers) / 1024 / 1024:.0f} MB across {num_workers} workers")
        
        if memory_limit_mb and (rows_per_run * 1000 * num_workers) / 1024 / 1024 > memory_limit_mb:
            log_info("MEMORY", f"⚠️  WARNING: Estimated memory exceeds limit - consider reducing --workers")

    pipeline_start = time.time()

    temp_dir = output_dir / "temp"
    final_dir = output_dir / "data"
    temp_dir.mkdir(parents=True, exist_ok=True)
    final_dir.mkdir(parents=True, exist_ok=True)

    # Stage 0: Concept mapping
    concept_map = None
    concept_df = None
    if code_mapping_type == "concept_id":
        log_section("[STAGE 0] Build concept mapping")
        
        concept_map = build_concept_map(omop_dir, verbose)
        if not concept_map:
            log_info("ERROR", "concept_id mapping requested but concept table not found")
            return
        
        original_size = len(concept_map)

        # Optimize concept map by pre-scanning (default: ON)
        if optimize_concepts:
            # Pre-scan to find used concept_ids
            used_concept_ids = prescan_concept_ids(omop_dir, config, num_workers, verbose)

            # Filter concept map
            concept_map = {cid: code for cid, code in concept_map.items() if cid in used_concept_ids}
            filtered_size = len(concept_map)

            if verbose:
                reduction_pct = 100 * (1 - filtered_size / original_size) if original_size > 0 else 0
                log_kv("STAGE 0", "optimized_map", f"{original_size:,} → {filtered_size:,} concepts ({reduction_pct:.1f}% reduction)")
        else:
            filtered_size = original_size

        # Build concept DataFrame ONCE in main process (shared across all workers via copy-on-write)
        concept_df = pl.DataFrame({"concept_id": list(concept_map.keys()), "concept_code": list(concept_map.values())})
        
        if verbose:
            memory_mb = concept_df.estimated_size() / 1024 / 1024
            log_kv("STAGE 0", "concept_df_rows", f"{len(concept_df):,}")
            log_kv("STAGE 0", "concept_df_size", f"~{memory_mb:.1f} MB")
            log_kv("STAGE 0", "status", "LOADED")

    # Stage 1: Partition
    log_section("[STAGE 1] Partitioning")
    stage1_start = time.time()

    files = discover_omop_files(omop_dir, config, code_mapping_type, strict)
    
    if verbose:
        log_kv("STAGE 1", "files_to_process", len(files))

    if not files:
        msg = "No files found to process"
        if strict:
            log_info("ERROR", msg)
            log_info("ERROR", "In strict mode, this is a fatal error.")
            sys.exit(1)
        else:
            log_info("WARN", msg)
            return

    # Greedy load balancing
    file_info = [(f, f[1].stat().st_size if f[1].exists() else 0) for f in files]
    file_info.sort(key=lambda x: x[1], reverse=True)

    worker_loads = [[] for _ in range(num_workers)]
    worker_sizes = [0] * num_workers

    for file_tuple, size in file_info:
        min_worker = worker_sizes.index(min(worker_sizes))
        worker_loads[min_worker].append(file_tuple)
        worker_sizes[min_worker] += size

    # Create shared progress counter for real-time updates
    # Use Manager for macOS compatibility (spawn vs fork)
    manager = mp.Manager()
    progress_counter = manager.Value("i", 0)

    worker_args = [
        (
            i,
            batch,
            config,
            num_shards,
            rows_per_run,
            temp_dir,
            compression,
            meds_schema,
            concept_df,
            progress_counter,
        )
        for i, batch in enumerate(worker_loads)
    ]

    # Run workers with progress tracking
    if verbose:
        log_kv("STAGE 1", "workers", num_workers)
        log_kv("STAGE 1", "polars_threads", polars_threads)
        log_kv("STAGE 1", "rows_per_run", f"{rows_per_run:,}")
    print()  # Blank line before progress bar

    with mp.Pool(processes=num_workers) as pool:
        # Start workers asynchronously
        async_result = pool.map_async(partition_worker, worker_args)

        # Progress bar tracking files processed (updates in real-time)
        with tqdm(total=len(files), desc="Files processed", unit="file", smoothing=0.1) as pbar:
            last_count = 0
            check_interval = 0.5  # Check every 500ms (less contention on Manager.Value)
            timeout_counter = 0
            max_timeout_checks = 600  # 5 minutes without progress before warning
            
            while not async_result.ready():
                try:
                    # Check current progress
                    current_count = progress_counter.value
                    if current_count > last_count:
                        pbar.update(current_count - last_count)
                        last_count = current_count
                        timeout_counter = 0  # Reset timeout on progress
                    else:
                        timeout_counter += 1
                        if timeout_counter >= max_timeout_checks:
                            tqdm.write(f"\n⚠️  No progress for {timeout_counter * check_interval / 60:.1f} minutes - workers may be processing large files...")
                            timeout_counter = 0  # Reset to avoid spam
                    
                    time.sleep(check_interval)
                except:
                    # Ignore errors accessing progress counter
                    time.sleep(check_interval)

            # Get results with timeout (while progress bar is still open)
            try:
                results = async_result.get(timeout=300)  # 5 minute timeout for final collection
            except mp.TimeoutError:
                tqdm.write("\n⚠️  WARNING: Workers timed out. Results may be incomplete.")
                pool.terminate()
                pool.join()
                return
            
            # Final reconciliation: use ground truth from worker results
            # The shared counter may lag due to synchronization delays
            actual_files_processed = sum(r["files_processed"] for r in results)
            
            # Update progress bar to reflect actual completion
            if actual_files_processed > last_count:
                pbar.update(actual_files_processed - last_count)
            elif actual_files_processed < last_count:
                # This shouldn't happen, but if counter overshot, log it
                tqdm.write(f"\n⚠️  Counter mismatch: shared counter={last_count}, actual={actual_files_processed}")
            
            # Final check: ensure progress bar accurately reflects completion
            # pbar.n is the current count, pbar.total is the expected total
            if actual_files_processed == pbar.total and pbar.n < pbar.total:
                # All files processed but progress bar hasn't caught up (sync lag)
                pbar.update(pbar.total - pbar.n)
            elif actual_files_processed != pbar.total:
                # Mismatch between expected and actual - this indicates a problem
                tqdm.write(f"\n⚠️  Expected {pbar.total} files, but processed {actual_files_processed}")

    stage1_elapsed = time.time() - stage1_start
    total_rows = sum(r["rows_processed"] for r in results)

    print()  # Blank line after progress bar
    if verbose:
        log_kv("STAGE 1", "time", f"{stage1_elapsed:.2f}s")
        log_kv("STAGE 1", "file_rate", f"{len(files)/stage1_elapsed:.2f} files/s")
        log_kv("STAGE 1", "rows_processed", f"{total_rows:,}")
        log_kv("STAGE 1", "throughput", f"{total_rows/stage1_elapsed:,.0f} rows/s")
        log_kv("STAGE 1", "output_dir", temp_dir)
        log_kv("STAGE 1", "merge_stage", "SKIPPED (--skip-merge)" if skip_merge else "NEXT")

    # Stage 2: Merge (optional - can be disabled)
    if skip_merge:
        if verbose:
            log_section("[SUMMARY] OMOP → MEDS ETL (Stage 1 only)")
            log_kv("SUMMARY", "stages_run", "Stage 0 (concept map), Stage 1 (partitioning)" if code_mapping_type == "concept_id" else "Stage 1 (partitioning)")
            log_kv("SUMMARY", "stages_skipped", "Stage 2 (merge)")
            log_kv("SUMMARY", "total_rows", f"{total_rows:,}")
            log_kv("SUMMARY", "total_time", f"{stage1_elapsed:.2f}s")
            log_kv("SUMMARY", "partitioned_output", temp_dir)
            log_kv("SUMMARY", "exit_status", "SUCCESS")
        else:
            # Simple summary for non-verbose
            print(f"✅ Stage 1 complete: {total_rows:,} rows in {stage1_elapsed:.2f}s ({total_rows/stage1_elapsed:,.0f} rows/s)")
        return
    
    log_section("[STAGE 2] Merging")
    if verbose:
        log_kv("STAGE 2", "shards", num_shards)
    stage2_start = time.time()

    merge_args = [
        (shard_id, temp_dir, final_dir, batch_size, meds_schema, compression) for shard_id in range(num_shards)
    ]

    # Run merge workers with progress tracking
    print()  # Blank line before progress bar
    with mp.Pool(processes=min(num_workers, num_shards)) as pool:
        # Use imap_unordered to get results as they complete
        results_iter = pool.imap_unordered(merge_worker, merge_args)

        # Progress bar tracking shards merged
        with tqdm(total=num_shards, desc="Shards merged", unit="shard") as pbar:
            results = []
            for result in results_iter:
                results.append(result)
                pbar.update(1)

    stage2_elapsed = time.time() - stage2_start
    total_rows_merged = sum(r["rows"] for r in results)

    print()  # Blank line after progress bar
    if verbose:
        log_kv("STAGE 2", "time", f"{stage2_elapsed:.2f}s")
        log_kv("STAGE 2", "rows_merged", f"{total_rows_merged:,}")
        log_kv("STAGE 2", "throughput", f"{total_rows_merged/stage2_elapsed:,.0f} rows/s")

    pipeline_elapsed = time.time() - pipeline_start

    if verbose:
        log_section("[SUMMARY] OMOP → MEDS ETL (Complete)")
        stages_run = "Stage 1 (partitioning), Stage 2 (merge)"
        if code_mapping_type == "concept_id":
            stages_run = "Stage 0 (concept map), " + stages_run
        log_kv("SUMMARY", "stages_run", stages_run)
        log_kv("SUMMARY", "total_rows", f"{total_rows_merged:,}")
        log_kv("SUMMARY", "total_time", f"{pipeline_elapsed:.2f}s")
        log_kv("SUMMARY", "final_output", final_dir)
        log_kv("SUMMARY", "exit_status", "SUCCESS")
    else:
        # Simple summary for non-verbose
        print(f"✅ Pipeline complete: {total_rows_merged:,} rows in {pipeline_elapsed:.2f}s")

    summary = {
        "code_mapping": code_mapping_type,
        "num_shards": num_shards,
        "num_workers": num_workers,
        "total_rows": total_rows_merged,
        "total_time_sec": pipeline_elapsed,
        "schema_columns": len(meds_schema),
    }

    with open(output_dir / "etl_summary.json", "w") as f:
        json.dump(summary, f, indent=2)


# ============================================================================
# MAIN
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Scalable OMOP to MEDS ETL Pipeline (Schema-Agnostic, DataFrame-Based)"
    )

    parser.add_argument("--omop_dir", required=True, help="OMOP data directory (Parquet files)")
    parser.add_argument("--output_dir", required=True, help="Output directory")
    parser.add_argument("--config", required=True, help="ETL config JSON")
    parser.add_argument("--code_mapping", choices=["source_value", "concept_id"], required=True)
    parser.add_argument("--shards", type=int, default=100, help="Number of output shards")
    parser.add_argument("--workers", type=int, default=mp.cpu_count(), help="Number of worker processes")
    parser.add_argument("--rows_per_run", type=int, default=50_000, help="Total rows to buffer before flushing (controls memory)")
    parser.add_argument("--batch_size", type=int, default=100_000, help="Batch size for Stage 2 merge")
    parser.add_argument("--polars_threads", type=int, default=None, help="Polars threads per worker")
    parser.add_argument("--memory_limit_mb", type=int, default=None, help="Memory limit (auto-tunes rows_per_run)")
    parser.add_argument(
        "--compression", 
        default="zstd",
        help="Compression algorithm (default: zstd). Use 'zstd:3' or 'zstd:7' for compression level control"
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--no-optimize-concepts",
        dest="optimize_concepts",
        action="store_false",
        help="Disable concept map optimization (default: enabled)",
    )
    parser.add_argument(
        "--skip-merge",
        action="store_true",
        help="Skip Stage 2 (merge) - only run Stage 1 (partition)",
    )
    parser.add_argument(
        "--no-strict",
        dest="strict",
        action="store_false",
        help="Disable strict mode (default: strict=True, exits on missing tables or invalid config)",
    )

    args = parser.parse_args()

    with open(args.config, "r") as f:
        config = json.load(f)
    
    # Store config filename for logging
    config["_config_file"] = Path(args.config).name

    run_pipeline(
        omop_dir=Path(args.omop_dir),
        output_dir=Path(args.output_dir),
        config=config,
        code_mapping_type=args.code_mapping,
        num_shards=args.shards,
        num_workers=args.workers,
        rows_per_run=args.rows_per_run,
        batch_size=args.batch_size,
        verbose=args.verbose,
        polars_threads=args.polars_threads,
        memory_limit_mb=args.memory_limit_mb,
        compression=args.compression,
        optimize_concepts=args.optimize_concepts,
        skip_merge=args.skip_merge,
        strict=args.strict,
    )


if __name__ == "__main__":
    main()
