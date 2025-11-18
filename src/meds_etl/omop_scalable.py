#!/usr/bin/env python3
"""
Scalable OMOP to MEDS ETL Pipeline v2

MEDS-compliant implementation with:
- subject_id (not person_id)
- Datetime type for timestamps
- Expanded metadata columns (not JSON strings)
- Dynamic schema from config file

Key differences from v1:
- Metadata expanded into separate nullable columns
- Column name mappings (visit_occurrence_id → visit_id, etc.)
- Proper MEDS datetime format

Usage:
    python omop_scalable_v2.py \\
        --omop_dir /path/to/omop \\
        --output_dir /path/to/meds \\
        --config omop_etl_simple_config.json \\
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


# ============================================================================
# STAGE 0: CONCEPT MAPPING (OPTIONAL)
# ============================================================================


def build_concept_map(omop_dir: Path, verbose: bool = False, include_custom: bool = True) -> Dict[int, str]:
    """
    Build a mapping from concept_id to concept_code.

    Optionally includes custom/site-specific concepts via concept_relationship.
    """
    if verbose:
        print("Building concept ID mapping from concept table...")

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
        if verbose:
            print("  WARNING: No concept table found, concept_id mapping unavailable")
        return {}

    total_concepts = 0

    # Use progress bar for concept file reading
    concept_files_iter = tqdm(concept_files, desc="  Reading concept files", disable=not verbose, unit="file")

    for file_path in concept_files_iter:
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
            if verbose:
                tqdm.write(f"  ERROR reading {file_path}: {e}")
            continue

    elapsed = time.time() - start_time
    standard_count = len(concept_map)

    if verbose:
        print(f"  Loaded {total_concepts:,} standard concepts in {elapsed:.1f}s")

    # ========== Add custom concept mappings (optional) ==========
    custom_count = 0
    if include_custom:
        custom_count = _add_custom_concept_mappings(omop_dir, concept_map, verbose)

    if verbose:
        print(f"\n  Total concepts: {len(concept_map):,}")
        if include_custom:
            print(f"    Standard: {standard_count:,}")
            print(f"    Custom: {custom_count:,}")
        print(f"  Memory: ~{len(concept_map) * 50 / 1024 / 1024:.1f} MB")

    return concept_map


def _add_custom_concept_mappings(omop_dir: Path, concept_map: Dict[int, str], verbose: bool) -> int:
    """
    Add custom/site-specific concept mappings via concept_relationship (flat, no hierarchy).

    Reads concept_relationship table and finds "Maps to" relationships where:
    - concept_id_1 > 2,000,000,000 (custom/site-specific)
    - concept_id_2 is in concept_map (standard concept)

    Adds flat mapping: concept_map[custom_id] = concept_map[standard_id]
    """
    CUSTOM_CONCEPT_ID_START = 2_000_000_000

    if verbose:
        print("  Loading custom concept mappings from concept_relationship...")

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
        if verbose:
            print("    No concept_relationship table found, skipping custom concepts")
        return 0

    custom_added = 0
    custom_start = time.time()

    # Use progress bar for relationship file reading
    relationship_files_iter = tqdm(
        relationship_files, desc="    Reading concept_relationship files", disable=not verbose, unit="file"
    )

    for file_path in relationship_files_iter:
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
                        custom_added += 1

        except Exception as e:
            if verbose:
                tqdm.write(f"    ERROR reading {file_path}: {e}")
            continue

    custom_elapsed = time.time() - custom_start

    if verbose:
        print(f"    Added {custom_added:,} custom concepts in {custom_elapsed:.1f}s")

    return custom_added


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


def prescan_worker_v2(args: Tuple) -> Set[int]:
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


def prescan_concept_ids_v2(omop_dir: Path, config: Dict, num_workers: int, verbose: bool = True) -> Set[int]:
    """
    Fast parallel pre-scan to collect all unique concept_ids used in OMOP data.

    Uses same greedy load balancing as main ETL.
    """
    # ALWAYS print pre-scan status (critical information)
    print("\n  Pre-scanning data to optimize concept map...")
    print(f"    Workers: {num_workers}")

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
        print("    No files to scan")
        return set()

    print(f"    Files to scan: {len(files_to_scan)}")

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
        worker_results = pool.map(prescan_worker_v2, worker_args)

    # Merge results
    all_concept_ids = set()
    for worker_set in worker_results:
        all_concept_ids.update(worker_set)

    elapsed = time.time() - start_time

    # ALWAYS print pre-scan results (critical information)
    print(f"    Found {len(all_concept_ids):,} unique concept_ids in {elapsed:.1f}s")

    return all_concept_ids


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


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


# Continued in next message...


# ============================================================================
# STAGE 1: TRANSFORMATION - OMOP → MEDS with Expanded Metadata
# ============================================================================


def transform_omop_to_meds_v2(
    df: pl.DataFrame,
    table_name: str,
    table_config: Dict,
    primary_key: str,
    code_mapping_type: str,
    concept_map: Optional[Dict[int, str]],
    meds_schema: Dict[str, type],
    col_types: Dict[str, str],
    concept_df: Optional[pl.DataFrame] = None,
) -> pl.DataFrame:
    """
    Transform OMOP DataFrame to MEDS format with EXPANDED METADATA COLUMNS.

    Key differences from v1:
    - Returns subject_id (not person_id)
    - Returns Datetime (not Int64 ms)
    - Returns separate metadata columns (not JSON string)

    Args:
        df: Input OMOP DataFrame (lowercase columns)
        table_name: OMOP table name
        table_config: Table configuration
        primary_key: Primary key column (person_id)
        code_mapping_type: "source_value" or "concept_id"
        concept_map: Concept mapping (if using concept_id)
        meds_schema: Full MEDS schema with all columns
        col_types: Metadata column type info from config

    Returns:
        MEDS DataFrame with all schema columns
    """
    pk_lower = primary_key.lower()

    if pk_lower not in df.columns:
        return pl.DataFrame(schema=meds_schema)

    # ===== 1. Extract subject_id (rename from person_id) =====
    # Double-cast: ensure string first, then to Int64 (handles mixed types safely)
    subject_id_expr = pl.col(pk_lower).cast(pl.Utf8, strict=False).cast(pl.Int64, strict=False).alias("subject_id")

    # ===== 2. Extract time as Datetime =====
    time_field = table_config.get("time_field", "").lower()
    time_fallbacks = [f.lower() for f in table_config.get("time_fallbacks", [])]

    time_candidates = [time_field] + time_fallbacks
    time_candidates = [c for c in time_candidates if c and c in df.columns]

    if not time_candidates:
        return pl.DataFrame(schema=meds_schema)

    # Parse as datetime directly (Polars Datetime, not ms Int64!)
    time_exprs = []
    for col in time_candidates:
        time_exprs.append(pl.col(col).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False, time_unit="us"))

    time_expr = pl.coalesce(time_exprs).alias("time")

    # ===== 3. Extract code =====
    code_mappings = table_config.get("code_mappings", {})

    if code_mapping_type == "source_value":
        source_config = code_mappings.get("source_value", {})
        code_field = source_config.get("field", "").lower()

        if code_field and code_field in df.columns:
            code_expr = pl.col(code_field).cast(pl.Utf8, strict=False).alias("code")
        else:
            code_expr = pl.lit(None).alias("code")

    elif code_mapping_type == "concept_id":
        # Map concept_id to concept_code using LEFT JOIN (fastest method!)
        if concept_df is None:
            raise ValueError("concept_id mapping requested but concept_df not provided")

        concept_config = code_mappings.get("concept_id", {})
        source_concept_id_field = concept_config.get("source_concept_id_field", "").lower()
        concept_id_field = concept_config.get("concept_id_field", "").lower()
        fallback_id = concept_config.get("fallback_concept_id")

        # Try source_concept_id first, then concept_id
        join_candidates = []
        if source_concept_id_field and source_concept_id_field in df.columns:
            join_candidates.append(source_concept_id_field)
        if concept_id_field and concept_id_field in df.columns:
            join_candidates.append(concept_id_field)

        if join_candidates:
            # Coalesce to get the best available concept_id (double-cast for safety)
            concept_id_col = pl.coalesce(
                [pl.col(c).cast(pl.Utf8, strict=False).cast(pl.Int64, strict=False) for c in join_candidates]
            ).alias("_concept_id_join")
            df = df.with_columns(concept_id_col)

            # LEFT JOIN with pre-built concept DataFrame (FAST - only ~80ms per 100K rows!)
            df = df.join(concept_df, left_on="_concept_id_join", right_on="concept_id", how="left")

            # Drop temp column if it exists
            if "_concept_id_join" in df.columns:
                df = df.drop("_concept_id_join")

            # Use the joined concept_code as code
            code_expr = pl.col("concept_code").alias("code")

            # Apply fallback for unmapped concepts
            if fallback_id and fallback_id in concept_map:
                code_expr = pl.coalesce([code_expr, pl.lit(concept_map[fallback_id])]).alias("code")
        else:
            code_expr = pl.lit(None).alias("code")
    else:
        code_expr = pl.lit(None).alias("code")

    # ===== 4. Extract numeric_value =====
    numeric_field = table_config.get("numeric_value_field", "").lower()
    if numeric_field and numeric_field in df.columns:
        # Double-cast: ensure string first, then to Float32 (handles mixed types like "%")
        numeric_expr = (
            pl.col(numeric_field).cast(pl.Utf8, strict=False).cast(pl.Float32, strict=False).alias("numeric_value")
        )
    else:
        numeric_expr = pl.lit(None, dtype=pl.Float32).alias("numeric_value")

    # ===== 5. Extract text_value =====
    text_field = table_config.get("text_value_field", "").lower()
    if text_field and text_field in df.columns:
        # Ensure string type (should already be string from CSV, but be explicit)
        text_expr = pl.col(text_field).cast(pl.Utf8, strict=False).alias("text_value")
    else:
        text_expr = pl.lit(None, dtype=pl.Utf8).alias("text_value")

    # ===== 6. Extract METADATA as SEPARATE COLUMNS =====
    metadata_exprs = []

    # Always add 'table' column
    metadata_exprs.append(pl.lit(table_name).alias("table"))

    # Add 'end' column (from time_end_field)
    time_end_field = table_config.get("time_end_field", "").lower()
    if time_end_field and time_end_field in df.columns:
        end_expr = (
            pl.col(time_end_field).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False, time_unit="us").alias("end")
        )
    else:
        end_expr = pl.lit(None, dtype=pl.Datetime("us")).alias("end")
    metadata_exprs.append(end_expr)

    # Add configured metadata fields as separate columns
    for meta_spec in table_config.get("metadata", []):
        meta_name_orig = meta_spec["name"].lower()
        meta_name_meds = COLUMN_RENAME_MAP.get(meta_spec["name"], meta_spec["name"])
        meta_type = meta_spec.get("type", "string")

        if meta_name_orig in df.columns:
            # IMPORTANT: Force to string FIRST, then cast to target type
            # This prevents Polars from inferring numeric types and failing on "%" etc.
            if meta_type == "int":
                meta_expr = pl.col(meta_name_orig).cast(pl.Utf8, strict=False).cast(pl.Int64, strict=False)
            elif meta_type == "float":
                meta_expr = pl.col(meta_name_orig).cast(pl.Utf8, strict=False).cast(pl.Float64, strict=False)
            else:
                # Already string, but ensure it
                meta_expr = pl.col(meta_name_orig).cast(pl.Utf8, strict=False)

            metadata_exprs.append(meta_expr.alias(meta_name_meds))

    # ===== 7. Build result with ALL schema columns =====
    select_exprs = [
        subject_id_expr,
        time_expr,
        code_expr,
        numeric_expr,
        text_expr,
    ] + metadata_exprs

    result_df = df.select(select_exprs)

    # ===== 8. Ensure ALL schema columns exist (fill missing with nulls) =====
    for col_name, col_type in meds_schema.items():
        if col_name not in result_df.columns:
            result_df = result_df.with_columns(pl.lit(None).cast(col_type).alias(col_name))

    # Reorder to match schema
    result_df = result_df.select(list(meds_schema.keys()))

    # ===== 9. FORCE final schema enforcement (prevents serialization errors) =====
    # Re-cast ALL columns one more time to ensure exact schema match
    final_cast_exprs = [pl.col(col_name).cast(col_type, strict=False) for col_name, col_type in meds_schema.items()]
    result_df = result_df.select(final_cast_exprs)

    return result_df


def transform_canonical_event_v2(
    df: pl.DataFrame,
    event_name: str,
    event_config: Dict,
    primary_key: str,
    meds_schema: Dict[str, type],
    col_types: Dict[str, str],
) -> pl.DataFrame:
    """
    Transform OMOP DataFrame to canonical MEDS event with expanded metadata.

    Similar to regular transform but uses fixed code from config.
    """
    pk_lower = primary_key.lower()

    if pk_lower not in df.columns:
        return pl.DataFrame(schema=meds_schema)

    # subject_id (double-cast for safety)
    subject_id_expr = pl.col(pk_lower).cast(pl.Utf8, strict=False).cast(pl.Int64, strict=False).alias("subject_id")

    # code (from config)
    code = event_config.get("code", f"MEDS_{event_name.upper()}")
    code_expr = pl.lit(code).alias("code")

    # time
    time_field = event_config.get("time_field", "").lower()
    time_fallbacks = [f.lower() for f in event_config.get("time_fallbacks", [])]

    time_candidates = [time_field] + time_fallbacks
    time_candidates = [c for c in time_candidates if c and c in df.columns]

    if not time_candidates:
        return pl.DataFrame(schema=meds_schema)

    time_exprs = []
    for col in time_candidates:
        time_exprs.append(pl.col(col).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False, time_unit="us"))

    time_expr = pl.coalesce(time_exprs).alias("time")

    # numeric_value and text_value (null for canonical events)
    numeric_expr = pl.lit(None, dtype=pl.Float32).alias("numeric_value")
    text_expr = pl.lit(None, dtype=pl.Utf8).alias("text_value")

    # Metadata
    metadata_exprs = []
    metadata_exprs.append(pl.lit(event_config.get("table", event_name)).alias("table"))
    metadata_exprs.append(pl.lit(None, dtype=pl.Datetime("us")).alias("end"))

    for meta_spec in event_config.get("metadata", []):
        meta_name_orig = meta_spec["name"].lower()
        meta_name_meds = COLUMN_RENAME_MAP.get(meta_spec["name"], meta_spec["name"])
        meta_type = meta_spec.get("type", "string")

        if meta_name_orig in df.columns:
            # IMPORTANT: Force to string FIRST, then cast to target type
            # This prevents Polars from inferring numeric types and failing on "%" etc.
            if meta_type == "int":
                meta_expr = pl.col(meta_name_orig).cast(pl.Utf8, strict=False).cast(pl.Int64, strict=False)
            elif meta_type == "float":
                meta_expr = pl.col(meta_name_orig).cast(pl.Utf8, strict=False).cast(pl.Float64, strict=False)
            else:
                # Already string, but ensure it
                meta_expr = pl.col(meta_name_orig).cast(pl.Utf8, strict=False)

            metadata_exprs.append(meta_expr.alias(meta_name_meds))

    select_exprs = [
        subject_id_expr,
        time_expr,
        code_expr,
        numeric_expr,
        text_expr,
    ] + metadata_exprs

    result_df = df.select(select_exprs)

    # Fill missing schema columns
    for col_name, col_type in meds_schema.items():
        if col_name not in result_df.columns:
            result_df = result_df.with_columns(pl.lit(None).cast(col_type).alias(col_name))

    result_df = result_df.select(list(meds_schema.keys()))

    # FORCE final schema enforcement (prevents serialization errors)
    final_cast_exprs = [pl.col(col_name).cast(col_type, strict=False) for col_name, col_type in meds_schema.items()]
    result_df = result_df.select(final_cast_exprs)

    return result_df


def process_omop_file_v2(
    file_path: Path,
    table_name: str,
    table_config: Dict,
    primary_key: str,
    code_mapping_type: str,
    concept_map: Optional[Dict[int, str]],
    meds_schema: Dict[str, type],
    col_types: Dict[str, str],
    is_canonical: bool = False,
    canonical_event_name: str = None,
    chunk_size: int = 100_000,
    concept_df: Optional[pl.DataFrame] = None,
) -> Tuple[List[Dict[str, Any]], int, int]:
    """
    Process OMOP file and extract MEDS events with expanded metadata.

    Uses streaming/chunked processing.

    Returns:
        Tuple of (events list, total_rows_input, total_rows_filtered)
    """
    all_events = []
    total_rows_before_filter = 0
    total_rows_after_filter = 0

    try:
        if str(file_path).endswith(".parquet"):
            lazy_df = pl.scan_parquet(file_path)
            lazy_df = lazy_df.rename({c: c.lower() for c in lazy_df.columns})

            df = lazy_df.collect(streaming=True)

            # CRITICAL: Force ALL columns to string type immediately after read
            # This prevents Polars from using inferred types that cause "%" errors
            string_cast_exprs = [pl.col(c).cast(pl.Utf8, strict=False) for c in df.columns]
            df = df.select(string_cast_exprs)

            total_rows = len(df)
            for start_idx in range(0, total_rows, chunk_size):
                end_idx = min(start_idx + chunk_size, total_rows)
                chunk_df = df.slice(start_idx, end_idx - start_idx)

                if is_canonical:
                    transformed = transform_canonical_event_v2(
                        chunk_df, canonical_event_name, table_config, primary_key, meds_schema, col_types
                    )
                else:
                    transformed = transform_omop_to_meds_v2(
                        chunk_df,
                        table_name,
                        table_config,
                        primary_key,
                        code_mapping_type,
                        concept_map,
                        meds_schema,
                        col_types,
                        concept_df,
                    )

                rows_before_filter = len(transformed)

                transformed = transformed.filter(
                    pl.col("subject_id").is_not_null() & pl.col("time").is_not_null() & pl.col("code").is_not_null()
                )

                rows_after_filter = len(transformed)

                total_rows_before_filter += rows_before_filter
                total_rows_after_filter += rows_after_filter

                # Force all columns to match schema types before to_dicts() (fixes mixed-type serialization errors)
                cast_exprs = []
                for col_name in transformed.columns:
                    if col_name in meds_schema:
                        cast_exprs.append(pl.col(col_name).cast(meds_schema[col_name], strict=False))
                    else:
                        cast_exprs.append(pl.col(col_name))

                if cast_exprs:
                    transformed = transformed.select(cast_exprs)

                # DEBUG: Try to serialize and catch which column fails
                try:
                    all_events.extend(transformed.to_dicts())
                except Exception as dict_err:
                    print(f"\n!!! ERROR during to_dicts() for {file_path}")
                    print(f"    Table: {table_name}, Chunk rows: {len(transformed)}")
                    print(f"    Schema: {transformed.schema}")
                    print(f"    Error: {dict_err}")
                    # Try to identify problematic column
                    for col in transformed.columns:
                        try:
                            _ = transformed.select([col]).to_dicts()
                        except Exception as col_err:
                            print(f"    ❌ Column '{col}' fails: {col_err}")
                    raise
        else:
            # Read CSV with NO schema inference - force everything to strings
            # schema_overrides with Utf8 for all columns prevents type inference errors
            reader = pl.read_csv_batched(
                file_path,
                infer_schema_length=0,  # Don't infer types
                batch_size=chunk_size,
                try_parse_dates=False,  # Don't auto-parse dates
            )

            while True:
                batch = reader.next_batches(1)
                if batch is None or len(batch) == 0:
                    break

                chunk_df = batch[0]

                # CRITICAL: Force ALL columns to string type immediately after read
                # This prevents Polars from using inferred types that cause "%" errors
                string_cast_exprs = [pl.col(c).cast(pl.Utf8, strict=False) for c in chunk_df.columns]
                chunk_df = chunk_df.select(string_cast_exprs)

                chunk_df = chunk_df.rename({c: c.lower() for c in chunk_df.columns})

                if is_canonical:
                    transformed = transform_canonical_event_v2(
                        chunk_df, canonical_event_name, table_config, primary_key, meds_schema, col_types
                    )
                else:
                    transformed = transform_omop_to_meds_v2(
                        chunk_df,
                        table_name,
                        table_config,
                        primary_key,
                        code_mapping_type,
                        concept_map,
                        meds_schema,
                        col_types,
                        concept_df,
                    )

                rows_before_filter = len(transformed)

                transformed = transformed.filter(
                    pl.col("subject_id").is_not_null() & pl.col("time").is_not_null() & pl.col("code").is_not_null()
                )

                rows_after_filter = len(transformed)

                total_rows_before_filter += rows_before_filter
                total_rows_after_filter += rows_after_filter

                # Force all columns to match schema types before to_dicts() (fixes mixed-type serialization errors)
                cast_exprs = []
                for col_name in transformed.columns:
                    if col_name in meds_schema:
                        cast_exprs.append(pl.col(col_name).cast(meds_schema[col_name], strict=False))
                    else:
                        cast_exprs.append(pl.col(col_name))

                if cast_exprs:
                    transformed = transformed.select(cast_exprs)

                # DEBUG: Try to serialize and catch which column fails
                try:
                    all_events.extend(transformed.to_dicts())
                except Exception as dict_err:
                    print(f"\n!!! ERROR during to_dicts() for {file_path}")
                    print(f"    Table: {table_name}, Chunk rows: {len(transformed)}")
                    print(f"    Schema: {transformed.schema}")
                    print(f"    Error: {dict_err}")
                    # Try to identify problematic column
                    for col in transformed.columns:
                        try:
                            _ = transformed.select([col]).to_dicts()
                        except Exception as col_err:
                            print(f"    ❌ Column '{col}' fails: {col_err}")
                    raise

    except Exception as e:
        print(f"\nERROR processing {file_path}")
        print(f"  Table: {table_name}")
        print(f"  Error: {e}")
        import traceback

        print("  Traceback:")
        traceback.print_exc()
        return [], 0, 0

    return all_events, total_rows_before_filter, total_rows_before_filter - total_rows_after_filter


# Continued with Stage 1 partitioning, Stage 2 merging, and main() ...
# (Similar structure to v1 but using meds_schema throughout)


# ============================================================================
# STAGE 1: PARTITION (same as v1 but with new schema)
# ============================================================================


def partition_worker_v2(args: Tuple) -> Dict:
    """Worker for Stage 1 - partition into shards with MEDS schema."""
    (
        worker_id,
        file_batch,
        config,
        num_shards,
        rows_per_run,
        temp_dir,
        code_mapping_type,
        concept_map,
        chunk_size,
        compression,
        meds_schema,
        col_types,
        concept_df,
        progress_counter,
    ) = args

    start_time = time.time()

    shard_buffers = {i: [] for i in range(num_shards)}
    run_sequence = 0
    rows_processed = 0
    rows_input = 0  # Total rows read from source
    rows_filtered = 0  # Rows dropped (null subject_id, time, or code)
    files_processed = 0

    primary_key = config["primary_key"]

    # concept_df is passed from main process (shared via copy-on-write on Unix/Mac)
    # No memory duplication across workers!

    for table_name, file_path, table_config, is_canonical, event_name in file_batch:
        try:
            events, file_rows_input, file_rows_filtered = process_omop_file_v2(
                file_path,
                table_name,
                table_config,
                primary_key,
                code_mapping_type,
                concept_map,
                meds_schema,
                col_types,
                is_canonical,
                event_name,
                chunk_size,
                concept_df,
            )

            # Track input vs output
            rows_input += file_rows_input
            rows_filtered += file_rows_filtered

            for event in events:
                shard_id = hash_subject_id(event["subject_id"], num_shards)
                shard_buffers[shard_id].append(event)
                rows_processed += 1

                if len(shard_buffers[shard_id]) >= rows_per_run:
                    write_shard_run_v2(
                        shard_id, shard_buffers[shard_id], temp_dir, worker_id, run_sequence, meds_schema, compression
                    )
                    shard_buffers[shard_id] = []
                    run_sequence += 1

            files_processed += 1
        except Exception as e:
            print(f"ERROR in worker {worker_id} processing {file_path}: {e}")
        finally:
            # ALWAYS increment progress counter, even on error
            if progress_counter is not None:
                # Manager.Value doesn't need get_lock() - it's automatically thread-safe
                progress_counter.value += 1

    for shard_id, buffer in shard_buffers.items():
        if buffer:
            write_shard_run_v2(shard_id, buffer, temp_dir, worker_id, run_sequence, meds_schema, compression)
            run_sequence += 1

    elapsed = time.time() - start_time

    return {
        "worker_id": worker_id,
        "files_processed": files_processed,
        "rows_processed": rows_processed,
        "rows_input": rows_input,
        "rows_filtered": rows_filtered,
        "elapsed_sec": elapsed,
    }


def normalize_events_to_schema(events: List[Dict], meds_schema: Dict[str, type]) -> List[Dict]:
    """
    Normalize event dicts to match schema (ensures all keys exist, pre-casts problematic types).

    This is CRITICAL for fast DataFrame creation:
    1. Ensures every dict has the same keys (prevents schema inference issues)
    2. Pre-casts string columns to prevent type conflicts

    Performance: O(n*m) where n=events, m=schema_cols, but very fast in practice.
    """
    if not events:
        return events

    # Identify string columns (most likely to have mixed types)
    string_cols = {col for col, dtype in meds_schema.items() if dtype == pl.Utf8}

    # Normalize each event
    for event in events:
        # 1. Fill missing keys with None (ensures all dicts have same structure)
        for col_name in meds_schema:
            if col_name not in event:
                event[col_name] = None

        # 2. Pre-cast string columns to str (prevents "%" errors)
        for col_name in string_cols:
            value = event.get(col_name)
            if value is not None and not isinstance(value, str):
                event[col_name] = str(value)

    return events


def write_shard_run_v2(
    shard_id: int,
    events: List[Dict],
    temp_dir: Path,
    worker_id: int,
    run_seq: int,
    meds_schema: Dict[str, type],
    compression: str = "lz4",
):
    """Write sorted run with MEDS schema (expanded metadata columns)."""
    if not events:
        return

    shard_dir = temp_dir / f"shard={shard_id}"
    shard_dir.mkdir(parents=True, exist_ok=True)

    # Normalize dicts: fill missing keys + pre-cast strings (prevents type inference errors)
    events = normalize_events_to_schema(events, meds_schema)

    # Create DataFrame WITH explicit schema_overrides
    # All dicts now have same keys, so Polars can build fast without full scan
    df = pl.DataFrame(events, schema_overrides=meds_schema)

    # Sort by (subject_id, time)
    df = df.sort(["subject_id", "time"])

    output_file = shard_dir / f"run-worker{worker_id}-{run_seq}.parquet"
    df.write_parquet(output_file, compression=compression)


# ============================================================================
# STAGE 2: MERGE (same k-way merge logic as v1, new schema)
# ============================================================================


class ParquetRunIteratorV2:
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


def kway_merge_shard_v2(
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

    heap = []
    for i, file_path in enumerate(run_files):
        try:
            it = ParquetRunIteratorV2(file_path, batch_size=50000)
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
            # Normalize batch to match schema
            batch = normalize_events_to_schema(batch, meds_schema)

            if writer is None:
                # Create DataFrame WITH explicit schema_overrides (fast - all dicts have same keys)
                df = pl.DataFrame(batch, schema_overrides=meds_schema)
                schema_pa = df.to_arrow().schema
                writer = pq.ParquetWriter(output_file, schema_pa, compression=compression)

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
        # Normalize final batch to match schema
        batch = normalize_events_to_schema(batch, meds_schema)

        # Create DataFrame for final batch WITH explicit schema_overrides (fast - all dicts have same keys)
        df = pl.DataFrame(batch, schema_overrides=meds_schema)

        if writer is None:
            df.write_parquet(output_file, compression=compression)
        else:
            table = df.to_arrow()
            writer.write_table(table)

    if writer is not None:
        writer.close()

    return total_rows


def merge_worker_v2(args: Tuple) -> Dict:
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

    row_count = kway_merge_shard_v2(run_files, output_file, batch_size, meds_schema, compression)

    elapsed = time.time() - start_time

    return {"shard_id": shard_id, "rows": row_count, "files_merged": len(run_files), "elapsed_sec": elapsed}


# ============================================================================
# PIPELINE ORCHESTRATION
# ============================================================================


def discover_omop_files(omop_dir: Path, config: Dict, code_mapping_type: str) -> List[Tuple]:
    """Discover OMOP files to process."""
    files = []

    for event_name, event_config in config.get("canonical_events", {}).items():
        table_name = event_config["table"]
        table_files = find_omop_table_files(omop_dir, table_name)

        for file_path in table_files:
            files.append((table_name, file_path, event_config, True, event_name))

    for table_name, table_config in config.get("tables", {}).items():
        code_mappings = table_config.get("code_mappings", {})
        if code_mapping_type not in code_mappings:
            print(f"WARNING: Table '{table_name}' does not have '{code_mapping_type}' mapping, skipping")
            continue

        table_files = find_omop_table_files(omop_dir, table_name)

        for file_path in table_files:
            files.append((table_name, file_path, table_config, False, None))

    return files


def run_pipeline_v2(
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
    chunk_size: Optional[int],
    compression: str,
    optimize_concepts: bool = True,
):
    """Run ETL pipeline v2 with MEDS schema."""
    # Build MEDS schema from config
    meds_schema = get_meds_schema_from_config(config)
    col_types = get_metadata_column_info(config)

    print(f"\n=== MEDS SCHEMA ===")
    print(f"Core columns: subject_id, time, code, numeric_value, text_value")
    print(f"Metadata columns: {len(meds_schema) - 5}")
    print(f"Total columns: {len(meds_schema)}")

    # Configure threading
    cpu_count = mp.cpu_count()
    if polars_threads is None:
        polars_threads = max(1, cpu_count // num_workers)

    os.environ["POLARS_MAX_THREADS"] = str(polars_threads)

    # Memory auto-tuning (same as v1)
    if memory_limit_mb:
        available_mb = memory_limit_mb * 0.7
        bytes_per_row = 1000
        total_rows_budget = int((available_mb * 1024 * 1024) / bytes_per_row)

        if chunk_size is None:
            chunk_size = int(total_rows_budget * 0.3)
            chunk_size = max(10000, min(chunk_size, 1000000))

        active_shards = max(1, int(num_shards * 0.2))
        auto_rows_per_run = int((total_rows_budget * 0.7) / active_shards)
        auto_rows_per_run = max(10000, min(auto_rows_per_run, 500000))

        if rows_per_run != auto_rows_per_run:
            print(f"Memory-tuned rows_per_run: {rows_per_run:,} → {auto_rows_per_run:,}")
            rows_per_run = auto_rows_per_run
    else:
        if chunk_size is None:
            chunk_size = 100000

    pipeline_start = time.time()

    temp_dir = output_dir / "temp"
    final_dir = output_dir / "data"
    temp_dir.mkdir(parents=True, exist_ok=True)
    final_dir.mkdir(parents=True, exist_ok=True)

    # Stage 0: Concept mapping
    concept_map = None
    concept_df = None
    if code_mapping_type == "concept_id":
        print("\n=== STAGE 0: BUILDING CONCEPT MAPPING ===")
        concept_map = build_concept_map(omop_dir, verbose)
        if not concept_map:
            print("ERROR: concept_id mapping requested but concept table not found")
            return
        print(f"Loaded {len(concept_map):,} concept mappings")

        # Optimize concept map by pre-scanning (default: ON)
        if optimize_concepts:
            original_size = len(concept_map)

            # Pre-scan to find used concept_ids
            used_concept_ids = prescan_concept_ids_v2(omop_dir, config, num_workers, verbose)

            # Filter concept map
            concept_map = {cid: code for cid, code in concept_map.items() if cid in used_concept_ids}
            filtered_size = len(concept_map)

            # ALWAYS print optimization stats (critical information)
            reduction_pct = 100 * (1 - filtered_size / original_size) if original_size > 0 else 0
            print(
                f"  Optimized concept map: {original_size:,} → {filtered_size:,} concepts ({reduction_pct:.1f}% reduction)"
            )

        # Build concept DataFrame ONCE in main process (shared across all workers via copy-on-write)
        print("Building shared concept DataFrame...")
        build_start = time.time()
        concept_df = pl.DataFrame({"concept_id": list(concept_map.keys()), "concept_code": list(concept_map.values())})
        build_elapsed = time.time() - build_start
        memory_mb = concept_df.estimated_size() / 1024 / 1024
        print(f"  Built concept DataFrame: {len(concept_df):,} rows, ~{memory_mb:.1f} MB, {build_elapsed:.2f}s")
        print(
            f"  Memory per worker (shared via copy-on-write): ~{memory_mb:.1f} MB (not {memory_mb * num_workers:.1f} MB!)"
        )

    # Stage 1: Partition
    print(f"\n=== STAGE 1: PARTITIONING ===")
    stage1_start = time.time()

    files = discover_omop_files(omop_dir, config, code_mapping_type)
    print(f"Found {len(files)} files to process")

    if not files:
        print("ERROR: No files found")
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
            code_mapping_type,
            concept_map,
            chunk_size,
            compression,
            meds_schema,
            col_types,
            concept_df,
            progress_counter,
        )
        for i, batch in enumerate(worker_loads)
    ]

    # Run workers with progress tracking
    print(f"\nProcessing {len(files)} files across {num_workers} workers...")

    with mp.Pool(processes=num_workers) as pool:
        # Start workers asynchronously
        async_result = pool.map_async(partition_worker_v2, worker_args)

        # Progress bar tracking files processed (updates in real-time)
        with tqdm(total=len(files), desc="Files processed", unit="file") as pbar:
            last_count = 0
            while not async_result.ready():
                # Check current progress
                current_count = progress_counter.value
                if current_count > last_count:
                    pbar.update(current_count - last_count)
                    last_count = current_count
                time.sleep(0.1)  # Check every 100ms

            # Final update
            current_count = progress_counter.value
            if current_count > last_count:
                pbar.update(current_count - last_count)

        # Get results
        results = async_result.get()

    stage1_elapsed = time.time() - stage1_start
    total_rows_output = sum(r["rows_processed"] for r in results)
    total_rows_input = sum(r.get("rows_input", 0) for r in results)
    total_rows_filtered = sum(r.get("rows_filtered", 0) for r in results)

    print(f"\nStage 1 completed:")
    print(f"  Rows output: {total_rows_output:,}")
    if total_rows_input > 0:
        filter_pct = 100 * total_rows_filtered / total_rows_input
        print(f"  Rows input: {total_rows_input:,}")
        print(f"  Rows filtered: {total_rows_filtered:,} ({filter_pct:.1f}%)")
        if code_mapping_type == "concept_id" and total_rows_filtered > 0:
            print(f"    └─ Note: Filtered rows likely unmapped concept_ids")
    print(f"  Time: {stage1_elapsed:.2f}s")
    print(f"  Throughput: {total_rows_output/stage1_elapsed:,.0f} rows/sec")

    # Stage 2: Merge
    print(f"\n=== STAGE 2: MERGING ===")
    print(f"Merging {num_shards} shards")
    stage2_start = time.time()

    merge_args = [
        (shard_id, temp_dir, final_dir, batch_size, meds_schema, compression) for shard_id in range(num_shards)
    ]

    # Run merge workers with progress tracking
    with mp.Pool(processes=min(num_workers, num_shards)) as pool:
        # Use imap_unordered to get results as they complete
        results_iter = pool.imap_unordered(merge_worker_v2, merge_args)

        # Progress bar tracking shards merged
        with tqdm(total=num_shards, desc="Shards merged", unit="shard") as pbar:
            results = []
            for result in results_iter:
                results.append(result)
                pbar.update(1)

    stage2_elapsed = time.time() - stage2_start
    total_rows = sum(r["rows"] for r in results)

    print(f"\nStage 2 completed:")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Time: {stage2_elapsed:.2f}s")
    print(f"  Throughput: {total_rows/stage2_elapsed:,.0f} rows/sec")

    pipeline_elapsed = time.time() - pipeline_start

    print(f"\n=== PIPELINE COMPLETE ===")
    print(f"Total time: {pipeline_elapsed:.2f}s")
    print(f"Output: {final_dir}")

    summary = {
        "code_mapping": code_mapping_type,
        "num_shards": num_shards,
        "num_workers": num_workers,
        "total_rows": total_rows,
        "total_time_sec": pipeline_elapsed,
        "schema_columns": len(meds_schema),
    }

    with open(output_dir / "etl_summary.json", "w") as f:
        json.dump(summary, f, indent=2)


# ============================================================================
# MAIN
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="Scalable OMOP to MEDS ETL Pipeline v2 (MEDS-compliant)")

    parser.add_argument("--omop_dir", required=True, help="OMOP data directory")
    parser.add_argument("--output_dir", required=True, help="Output directory")
    parser.add_argument("--config", required=True, help="ETL config JSON")
    parser.add_argument("--code_mapping", choices=["source_value", "concept_id"], required=True)
    parser.add_argument("--shards", type=int, default=100)
    parser.add_argument("--workers", type=int, default=mp.cpu_count())
    parser.add_argument("--rows_per_run", type=int, default=500000)
    parser.add_argument("--batch_size", type=int, default=100000)
    parser.add_argument("--polars_threads", type=int, default=None)
    parser.add_argument("--memory_limit_mb", type=int, default=None)
    parser.add_argument("--chunk_size", type=int, default=None)
    parser.add_argument("--compression", choices=["lz4", "zstd", "snappy", "gzip"], default="lz4")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--no-optimize-concepts",
        dest="optimize_concepts",
        action="store_false",
        help="Disable concept map optimization (default: enabled for faster joins)",
    )

    args = parser.parse_args()

    print("=== SCALABLE OMOP TO MEDS ETL v2 ===")
    print(f"OMOP directory: {args.omop_dir}")
    print(f"Output directory: {args.output_dir}")
    print(f"Config: {args.config}")

    with open(args.config, "r") as f:
        config = json.load(f)

    run_pipeline_v2(
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
        chunk_size=args.chunk_size,
        compression=args.compression,
        optimize_concepts=args.optimize_concepts,
    )


if __name__ == "__main__":
    main()
