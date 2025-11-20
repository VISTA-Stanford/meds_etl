"""
Refactored OMOP to MEDS ETL pipeline.

Combines best practices from omop.py and omop_scalable.py:
- Config validation from omop_scalable.py
- Fast DataFrame joins without sorting from omop_scalable.py
- MEDS Unsorted output format from omop.py
- Support for both meds_etl_cpp and pure Python backends

Architecture:
  Stage 1: OMOP → MEDS Unsorted (fast, parallel, no sorting)
  Stage 2: MEDS Unsorted → MEDS (via meds_etl_cpp or unsorted.sort())
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import pickle
import shutil
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

import meds
import meds_etl
import meds_etl.unsorted

# ============================================================================
# SCHEMA UTILITIES
# ============================================================================


def config_type_to_polars(type_str: str) -> type:
    """Convert config type string to Polars dtype."""
    type_map = {
        "int": pl.Int64,
        "integer": pl.Int64,
        "int64": pl.Int64,
        "float": pl.Float64,
        "float64": pl.Float64,
        "double": pl.Float64,
        "string": pl.Utf8,
        "str": pl.Utf8,
        "utf8": pl.Utf8,
        "datetime": pl.Datetime("us"),
        "timestamp": pl.Datetime("us"),
        "date": pl.Date,
        "boolean": pl.Boolean,
        "bool": pl.Boolean,
    }
    return type_map.get(type_str.lower(), pl.Utf8)


def get_metadata_column_info(config: Dict) -> Dict[str, str]:
    """
    Extract metadata column types from config.
    
    Returns mapping: column_name → type_string (from config)
    
    Validates that columns with the same name have consistent types across all tables.
    """
    col_types = {}
    col_sources = {}  # Track where each type came from for error messages
    
    # From canonical events
    for event_name, event_config in config.get("canonical_events", {}).items():
        for meta_spec in event_config.get("metadata", []):
            col_name = meta_spec["name"]
            col_type = meta_spec.get("type", "string")
            
            if col_name in col_types:
                # Check consistency
                if col_types[col_name] != col_type:
                    raise ValueError(
                        f"Metadata column '{col_name}' has inconsistent types in config:\n"
                        f"  {col_sources[col_name]}: {col_types[col_name]}\n"
                        f"  canonical_event '{event_name}': {col_type}\n"
                        f"All instances of the same column must have the same type!"
                    )
            else:
                col_types[col_name] = col_type
                col_sources[col_name] = f"canonical_event '{event_name}'"
    
    # From tables
    for table_name, table_config in config.get("tables", {}).items():
        for meta_spec in table_config.get("metadata", []):
            col_name = meta_spec["name"]
            col_type = meta_spec.get("type", "string")
            
            if col_name in col_types:
                # Check consistency
                if col_types[col_name] != col_type:
                    raise ValueError(
                        f"Metadata column '{col_name}' has inconsistent types in config:\n"
                        f"  {col_sources[col_name]}: {col_types[col_name]}\n"
                        f"  table '{table_name}': {col_type}\n"
                        f"All instances of the same column must have the same type!"
                    )
            else:
                col_types[col_name] = col_type
                col_sources[col_name] = f"table '{table_name}'"
    
    return col_types


def get_meds_schema_from_config(config: Dict) -> Dict[str, type]:
    """
    Build global MEDS schema from config.
    
    Core MEDS columns (FIXED):
    - subject_id: Int64
    - time: Datetime(us)
    - code: Utf8
    - numeric_value: Float64 (nullable)
    - text_value: Utf8 (nullable)
    - end: Datetime(us) (nullable)
    
    Plus all metadata columns from config with consistent types.
    
    This ensures ALL output files have the same schema.
    """
    # Core MEDS schema (FIXED - do not change these!)
    schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float64,  # FIXED: Always Float64
        "text_value": pl.Utf8,
        "end": pl.Datetime("us"),  # Optional end time
    }
    
    # Get metadata column type info from config (validates consistency)
    col_type_info = get_metadata_column_info(config)
    
    # Add metadata columns to schema with proper types from config
    for col_name, type_str in sorted(col_type_info.items()):
        schema[col_name] = config_type_to_polars(type_str)
    
    return schema


# ============================================================================
# CONFIGURATION VALIDATION
# ============================================================================


def validate_config_against_data(omop_dir: Path, config: Dict, code_mapping_choice: str, verbose: bool = False) -> None:
    """
    Validate ETL configuration against actual OMOP data schema.
    
    Ensures all configured fields exist in the data with correct types.
    Also checks that the chosen code mapping method is defined for all tables.
    
    Args:
        code_mapping_choice: One of "concept_id", "source_value", "template"
    
    Exits with error if validation fails.
    """
    if verbose:
        print("\n" + "=" * 70)
        print("VALIDATING ETL CONFIG AGAINST DATA SCHEMA")
        print("=" * 70)
    
    errors = []
    tables_to_check = {}
    
    # Collect all tables from canonical_events
    for event_name, event_config in config.get("canonical_events", {}).items():
        table_name = event_config["table"]
        if table_name not in tables_to_check:
            tables_to_check[table_name] = []
        tables_to_check[table_name].append(event_config)
    
    # Collect all tables from tables section
    for table_name, table_config in config.get("tables", {}).items():
        if table_name not in tables_to_check:
            tables_to_check[table_name] = []
        tables_to_check[table_name].append(table_config)
    
    # Validate each table
    for table_name, configs in tables_to_check.items():
        table_dir = omop_dir / table_name
        
        # Find first parquet file
        sample_file = None
        if table_dir.is_dir():
            parquet_files = list(table_dir.glob("*.parquet"))
            if parquet_files:
                sample_file = parquet_files[0]
        elif (omop_dir / f"{table_name}.parquet").exists():
            sample_file = omop_dir / f"{table_name}.parquet"
        
        if not sample_file:
            errors.append(f"Table '{table_name}': No Parquet files found")
            continue
        
        # Read schema
        try:
            df_schema = pl.scan_parquet(sample_file).collect_schema()
        except Exception as e:
            errors.append(f"Table '{table_name}': Error reading schema: {e}")
            continue
        
        # Validate each config for this table
        for table_config in configs:
            # Check subject_id_field
            subject_id_field = table_config.get("subject_id_field", config.get("primary_key", "person_id"))
            if subject_id_field not in df_schema.names():
                errors.append(
                    f"Table '{table_name}': subject_id_field '{subject_id_field}' not found. "
                    f"Available: {df_schema.names()}"
                )
            
            # Check time_start field (support both old and new naming)
            time_start_field = table_config.get("time_start") or table_config.get("time_field") or table_config.get("datetime_field")
            if time_start_field and time_start_field not in df_schema.names():
                errors.append(f"Table '{table_name}': time_start field '{time_start_field}' not found")
            
            # Check time_start_fallbacks
            time_start_fallbacks = table_config.get("time_start_fallbacks") or table_config.get("time_fallbacks", [])
            for fallback in time_start_fallbacks:
                if fallback and fallback not in df_schema.names():
                    errors.append(f"Table '{table_name}': time_start_fallback '{fallback}' not found")
            
            # Check time_end field (optional)
            time_end_field = table_config.get("time_end") or table_config.get("time_end_field")
            if time_end_field and time_end_field not in df_schema.names():
                errors.append(f"Table '{table_name}': time_end field '{time_end_field}' not found")
            
            # Check time_end_fallbacks (optional)
            time_end_fallbacks = table_config.get("time_end_fallbacks", [])
            for fallback in time_end_fallbacks:
                if fallback and fallback not in df_schema.names():
                    errors.append(f"Table '{table_name}': time_end_fallback '{fallback}' not found")
            
            # Check code mappings
            code_mappings = table_config.get("code_mappings", {})
            is_canonical = "code" in table_config  # Canonical events have fixed codes
            
            # Ensure the chosen code mapping method is defined (unless it's a canonical event)
            if not is_canonical and code_mapping_choice not in code_mappings:
                errors.append(
                    f"Table '{table_name}': Code mapping method '{code_mapping_choice}' not defined in config. "
                    f"Available: {list(code_mappings.keys())}"
                )
            
            # Validate fields for source_value mapping
            if "source_value" in code_mappings:
                code_field = code_mappings["source_value"].get("field")
                if code_field and code_field not in df_schema.names():
                    errors.append(f"Table '{table_name}': source_value field '{code_field}' not found")
            
            # Validate fields for concept_id mapping
            if "concept_id" in code_mappings:
                concept_id_field = code_mappings["concept_id"].get("concept_id_field")
                if concept_id_field and concept_id_field not in df_schema.names():
                    errors.append(f"Table '{table_name}': concept_id_field '{concept_id_field}' not found")
                
                source_concept_id_field = code_mappings["concept_id"].get("source_concept_id_field")
                if source_concept_id_field and source_concept_id_field not in df_schema.names():
                    errors.append(
                        f"Table '{table_name}': source_concept_id_field '{source_concept_id_field}' not found"
                    )
            
            # Validate fields for template mapping
            if "template" in code_mappings:
                template = code_mappings["template"]
                # Extract field references like {field_name}
                import re
                field_refs = re.findall(r'\{(\w+)\}', template)
                for field in field_refs:
                    if field not in df_schema.names():
                        errors.append(
                            f"Table '{table_name}': template field '{field}' not found in data"
                        )
            
            # Check metadata columns
            for meta_spec in table_config.get("metadata", []):
                meta_name = meta_spec.get("name")
                if meta_name and meta_name not in df_schema.names():
                    errors.append(f"Table '{table_name}': metadata column '{meta_name}' not found")
    
    if errors:
        print("\n❌ VALIDATION FAILED\n")
        print("The following errors were found:\n")
        for error in errors:
            print(f"  ✗ {error}")
        print()
        sys.exit(1)
    
    if verbose:
        print(f"\n✅ VALIDATION PASSED ({len(tables_to_check)} tables validated)")


# ============================================================================
# CONCEPT MAP BUILDING
# ============================================================================


def build_concept_map(omop_dir: Path, verbose: bool = False) -> Tuple[pl.DataFrame, Dict[int, Any]]:
    """
    Build concept ID → concept code mapping from OMOP concept table.
    
    Returns:
        concept_df: Polars DataFrame with [concept_id, code] columns
        code_metadata: Dict[code -> metadata]
    """
    if verbose:
        print("\n" + "=" * 70)
        print("BUILDING CONCEPT MAP")
        print("=" * 70)
    
    code_metadata = {}
    concept_dfs = []
    
    # Find concept files
    concept_dir = omop_dir / "concept"
    concept_files = []
    if concept_dir.is_dir():
        concept_files = list(concept_dir.glob("*.parquet"))
    elif (omop_dir / "concept.parquet").exists():
        concept_files = [omop_dir / "concept.parquet"]
    
    if not concept_files:
        print("⚠️  No concept files found - concept mapping will be unavailable")
        return pl.DataFrame(schema={"concept_id": pl.Int64, "code": pl.Utf8}), {}
    
    # Load concepts as DataFrames (keep as DF for fast joins!)
    for concept_file in tqdm(concept_files, desc="Loading concepts"):
        df = pl.read_parquet(concept_file)
        
        # Build DataFrame with concept_id -> code mapping
        concept_df = df.select(
            concept_id=pl.col("concept_id").cast(pl.Int64),
            code=pl.col("vocabulary_id") + "/" + pl.col("concept_code"),
            name=pl.col("concept_name"),
        )
        
        concept_dfs.append(concept_df)
        
        # Build code metadata for custom concepts (concept_id > 2B) 
        custom_df = concept_df.filter(pl.col("concept_id") > 2_000_000_000)
        for row in custom_df.iter_rows(named=True):
            code_metadata[row["code"]] = {
                "code": row["code"],
                "description": row["name"],
                "parent_codes": [],
            }
    
    # Concatenate all concept DataFrames
    concept_df_combined = pl.concat(concept_dfs, rechunk=True)
    
    # Load concept relationships (for custom concept mappings)
    rel_dir = omop_dir / "concept_relationship"
    rel_files = []
    if rel_dir.is_dir():
        rel_files = list(rel_dir.glob("*.parquet"))
    elif (omop_dir / "concept_relationship.parquet").exists():
        rel_files = [omop_dir / "concept_relationship.parquet"]
    
    if rel_files:
        # Build temp dict for relationships
        concept_id_to_code = dict(zip(
            concept_df_combined["concept_id"].to_list(),
            concept_df_combined["code"].to_list()
        ))
        
        for rel_file in tqdm(rel_files, desc="Loading concept relationships"):
            df = pl.read_parquet(rel_file)
            
            # Find "Maps to" relationships for custom concepts
            custom_rels = df.filter(
                (pl.col("concept_id_1") > 2_000_000_000) &
                (pl.col("relationship_id") == "Maps to") &
                (pl.col("concept_id_1") != pl.col("concept_id_2"))
            ).select(
                concept_id_1=pl.col("concept_id_1").cast(pl.Int64),
                concept_id_2=pl.col("concept_id_2").cast(pl.Int64),
            ).to_dict(as_series=False)
            
            for cid1, cid2 in zip(custom_rels["concept_id_1"], custom_rels["concept_id_2"]):
                if cid1 in concept_id_to_code and cid2 in concept_id_to_code:
                    code1 = concept_id_to_code[cid1]
                    code2 = concept_id_to_code[cid2]
                    if code1 in code_metadata:
                        code_metadata[code1]["parent_codes"].append(code2)
    
    if verbose:
        print(f"\n  Total concepts: {len(concept_df_combined):,}")
        print(f"  Custom concepts: {len(code_metadata):,}")
    
    # Keep only concept_id and code columns for joining
    return concept_df_combined.select(["concept_id", "code"]), code_metadata


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
    
    return columns


def fast_scan_file_for_concept_ids(file_path: Path, concept_id_columns: List[str]) -> Set[int]:
    """
    Fast scan of a single file to extract unique concept_ids.
    
    Uses Polars lazy evaluation to only read specified columns.
    """
    concept_ids = set()
    
    try:
        # Use lazy scan (doesn't load full file into memory)
        lazy_df = pl.scan_parquet(file_path)
        
        # Check which concept_id columns exist
        schema = lazy_df.collect_schema()
        existing_cols = [col for col in concept_id_columns if col in schema.names()]
        
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
    
    except Exception:
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


def prescan_concept_ids(omop_dir: Path, config: Dict, num_workers: int, verbose: bool = False) -> Set[int]:
    """
    Fast parallel pre-scan to collect all unique concept_ids used in OMOP data.
    
    Uses greedy load balancing for optimal worker distribution.
    
    Returns:
        Set of unique concept_ids found in the data
    """
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
        table_files = find_omop_table_files(omop_dir, table_name)
        
        for file_path in table_files:
            files_to_scan.append((file_path, concept_id_columns))
    
    if not files_to_scan:
        if verbose:
            print("  No files to scan for concept optimization")
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
    
    if verbose:
        print(f"  Pre-scanned {len(files_to_scan)} files, found {len(all_concept_ids):,} unique concept_ids")
    
    return all_concept_ids


# ============================================================================
# FILE DISCOVERY
# ============================================================================


def find_omop_table_files(omop_dir: Path, table_name: str) -> List[Path]:
    """Find all Parquet files for an OMOP table."""
    table_dir = omop_dir / table_name
    
    if table_dir.is_dir():
        return sorted(table_dir.glob("*.parquet"))
    elif (omop_dir / f"{table_name}.parquet").exists():
        return [omop_dir / f"{table_name}.parquet"]
    else:
        return []


# ============================================================================
# FAST TRANSFORM (NO SORTING)
# ============================================================================


def transform_to_meds_unsorted(
    df: pl.DataFrame,
    table_config: Dict,
    primary_key: str,
    code_mapping_choice: str,
    meds_schema: Dict[str, type],
    concept_df: Optional[pl.DataFrame] = None,
    fixed_code: Optional[str] = None,
) -> pl.DataFrame:
    """
    Transform OMOP DataFrame to MEDS Unsorted format.
    
    Fast transformation using vectorized operations, no sorting.
    Uses DataFrame joins for concept mapping (100x faster than dict replace).
    Produces output compatible with meds_etl_cpp or unsorted.sort().
    
    Args:
        code_mapping_choice: One of "concept_id", "source_value", "template"
        meds_schema: Global MEDS schema (ensures consistent types across all files)
    """
    # 1. subject_id
    subject_id_field = table_config.get("subject_id_field", primary_key)
    subject_id = pl.col(subject_id_field).cast(pl.Int64).alias("subject_id")
    
    # 2. time (from time_start field in config) → "time" column in MEDS
    # Support both old naming (time_field, datetime_field) and new naming (time_start)
    time_start_field = table_config.get("time_start") or table_config.get("time_field") or table_config.get("datetime_field")
    time_start_fallbacks = table_config.get("time_start_fallbacks") or table_config.get("time_fallbacks", [])
    
    time_options = []
    if time_start_field:
        time_options.append(pl.col(time_start_field).cast(pl.Datetime("us")))
    for fallback in time_start_fallbacks:
        time_options.append(pl.col(fallback).cast(pl.Datetime("us")))
    
    if not time_options:
        raise ValueError(f"No time_start field configured for table")
    
    time = pl.coalesce(time_options).alias("time") if len(time_options) > 1 else time_options[0].alias("time")
    
    # 3. Build base expressions
    code_mappings = table_config.get("code_mappings", {})
    base_exprs = [subject_id, time]
    
    # Use the specified code mapping method (from CLI argument)
    needs_concept_join = False
    
    if fixed_code:
        # Canonical event with fixed code (e.g., "MEDS_BIRTH")
        base_exprs.append(pl.lit(fixed_code).alias("code"))
    
    elif code_mapping_choice == "template":
        # Template-based code generation
        if "template" not in code_mappings:
            raise ValueError(f"Template mapping requested but not defined in config")
        
        template = code_mappings["template"]
        import re
        parts = re.split(r'(\{[^}]+\})', template)
        
        exprs = []
        for part in parts:
            if part.startswith('{') and part.endswith('}'):
                field = part[1:-1]
                exprs.append(pl.col(field).cast(pl.Utf8).fill_null(""))
            elif part:
                exprs.append(pl.lit(part))
        
        base_exprs.append(pl.concat_str(exprs).alias("code"))
    
    elif code_mapping_choice == "source_value":
        # Direct source value
        if "source_value" not in code_mappings:
            raise ValueError(f"source_value mapping requested but not defined in config")
        
        code_field = code_mappings["source_value"].get("field")
        if code_field:
            base_exprs.append(pl.col(code_field).cast(pl.Utf8).alias("code"))
        else:
            raise ValueError(f"source_value mapping missing 'field' key")
    
    elif code_mapping_choice == "concept_id":
        # Concept ID mapping - will do join later
        if "concept_id" not in code_mappings:
            raise ValueError(f"concept_id mapping requested but not defined in config")
        
        concept_config = code_mappings["concept_id"]
        concept_id_field = concept_config.get("concept_id_field")
        source_concept_id_field = concept_config.get("source_concept_id_field")
        fallback_concept_id = concept_config.get("fallback_concept_id")
        
        # Check if concept_id fields are actually defined
        if not concept_id_field and not source_concept_id_field:
            # No fields defined - use fallback_concept_id if provided
            if fallback_concept_id is not None:
                # Use fallback as literal concept_id, will be joined to get code
                if concept_df is None or len(concept_df) == 0:
                    raise ValueError(f"fallback_concept_id specified but concept_df not provided")
                
                needs_concept_join = True
                base_exprs.append(pl.lit(fallback_concept_id).cast(pl.Int64).alias("concept_id"))
            else:
                raise ValueError(
                    f"concept_id mapping requested but no concept_id_field, "
                    f"source_concept_id_field, or fallback_concept_id defined"
                )
        else:
            # Standard concept_id mapping with fields
            if concept_df is None or len(concept_df) == 0:
                raise ValueError(f"concept_id mapping requested but concept_df not provided")
            
            needs_concept_join = True
            
            # Build expression with fallback chain: source_concept_id → concept_id → fallback
            exprs_to_coalesce = []
            if source_concept_id_field:
                exprs_to_coalesce.append(pl.col(source_concept_id_field).cast(pl.Int64))
            if concept_id_field:
                exprs_to_coalesce.append(pl.col(concept_id_field).cast(pl.Int64))
            if fallback_concept_id is not None:
                exprs_to_coalesce.append(pl.lit(fallback_concept_id).cast(pl.Int64))
            
            if len(exprs_to_coalesce) > 1:
                base_exprs.append(pl.coalesce(exprs_to_coalesce).alias("concept_id"))
            else:
                base_exprs.append(exprs_to_coalesce[0].alias("concept_id"))
    
    else:
        raise ValueError(f"Unknown code_mapping_choice: {code_mapping_choice}")
    
    # 4. numeric_value and text_value
    numeric_value_field = table_config.get("numeric_value_field")
    text_value_field = table_config.get("text_value_field")
    
    if numeric_value_field:
        base_exprs.append(pl.col(numeric_value_field).cast(pl.Float64).alias("numeric_value"))
    else:
        base_exprs.append(pl.lit(None, dtype=pl.Float64).alias("numeric_value"))
    
    if text_value_field:
        base_exprs.append(pl.col(text_value_field).cast(pl.Utf8).alias("text_value"))
    else:
        base_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("text_value"))
    
    # 5. end time (optional, with fallbacks)
    # Support both old naming (time_end_field) and new naming (time_end)
    time_end_field = table_config.get("time_end") or table_config.get("time_end_field")
    time_end_fallbacks = table_config.get("time_end_fallbacks", [])
    
    end_options = []
    if time_end_field:
        end_options.append(pl.col(time_end_field).cast(pl.Datetime("us")))
    for fallback in time_end_fallbacks:
        end_options.append(pl.col(fallback).cast(pl.Datetime("us")))
    
    if end_options:
        end_expr = pl.coalesce(end_options).alias("end") if len(end_options) > 1 else end_options[0].alias("end")
        base_exprs.append(end_expr)
    else:
        base_exprs.append(pl.lit(None, dtype=pl.Datetime("us")).alias("end"))
    
    # 6. metadata columns (use config_type_to_polars for consistency)
    for meta_spec in table_config.get("metadata", []):
        meta_name = meta_spec["name"]
        meta_type = meta_spec.get("type", "string")
        meta_polars_type = config_type_to_polars(meta_type)
        base_exprs.append(pl.col(meta_name).cast(meta_polars_type).alias(meta_name))
    
    # Build base DataFrame
    result = df.select(base_exprs)
    
    # Perform concept join if needed (FAST!)
    if needs_concept_join:
        result = result.join(concept_df, on="concept_id", how="left").drop("concept_id")
    
    # Filter out rows with null codes
    result = result.filter(pl.col("code").is_not_null())
    
    # 7. Enforce global schema: add missing columns, reorder, and cast to correct types
    # This ensures ALL files have EXACTLY the same schema (critical for meds_etl.unsorted.sort)
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
    
    return result


# ============================================================================
# WORKER FUNCTIONS
# ============================================================================


def process_omop_file_worker(args: Tuple) -> Dict:
    """
    Worker function to process a single OMOP file.
    
    Reads OMOP Parquet → transforms to MEDS Unsorted → writes output.
    NO sorting, NO sharding (done later by meds_etl_cpp or unsorted.sort).
    """
    (
        file_path,
        table_name,
        table_config,
        primary_key,
        code_mapping_choice,
        output_dir,
        meds_schema,
        concept_df_data,
        is_canonical,
        fixed_code,
    ) = args
    
    # Deserialize concept DataFrame
    concept_df = pickle.loads(concept_df_data) if concept_df_data else None
    concept_df_size = len(concept_df) if concept_df is not None else 0
    
    try:
        # Read OMOP file
        df = pl.read_parquet(file_path)
        input_rows = len(df)
        
        if input_rows == 0:
            return {
                "file": file_path.name,
                "table": table_name,
                "input_rows": 0,
                "output_rows": 0,
                "filtered_rows": 0,
                "concept_df_size": concept_df_size,
                "success": True,
            }
        
        # Transform to MEDS Unsorted (with fast DataFrame join!)
        meds_df = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key=primary_key,
            code_mapping_choice=code_mapping_choice,
            meds_schema=meds_schema,
            concept_df=concept_df,
            fixed_code=fixed_code,
        )
        
        output_rows = len(meds_df)
        filtered_rows = input_rows - output_rows
        
        # ALWAYS write the file for debugging, even if empty
        output_file = output_dir / f"{table_name}_{uuid.uuid4()}.parquet"
        meds_df.write_parquet(output_file, compression="lz4")
        
        return {
            "file": file_path.name,
            "table": table_name,
            "input_rows": input_rows,
            "output_rows": output_rows,
            "filtered_rows": filtered_rows,
            "concept_df_size": concept_df_size,
            "has_concept_mapping": "concept_id" in table_config.get("code_mappings", {}),
            "success": True,
        }
    
    except Exception as e:
        import traceback
        return {
            "file": file_path.name,
            "table": table_name,
            "input_rows": 0,
            "output_rows": 0,
            "filtered_rows": 0,
            "concept_df_size": concept_df_size,
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
        }


# ============================================================================
# MAIN ETL PIPELINE
# ============================================================================


def run_omop_to_meds_etl(
    omop_dir: Path,
    output_dir: Path,
    config_path: Path,
    num_workers: int = 8,
    num_shards: Optional[int] = None,
    backend: str = "polars",
    code_mapping_mode: str = "auto",
    verbose: bool = False,
    optimize_concepts: bool = True,
):
    """
    Run OMOP to MEDS ETL pipeline.
    
    Architecture (same as omop.py):
    
    Stage 1: OMOP → MEDS Unsorted (Python/Polars)
        - Fast DataFrame joins to create MEDS schema
        - Global schema enforcement (consistent types)
        - Writes unsorted Parquet files to temp/unsorted_data/
        - 1:1 with input files (no sorting, no sharding)
        - Compressed with lz4 (fast writes)
    
    Stage 2: External Sort (meds_etl_cpp or Python fallback)
        - Partitions by subject_id hash into shards
        - Sorts each shard by (subject_id, time, code)
        - Memory-bounded (meds_etl_cpp uses optimized C++ k-way merge)
        - Multi-threaded (1 thread per shard)
        - Writes final data/{shard_id}.parquet files
    
    Args:
        num_shards: Number of output shards (default: from config or 100)
        code_mapping_mode: Code mapping strategy: 'auto', 'concept_id', 'source_value', or 'template'
        optimize_concepts: If True, pre-scan data to optimize concept map (99% memory reduction)
        backend: Legacy parameter, ignored (Stage 2 always uses external sort)
    
    Recommended: Install meds_etl_cpp for memory-bounded Stage 2 sorting.
    """
    print("\n" + "=" * 70)
    print("OMOP → MEDS ETL PIPELINE (REFACTORED)")
    print("=" * 70)
    
    # Load config
    with open(config_path, "r") as f:
        config = json.load(f)
    
    primary_key = config.get("primary_key", "person_id")
    
    # Setup directories
    output_dir = Path(output_dir)
    temp_dir = output_dir / "temp"
    unsorted_dir = temp_dir / "unsorted_data"
    metadata_dir = temp_dir / "metadata"
    final_dir = output_dir / "data"
    
    # Clean and create directories
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)
    unsorted_dir.mkdir()
    metadata_dir.mkdir()
    
    # Determine actual code mapping choice
    if code_mapping_mode == "auto":
        # Try concept_id first, fall back to source_value
        code_mapping_choice = "concept_id"  # Will validate later if it exists
    else:
        code_mapping_choice = code_mapping_mode
    
    # Validate config (including that chosen code mapping exists for all tables)
    validate_config_against_data(omop_dir, config, code_mapping_choice, verbose=verbose)
    
    # Build concept map (as DataFrame for fast joins!) - only if needed
    concept_df = pl.DataFrame(schema={"concept_id": pl.Int64, "code": pl.Utf8})  # Empty default
    code_metadata = {}
    
    if code_mapping_choice == "concept_id":
        print(f"\n{'=' * 70}")
        print("BUILDING CONCEPT MAP")
        print(f"{'=' * 70}")
        
        concept_df, code_metadata = build_concept_map(omop_dir, verbose=verbose)
        
        if len(concept_df) == 0:
            print(f"\n❌ ERROR: concept_id mapping requested but concept table not found!")
            print(f"   Use --code_mapping source_value or template instead.")
            sys.exit(1)
        else:
            print(f"  ✅ Loaded {len(concept_df):,} concepts")
            
            # Optimize concept map by pre-scanning (99% memory reduction!)
            if optimize_concepts:
                if verbose:
                    print(f"\n📊 Optimizing concept map (pre-scanning data to find used concepts)...")
                
                original_size = len(concept_df)
                
                # Pre-scan to find which concept_ids are actually used
                used_concept_ids = prescan_concept_ids(omop_dir, config, num_workers, verbose=verbose)
                
                if used_concept_ids:
                    # Filter concept DataFrame to only used concepts
                    concept_df = concept_df.filter(pl.col("concept_id").is_in(list(used_concept_ids)))
                    filtered_size = len(concept_df)
                    
                    if verbose:
                        reduction_pct = 100 * (1 - filtered_size / original_size) if original_size > 0 else 0
                        memory_before_mb = original_size * 100 / 1024 / 1024  # Rough estimate
                        memory_after_mb = filtered_size * 100 / 1024 / 1024
                        print(f"  ✅ Optimized: {original_size:,} → {filtered_size:,} concepts ({reduction_pct:.1f}% reduction)")
                        print(f"  💾 Memory: ~{memory_before_mb:.1f} MB → ~{memory_after_mb:.1f} MB")
    else:
        print(f"\n📋 Code mapping mode: {code_mapping_choice} (skipping concept map)")
    
    concept_df_data = pickle.dumps(concept_df)
    
    # Save metadata
    dataset_metadata = {
        "dataset_name": "OMOP",
        "dataset_version": time.strftime("%Y-%m-%d"),
        "etl_name": "meds_etl.omop_refactor",
        "etl_version": meds_etl.__version__,
        "meds_version": meds.__version__,
    }
    
    with open(metadata_dir / "dataset.json", "w") as f:
        json.dump(dataset_metadata, f, indent=2)
    
    if code_metadata:
        table = pa.Table.from_pylist(list(code_metadata.values()), meds.code_metadata_schema())
        pq.write_table(table, metadata_dir / "codes.parquet")
    
    # Copy metadata to final location
    final_metadata_dir = output_dir / "metadata"
    if final_metadata_dir.exists():
        shutil.rmtree(final_metadata_dir)
    shutil.copytree(metadata_dir, final_metadata_dir)
    
    # ========================================================================
    # STAGE 1: OMOP → MEDS Unsorted
    # ========================================================================
    
    print("\n" + "=" * 70)
    print("STAGE 1: OMOP → MEDS UNSORTED")
    print("=" * 70)
    
    # Build global MEDS schema (ensures consistent types across all files)
    meds_schema = get_meds_schema_from_config(config)
    if verbose:
        print(f"\n📋 Global MEDS schema:")
        print(f"  Core columns: subject_id, time, code, numeric_value, text_value, end")
        print(f"  Metadata columns: {len(meds_schema) - 6}")
    
    # Collect all files to process
    tasks = []
    
    # Process canonical events
    for event_name, event_config in config.get("canonical_events", {}).items():
        table_name = event_config["table"]
        files = find_omop_table_files(omop_dir, table_name)
        
        fixed_code = event_config.get("code", f"MEDS_{event_name.upper()}")
        
        for file_path in files:
            tasks.append((
                file_path,
                table_name,
                event_config,
                primary_key,
                code_mapping_choice,
                unsorted_dir,
                meds_schema,
                concept_df_data,
                True,  # is_canonical
                fixed_code,
            ))
    
    # Process regular tables
    for table_name, table_config in config.get("tables", {}).items():
        files = find_omop_table_files(omop_dir, table_name)
        
        for file_path in files:
            tasks.append((
                file_path,
                table_name,
                table_config,
                primary_key,
                code_mapping_choice,
                unsorted_dir,
                meds_schema,
                concept_df_data,
                False,  # is_canonical
                None,  # fixed_code
            ))
    
    print(f"\nProcessing {len(tasks)} files with {num_workers} workers...")
    
    # DEBUG: Process first file only for testing
    if verbose and len(tasks) > 0:
        print(f"\n🔍 DEBUG: Processing first file to test...")
        test_result = process_omop_file_worker(tasks[0])
        print(f"   Test result: {test_result}")
        if test_result['success']:
            print(f"   ✅ First file processed successfully")
            print(f"   Input: {test_result['input_rows']:,} rows")
            print(f"   Output: {test_result['output_rows']:,} rows")
            print(f"   Concept DF size: {test_result.get('concept_df_size', 0):,}")
        else:
            print(f"   ❌ First file failed: {test_result.get('error', 'Unknown')}")
        print()
    
    # Process in parallel
    stage1_start = time.time()
    
    if num_workers > 1:
        os.environ["POLARS_MAX_THREADS"] = "1"
        with mp.Pool(processes=num_workers) as pool:
            results = list(tqdm(
                pool.imap_unordered(process_omop_file_worker, tasks),
                total=len(tasks),
                desc="Processing OMOP files",
            ))
    else:
        results = []
        for task in tqdm(tasks, desc="Processing OMOP files"):
            results.append(process_omop_file_worker(task))
    
    stage1_elapsed = time.time() - stage1_start
    
    # Report results
    successes = [r for r in results if r["success"]]
    failures = [r for r in results if not r["success"]]
    total_input_rows = sum(r.get("input_rows", 0) for r in successes)
    total_output_rows = sum(r.get("output_rows", 0) for r in successes)
    total_filtered_rows = sum(r.get("filtered_rows", 0) for r in successes)
    
    print(f"\n{'=' * 70}")
    print("STAGE 1 RESULTS")
    print(f"{'=' * 70}")
    print(f"Files processed:  {len(results)}")
    print(f"Input rows:       {total_input_rows:,}")
    print(f"Output rows:      {total_output_rows:,}")
    print(f"Filtered rows:    {total_filtered_rows:,}")
    
    if total_input_rows > 0:
        retention_pct = 100 * total_output_rows / total_input_rows
        print(f"Retention:        {retention_pct:.1f}%")
    
    print(f"Time:             {stage1_elapsed:.2f}s")
    
    # Detailed breakdown by table
    print(f"\n📊 Per-table breakdown:")
    by_table = {}
    for r in successes:
        table = r.get("table", "unknown")
        if table not in by_table:
            by_table[table] = {
                "files": 0,
                "input": 0,
                "output": 0,
                "filtered": 0,
                "has_concept_mapping": r.get("has_concept_mapping", False),
            }
        by_table[table]["files"] += 1
        by_table[table]["input"] += r.get("input_rows", 0)
        by_table[table]["output"] += r.get("output_rows", 0)
        by_table[table]["filtered"] += r.get("filtered_rows", 0)
    
    # Check concept DF size from first result
    concept_df_size = successes[0].get("concept_df_size", 0) if successes else 0
    print(f"   Concept DataFrame size: {concept_df_size:,} concepts")
    print()
    
    for table, stats in sorted(by_table.items(), key=lambda x: x[1]["input"], reverse=True):
        retention = 100 * stats['output'] / stats['input'] if stats['input'] > 0 else 0
        mapping = "✓ concept_id" if stats['has_concept_mapping'] else "  source/template"
        print(f"   {table:20s} [{mapping}]  {stats['files']:3d} files  {stats['input']:10,} → {stats['output']:10,} rows ({retention:5.1f}%)")
    
    if failures:
        print(f"\n⚠️  {len(failures)} files failed:")
        for f in failures[:10]:  # Show first 10
            print(f"   - {f.get('table', 'unknown'):20s} / {f['file']}: {f.get('error', 'Unknown error')}")
            if "traceback" in f and verbose:
                print(f"     {f['traceback']}")
    
    # Check what was actually written to disk
    written_files = list(unsorted_dir.glob("*.parquet"))
    print(f"\n💾 Files written to {unsorted_dir}:")
    print(f"   Total files: {len(written_files)}")
    if len(written_files) > 0:
        total_size = sum(f.stat().st_size for f in written_files) / 1024 / 1024
        print(f"   Total size:  {total_size:.1f} MB")
        print(f"   Files: {[f.name for f in written_files[:5]]}")
        if len(written_files) > 5:
            print(f"          ... and {len(written_files) - 5} more")
    else:
        print(f"   ⚠️  NO FILES WRITTEN!")
    
    # ========================================================================
    # STAGE 2: External sort (partition + sort via meds_etl_cpp or Python)
    # ========================================================================
    
    print("\n" + "=" * 70)
    print("STAGE 2: EXTERNAL SORT (partition + sort)")
    print("=" * 70)
    
    stage2_start = time.time()
    
    # Determine number of shards (priority: CLI arg > config > default)
    if num_shards is None:
        num_shards = config.get("num_shards", 100)
    
    if verbose:
        print(f"\n📊 Configuration:")
        print(f"  Shards: {num_shards}")
        print(f"  Workers: {num_workers}")
    
    # Determine which backend to use
    use_cpp = False
    
    if backend in ["cpp", "auto"]:
        try:
            import meds_etl_cpp
            use_cpp = True
        except ImportError:
            if backend == "cpp":
                print(f"\n❌ ERROR: meds_etl_cpp not available but --backend cpp was specified")
                sys.exit(1)
            elif backend == "auto":
                print(f"\n⚠️  meds_etl_cpp not available, using Python fallback...")
    
    if use_cpp:
        print(f"\n✅ Using meds_etl_cpp (C++) for external sort...")
        print(f"   Memory-bounded, multi-threaded, optimized k-way merge")
        
        import meds_etl_cpp
        meds_etl_cpp.perform_etl(
            str(temp_dir),  # Source: temp/ with metadata/ and unsorted_data/
            str(output_dir / "result"),  # Target
            num_shards,
            num_workers,
        )
        
        print(f"   ✅ C++ external sort complete")
    else:
        print(f"\n⚠️  Using Python/Polars for sorting...")
        print(f"   Warning: Python sorting loads entire shards into memory!")
        print(f"   For large datasets, install meds_etl_cpp for memory-bounded sorting.")
        
        # Python fallback (loads shards into memory - not ideal for large data)
        meds_etl.unsorted.sort(
            source_unsorted_path=str(temp_dir),
            target_meds_path=str(output_dir / "result"),
            num_shards=num_shards,
            num_proc=num_workers,
            backend="polars",
        )
    
    # Move final data to output
    result_data_dir = output_dir / "result" / "data"
    if result_data_dir.exists():
        if final_dir.exists():
            shutil.rmtree(final_dir)
        shutil.move(str(result_data_dir), str(final_dir))
        shutil.rmtree(output_dir / "result")
    
    stage2_elapsed = time.time() - stage2_start
    
    if verbose:
        print(f"\n📊 Stage 2 Results:")
        print(f"  Time: {stage2_elapsed:.2f}s")
    
    print(f"\n✅ Stage 2 complete: {stage2_elapsed:.2f}s")
    
    # Cleanup
    print(f"\nCleaning up temporary directory...")
    shutil.rmtree(temp_dir)
    
    total_elapsed = stage1_elapsed + stage2_elapsed
    
    print("\n" + "=" * 70)
    print("ETL COMPLETE")
    print("=" * 70)
    print(f"Total time:   {total_elapsed:.2f}s")
    print(f"Total rows:   {total_output_rows:,}")
    if total_output_rows > 0:
        print(f"Throughput:   {total_output_rows/total_elapsed:,.0f} rows/s")
    print(f"Output dir:   {final_dir}")
    print("=" * 70)


# ============================================================================
# CLI
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="OMOP to MEDS ETL Pipeline (Refactored)"
    )
    
    parser.add_argument(
        "--omop_dir",
        required=True,
        help="Path to OMOP data directory (Parquet files)",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Output directory for MEDS data",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to ETL config JSON file",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=mp.cpu_count(),
        help="Number of worker processes (default: all CPUs)",
    )
    parser.add_argument(
        "--shards",
        type=int,
        default=None,
        help="Number of output shards (default: from config or 100)",
    )
    parser.add_argument(
        "--backend",
        choices=["cpp", "polars", "auto"],
        default="auto",
        help="Stage 2 backend: 'auto' (try cpp, fallback polars), 'cpp' (meds_etl_cpp only), 'polars' (Python only). Default: auto",
    )
    parser.add_argument(
        "--code_mapping",
        choices=["auto", "concept_id", "source_value", "template"],
        default="auto",
        help="Code mapping strategy (default: auto - use what's in config)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--no-optimize-concepts",
        dest="optimize_concepts",
        action="store_false",
        help="Disable concept map optimization (default: enabled)",
    )
    
    args = parser.parse_args()
    
    run_omop_to_meds_etl(
        omop_dir=Path(args.omop_dir),
        output_dir=Path(args.output_dir),
        config_path=Path(args.config),
        num_workers=args.workers,
        num_shards=args.shards,
        backend=args.backend,
        code_mapping_mode=args.code_mapping,
        verbose=args.verbose,
        optimize_concepts=args.optimize_concepts,
    )


if __name__ == "__main__":
    main()

