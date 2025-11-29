"""
OMOP to MEDS ETL with Polars Streaming External Sort

This version uses Polars 1.x streaming engine for Stage 2 (external sort):
- Stage 1: OMOP → MEDS Unsorted (same as omop_refactor.py)
- Stage 2: Streaming external sort using Polars lazy evaluation + sink_parquet

Key advantage: Polars' native k-way merge is faster than manual implementations,
and uses bounded memory through streaming.

Based on external_sort.py approach.
"""

import gc
import os
import shutil

# Import Stage 1 from omop_refactor.py
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))

import json
import pickle
import re
import time
import uuid

from tqdm import tqdm

# ============================================================================
# SCHEMA UTILITIES (from omop_refactor.py)
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

    Returns mapping: output_column_name → type_string (from config)

    Handles aliasing: uses "alias" if present, otherwise uses "name"

    Validates that columns with the same name have consistent types across all tables.

    Supports both "properties" (preferred) and "metadata" (backwards compat) keys.
    """
    col_types = {}
    col_sources = {}  # Track where each type came from for error messages

    # From canonical events
    for event_name, event_config in config.get("canonical_events", {}).items():
        # Support both "properties" and "metadata" for backwards compatibility
        properties = event_config.get("properties") or event_config.get("metadata", [])

        for meta_spec in properties:
            # Output column name (use alias if present, else use name)
            col_name = meta_spec.get("alias", meta_spec["name"])
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
        # Support both "properties" and "metadata" for backwards compatibility
        properties = table_config.get("properties") or table_config.get("metadata", [])

        for meta_spec in properties:
            # Output column name (use alias if present, else use name)
            col_name = meta_spec.get("alias", meta_spec["name"])
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
# CONFIGURATION VALIDATION (from omop_refactor.py)
# ============================================================================


def validate_config_against_data(omop_dir: Path, config: Dict, verbose: bool = False) -> None:
    """
    Validate ETL configuration against actual OMOP data schema.

    Ensures all configured fields exist in the data with correct types.
    Also checks that the chosen code mapping method is defined for all tables.

    Exits with error if validation fails.
    """
    if verbose:
        print("\n" + "=" * 70)
        print("VALIDATING ETL CONFIG AGAINST DATA SCHEMA")
        print("=" * 70)

    errors = []
    tables_to_check = {}

    # Collect all tables from canonical_events
    for _event_name, event_config in config.get("canonical_events", {}).items():
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
            time_start_field = (
                table_config.get("time_start") or table_config.get("time_field") or table_config.get("datetime_field")
            )
            if time_start_field:
                # Check if it's new template syntax
                if any(marker in time_start_field for marker in ["@", "$", "{", "||"]):
                    # Extract column names from template
                    from meds_etl.config_integration import extract_column_names_from_template

                    column_names = extract_column_names_from_template(time_start_field)
                    for col_name in column_names:
                        if col_name not in df_schema.names():
                            errors.append(
                                f"Table '{table_name}': time_start template references column '{col_name}' which was not found"
                            )
                else:
                    # Old format - direct column name
                    if time_start_field not in df_schema.names():
                        errors.append(f"Table '{table_name}': time_start field '{time_start_field}' not found")

            # Check time_start_fallbacks
            time_start_fallbacks = table_config.get("time_start_fallbacks") or table_config.get("time_fallbacks", [])
            for fallback in time_start_fallbacks:
                if fallback and fallback not in df_schema.names():
                    errors.append(f"Table '{table_name}': time_start_fallback '{fallback}' not found")

            # Check time_end field (optional)
            time_end_field = table_config.get("time_end") or table_config.get("time_end_field")
            if time_end_field:
                # Check if it's new template syntax
                if any(marker in time_end_field for marker in ["@", "$", "{", "||"]):
                    # Extract column names from template
                    from meds_etl.config_integration import extract_column_names_from_template

                    column_names = extract_column_names_from_template(time_end_field)
                    for col_name in column_names:
                        if col_name not in df_schema.names():
                            errors.append(
                                f"Table '{table_name}': time_end template references column '{col_name}' which was not found"
                            )
                else:
                    # Old format - direct column name
                    if time_end_field not in df_schema.names():
                        errors.append(f"Table '{table_name}': time_end field '{time_end_field}' not found")

            # Check time_end_fallbacks (optional)
            time_end_fallbacks = table_config.get("time_end_fallbacks", [])
            for fallback in time_end_fallbacks:
                if fallback and fallback not in df_schema.names():
                    errors.append(f"Table '{table_name}': time_end_fallback '{fallback}' not found")

            # Check code mappings
            code_mappings = table_config.get("code_mappings", {})
            is_canonical = "code" in table_config  # Canonical events have fixed codes

            # Ensure SOME code mapping method is defined (unless it's a canonical event)
            # After compilation, different tables may have different mapping types
            if not is_canonical and not code_mappings:
                errors.append(f"Table '{table_name}': No code mappings defined in config.")

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

            # Validate fields for template mapping (can be in source_value or concept_id)
            for mapping_type in ["source_value", "concept_id"]:
                if mapping_type in code_mappings:
                    mapping_config = code_mappings[mapping_type]
                    if isinstance(mapping_config, dict) and "template" in mapping_config:
                        template = mapping_config["template"]
                        # Extract field references like {field_name}
                        field_refs = re.findall(r"\{(\w+)\}", template)
                        for field in field_refs:
                            if field not in df_schema.names():
                                errors.append(
                                    f"Table '{table_name}': {mapping_type} template field '{field}' not found in data"
                                )

            # Check metadata/properties columns (support both names)
            for meta_spec in table_config.get("metadata", []) + table_config.get("properties", []):
                # Check for new "value" field (templated format)
                if "value" in meta_spec:
                    value = meta_spec["value"]
                    if isinstance(value, str) and any(marker in value for marker in ["@", "$", "{", "||"]):
                        # Extract column names from template
                        from meds_etl.config_integration import extract_column_names_from_template

                        column_names = extract_column_names_from_template(value)
                        for col_name in column_names:
                            if col_name not in df_schema.names():
                                errors.append(
                                    f"Table '{table_name}': property/metadata '{meta_spec.get('name')}' template references column '{col_name}' which was not found"
                                )
                # After compilation: check actual source fields
                elif "literal" not in meta_spec:
                    # Check the actual source column, not the output name
                    if "concept_lookup_field" in meta_spec:
                        # Concept lookup - validate the concept_id field exists
                        source_col = meta_spec["concept_lookup_field"]
                        if source_col not in df_schema.names():
                            errors.append(
                                f"Table '{table_name}': property/metadata '{meta_spec.get('name')}' concept lookup field '{source_col}' not found"
                            )
                    elif "source" in meta_spec:
                        # Simple column alias - validate source exists
                        source_col = meta_spec["source"]
                        if source_col not in df_schema.names():
                            errors.append(
                                f"Table '{table_name}': property/metadata '{meta_spec.get('name')}' source column '{source_col}' not found"
                            )
                    else:
                        # No source/concept_lookup_field - assume name is the source column (old format)
                        meta_name = meta_spec.get("name")
                        if meta_name and meta_name not in df_schema.names():
                            errors.append(f"Table '{table_name}': metadata/property column '{meta_name}' not found")

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
# CODE MAPPING HELPERS
# ============================================================================


def table_requires_concept_lookup(table_config: Dict[str, Any]) -> bool:
    """Determine if a table configuration needs concept table lookups."""
    properties = table_config.get("properties") or table_config.get("metadata", [])
    for prop in properties:
        if "concept_lookup_field" in prop:
            return True

    code_mappings = table_config.get("code_mappings", {})
    concept_config = code_mappings.get("concept_id")
    if not concept_config:
        return False

    if "template" in concept_config:
        return bool(concept_config.get("concept_fields"))

    return any(key in concept_config for key in ("concept_id_field", "source_concept_id_field", "fallback_concept_id"))


def config_requires_concept_lookup(config: Dict[str, Any]) -> bool:
    """Check whether any table or canonical event requires concept lookups."""
    for section in ("canonical_events", "tables"):
        for table_config in config.get(section, {}).values():
            if table_requires_concept_lookup(table_config):
                return True
    return False


def describe_code_mapping(table_config: Dict[str, Any], is_canonical: bool, fixed_code: Optional[str]) -> str:
    """Provide a short description of the code mapping strategy for reporting."""
    if is_canonical or fixed_code:
        return "fixed_code"

    code_mappings = table_config.get("code_mappings", {})
    if not code_mappings:
        return "unknown"

    parts = []
    if "concept_id" in code_mappings:
        parts.append("concept_id")
    if "source_value" in code_mappings:
        parts.append("source_value")

    return "+".join(parts) if parts else "unknown"


# ============================================================================
# CONCEPT MAP BUILDING (from omop_refactor.py)
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
        concept_id_to_code = dict(
            zip(concept_df_combined["concept_id"].to_list(), concept_df_combined["code"].to_list(), strict=False)
        )

        for rel_file in tqdm(rel_files, desc="Loading concept relationships"):
            df = pl.read_parquet(rel_file)

            # Find "Maps to" relationships for custom concepts
            custom_rels = (
                df.filter(
                    (pl.col("concept_id_1") > 2_000_000_000)
                    & (pl.col("relationship_id") == "Maps to")
                    & (pl.col("concept_id_1") != pl.col("concept_id_2"))
                )
                .select(
                    concept_id_1=pl.col("concept_id_1").cast(pl.Int64),
                    concept_id_2=pl.col("concept_id_2").cast(pl.Int64),
                )
                .to_dict(as_series=False)
            )

            for cid1, cid2 in zip(custom_rels["concept_id_1"], custom_rels["concept_id_2"], strict=False):
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
        properties = table_config.get("properties") or table_config.get("metadata", [])
        for prop in properties:
            field = prop.get("concept_lookup_field")
            if field:
                columns.append(field)
        return columns

    # Get concept_id field
    concept_id_field = concept_id_config.get("concept_id_field", f"{table_name}_concept_id")
    columns.append(concept_id_field)

    # Get source_concept_id field
    source_concept_id_field = concept_id_config.get("source_concept_id_field")
    if source_concept_id_field:
        columns.append(source_concept_id_field)

    properties = table_config.get("properties") or table_config.get("metadata", [])
    for prop in properties:
        field = prop.get("concept_lookup_field")
        if field:
            columns.append(field)

    return columns


def validate_parquet_file(file_path: Path) -> bool:
    """
    Quick validation check for parquet file health.

    Returns:
        True if file appears readable, False otherwise
    """
    try:
        # Try to read just the schema (fast)
        import pyarrow.parquet as pq

        pq.read_schema(file_path)
        return True
    except Exception:
        return False


def fast_scan_file_for_concept_ids(file_path: Path, concept_id_columns: List[str]) -> Tuple[Set[int], Optional[str]]:
    """
    Fast scan of a single file to extract unique concept_ids.

    Uses Polars lazy evaluation to only read specified columns.

    Returns:
        (concept_ids_set, error_message)
        error_message is None if successful
    """
    concept_ids = set()

    try:
        # Quick validation first
        if not validate_parquet_file(file_path):
            return concept_ids, f"File validation failed: {file_path.name}"

        # Use lazy scan (doesn't load full file into memory)
        lazy_df = pl.scan_parquet(file_path)

        # Check which concept_id columns exist
        schema = lazy_df.collect_schema()
        existing_cols = [col for col in concept_id_columns if col in schema.names()]

        if not existing_cols:
            return concept_ids, None

        # Select only concept_id columns and get unique values
        for col in existing_cols:
            unique_values = lazy_df.select(pl.col(col).cast(pl.Int64)).unique().collect()

            # Add to set (filter out nulls)
            for row in unique_values.iter_rows():
                concept_id = row[0]
                if concept_id is not None and concept_id > 0:
                    concept_ids.add(concept_id)

        return concept_ids, None

    except Exception as e:
        return concept_ids, f"{file_path.name}: {str(e)}"


def prescan_worker(args: Tuple) -> Dict[str, Any]:
    """
    Worker process to scan a batch of files for concept_ids.

    Returns:
        Dict with concept_ids, file count, and any errors
    """
    worker_id, file_batch = args

    worker_concept_ids = set()
    files_processed = 0
    errors = []

    for file_path, concept_id_columns in file_batch:
        try:
            concept_ids, error = fast_scan_file_for_concept_ids(file_path, concept_id_columns)
            worker_concept_ids.update(concept_ids)
            files_processed += 1

            if error:
                errors.append(error)

        except Exception as e:
            errors.append(f"{file_path.name}: Unexpected error: {str(e)}")

    return {
        "worker_id": worker_id,
        "concept_ids": worker_concept_ids,
        "files_processed": files_processed,
        "errors": errors,
    }


def prescan_concept_ids(
    omop_dir: Path,
    config: Dict,
    num_workers: int,
    verbose: bool = False,
    timeout_per_worker: int = 600,  # 10 minutes per worker
) -> Set[int]:
    """
    Fast parallel pre-scan to collect all unique concept_ids used in OMOP data.

    Uses greedy load balancing for optimal worker distribution.
    Includes timeout protection and error reporting.

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

    print(f"  Pre-scanning {len(files_to_scan)} files...")

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
    worker_args = list(enumerate(worker_loads))

    # Run workers in parallel with timeout protection
    import multiprocessing as mp

    all_concept_ids = set()
    total_errors = []

    try:
        with mp.Pool(processes=num_workers) as pool:
            # Use map_async for timeout support
            async_result = pool.map_async(prescan_worker, worker_args)

            # Wait with timeout
            try:
                worker_results = async_result.get(timeout=timeout_per_worker)

                # Merge results
                total_files_processed = 0
                for result in worker_results:
                    all_concept_ids.update(result["concept_ids"])
                    total_files_processed += result["files_processed"]
                    total_errors.extend(result["errors"])

                if verbose:
                    print(f"  ✓ Pre-scanned {total_files_processed}/{len(files_to_scan)} files successfully")
                    print(f"  ✓ Found {len(all_concept_ids):,} unique concept_ids")

                    if total_errors:
                        print(f"  ⚠️  {len(total_errors)} files had errors:")
                        for error in total_errors[:5]:  # Show first 5 errors
                            print(f"     - {error}")
                        if len(total_errors) > 5:
                            print(f"     ... and {len(total_errors) - 5} more")

            except mp.TimeoutError:
                print(f"  ⚠️  WARNING: Pre-scan timed out after {timeout_per_worker}s")
                print("  ⚠️  Continuing without concept optimization (will use full concept table)")
                pool.terminate()
                pool.join()
                return set()  # Return empty set to use full concept table

    except Exception as e:
        print(f"  ⚠️  WARNING: Pre-scan failed: {e}")
        print("  ⚠️  Continuing without concept optimization (will use full concept table)")
        return set()

    return all_concept_ids


# ============================================================================
# FILE DISCOVERY (from omop_refactor.py)
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
# TRANSFORMATION UTILITIES (from omop_refactor.py)
# ============================================================================


def apply_transforms(expr: pl.Expr, transforms: list) -> pl.Expr:
    """
    Apply a list of transformations to a Polars expression.

    Supported transforms:
    - {"type": "replace", "pattern": "X", "replacement": "Y"} - Replace substring
    - {"type": "regex_replace", "pattern": "X", "replacement": "Y"} - Regex replace
    - {"type": "lower"} - Convert to lowercase
    - {"type": "upper"} - Convert to uppercase
    - {"type": "strip"} - Strip whitespace
    - {"type": "strip_chars", "characters": "X"} - Strip specific characters

    Args:
        expr: Polars expression to transform
        transforms: List of transform dictionaries

    Returns:
        Transformed Polars expression
    """
    for transform in transforms:
        transform_type = transform.get("type")

        if transform_type == "replace":
            # Simple string replacement
            pattern = transform.get("pattern", "")
            replacement = transform.get("replacement", "")
            expr = expr.str.replace_all(pattern, replacement, literal=True)

        elif transform_type == "regex_replace":
            # Regex replacement
            pattern = transform.get("pattern", "")
            replacement = transform.get("replacement", "")
            expr = expr.str.replace_all(pattern, replacement, literal=False)

        elif transform_type == "lower":
            # Convert to lowercase
            expr = expr.str.to_lowercase()

        elif transform_type == "upper":
            # Convert to uppercase
            expr = expr.str.to_uppercase()

        elif transform_type == "strip":
            # Strip leading/trailing whitespace
            expr = expr.str.strip_chars()

        elif transform_type == "strip_chars":
            # Strip specific characters
            characters = transform.get("characters", "")
            expr = expr.str.strip_chars(characters)

        elif transform_type == "split":
            delimiter = transform.get("delimiter", "")
            index = transform.get("index")
            split_expr = expr.str.split(delimiter)
            if index is not None:
                expr = split_expr.list.get(int(index), null_on_oob=True)
            else:
                expr = split_expr

        else:
            # Unknown transform type - skip
            pass

    return expr


def build_template_expression(
    template: str,
    transforms: Optional[List[Dict[str, Any]]] = None,
    column_transforms: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> pl.Expr:
    """
    Build a Polars expression from a template string with optional transforms.

    Args:
        template: Template string like "PREFIX/{field1}/{field2}"
        transforms: DEPRECATED - transforms to apply to entire result (kept for backwards compat)
        column_transforms: Dict mapping column names to their specific transforms
    """
    import re

    parts = re.split(r"(\{[^}]+\})", template)

    exprs = []
    for part in parts:
        if part.startswith("{") and part.endswith("}"):
            field = part[1:-1]
            col_expr = pl.col(field).cast(pl.Utf8).fill_null("")

            # Apply column-specific transforms if provided
            if column_transforms and field in column_transforms:
                col_expr = apply_transforms(col_expr, column_transforms[field])

            exprs.append(col_expr)
        elif part:
            exprs.append(pl.lit(part))

    code_expr = pl.concat_str(exprs)

    # Apply global transforms (for backwards compatibility)
    if transforms:
        code_expr = apply_transforms(code_expr, transforms)

    return code_expr


def transform_to_meds_unsorted(
    df: pl.DataFrame,
    table_config: Dict,
    primary_key: str,
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
        meds_schema: Global MEDS schema (ensures consistent types across all files)
    """
    # 1. subject_id
    subject_id_field = table_config.get("subject_id_field", primary_key)
    subject_id = pl.col(subject_id_field).cast(pl.Int64).alias("subject_id")

    # 2. time (from time_start field in config) → "time" column in MEDS
    # Support both old naming (time_field, datetime_field) and new naming (time_start)
    time_start_field = (
        table_config.get("time_start") or table_config.get("time_field") or table_config.get("datetime_field")
    )
    time_start_fallbacks = table_config.get("time_start_fallbacks") or table_config.get("time_fallbacks", [])

    time_options = []
    if time_start_field:
        time_options.append(pl.col(time_start_field).cast(pl.Datetime("us")))
    for fallback in time_start_fallbacks:
        time_options.append(pl.col(fallback).cast(pl.Datetime("us")))

    if not time_options:
        raise ValueError("No time_start field configured for table")

    time = pl.coalesce(time_options).alias("time") if len(time_options) > 1 else time_options[0].alias("time")

    # 3. Build base expressions
    code_mappings = table_config.get("code_mappings", {})
    base_exprs = [subject_id, time]

    code_candidates: List[str] = []
    needs_concept_join = False
    concept_join_alias = None
    code_is_fixed = False

    if fixed_code:
        code_is_fixed = True
        base_exprs.append(pl.lit(fixed_code).alias("code"))
    else:
        if not code_mappings:
            raise ValueError("No code mappings defined for table")

        # Concept ID strategy
        if "concept_id" in code_mappings:
            concept_config = code_mappings["concept_id"]

            if "template" in concept_config:
                alias = "code_concept_template"
                concept_fields_to_map = concept_config.get("concept_fields", [])

                if concept_fields_to_map and concept_df is not None and len(concept_df) > 0:
                    temp_df = df
                    for i, concept_field in enumerate(concept_fields_to_map):
                        code_alias = f"_concept_code_{i}_{concept_field}"
                        temp_df = temp_df.join(
                            concept_df.select(
                                [pl.col("concept_id").alias(f"_join_id_{i}"), pl.col("code").alias(code_alias)]
                            ),
                            left_on=concept_field,
                            right_on=f"_join_id_{i}",
                            how="left",
                        )

                    import re

                    parts = re.split(r"(\{[^}]+\})", concept_config["template"])
                    exprs = []
                    for part in parts:
                        if part.startswith("{") and part.endswith("}"):
                            field = part[1:-1]
                            if field in concept_fields_to_map:
                                idx = concept_fields_to_map.index(field)
                                code_alias = f"_concept_code_{idx}_{field}"
                                exprs.append(pl.col(code_alias).cast(pl.Utf8).fill_null(f"UNKNOWN_{field.upper()}"))
                            else:
                                exprs.append(pl.col(field).cast(pl.Utf8).fill_null(""))
                        elif part:
                            exprs.append(pl.lit(part))

                    code_expr = pl.concat_str(exprs)
                    transforms = concept_config.get("transforms", [])
                    code_expr = apply_transforms(code_expr, transforms)
                    df = temp_df
                    base_exprs.append(code_expr.alias(alias))
                else:
                    code_expr = build_template_expression(
                        concept_config["template"],
                        transforms=concept_config.get("transforms"),
                        column_transforms=concept_config.get("column_transforms"),
                    )
                    base_exprs.append(code_expr.alias(alias))

                code_candidates.append(alias)
            else:
                concept_id_field = concept_config.get("concept_id_field")
                source_concept_id_field = concept_config.get("source_concept_id_field")
                fallback_concept_id = concept_config.get("fallback_concept_id")

                if concept_df is None or len(concept_df) == 0:
                    raise ValueError("concept_id mapping requested but concept_df not provided")

                if not concept_id_field and not source_concept_id_field and fallback_concept_id is None:
                    raise ValueError(
                        "concept_id mapping requested but no concept_id_field, "
                        "source_concept_id_field, fallback_concept_id, or template defined"
                    )

                needs_concept_join = True
                concept_join_alias = "code_concept_lookup"
                code_candidates.append(concept_join_alias)

                exprs_to_coalesce: List[pl.Expr] = []
                if source_concept_id_field:
                    exprs_to_coalesce.append(pl.col(source_concept_id_field).cast(pl.Int64))
                if concept_id_field:
                    exprs_to_coalesce.append(pl.col(concept_id_field).cast(pl.Int64))
                if fallback_concept_id is not None:
                    exprs_to_coalesce.append(pl.lit(fallback_concept_id).cast(pl.Int64))

                if not exprs_to_coalesce:
                    raise ValueError("concept_id mapping requires at least one source for concept ids")

                if len(exprs_to_coalesce) > 1:
                    base_exprs.append(pl.coalesce(exprs_to_coalesce).alias("concept_id"))
                else:
                    base_exprs.append(exprs_to_coalesce[0].alias("concept_id"))

        # Source value strategy
        if "source_value" in code_mappings:
            source_value_config = code_mappings["source_value"]
            alias = "code_source_value"

            if "template" in source_value_config:
                code_expr = build_template_expression(
                    source_value_config["template"],
                    transforms=source_value_config.get("transforms"),
                    column_transforms=source_value_config.get("column_transforms"),
                )
                base_exprs.append(code_expr.alias(alias))
            elif "field" in source_value_config:
                code_field = source_value_config["field"]
                base_exprs.append(pl.col(code_field).cast(pl.Utf8).alias(alias))
            else:
                raise ValueError("source_value mapping must have either 'field' or 'template'")

            code_candidates.append(alias)

        if not code_is_fixed and not code_candidates:
            raise ValueError("No code mappings defined for table")

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

    # 6. Property columns (support both "properties" and "metadata" for backwards compat)
    properties = table_config.get("properties") or table_config.get("metadata", [])
    concept_property_requests: List[Dict[str, Any]] = []

    for prop_spec in properties:
        prop_name = prop_spec.get("name")  # Output column name
        if prop_name is None:
            continue
        prop_type = prop_spec.get("type", "string")
        prop_polars_type = config_type_to_polars(prop_type)

        # Determine output column name
        output_name = prop_spec.get("alias", prop_name)  # Use alias if provided, else use name

        # Handle different property types:
        if "literal" in prop_spec:
            # Literal value (constant for all rows)
            literal_value = prop_spec["literal"]
            base_exprs.append(pl.lit(literal_value).cast(prop_polars_type).alias(output_name))
        elif "concept_lookup_field" in prop_spec:
            concept_field = prop_spec["concept_lookup_field"]
            temp_col = f"__prop_concept_id_{len(concept_property_requests)}"
            temp_alias = f"__prop_concept_code_{len(concept_property_requests)}"
            base_exprs.append(pl.col(concept_field).cast(pl.Int64).alias(temp_col))
            concept_property_requests.append(
                {
                    "concept_col": temp_col,
                    "code_alias": temp_alias,
                    "output_name": output_name,
                    "type": prop_polars_type,
                }
            )
        else:
            # Standard column or aliased column
            source_field = prop_spec.get("source", prop_name)
            base_exprs.append(pl.col(source_field).cast(prop_polars_type).alias(output_name))

    # Build base DataFrame
    result = df.select(base_exprs)

    # Perform concept join if needed (FAST!)
    if needs_concept_join:
        join_alias = concept_join_alias or "code"
        join_df = concept_df.rename({"code": join_alias})
        result = result.join(join_df, on="concept_id", how="left").drop("concept_id")

    if concept_property_requests:
        if concept_df is None or len(concept_df) == 0:
            raise ValueError("Property concept lookup requested but concept_df not provided")
        for req in concept_property_requests:
            concept_col = req["concept_col"]
            code_alias = req["code_alias"]
            output_name = req["output_name"]
            prop_type = req["type"]

            join_df = concept_df.rename({"code": code_alias})
            result = result.join(join_df, left_on=concept_col, right_on="concept_id", how="left")

            if concept_col in result.columns:
                result = result.drop(concept_col)
            if "concept_id" in result.columns:
                result = result.drop("concept_id")

            if code_alias in result.columns:
                result = result.rename({code_alias: output_name})
            result = result.with_columns(pl.col(output_name).cast(prop_type))

    # Combine candidate code columns, respecting fallback order
    if not code_is_fixed:
        available_candidates = [alias for alias in code_candidates if alias in result.columns]
        if not available_candidates:
            raise ValueError("No code mapping expressions materialized for table")

        result = result.with_columns(pl.coalesce([pl.col(alias) for alias in available_candidates]).alias("code"))
        drop_cols = [alias for alias in available_candidates if alias != "code"]
        if drop_cols:
            result = result.drop(drop_cols)

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


def process_omop_file_worker(args: Tuple) -> Dict:
    """
    Worker function to process a single OMOP file.

    Reads OMOP Parquet → transforms to MEDS Unsorted → writes output.
    NO sorting, NO sharding (done later by streaming sort).
    """
    (
        file_path,
        table_name,
        table_config,
        primary_key,
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
            "code_mapping_used": describe_code_mapping(table_config, is_canonical, fixed_code),
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
# STREAMING EXTERNAL SORT (Polars 1.x)
# ============================================================================


def _partition_file_worker(args):
    """
    Worker function to partition a single unsorted file into sorted runs.

    Returns a list of run counts per shard.
    """
    file_idx, unsorted_file, runs_dir, num_shards, chunk_rows, compression, verbose, low_memory = args

    run_counts = [0] * num_shards

    try:
        # Read file (only once!) with low_memory flag
        df = pl.read_parquet(unsorted_file, low_memory=low_memory)

        if len(df) == 0:
            return run_counts

        # Add shard_id column
        df = df.with_columns([(pl.col("subject_id") % num_shards).alias("_shard_id")])

        # Partition to each shard
        for shard_id in range(num_shards):
            # Filter for this shard
            shard_data = df.filter(pl.col("_shard_id") == shard_id).drop("_shard_id")

            if len(shard_data) == 0:
                continue

            # Process in chunks if data is large
            num_rows = len(shard_data)
            num_chunks = (num_rows + chunk_rows - 1) // chunk_rows

            for chunk_idx in range(num_chunks):
                start_idx = chunk_idx * chunk_rows
                end_idx = min((chunk_idx + 1) * chunk_rows, num_rows)

                chunk = shard_data[start_idx:end_idx]

                # Sort chunk
                sorted_chunk = chunk.sort(["subject_id", "time"])

                # Write as run
                shard_dir = runs_dir / f"shard_{shard_id}"
                run_file = shard_dir / f"run_{file_idx:05d}_{chunk_idx:05d}.parquet"
                sorted_chunk.write_parquet(run_file, compression=compression)

                run_counts[shard_id] += 1

            # Cleanup shard data
            del shard_data

        # Cleanup file data and force garbage collection
        del df
        gc.collect()

    except Exception as e:
        if verbose:
            print(f"\n  WARNING: Error processing {unsorted_file.name}: {e}")

    return run_counts


def partition_to_sorted_runs(
    unsorted_dir: Path,
    runs_dir: Path,
    num_shards: int,
    chunk_rows: int = 10_000_000,
    compression: str = "lz4",
    verbose: bool = False,
    low_memory: bool = True,
    num_workers: int = 1,
) -> None:
    """
    Stage 2.1: Partition unsorted data into sorted runs (OPTIMIZED + PARALLEL).

    Reads each unsorted file ONCE and partitions to all shards:
    - Read unsorted file
    - Add shard_id column (subject_id % num_shards)
    - For each shard: filter, sort, write run

    This is much faster than reading each file N times (once per shard).

    Args:
        low_memory: Use Polars low_memory mode for smaller batches
        num_workers: Number of parallel workers for processing files
    """
    print("\n[Stage 2.1] Creating sorted runs (optimized + parallel)...")
    print(f"  Shards: {num_shards}")
    print(f"  Workers: {num_workers}")
    print(f"  Chunk size: {chunk_rows:,} rows")
    print("  Strategy: Read-once partitioning")

    runs_dir.mkdir(parents=True, exist_ok=True)

    # Create shard directories
    for shard_id in range(num_shards):
        shard_dir = runs_dir / f"shard_{shard_id}"
        shard_dir.mkdir(exist_ok=True)

    # Find all unsorted files
    unsorted_files = sorted(unsorted_dir.glob("*.parquet"))

    if not unsorted_files:
        print("  WARNING: No unsorted files found")
        return

    print(f"  Unsorted files: {len(unsorted_files)}")

    # Prepare worker arguments
    worker_args = [
        (file_idx, unsorted_file, runs_dir, num_shards, chunk_rows, compression, verbose, low_memory)
        for file_idx, unsorted_file in enumerate(unsorted_files)
    ]

    # Process files in parallel or sequentially
    if num_workers == 1:
        # Sequential processing
        all_run_counts = []
        for args in tqdm(worker_args, desc="Partitioning files"):
            run_counts = _partition_file_worker(args)
            all_run_counts.append(run_counts)
    else:
        # Parallel processing
        import multiprocessing as mp

        with mp.Pool(processes=num_workers) as pool:
            all_run_counts = list(
                tqdm(
                    pool.imap_unordered(_partition_file_worker, worker_args),
                    total=len(worker_args),
                    desc="Partitioning files",
                )
            )

    # Aggregate run counts across all files
    total_run_counts = [0] * num_shards
    for run_counts in all_run_counts:
        for shard_id in range(num_shards):
            total_run_counts[shard_id] += run_counts[shard_id]

    # Always show run statistics (useful for debugging)
    print("\n  ✓ Partitioning complete")
    print(f"  Total runs created: {sum(total_run_counts)}")
    print(f"  Runs per shard (avg): {sum(total_run_counts) / num_shards:.1f}")

    if verbose:
        print("\n  Detailed runs per shard:")
        for shard_id, count in enumerate(total_run_counts):
            print(f"    Shard {shard_id:3d}: {count:4d} runs")


def streaming_merge_shard(
    shard_dir: Path,
    output_file: Path,
    compression: str = "zstd",
    verbose: bool = False,
    low_memory: bool = True,
    row_group_size: int = 100_000,
) -> int:
    """
    Stage 2.2: Streaming merge of sorted runs using Polars.

    Uses Polars' lazy evaluation + streaming engine:
    - Lazy scan all run files
    - Concat them
    - Sort (triggers streaming k-way merge)
    - Sink to output (streaming write)

    This is memory-bounded and efficient (native Rust implementation).

    Args:
        low_memory: Use Polars low_memory mode for smaller batches
        row_group_size: Rows per row group in output Parquet (smaller = less memory)
    """
    run_files = sorted(shard_dir.glob("run_*.parquet"))

    if not run_files:
        return 0

    # Lazy scan all runs with low_memory flag
    scans = [pl.scan_parquet(str(f), low_memory=low_memory) for f in run_files]

    # Concat + sort + streaming sink
    # Polars automatically uses streaming k-way merge here!
    (
        pl.concat(scans, how="vertical")
        .sort(["subject_id", "time"])
        .sink_parquet(
            str(output_file),
            compression=compression,
            maintain_order=True,  # Important for sorted output
            row_group_size=row_group_size,  # Control memory usage
        )
    )

    # Get row count
    row_count = pl.scan_parquet(output_file).select(pl.len()).collect().item()

    # Force garbage collection
    gc.collect()

    return row_count


def _merge_shard_worker(args):
    """
    Worker function for parallel shard merging.
    Must be at module level to be picklable by multiprocessing.
    """
    shard_dir, output_file, compression, low_memory, row_group_size = args

    rows = streaming_merge_shard(
        shard_dir,
        output_file,
        compression=compression,
        verbose=False,
        low_memory=low_memory,
        row_group_size=row_group_size,
    )

    # Explicit cleanup
    gc.collect()

    return rows


def streaming_external_sort(
    unsorted_dir: Path,
    output_dir: Path,
    num_shards: int,
    chunk_rows: int = 10_000_000,
    run_compression: str = "lz4",
    final_compression: str = "zstd",
    merge_workers: int = 0,
    verbose: bool = False,
    run_partition: bool = True,
    run_merge: bool = True,
    low_memory: bool = True,
    row_group_size: int = 100_000,
    num_workers: int = 1,
) -> None:
    """
    Complete streaming external sort using Polars 1.x.

    Two stages:
    1. Partition unsorted data into sorted runs (chunked processing)
    2. Streaming k-way merge of runs (Polars native)

    Memory-bounded, fast, and simple.

    Args:
        merge_workers: Number of parallel merge workers (0=auto, 1=sequential, N=parallel)
        low_memory: Use Polars low_memory mode (smaller batches, less RAM)
        row_group_size: Rows per Parquet row group (smaller = less memory)
        num_workers: Number of parallel workers for Stage 2.1 partitioning
    """
    print("\n" + "=" * 70)
    print("STAGE 2: STREAMING EXTERNAL SORT")
    print("=" * 70)

    stage2_start = time.time()

    # Create temporary runs directory
    runs_dir = output_dir / "runs_temp"
    final_dir = output_dir / "data"
    final_dir.mkdir(parents=True, exist_ok=True)

    # Stage 2.1: Create sorted runs
    if run_partition:
        partition_to_sorted_runs(
            unsorted_dir=unsorted_dir,
            runs_dir=runs_dir,
            num_shards=num_shards,
            chunk_rows=chunk_rows,
            compression=run_compression,
            verbose=verbose,
            low_memory=low_memory,
            num_workers=num_workers,
        )
    else:
        print("\nSkipping Stage 2.1 (partition)")
        # Check if runs exist
        if not runs_dir.exists():
            print(f"ERROR: No runs found at {runs_dir}")
            print("  Run with --pipeline partition first!")
            sys.exit(1)

    # Stage 2.2: Streaming merge per shard
    total_rows = 0

    if run_merge:
        # Prepare merge tasks (include all memory config in args)
        merge_tasks = []
        for shard_id in range(num_shards):
            shard_dir = runs_dir / f"shard_{shard_id}"
            if shard_dir.exists():
                output_file = final_dir / f"{shard_id}.parquet"
                merge_tasks.append((shard_dir, output_file, final_compression, low_memory, row_group_size))

        # Determine number of workers
        import multiprocessing as mp

        if merge_workers == 0:
            # Auto: use half the available cores (leave room for I/O)
            num_merge_workers = max(1, mp.cpu_count() // 2)
        else:
            num_merge_workers = merge_workers

        # Sequential vs Parallel
        if num_merge_workers == 1:
            # Sequential merge (low memory)
            print("\n[Stage 2.2] Streaming merge (sequential)...")
            row_counts = []
            for task in tqdm(merge_tasks, desc="Merging shards"):
                rows = _merge_shard_worker(task)
                row_counts.append(rows)
        else:
            # Parallel merge (faster but more memory)
            print(f"\n[Stage 2.2] Streaming merge (parallel, {num_merge_workers} workers)...")
            with mp.Pool(processes=num_merge_workers) as pool:
                row_counts = list(
                    tqdm(
                        pool.imap_unordered(_merge_shard_worker, merge_tasks),
                        total=len(merge_tasks),
                        desc="Merging shards",
                    )
                )

        total_rows = sum(row_counts)

        # Cleanup runs
        if run_partition:  # Only cleanup if we created the runs in this session
            shutil.rmtree(runs_dir)
    else:
        print("\nSkipping Stage 2.2 (merge)")

    stage2_elapsed = time.time() - stage2_start

    print("\n" + "=" * 70)
    print("STAGE 2 COMPLETE: STREAMING SORT")
    print("=" * 70)
    print(f"Total rows:    {total_rows:,}")
    print(f"Shards:        {num_shards}")
    print(f"Chunk size:    {chunk_rows:,} rows")
    print(f"Time:          {stage2_elapsed:.2f}s")
    if stage2_elapsed > 0:
        print(f"Throughput:    {total_rows / stage2_elapsed / 1_000_000:.2f}M rows/sec")

    # Per-shard breakdown
    if verbose:
        print("\nPer-shard breakdown:")
        for shard_id in range(num_shards):
            output_file = final_dir / f"{shard_id}.parquet"
            if output_file.exists():
                size_mb = output_file.stat().st_size / 1024 / 1024
                rows = pl.scan_parquet(output_file).select(pl.len()).collect().item()
                print(f"  Shard {shard_id:3d}: {rows:12,} rows  ({size_mb:8.1f} MB)")

    print("=" * 70)


# ============================================================================
# MAIN ETL WITH STREAMING SORT
# ============================================================================


def run_omop_to_meds_streaming(
    omop_dir: Path,
    output_dir: Path,
    config_path: Path,
    num_workers: int = 8,
    num_shards: Optional[int] = None,
    verbose: bool = False,
    optimize_concepts: bool = True,
    chunk_rows: int = 10_000_000,
    run_compression: str = "lz4",
    final_compression: str = "zstd",
    merge_workers: int = 0,
    pipeline_stages: str = "all",
    # Memory configuration
    low_memory: bool = False,
    row_group_size: int = 100_000,
    polars_threads: Optional[int] = None,
    rayon_threads: Optional[int] = None,
    process_method: str = "spawn",
    force_refresh: bool = False,
):
    """
    Run OMOP to MEDS ETL with streaming external sort.

    Same as omop_refactor.py but uses Polars streaming for Stage 2.

    Args:
        chunk_rows: Rows per sorted run (default 10M - adjust based on RAM)
        run_compression: Compression for intermediate runs (lz4, zstd, snappy)
        final_compression: Compression for final output (zstd, snappy, lz4)
        pipeline_stages: Which stages to run:
            - "all": Run full pipeline (default)
            - "transform": Only OMOP→MEDS transform (Stage 1)
            - "partition": Only partition to sorted runs (Stage 2.1)
            - "merge": Only merge runs (Stage 2.2)
            - "sort": Run both partition + merge (Stage 2)

        Memory Configuration:
            low_memory: Use Polars low_memory mode (smaller batches, less RAM).
                       Default False. Enable with --low_memory for 32-64GB laptops.
            row_group_size: Rows per Parquet row group. Smaller = less memory.
                           Default 100k (good for 32-64GB laptops).
            polars_threads: Max threads for Polars (None = use env var, 1 = single-threaded).
                           Overrides POLARS_MAX_THREADS env var.
            rayon_threads: Max threads for Rayon (None = use env var, 1 = single-threaded).
                          Overrides RAYON_NUM_THREADS env var.
            process_method: Multiprocessing start method ('spawn' or 'fork').
                           Default 'spawn' for better memory isolation.
                           'fork' is faster but can cause memory bloat.
                           CRITICAL: Must be set early to prevent deadlocks on Linux.
    """
    # ========================================================================
    # CONFIGURE MULTIPROCESSING AND THREADING (MUST BE FIRST!)
    # ========================================================================
    # CRITICAL: This MUST happen before ANY multiprocessing operations,
    # including prescan_concept_ids() which creates its own pool.
    # On Linux, default 'fork' + Polars threads = deadlock.
    # On Mac, default is already 'spawn' so this prevents Linux hangs.

    import multiprocessing as mp

    # Set multiprocessing start method FIRST
    try:
        mp.set_start_method(process_method, force=True)
        if verbose:
            print(f"Multiprocessing method: '{process_method}' (critical for Linux stability)")
    except RuntimeError:
        # Already set - check if it matches
        current_method = mp.get_start_method()
        if current_method != process_method and verbose:
            print(f"Warning: multiprocessing method already set to '{current_method}' (requested '{process_method}')")
        elif verbose:
            print(f"Multiprocessing method: '{current_method}' (already set)")

    # Configure Polars threading (CLI args override environment variables)
    if polars_threads is not None:
        os.environ["POLARS_MAX_THREADS"] = str(polars_threads)
        if verbose:
            print(f"Set POLARS_MAX_THREADS={polars_threads} (overriding env var)")
    elif "POLARS_MAX_THREADS" not in os.environ:
        # Default: single-threaded per worker for multiprocessing
        os.environ["POLARS_MAX_THREADS"] = "1"
        if verbose:
            print("Set POLARS_MAX_THREADS=1 (default for multiprocessing)")

    # Configure Rayon threading (used by Polars internally)
    if rayon_threads is not None:
        os.environ["RAYON_NUM_THREADS"] = str(rayon_threads)
        if verbose:
            print(f"Set RAYON_NUM_THREADS={rayon_threads} (overriding env var)")
    elif "RAYON_NUM_THREADS" not in os.environ:
        # Default: single-threaded per worker
        os.environ["RAYON_NUM_THREADS"] = "1"
        if verbose:
            print("Set RAYON_NUM_THREADS=1 (default for multiprocessing)")

    # ========================================================================
    # PARSE PIPELINE STAGES
    # ========================================================================

    # Parse pipeline stages
    stages_to_run = set()
    if pipeline_stages == "all":
        stages_to_run = {"transform", "partition", "merge"}
    elif pipeline_stages == "transform":
        stages_to_run = {"transform"}
    elif pipeline_stages == "partition":
        stages_to_run = {"partition"}
    elif pipeline_stages == "merge":
        stages_to_run = {"merge"}
    elif pipeline_stages == "sort":
        stages_to_run = {"partition", "merge"}
    else:
        raise ValueError(f"Invalid pipeline_stages: {pipeline_stages}. Must be: all, transform, partition, merge, sort")

    # Load config
    with open(config_path, "r") as f:
        config = json.load(f)

    # Compile templated config to old format for performance
    # This ensures templates are parsed ONCE, not for every file
    from meds_etl.config_compiler import compile_config

    config = compile_config(config)

    # Determine num_shards
    if num_shards is None:
        num_shards = config.get("num_shards", 100)

    # Setup directories
    output_dir = Path(output_dir)
    temp_dir = output_dir / "temp"
    unsorted_dir = temp_dir / "unsorted_data"

    # Handle existing output directory
    if output_dir.exists():
        # Check if this looks like a previous ETL output (has data/ or metadata/ or temp/)
        has_etl_output = any(
            [
                (output_dir / "data").exists(),
                (output_dir / "metadata").exists(),
                (output_dir / "temp").exists(),
            ]
        )

        if has_etl_output:
            if force_refresh:
                print("\n🔄 Force refresh enabled - removing existing output directory...")
                shutil.rmtree(output_dir)
                print(f"   ✓ Removed: {output_dir}")
                output_dir.mkdir(parents=True, exist_ok=True)
            else:
                print("\n❌ ERROR: Output directory already exists and contains ETL data!")
                print(f"   Directory: {output_dir}")
                print(f"   Found: {[d.name for d in output_dir.iterdir() if d.is_dir()]}")
                print("\n   Options:")
                print("   1. Use --force-refresh to overwrite existing data")
                print("   2. Choose a different output directory")
                print(f"   3. Manually delete the directory: rm -rf {output_dir}")
                sys.exit(1)

    # Run Stage 1 using original implementation
    # This creates unsorted MEDS files
    print("\n" + "=" * 70)
    print("STREAMING ETL MODE")
    print("=" * 70)
    print(f"Pipeline stages: {', '.join(sorted(stages_to_run))}")
    print(f"Run compression: {run_compression}")
    print(f"Final compression: {final_compression}")
    print("Using Polars 1.x streaming for Stage 2")

    import multiprocessing as mp

    import meds
    import pyarrow as pa
    import pyarrow.parquet as pq

    import meds_etl

    primary_key = config.get("primary_key", "person_id")

    # Clean and create directories
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)
    unsorted_dir.mkdir()
    metadata_dir = temp_dir / "metadata"
    metadata_dir.mkdir()

    # Validate config
    validate_config_against_data(omop_dir, config, verbose=verbose)

    concept_lookup_needed = config_requires_concept_lookup(config)

    # Build concept map if needed
    concept_df: Optional[pl.DataFrame] = None
    code_metadata: Dict[int, Any] = {}

    if concept_lookup_needed:
        print(f"\n{'=' * 70}")
        print("BUILDING CONCEPT MAP")
        print(f"{'=' * 70}")

        concept_df, code_metadata = build_concept_map(omop_dir, verbose=verbose)

        if len(concept_df) == 0:
            print("\nERROR: concept_id mappings requested but concept table not found!")
            sys.exit(1)

        print(f"  Loaded {len(concept_df):,} concepts")

        if optimize_concepts:
            original_size = len(concept_df)

            if verbose:
                print("\nOptimizing concept map (pre-scanning data to find used concepts)...")
                print(f"  Using {num_workers} workers to scan OMOP files")
                print("  (If this hangs on Linux, ensure --process_method spawn is set)")

            used_concept_ids = prescan_concept_ids(omop_dir, config, num_workers, verbose=verbose)

            if used_concept_ids:
                concept_df = concept_df.filter(pl.col("concept_id").is_in(list(used_concept_ids)))
                if verbose:
                    print(f"  Optimized: {original_size:,} -> {len(concept_df):,} concepts")
    else:
        print("\n📋 Config does not require concept lookups - skipping concept map build")

    concept_df_data = pickle.dumps(concept_df) if concept_df is not None else None

    # Save metadata
    dataset_metadata = {
        "dataset_name": "OMOP",
        "dataset_version": time.strftime("%Y-%m-%d"),
        "etl_name": "meds_etl.omop_refactor_streaming",
        "etl_version": meds_etl.__version__,
        "meds_version": meds.__version__,
    }

    with open(metadata_dir / "dataset.json", "w") as f:
        json.dump(dataset_metadata, f, indent=2)

    if code_metadata:
        table = pa.Table.from_pylist(list(code_metadata.values()), meds.code_metadata_schema())
        pq.write_table(table, metadata_dir / "codes.parquet")

    # Copy metadata
    final_metadata_dir = output_dir / "metadata"
    if final_metadata_dir.exists():
        shutil.rmtree(final_metadata_dir)
    shutil.copytree(metadata_dir, final_metadata_dir)

    # ========================================================================
    # STAGE 1: OMOP → MEDS Unsorted (reuse from omop_refactor.py)
    # ========================================================================

    stage1_elapsed = 0
    total_output_rows = 0

    if "transform" in stages_to_run:
        print("\n" + "=" * 70)
        print("STAGE 1: OMOP → MEDS UNSORTED")
        print("=" * 70)

        meds_schema = get_meds_schema_from_config(config)

        # Collect tasks
        tasks = []

        # Canonical events
        for event_name, event_config in config.get("canonical_events", {}).items():
            table_name = event_config["table"]
            files = find_omop_table_files(omop_dir, table_name)
            fixed_code = event_config.get("code", f"MEDS_{event_name.upper()}")

            for file_path in files:
                tasks.append(
                    (
                        file_path,
                        table_name,
                        event_config,
                        primary_key,
                        unsorted_dir,
                        meds_schema,
                        concept_df_data,
                        True,
                        fixed_code,
                    )
                )

        # Regular tables
        for table_name, table_config in config.get("tables", {}).items():
            files = find_omop_table_files(omop_dir, table_name)

            for file_path in files:
                tasks.append(
                    (
                        file_path,
                        table_name,
                        table_config,
                        primary_key,
                        unsorted_dir,
                        meds_schema,
                        concept_df_data,
                        False,
                        None,
                    )
                )

        print(f"\nProcessing {len(tasks)} files with {num_workers} workers...")

        stage1_start = time.time()

        if num_workers > 1:
            # Thread config already set globally above
            with mp.Pool(processes=num_workers) as pool:
                results = list(
                    tqdm(
                        pool.imap_unordered(process_omop_file_worker, tasks),
                        total=len(tasks),
                        desc="Processing OMOP files",
                    )
                )
        else:
            results = [process_omop_file_worker(task) for task in tqdm(tasks, desc="Processing")]

        stage1_elapsed = time.time() - stage1_start

        # Report Stage 1 results (detailed, like omop_refactor.py)
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
        print("\nPer-table breakdown:")
        by_table = {}
        for r in successes:
            table = r.get("table", "unknown")
            if table not in by_table:
                by_table[table] = {
                    "files": 0,
                    "input": 0,
                    "output": 0,
                    "filtered": 0,
                    "code_mapping_used": r.get("code_mapping_used", "unknown"),
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
            retention = 100 * stats["output"] / stats["input"] if stats["input"] > 0 else 0
            mapping_used = stats["code_mapping_used"]

            # Simplify display: if using template, just show "template" regardless of base strategy
            if "+template" in mapping_used:
                mapping_used = "template"

            print(
                f"   {table:30s} [{mapping_used:20s}] {stats['files']:4d} files  "
                f"{stats['input']:>13,} -> {stats['output']:>13,} rows ({retention:5.1f}%)"
            )

        if failures:
            print(f"\nWARNING: {len(failures)} files failed:")
            for f in failures[:10]:  # Show first 10
                print(f"  - {f.get('table', 'unknown'):20s} / {f['file']}: {f.get('error', 'Unknown error')}")

        # Check what was actually written to disk
        written_files = list(unsorted_dir.glob("*.parquet"))
        print(f"\nFiles written to {unsorted_dir}:")
        print(f"  Total files: {len(written_files)}")
        if len(written_files) > 0:
            total_size = sum(f.stat().st_size for f in written_files) / 1024 / 1024
            print(f"  Total size:  {total_size:.1f} MB")
        print(f"{'=' * 70}")
    else:
        print("\nSkipping Stage 1 (transform)")
        # Check if unsorted data exists
        if not unsorted_dir.exists() or not list(unsorted_dir.glob("*.parquet")):
            print(f"ERROR: No unsorted data found at {unsorted_dir}")
            print("  Run with --pipeline transform first!")
            sys.exit(1)

    # ========================================================================
    # STAGE 2: Streaming External Sort (Polars 1.x)
    # ========================================================================

    stage2_elapsed = 0

    if "partition" in stages_to_run or "merge" in stages_to_run:
        stage2_start = time.time()

        # Determine which sub-stages to run
        run_partition = "partition" in stages_to_run
        run_merge = "merge" in stages_to_run

        # Call sort with stage control
        streaming_external_sort(
            unsorted_dir=unsorted_dir,
            output_dir=output_dir,
            num_shards=num_shards,
            chunk_rows=chunk_rows,
            run_compression=run_compression,
            final_compression=final_compression,
            merge_workers=merge_workers,
            verbose=verbose,
            run_partition=run_partition,
            run_merge=run_merge,
            low_memory=low_memory,
            row_group_size=row_group_size,
            num_workers=num_workers,
        )

        stage2_elapsed = time.time() - stage2_start
    else:
        print("\nSkipping Stage 2 (sort)")

    # Cleanup temp
    print("\nCleaning up temporary directory...")
    shutil.rmtree(temp_dir)
    print(f"  ✓ Removed: {temp_dir}")

    total_elapsed = stage1_elapsed + stage2_elapsed

    print("\n" + "=" * 70)
    print("ETL COMPLETE")
    print("=" * 70)
    print(f"Stage 1 time: {stage1_elapsed:.2f}s")
    print(f"Stage 2 time: {stage2_elapsed:.2f}s")
    print(f"Total time:   {total_elapsed:.2f}s")
    print(f"Total rows:   {total_output_rows:,}")
    if total_output_rows > 0:
        print(f"Throughput:   {total_output_rows/total_elapsed:,.0f} rows/s")
    print(f"Output dir:   {output_dir / 'data'}")
    print("=" * 70)


# ============================================================================
# CLI
# ============================================================================


def main():
    """Entry point for meds_etl_omop_streaming command."""
    import argparse

    parser = argparse.ArgumentParser(description="OMOP to MEDS ETL with Polars Streaming")

    parser.add_argument("--omop_dir", required=True, help="OMOP data directory")
    parser.add_argument("--output_dir", required=True, help="Output directory")
    parser.add_argument("--config", required=True, help="ETL config JSON")
    parser.add_argument("--workers", type=int, default=8, help="Stage 1 workers")
    parser.add_argument("--shards", type=int, default=None, help="Number of shards")
    parser.add_argument("--chunk_rows", type=int, default=10_000_000, help="Rows per sorted run")
    parser.add_argument(
        "--run_compression",
        choices=["lz4", "zstd", "snappy", "uncompressed"],
        default="lz4",
        help="Compression for intermediate runs (default: lz4)",
    )
    parser.add_argument(
        "--final_compression",
        choices=["zstd", "snappy", "lz4", "uncompressed"],
        default="zstd",
        help="Compression for final output (default: zstd)",
    )
    parser.add_argument(
        "--pipeline",
        choices=["all", "transform", "partition", "merge", "sort"],
        default="all",
        help="Which pipeline stages to run (default: all)",
    )
    parser.add_argument(
        "--merge_workers",
        type=int,
        default=0,
        help="Merge workers (0=auto/half cores, 1=sequential/low memory, N=parallel)",
    )

    # Memory configuration arguments
    memory_group = parser.add_argument_group(
        "Memory Configuration", "Control memory usage and profiling (important for laptops)"
    )
    memory_group.add_argument(
        "--low_memory",
        action="store_true",
        help="Enable Polars low_memory mode (smaller batches, less RAM). Recommended for 32-64GB laptops.",
    )
    memory_group.add_argument(
        "--row_group_size",
        type=int,
        default=100_000,
        help="Rows per Parquet row group. Smaller = less memory. (default: 100_000)",
    )
    memory_group.add_argument(
        "--polars_threads",
        type=int,
        default=None,
        help="Max threads for Polars (overrides POLARS_MAX_THREADS env var). Default: 1 for multiprocessing",
    )
    memory_group.add_argument(
        "--rayon_threads",
        type=int,
        default=None,
        help="Max threads for Rayon (overrides RAYON_NUM_THREADS env var). Default: 1 for multiprocessing",
    )
    memory_group.add_argument(
        "--process_method",
        choices=["spawn", "fork"],
        default="spawn",
        help="Multiprocessing start method. 'spawn' = better memory isolation (default), 'fork' = faster startup",
    )

    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--no-optimize-concepts", dest="optimize_concepts", action="store_false")
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Force refresh: delete existing output directory if it exists (default: error if output exists)",
    )

    args = parser.parse_args()

    run_omop_to_meds_streaming(
        omop_dir=Path(args.omop_dir),
        output_dir=Path(args.output_dir),
        config_path=Path(args.config),
        num_workers=args.workers,
        num_shards=args.shards,
        verbose=args.verbose,
        optimize_concepts=args.optimize_concepts,
        chunk_rows=args.chunk_rows,
        run_compression=args.run_compression,
        final_compression=args.final_compression,
        merge_workers=args.merge_workers,
        pipeline_stages=args.pipeline,
        # Memory configuration
        low_memory=args.low_memory,
        row_group_size=args.row_group_size,
        polars_threads=args.polars_threads,
        rayon_threads=args.rayon_threads,
        process_method=args.process_method,
        force_refresh=args.force_refresh,
    )


if __name__ == "__main__":
    main()
