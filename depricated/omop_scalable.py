#!/usr/bin/env python3
"""
Scalable OMOP to MEDS ETL Pipeline

A modular, student-friendly implementation that processes OMOP data in stages:
  Stage 0 (optional): Build concept ID mapping from OMOP concept table
  Stage 1: Partition OMOP tables into sharded runs
  Stage 2: Merge shards using k-way merge sort

Features:
- Out-of-core processing for large datasets
- Configurable code mapping (source_value OR concept_id)
- Memory-efficient sharded architecture
- Clear, modular design for education

Usage:
    python omop_scalable.py \\
        --omop_dir /path/to/omop \\
        --output_dir /path/to/meds \\
        --config omop_etl_simple_config.json \\
        --code_mapping source_value \\
        --shards 100 \\
        --workers 14
"""

import argparse
import gzip
import hashlib
import heapq
import json
import multiprocessing as mp
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    print("ERROR: polars required. Install with: pip install polars")
    sys.exit(1)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    print("WARNING: pyarrow not available, some features may be slower")

try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

    # Fallback: tqdm is just a passthrough
    def tqdm(iterable, **kwargs):
        return iterable


# ============================================================================
# STAGE 0: CONCEPT MAPPING (OPTIONAL)
# ============================================================================


def build_concept_map(omop_dir: Path, verbose: bool = False, include_custom: bool = True) -> Dict[int, str]:
    """
    Build a mapping from concept_id to concept_code (e.g., 123456 -> "SNOMED/12345").

    This reads the OMOP concept table and creates a dictionary for fast lookups.
    Optionally includes custom/site-specific concepts via concept_relationship.

    Args:
        omop_dir: Directory containing OMOP tables
        verbose: Print progress messages
        include_custom: If True, resolve custom concepts (ID > 2B) via concept_relationship

    Returns:
        Dictionary mapping concept_id (int) -> concept_code (str)

    Memory considerations:
        - For STARR-OMOP: ~1M concepts = ~50MB in memory
        - Custom concepts add minimal overhead (flat mapping, no hierarchy)
    """
    if verbose:
        print("Building concept ID mapping from concept table...")

    start_time = time.time()
    concept_map = {}

    # Look for concept table files
    concept_dir = omop_dir / "concept"
    concept_files = []

    if concept_dir.exists() and concept_dir.is_dir():
        # Sharded concept table
        concept_files = list(concept_dir.glob("*.csv")) + list(concept_dir.glob("*.csv.gz"))
        concept_files += list(concept_dir.glob("*.parquet"))
    else:
        # Single concept file
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
            # Read concept file
            if str(file_path).endswith(".parquet"):
                df = pl.read_parquet(file_path)
            else:
                df = pl.read_csv(file_path, infer_schema_length=0)

            # Normalize column names to lowercase
            df = df.rename({c: c.lower() for c in df.columns})

            # Build concept_code = vocabulary_id/concept_code
            if "concept_id" in df.columns and "vocabulary_id" in df.columns and "concept_code" in df.columns:
                df = df.select(
                    [
                        pl.col("concept_id").cast(pl.Int64),
                        (pl.col("vocabulary_id") + pl.lit("/") + pl.col("concept_code")).alias("full_code"),
                    ]
                )

                # Add to map
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

    Args:
        omop_dir: OMOP data directory
        concept_map: Existing concept map (modified in place)
        verbose: Print progress

    Returns:
        Number of custom concepts added
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


# ============================================================================
# PRE-SCANNING FOR CONCEPT OPTIMIZATION
# ============================================================================


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
    with mp.Pool(processes=num_workers) as pool:
        worker_results = pool.map(prescan_worker, worker_args)

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


def hash_person_id(person_id: str, num_shards: int) -> int:
    """
    Hash a person_id to determine which shard it belongs to.

    Uses MD5 hash for consistent distribution across shards.
    """
    hash_val = int(hashlib.md5(str(person_id).encode()).hexdigest(), 16)
    return hash_val % num_shards


def parse_omop_datetime(value: Any) -> Optional[int]:
    """
    Parse OMOP datetime to milliseconds since epoch.

    Supports:
    - Datetime strings: "2023-01-15 10:30:00"
    - Date strings: "2023-01-15"
    - Already numeric (passthrough)

    Returns:
        Milliseconds since epoch, or None if parse fails
    """
    if value is None:
        return None

    try:
        # If already numeric, assume it's a timestamp
        if isinstance(value, (int, float)):
            return int(value)

        # Try parsing as datetime string
        value_str = str(value).strip()
        if not value_str:
            return None

        # Use Polars to parse (handles multiple formats)
        dt = pl.Series([value_str]).str.to_datetime(strict=False)
        if dt[0] is not None:
            return int(dt[0].timestamp() * 1000)  # Convert to milliseconds

        return None

    except Exception:
        return None


def find_omop_table_files(omop_dir: Path, table_name: str) -> List[Path]:
    """
    Find all files for an OMOP table.

    OMOP tables can be:
    - Single file: measurement.csv
    - Sharded directory: measurement/00000.csv.gz, measurement/00001.csv.gz, ...
    - Parquet format: measurement.parquet or measurement/00000.parquet, ...

    Args:
        omop_dir: Root OMOP directory
        table_name: Table name (e.g., "measurement", "condition_occurrence")

    Returns:
        List of file paths
    """
    files = []

    # Check for directory (sharded table)
    table_dir = omop_dir / table_name
    if table_dir.exists() and table_dir.is_dir():
        files.extend(table_dir.glob("*.csv"))
        files.extend(table_dir.glob("*.csv.gz"))
        files.extend(table_dir.glob("*.parquet"))
        return sorted(files)

    # Check for single file
    for ext in [".csv", ".csv.gz", ".parquet"]:
        file_path = omop_dir / f"{table_name}{ext}"
        if file_path.exists():
            files.append(file_path)

    return files


# ============================================================================
# STAGE 1: PARTITION - Read OMOP tables and partition into sharded runs
# ============================================================================


def extract_event_from_omop_row(
    row: Dict[str, Any],
    table_name: str,
    table_config: Dict,
    primary_key: str,
    code_mapping_type: str,
    concept_map: Optional[Dict[int, str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Extract a single MEDS event from an OMOP table row.

    This is the core transformation: OMOP row -> MEDS event

    Args:
        row: Dictionary with OMOP column values
        table_name: Name of OMOP table
        table_config: Configuration for this table
        primary_key: Name of primary key column (typically "person_id")
        code_mapping_type: Either "source_value" or "concept_id"
        concept_map: Optional concept ID mapping (only used if code_mapping_type="concept_id")

    Returns:
        MEDS event dictionary or None if invalid

    MEDS event schema:
        - person_id: Patient identifier (string)
        - time: Event timestamp in milliseconds (int)
        - code: Event code (string)
        - numeric_value: Optional numeric value (float or None)
        - text_value: Optional text value (string or None)
        - metadata: JSON string with additional fields
    """
    # Extract person_id
    person_id = row.get(primary_key.lower())
    if not person_id:
        return None

    # Extract timestamp
    time_field = table_config.get("time_field", "").lower()
    time_value = row.get(time_field)
    timestamp_ms = parse_omop_datetime(time_value)

    if timestamp_ms is None:
        # Try fallbacks
        for fallback in table_config.get("time_fallbacks", []):
            time_value = row.get(fallback.lower())
            timestamp_ms = parse_omop_datetime(time_value)
            if timestamp_ms is not None:
                break

    if timestamp_ms is None:
        return None  # Skip events without valid timestamp

    # Extract code using configured mapping
    code = None
    code_mappings = table_config.get("code_mappings", {})

    if code_mapping_type == "source_value":
        # Direct source_value extraction (no mapping)
        source_config = code_mappings.get("source_value", {})
        code_field = source_config.get("field", "").lower()
        if code_field and code_field in row:
            code = str(row[code_field]) if row[code_field] is not None else None

    elif code_mapping_type == "concept_id":
        # Map concept_id to concept_code
        if concept_map is None:
            raise ValueError("concept_id mapping requested but concept_map not provided")

        concept_config = code_mappings.get("concept_id", {})

        # Try source_concept_id first (preferred)
        source_concept_id_field = concept_config.get("source_concept_id_field", "").lower()
        if source_concept_id_field and source_concept_id_field in row:
            concept_id = row[source_concept_id_field]
            if concept_id and int(concept_id) != 0:
                code = concept_map.get(int(concept_id))

        # Fall back to concept_id
        if not code:
            concept_id_field = concept_config.get("concept_id_field", "").lower()
            if concept_id_field and concept_id_field in row:
                concept_id = row[concept_id_field]
                if concept_id and int(concept_id) != 0:
                    code = concept_map.get(int(concept_id))

        # Final fallback
        if not code:
            fallback_id = concept_config.get("fallback_concept_id")
            if fallback_id:
                code = concept_map.get(int(fallback_id))

    if not code:
        return None  # Skip events without valid code

    # Extract values
    numeric_value = None
    numeric_field = table_config.get("numeric_value_field", "").lower()
    if numeric_field and numeric_field in row:
        try:
            numeric_value = float(row[numeric_field]) if row[numeric_field] is not None else None
        except (ValueError, TypeError):
            pass

    text_value = None
    text_field = table_config.get("text_value_field", "").lower()
    if text_field and text_field in row:
        text_value = str(row[text_field]) if row[text_field] is not None else None

    # Extract metadata fields
    metadata = {"table": table_name}
    for meta_spec in table_config.get("metadata", []):
        meta_name = meta_spec["name"].lower()
        if meta_name in row and row[meta_name] is not None:
            metadata[meta_name] = row[meta_name]

    # Add end timestamp if configured
    time_end_field = table_config.get("time_end_field", "").lower()
    if time_end_field and time_end_field in row:
        end_time = parse_omop_datetime(row[time_end_field])
        if end_time:
            metadata["end_time"] = end_time

    return {
        "person_id": str(person_id),
        "time": timestamp_ms,
        "code": code,
        "numeric_value": numeric_value,
        "text_value": text_value,
        "metadata": json.dumps(metadata),
    }


def extract_canonical_event(
    row: Dict[str, Any], event_name: str, event_config: Dict, primary_key: str
) -> Optional[Dict[str, Any]]:
    """
    Extract a canonical event (BIRTH, DEATH, etc.) from an OMOP row.

    Canonical events have fixed codes defined in the config.

    Args:
        row: OMOP row dictionary
        event_name: Name of canonical event ("birth", "death", etc.)
        event_config: Configuration for this canonical event
        primary_key: Primary key column name

    Returns:
        MEDS event or None
    """
    person_id = row.get(primary_key.lower())
    if not person_id:
        return None

    # Get canonical code
    code = event_config.get("code", f"MEDS_{event_name.upper()}")

    # Extract timestamp (with special handling for birth)
    time_field = event_config.get("time_field", "").lower()
    time_value = row.get(time_field)
    timestamp_ms = parse_omop_datetime(time_value)

    # For birth events, construct from year/month/day if datetime not available
    if not timestamp_ms and event_name == "birth":
        year = row.get("year_of_birth")
        month = row.get("month_of_birth", 1)
        day = row.get("day_of_birth", 1)
        if year:
            try:
                dt = pl.datetime(int(year), int(month), int(day))
                timestamp_ms = int(dt.timestamp() * 1000)
            except:
                pass

    if not timestamp_ms:
        return None

    # Extract metadata
    metadata = {"table": event_config["table"], "canonical_event": event_name}
    for meta_spec in event_config.get("metadata", []):
        meta_name = meta_spec["name"].lower()
        if meta_name in row and row[meta_name] is not None:
            metadata[meta_name] = row[meta_name]

    return {
        "person_id": str(person_id),
        "time": timestamp_ms,
        "code": code,
        "numeric_value": None,
        "text_value": None,
        "metadata": json.dumps(metadata),
    }


def transform_omop_to_meds_vectorized(
    df: pl.DataFrame,
    table_name: str,
    table_config: Dict,
    primary_key: str,
    code_mapping_type: str,
    concept_map: Optional[Dict[int, str]],
    concept_df: Optional[pl.DataFrame] = None,
) -> pl.DataFrame:
    """
    Transform OMOP DataFrame to MEDS format using vectorized Polars operations.

    This is 10-100x faster than row-by-row processing!

    Args:
        df: Input OMOP DataFrame (with lowercase column names)
        table_name: OMOP table name
        table_config: Table configuration
        primary_key: Primary key column
        code_mapping_type: "source_value" or "concept_id"
        concept_map: Concept mapping dictionary

    Returns:
        MEDS DataFrame with columns: person_id, time, code, numeric_value, text_value, metadata
    """
    pk_lower = primary_key.lower()

    # ===== 1. Extract person_id =====
    if pk_lower not in df.columns:
        return pl.DataFrame()  # Empty result

    # ===== 2. Extract time with fallbacks =====
    time_field = table_config.get("time_field", "").lower()
    time_fallbacks = [f.lower() for f in table_config.get("time_fallbacks", [])]

    # Build coalesce expression for time
    time_candidates = [time_field] + time_fallbacks
    time_candidates = [c for c in time_candidates if c and c in df.columns]

    if not time_candidates:
        return pl.DataFrame()  # Can't proceed without time

    # Parse datetime and convert to milliseconds
    # Try each candidate in order using coalesce
    time_exprs = []
    for col in time_candidates:
        # Try parsing as datetime, then convert to epoch milliseconds
        time_exprs.append(pl.col(col).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False).dt.epoch(time_unit="ms"))

    time_expr = pl.coalesce(time_exprs).alias("time")

    # ===== 3. Extract code =====
    code_mappings = table_config.get("code_mappings", {})

    if code_mapping_type == "source_value":
        # Direct source value extraction
        source_config = code_mappings.get("source_value", {})
        code_field = source_config.get("field", "").lower()

        if code_field and code_field in df.columns:
            code_expr = pl.col(code_field).cast(pl.Utf8).alias("code")
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
            # Coalesce to get the best available concept_id
            concept_id_col = pl.coalesce([pl.col(c).cast(pl.Int64) for c in join_candidates]).alias("_concept_id_join")
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
        numeric_expr = pl.col(numeric_field).cast(pl.Float32, strict=False).alias("numeric_value")
    else:
        numeric_expr = pl.lit(None, dtype=pl.Float32).alias("numeric_value")

    # ===== 5. Extract text_value =====
    text_field = table_config.get("text_value_field", "").lower()
    if text_field and text_field in df.columns:
        text_expr = pl.col(text_field).cast(pl.Utf8).alias("text_value")
    else:
        text_expr = pl.lit(None, dtype=pl.Utf8).alias("text_value")

    # ===== 6. Build metadata JSON =====
    # For metadata, we'll create a struct and then convert to JSON string
    metadata_fields = table_config.get("metadata", [])
    metadata_exprs = [pl.lit(table_name).alias("table")]

    for meta_spec in metadata_fields:
        meta_name = meta_spec["name"].lower()
        if meta_name in df.columns:
            metadata_exprs.append(pl.col(meta_name).alias(meta_name))

    # Add end_time if configured
    time_end_field = table_config.get("time_end_field", "").lower()
    if time_end_field and time_end_field in df.columns:
        end_time_expr = (
            pl.col(time_end_field).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False).dt.epoch(time_unit="ms")
        ).alias("end_time")
        metadata_exprs.append(end_time_expr)

    # Create struct and convert to JSON - pass list of expressions, not dict
    metadata_expr = pl.struct(metadata_exprs).struct.json_encode().alias("metadata")

    # ===== 7. Select and build final MEDS schema =====
    result_df = df.select(
        [
            pl.col(pk_lower).cast(pl.Utf8).alias("person_id"),
            time_expr,
            code_expr,
            numeric_expr,
            text_expr,
            metadata_expr,
        ]
    )

    return result_df


def transform_canonical_event_vectorized(
    df: pl.DataFrame, event_name: str, event_config: Dict, primary_key: str
) -> pl.DataFrame:
    """
    Transform OMOP DataFrame to canonical MEDS events (BIRTH, DEATH) using vectorized operations.

    Args:
        df: Input OMOP DataFrame (with lowercase column names)
        event_name: Name of canonical event
        event_config: Canonical event configuration
        primary_key: Primary key column

    Returns:
        MEDS DataFrame
    """
    pk_lower = primary_key.lower()

    if pk_lower not in df.columns:
        return pl.DataFrame()

    # Get canonical code
    code = event_config.get("code", f"MEDS_{event_name.upper()}")

    # Extract time with fallbacks
    time_field = event_config.get("time_field", "").lower()
    time_fallbacks = [f.lower() for f in event_config.get("time_fallbacks", [])]

    time_candidates = [time_field] + time_fallbacks
    time_candidates = [c for c in time_candidates if c and c in df.columns]

    if not time_candidates:
        return pl.DataFrame()

    # Parse time
    time_exprs = []
    for col in time_candidates:
        time_exprs.append(pl.col(col).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False).dt.epoch(time_unit="ms"))

    time_expr = pl.coalesce(time_exprs).alias("time")

    # Build metadata
    metadata_fields = event_config.get("metadata", [])
    metadata_exprs = [pl.lit(event_config.get("table", event_name)).alias("table")]

    for meta_spec in metadata_fields:
        meta_name = meta_spec["name"].lower()
        if meta_name in df.columns:
            metadata_exprs.append(pl.col(meta_name).alias(meta_name))

    metadata_expr = pl.struct(metadata_exprs).struct.json_encode().alias("metadata")

    # Build result
    result_df = df.select(
        [
            pl.col(pk_lower).cast(pl.Utf8).alias("person_id"),
            time_expr,
            pl.lit(code).alias("code"),
            pl.lit(None, dtype=pl.Float32).alias("numeric_value"),
            pl.lit(None, dtype=pl.Utf8).alias("text_value"),
            metadata_expr,
        ]
    )

    return result_df


def process_omop_file(
    file_path: Path,
    table_name: str,
    table_config: Dict,
    primary_key: str,
    code_mapping_type: str,
    concept_map: Optional[Dict[int, str]],
    concept_df: Optional[pl.DataFrame],
    is_canonical: bool = False,
    canonical_event_name: str = None,
    chunk_size: int = 100000,
) -> Tuple[List[Dict[str, Any]], int, int]:
    """
    Process a single OMOP file and extract MEDS events using VECTORIZED Polars operations.

    Uses STREAMING/CHUNKED processing to limit memory usage per file.
    Processes files in chunks to stay within memory limits.

    Args:
        file_path: Path to OMOP file
        table_name: Table name
        table_config: Table configuration
        primary_key: Primary key column
        code_mapping_type: "source_value" or "concept_id"
        concept_map: Concept mapping (if using concept_id)
        is_canonical: Whether this is a canonical event
        canonical_event_name: Name of canonical event if applicable
        chunk_size: Rows per chunk for memory-limited processing

    Returns:
        Tuple of (events list, total_rows_input, total_rows_filtered)
    """
    all_events = []
    total_rows_before_filter = 0
    total_rows_after_filter = 0

    try:
        if str(file_path).endswith(".parquet"):
            # For parquet, use streaming scan
            lazy_df = pl.scan_parquet(file_path)
            lazy_df = lazy_df.rename({c: c.lower() for c in lazy_df.columns})

            # Collect with streaming enabled (Polars handles memory internally)
            df = lazy_df.collect(streaming=True)

            # Process in chunks to limit memory
            total_rows = len(df)
            for start_idx in range(0, total_rows, chunk_size):
                end_idx = min(start_idx + chunk_size, total_rows)
                chunk_df = df.slice(start_idx, end_idx - start_idx)

                # Transform chunk
                if is_canonical:
                    transformed = transform_canonical_event_vectorized(
                        chunk_df, canonical_event_name, table_config, primary_key
                    )
                else:
                    transformed = transform_omop_to_meds_vectorized(
                        chunk_df, table_name, table_config, primary_key, code_mapping_type, concept_map, concept_df
                    )

                # Filter null rows
                transformed = transformed.filter(
                    pl.col("person_id").is_not_null() & pl.col("time").is_not_null() & pl.col("code").is_not_null()
                )

                # Add to results
                all_events.extend(transformed.to_dicts())
        else:
            # For CSV, use batched reading (most memory-efficient)
            reader = pl.read_csv_batched(file_path, infer_schema_length=0, batch_size=chunk_size)

            while True:
                batch = reader.next_batches(1)
                if batch is None or len(batch) == 0:
                    break

                df = batch[0]
                # Normalize column names
                df = df.rename({c: c.lower() for c in df.columns})

                # Transform batch
                if is_canonical:
                    transformed = transform_canonical_event_vectorized(
                        df, canonical_event_name, table_config, primary_key
                    )
                else:
                    transformed = transform_omop_to_meds_vectorized(
                        df, table_name, table_config, primary_key, code_mapping_type, concept_map, concept_df
                    )

                # Track rows before filtering for stats
                rows_before_filter = len(transformed)

                # Filter null rows
                transformed = transformed.filter(
                    pl.col("person_id").is_not_null() & pl.col("time").is_not_null() & pl.col("code").is_not_null()
                )

                rows_after_filter = len(transformed)

                # Accumulate stats
                total_rows_before_filter += rows_before_filter
                total_rows_after_filter += rows_after_filter

                # Add to results
                all_events.extend(transformed.to_dicts())

    except Exception as e:
        print(f"ERROR processing {file_path}: {e}")
        import traceback

        traceback.print_exc()
        return [], 0, 0

    return all_events, total_rows_before_filter, total_rows_before_filter - total_rows_after_filter


def partition_worker(args: Tuple) -> Dict:
    """
    Worker process for Stage 1: Partition OMOP files into sharded runs.

    Each worker:
    1. Processes a batch of OMOP files
    2. Extracts MEDS events
    3. Hashes events by person_id into shards
    4. Writes sorted runs for each shard

    Args:
        args: (worker_id, file_batch, config, num_shards, rows_per_run, temp_dir,
               code_mapping_type, concept_map, concept_df, chunk_size, compression, progress_counter)
    """
    (
        worker_id,
        file_batch,
        config,
        num_shards,
        rows_per_run,
        temp_dir,
        code_mapping_type,
        concept_map,
        concept_df,
        chunk_size,
        compression,
        progress_counter,
    ) = args

    start_time = time.time()

    # Create buffers for each shard
    shard_buffers = {i: [] for i in range(num_shards)}
    run_sequence = 0
    rows_processed = 0
    rows_input = 0  # Total rows read from source
    rows_filtered = 0  # Rows dropped (null person_id, time, or code)
    files_processed = 0

    primary_key = config["primary_key"]

    # concept_df is passed from main process (shared via copy-on-write on Unix/Mac)
    # No memory duplication across workers!

    for table_name, file_path, table_config, is_canonical, event_name in file_batch:
        # Process file in chunks to limit memory
        events, file_rows_input, file_rows_filtered = process_omop_file(
            file_path,
            table_name,
            table_config,
            primary_key,
            code_mapping_type,
            concept_map,
            concept_df,
            is_canonical,
            event_name,
            chunk_size,
        )

        # Track input vs output
        rows_input += file_rows_input
        rows_filtered += file_rows_filtered

        # Partition into shards
        for event in events:
            shard_id = hash_person_id(event["person_id"], num_shards)
            shard_buffers[shard_id].append(event)
            rows_processed += 1

            # Flush if buffer full
            if len(shard_buffers[shard_id]) >= rows_per_run:
                write_shard_run(shard_id, shard_buffers[shard_id], temp_dir, worker_id, run_sequence, compression)
                shard_buffers[shard_id] = []
                run_sequence += 1

        files_processed += 1

        # Update shared progress counter
        if progress_counter is not None:
            # Manager.Value doesn't need get_lock() - it's automatically thread-safe
            progress_counter.value += 1

    # Flush remaining buffers
    for shard_id, buffer in shard_buffers.items():
        if buffer:
            write_shard_run(shard_id, buffer, temp_dir, worker_id, run_sequence, compression)
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


def write_shard_run(
    shard_id: int, events: List[Dict], temp_dir: Path, worker_id: int, run_seq: int, compression: str = "lz4"
):
    """
    Write a sorted run of events to disk.

    Events are sorted by (person_id, time) before writing.
    Uses Parquet format for efficient storage.

    Args:
        shard_id: Shard identifier
        events: List of MEDS events
        temp_dir: Temporary directory
        worker_id: Worker identifier
        run_seq: Run sequence number
        compression: Compression algorithm (lz4, zstd, snappy, etc.)
    """
    if not events:
        return

    # Create shard directory
    shard_dir = temp_dir / f"shard={shard_id}"
    shard_dir.mkdir(parents=True, exist_ok=True)

    # Define explicit schema to handle mixed None/value types across chunks
    schema = {
        "person_id": pl.Int64,
        "time": pl.Int64,
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
        "metadata": pl.Utf8,
    }

    # Convert to DataFrame with explicit schema and sort
    df = pl.DataFrame(events, schema=schema)
    df = df.sort(["person_id", "time"])

    # Write to parquet
    output_file = shard_dir / f"run-worker{worker_id}-{run_seq}.parquet"
    df.write_parquet(output_file, compression=compression)


# ============================================================================
# STAGE 2: MERGE - Merge sorted runs within each shard
# ============================================================================


class ParquetRunIterator:
    """
    Streaming iterator over a sorted parquet run file for k-way merge.

    Uses batched reading to minimize memory footprint - critical for large files!
    Only keeps a small buffer in memory at a time.
    """

    def __init__(self, file_path: Path, batch_size: int = 50000):
        self.file_path = file_path
        self.batch_size = batch_size

        # Use scan_parquet for lazy loading
        self.lazy_df = pl.scan_parquet(file_path)
        self.total_rows = self.lazy_df.select(pl.count()).collect().item()

        self.offset = 0
        self.buffer = []
        self.buffer_index = 0

        # Load first batch
        self._load_next_batch()

    def _load_next_batch(self):
        """Load next batch of rows from file into buffer."""
        if self.offset >= self.total_rows:
            self.buffer = []
            self.buffer_index = 0
            return

        # Read batch using slice
        batch_df = self.lazy_df.slice(self.offset, self.batch_size).collect()
        self.buffer = batch_df.to_dicts()
        self.buffer_index = 0
        self.offset += len(self.buffer)

    def __iter__(self):
        return self

    def __next__(self):
        # If current buffer exhausted, load next batch
        if self.buffer_index >= len(self.buffer):
            self._load_next_batch()

        # If still no data, we're done
        if not self.buffer:
            raise StopIteration

        row = self.buffer[self.buffer_index]
        self.buffer_index += 1
        return row


def kway_merge_shard(
    run_files: List[Path], output_file: Path, batch_size: int = 100000, compression: str = "lz4"
) -> int:
    """
    Merge multiple sorted run files using k-way merge with STREAMING.

    Uses a priority queue (heap) to efficiently merge without loading
    all data into memory at once. Each run file is read in batches.

    Args:
        run_files: List of sorted parquet files
        output_file: Output file path
        batch_size: Rows to batch before writing
        compression: Compression algorithm

    Returns:
        Total rows written
    """
    if not run_files:
        return 0

    # Define explicit schema to prevent inference issues across files
    schema = {
        "person_id": pl.Int64,
        "time": pl.Int64,
        "code": pl.Utf8,
        "numeric_value": pl.Float64,
        "text_value": pl.Utf8,
        "metadata": pl.Utf8,
    }

    # Initialize PyArrow writer for incremental writing (no full file reloads!)
    writer = None
    schema_pa = None

    # Open all run files as STREAMING iterators
    heap = []
    for i, file_path in enumerate(run_files):
        try:
            # Batch size for file reading - smaller = less memory per file
            it = ParquetRunIterator(file_path, batch_size=50000)
            first_row = next(it)
            # Push to heap: (person_id, time, file_idx, row, iterator)
            sort_key = (first_row["person_id"], first_row["time"])
            heapq.heappush(heap, (sort_key, i, first_row, it))
        except StopIteration:
            pass  # Empty file

    # Accumulate rows in batches for writing
    batch = []
    total_rows = 0

    # K-way merge
    while heap:
        sort_key, file_idx, row, iterator = heapq.heappop(heap)

        batch.append(row)
        total_rows += 1

        # Write batch when full
        if len(batch) >= batch_size:
            if writer is None:
                # Initialize writer on first batch with explicit schema
                df = pl.DataFrame(batch, schema=schema)
                schema_pa = df.to_arrow().schema
                writer = pq.ParquetWriter(output_file, schema_pa, compression=compression)

            # Write batch incrementally (no file reload!)
            table = pl.DataFrame(batch, schema=schema).to_arrow()
            writer.write_table(table)
            batch = []

        # Get next row from same file
        try:
            next_row = next(iterator)
            next_key = (next_row["person_id"], next_row["time"])
            heapq.heappush(heap, (next_key, file_idx, next_row, iterator))
        except StopIteration:
            pass  # File exhausted

    # Write final batch
    if batch:
        if writer is None:
            # Edge case: all data fits in one batch
            df = pl.DataFrame(batch, schema=schema)
            df.write_parquet(output_file, compression=compression)
        else:
            table = pl.DataFrame(batch, schema=schema).to_arrow()
            writer.write_table(table)

    # Close writer
    if writer is not None:
        writer.close()

    return total_rows


def write_parquet_batch(batch: List[Dict], output_file: Path, append: bool = False, compression: str = "lz4"):
    """
    Write a batch of rows to parquet file.

    NOTE: This function is DEPRECATED in favor of incremental writing in kway_merge_shard.
    Keeping for backwards compatibility but should not be used for large files.

    Args:
        batch: List of event dictionaries
        output_file: Output path
        append: Whether to append to existing file
        compression: Compression algorithm
    """
    df = pl.DataFrame(batch)

    if append and output_file.exists():
        # WARNING: This loads entire file into memory - only use for small files!
        existing = pl.read_parquet(output_file)
        df = pl.concat([existing, df])

    df.write_parquet(output_file, compression=compression)


def merge_worker(args: Tuple) -> Dict:
    """
    Worker process for Stage 2: Merge all runs for a shard.

    Args:
        args: (shard_id, temp_dir, output_dir, batch_size, compression)
    """
    shard_id, temp_dir, output_dir, batch_size, compression = args

    start_time = time.time()

    shard_dir = temp_dir / f"shard={shard_id}"
    if not shard_dir.exists():
        return {"shard_id": shard_id, "rows": 0, "elapsed_sec": 0}

    # Find all run files
    run_files = list(shard_dir.glob("run-*.parquet"))
    if not run_files:
        return {"shard_id": shard_id, "rows": 0, "elapsed_sec": 0}

    # Create output directory
    output_shard_dir = output_dir / f"shard={shard_id}"
    output_shard_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_shard_dir / "part-0000.parquet"

    # Merge runs with streaming (OUT-OF-CORE!)
    row_count = kway_merge_shard(run_files, output_file, batch_size, compression)

    elapsed = time.time() - start_time

    return {"shard_id": shard_id, "rows": row_count, "files_merged": len(run_files), "elapsed_sec": elapsed}


# ============================================================================
# PIPELINE ORCHESTRATION
# ============================================================================


def discover_omop_files(omop_dir: Path, config: Dict, code_mapping_type: str) -> List[Tuple]:
    """
    Discover all OMOP files to process based on config.

    Returns:
        List of (table_name, file_path, table_config, is_canonical, event_name) tuples
    """
    files = []

    # Process canonical events
    for event_name, event_config in config.get("canonical_events", {}).items():
        table_name = event_config["table"]
        table_files = find_omop_table_files(omop_dir, table_name)

        for file_path in table_files:
            files.append((table_name, file_path, event_config, True, event_name))

    # Process regular tables
    for table_name, table_config in config.get("tables", {}).items():
        # Check if this table has the requested code mapping
        code_mappings = table_config.get("code_mappings", {})
        if code_mapping_type not in code_mappings:
            print(f"WARNING: Table '{table_name}' does not have '{code_mapping_type}' mapping, skipping")
            continue

        table_files = find_omop_table_files(omop_dir, table_name)

        for file_path in table_files:
            files.append((table_name, file_path, table_config, False, None))

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
    polars_threads: Optional[int] = None,
    memory_limit_mb: Optional[int] = None,
    chunk_size: Optional[int] = None,
    compression: str = "lz4",
    optimize_concepts: bool = True,
):
    """
    Run the complete ETL pipeline.

    Stages:
        0. Build concept mapping (if using concept_id mapping)
        1. Partition OMOP files into sharded runs
        2. Merge runs within each shard
    """
    import os

    # Configure Polars threading to avoid oversubscription
    # When using multiprocessing, limit Polars internal threads per worker
    cpu_count = mp.cpu_count()
    if polars_threads is None:
        polars_threads = max(1, cpu_count // num_workers)

    os.environ["POLARS_MAX_THREADS"] = str(polars_threads)

    # Calculate chunk_size and rows_per_run from memory limit if provided
    # Estimate: ~1000 bytes per row in memory (dict overhead + data)
    if memory_limit_mb:
        # Total available memory for data (reserve 30% for Polars/overhead)
        available_mb = memory_limit_mb * 0.7
        bytes_per_row = 1000  # Conservative estimate for Python dict in memory
        total_rows_budget = int((available_mb * 1024 * 1024) / bytes_per_row)

        # Split budget: 30% for chunk reading, 70% for shard buffers
        if chunk_size is None:
            chunk_size = int(total_rows_budget * 0.3)
            chunk_size = max(10000, min(chunk_size, 1000000))  # Between 10K and 1M rows

        # Critically important: limit rows_per_run based on memory budget
        # Each worker has num_shards buffers, but typically only a few fill up
        # Assume ~20% of shards are active at once
        active_shards = max(1, int(num_shards * 0.2))
        auto_rows_per_run = int((total_rows_budget * 0.7) / active_shards)
        auto_rows_per_run = max(10000, min(auto_rows_per_run, 500000))  # Between 10K and 500K

        # Override rows_per_run with auto-tuned value
        if rows_per_run != auto_rows_per_run:
            print(f"\n=== AUTO-TUNED FROM MEMORY LIMIT ===")
            print(f"Memory budget per worker: {memory_limit_mb} MB")
            print(f"Estimated active shards: {active_shards} (of {num_shards} total)")
            print(f"Chunk size: {chunk_size:,} rows")
            print(f"Overriding --rows_per_run {rows_per_run:,} → {auto_rows_per_run:,} rows (memory-tuned)")
            rows_per_run = auto_rows_per_run
    else:
        if chunk_size is None:
            chunk_size = 100000  # Default

    print(f"\n=== MEMORY & THREAD CONFIGURATION ===")
    print(f"CPU cores: {cpu_count}")
    print(f"Workers (processes): {num_workers}")
    print(f"Polars threads per worker: {polars_threads}")
    print(f"Total threads: {num_workers * polars_threads}")
    if memory_limit_mb:
        print(f"Memory limit per worker: {memory_limit_mb} MB")
    print(f"Chunk size (rows per batch): {chunk_size:,}")

    pipeline_start = time.time()

    # Create directories
    temp_dir = output_dir / "temp"
    final_dir = output_dir / "data"
    temp_dir.mkdir(parents=True, exist_ok=True)
    final_dir.mkdir(parents=True, exist_ok=True)

    # ========== STAGE 0: Build concept mapping (if needed) ==========
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
            used_concept_ids = prescan_concept_ids(omop_dir, config, num_workers, verbose)

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

    # ========== STAGE 1: Partition ==========
    print(f"\n=== STAGE 1: PARTITIONING ===")
    print(f"Code mapping: {code_mapping_type}")
    print(f"Shards: {num_shards}")
    print(f"Workers: {num_workers}")

    stage1_start = time.time()

    # Discover files
    files = discover_omop_files(omop_dir, config, code_mapping_type)
    print(f"Found {len(files)} files to process")

    if not files:
        print("ERROR: No files found to process")
        return

    # Distribute files across workers using greedy load balancing
    # This ensures each worker gets approximately equal total file size
    print("Balancing workload across workers...")

    # Get file sizes
    file_info = []
    for file_tuple in files:
        file_path = file_tuple[1]  # (table_name, file_path, table_config, is_canonical, event_name)
        try:
            file_size = file_path.stat().st_size
        except OSError:
            file_size = 0
        file_info.append((file_tuple, file_size))

    # Sort by size (largest first) for better greedy assignment
    file_info.sort(key=lambda x: x[1], reverse=True)

    # Greedy assignment: assign each file to worker with smallest current load
    worker_loads = [[] for _ in range(num_workers)]
    worker_sizes = [0] * num_workers

    for file_tuple, size in file_info:
        # Find worker with smallest load
        min_worker = worker_sizes.index(min(worker_sizes))
        worker_loads[min_worker].append(file_tuple)
        worker_sizes[min_worker] += size

    # Print load distribution
    total_size = sum(worker_sizes)
    for i, size in enumerate(worker_sizes):
        pct = 100 * size / total_size if total_size > 0 else 0
        print(f"  Worker {i}: {len(worker_loads[i])} files, {size / 1024**3:.2f} GB ({pct:.1f}%)")

    # Create shared progress counter for real-time updates
    # Use Manager for macOS compatibility (spawn vs fork)
    manager = mp.Manager()
    progress_counter = manager.Value("i", 0)

    # Create worker arguments (concept_df is shared via copy-on-write fork)
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
            concept_df,
            chunk_size,
            compression,
            progress_counter,
        )
        for i, batch in enumerate(worker_loads)
    ]

    # Run workers with progress tracking
    print(f"\nProcessing {len(files)} files across {num_workers} workers...")

    with mp.Pool(processes=num_workers) as pool:
        # Start workers asynchronously
        async_result = pool.map_async(partition_worker, worker_args)

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

    # ========== STAGE 2: Merge ==========
    print(f"\n=== STAGE 2: MERGING ===")
    print(f"Merging {num_shards} shards")

    stage2_start = time.time()

    # Create worker arguments
    merge_args = [(shard_id, temp_dir, final_dir, batch_size, compression) for shard_id in range(num_shards)]

    # Run merge workers with progress tracking
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
    total_rows = sum(r["rows"] for r in results)

    print(f"\nStage 2 completed:")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Time: {stage2_elapsed:.2f}s")
    print(f"  Throughput: {total_rows/stage2_elapsed:,.0f} rows/sec")

    # ========== Complete ==========
    pipeline_elapsed = time.time() - pipeline_start

    print(f"\n=== PIPELINE COMPLETE ===")
    print(f"Total time: {pipeline_elapsed:.2f}s ({pipeline_elapsed/60:.1f} minutes)")
    print(f"Output directory: {final_dir}")

    # Write summary
    summary = {
        "code_mapping": code_mapping_type,
        "num_shards": num_shards,
        "num_workers": num_workers,
        "total_rows": total_rows,
        "total_time_sec": pipeline_elapsed,
        "stage1_time_sec": stage1_elapsed,
        "stage2_time_sec": stage2_elapsed,
    }

    with open(output_dir / "etl_summary.json", "w") as f:
        json.dump(summary, f, indent=2)


# ============================================================================
# MAIN
# ============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Scalable OMOP to MEDS ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using source values (no concept mapping)
  python omop_scalable.py \\
      --omop_dir /data/omop \\
      --output_dir /data/meds \\
      --config omop_etl_simple_config.json \\
      --code_mapping source_value \\
      --shards 100 --workers 14
  
  # Using concept ID mapping  
  python omop_scalable.py \\
      --omop_dir /data/omop \\
      --output_dir /data/meds \\
      --config omop_etl_simple_config.json \\
      --code_mapping concept_id \\
      --shards 100 --workers 14

Performance Tuning:
  The ETL uses TWO levels of parallelism:
  1. Process-level: Multiple workers process files in parallel
  2. Thread-level: Polars internally parallelizes operations within each file
  
  By default, POLARS_MAX_THREADS = CPU_CORES / NUM_WORKERS to avoid oversubscription.
  
  Memory Management:
  - Use --memory_limit_mb to set per-worker memory limit (e.g., --memory_limit_mb 1024 for 1GB)
  - Auto-tunes BOTH chunk_size (file reading) AND rows_per_run (shard buffer size)
  - Without --memory_limit_mb, defaults use 500K rows_per_run (may use 5-10GB per worker!)
  - Files are processed in chunks to stay within memory limits
  
  Memory Tuning Formula (when --memory_limit_mb is set):
  - Total budget = memory_limit_mb * 0.7 (30% reserved for overhead)
  - 30% of budget → chunk_size (file reading batches)
  - 70% of budget → rows_per_run (shard buffer accumulation)
  - rows_per_run is divided by ~20% active shards
  
  Recommendations:
  - Many small files: Use more workers (--workers 16), fewer Polars threads
  - Few large files: Use fewer workers (--workers 4), more Polars threads
  - Memory-constrained: ALWAYS use --memory_limit_mb (e.g., --memory_limit_mb 1024 for 1GB)
    * 8 workers × 1GB = 8GB total system memory usage
    * Without memory limit, each worker may use 5-10GB!
  - Total threads ≈ CPU cores for best performance
  
  Example with memory limit:
  python omop_scalable.py \\
      --omop_dir /data/omop \\
      --output_dir /data/meds \\
      --config config.json \\
      --code_mapping source_value \\
      --workers 8 \\
      --memory_limit_mb 1024 \\
      --shards 100
        """,
    )

    parser.add_argument("--omop_dir", required=True, help="Root directory containing OMOP tables")
    parser.add_argument("--output_dir", required=True, help="Output directory for MEDS data")
    parser.add_argument("--config", required=True, help="Path to ETL configuration JSON file")
    parser.add_argument(
        "--code_mapping",
        choices=["source_value", "concept_id"],
        required=True,
        help="Code mapping strategy: source_value (direct) or concept_id (via OMOP concept table)",
    )
    parser.add_argument("--shards", type=int, default=100, help="Number of shards (default: 100)")
    parser.add_argument("--workers", type=int, default=mp.cpu_count(), help="Number of workers (default: CPU count)")
    parser.add_argument(
        "--rows_per_run",
        type=int,
        default=500000,
        help="Rows per run file (default: 500000, auto-tuned if --memory_limit_mb is set)",
    )
    parser.add_argument("--batch_size", type=int, default=100000, help="Batch size for merging (default: 100000)")
    parser.add_argument(
        "--polars_threads",
        type=int,
        default=None,
        help="Polars threads per worker (default: auto = CPU_CORES / WORKERS)",
    )
    parser.add_argument(
        "--memory_limit_mb",
        type=int,
        default=None,
        help="Memory limit per worker in MB (e.g., 1024 for 1GB). Auto-calculates chunk_size.",
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=None,
        help="Rows per chunk when processing files (default: 100000 or auto from --memory_limit_mb)",
    )
    parser.add_argument(
        "--compression",
        choices=["lz4", "zstd", "snappy", "gzip", "brotli", "uncompressed"],
        default="lz4",
        help="Parquet compression algorithm (default: lz4 for speed, zstd for size)",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument(
        "--no-optimize-concepts",
        dest="optimize_concepts",
        action="store_false",
        help="Disable concept map optimization (default: enabled for faster joins)",
    )

    args = parser.parse_args()

    # Print configuration
    print("=== SCALABLE OMOP TO MEDS ETL ===")
    print(f"OMOP directory: {args.omop_dir}")
    print(f"Output directory: {args.output_dir}")
    print(f"Config file: {args.config}")
    print(f"Code mapping: {args.code_mapping}")
    print(f"Shards: {args.shards}")
    print(f"Workers: {args.workers}")

    # Load config
    with open(args.config, "r") as f:
        config = json.load(f)

    # Run pipeline
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
        chunk_size=args.chunk_size,
        compression=args.compression,
        optimize_concepts=args.optimize_concepts,
    )


if __name__ == "__main__":
    main()
