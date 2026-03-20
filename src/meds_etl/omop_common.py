"""
Shared OMOP ETL logic for omop.py and omop_streaming.py.

Contains all Stage 1 (OMOP → MEDS Unsorted) functionality:
- Schema utilities
- Configuration validation
- Concept map building and pre-scanning
- OMOP → MEDS transformation
- Worker functions
- File discovery
"""

from __future__ import annotations

import multiprocessing as mp
import pickle
import re
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import polars as pl
import pyarrow.parquet as pq
from tqdm import tqdm

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


def get_property_column_info(config: Dict) -> Dict[str, str]:
    """
    Extract property column types from config.

    Returns mapping: output_column_name → type_string (from config)

    Handles aliasing: uses "alias" if present, otherwise uses "name".
    Validates that columns with the same name have consistent types across all tables.
    Supports both "properties" (preferred) and "metadata" (backwards compat) keys.
    """
    col_types = {}
    col_sources = {}

    for event_name, event_config in config.get("canonical_events", {}).items():
        properties = event_config.get("properties") or event_config.get("metadata", [])
        for meta_spec in properties:
            col_name = meta_spec.get("alias", meta_spec["name"])
            col_type = meta_spec.get("type", "string")

            if col_name in col_types:
                if col_types[col_name] != col_type:
                    raise ValueError(
                        f"Property column '{col_name}' has inconsistent types in config:\n"
                        f"  {col_sources[col_name]}: {col_types[col_name]}\n"
                        f"  canonical_event '{event_name}': {col_type}\n"
                        f"All instances of the same column must have the same type!"
                    )
            else:
                col_types[col_name] = col_type
                col_sources[col_name] = f"canonical_event '{event_name}'"

    for table_name, table_config in config.get("tables", {}).items():
        properties = table_config.get("properties") or table_config.get("metadata", [])
        for meta_spec in properties:
            col_name = meta_spec.get("alias", meta_spec["name"])
            col_type = meta_spec.get("type", "string")

            if col_name in col_types:
                if col_types[col_name] != col_type:
                    raise ValueError(
                        f"Property column '{col_name}' has inconsistent types in config:\n"
                        f"  {col_sources[col_name]}: {col_types[col_name]}\n"
                        f"  table '{table_name}': {col_type}\n"
                        f"All instances of the same column must have the same type!"
                    )
            else:
                col_types[col_name] = col_type
                col_sources[col_name] = f"table '{table_name}'"

    return col_types


# Backwards-compatible alias
get_metadata_column_info = get_property_column_info


def get_meds_schema_from_config(config: Dict) -> Dict[str, type]:
    """
    Build global MEDS schema from config.

    Core MEDS columns (FIXED):
    - subject_id: Int64
    - time: Datetime(us)
    - code: Utf8
    - numeric_value: Float32 (nullable, matches MEDS standard)
    - text_value: Utf8 (nullable)
    - end: Datetime(us) (nullable)

    Plus all property columns from config with consistent types.
    This ensures ALL output files have the same schema.
    """
    schema = {
        "subject_id": pl.Int64,
        "time": pl.Datetime("us"),
        "code": pl.Utf8,
        "numeric_value": pl.Float32,
        "text_value": pl.Utf8,
        "end": pl.Datetime("us"),
    }

    col_type_info = get_property_column_info(config)

    for col_name, type_str in sorted(col_type_info.items()):
        schema[col_name] = config_type_to_polars(type_str)

    return schema


# ============================================================================
# CONFIGURATION VALIDATION
# ============================================================================


def validate_config_against_data(omop_dir: Path, config: Dict, verbose: bool = False) -> None:
    """
    Validate ETL configuration against actual OMOP data schema.

    Ensures all configured fields exist in the data with correct types.
    Exits with error if validation fails.
    """
    from meds_etl.config_integration import extract_column_names_from_template

    if verbose:
        print("\n" + "=" * 70)
        print("VALIDATING ETL CONFIG AGAINST DATA SCHEMA")
        print("=" * 70)

    errors = []
    tables_to_check: Dict[str, List] = {}

    for _event_name, event_config in config.get("canonical_events", {}).items():
        table_name = event_config["table"]
        if table_name not in tables_to_check:
            tables_to_check[table_name] = []
        tables_to_check[table_name].append(event_config)

    for table_name, table_config in config.get("tables", {}).items():
        if table_name not in tables_to_check:
            tables_to_check[table_name] = []
        tables_to_check[table_name].append(table_config)

    for table_name, configs in tables_to_check.items():
        table_dir = omop_dir / table_name

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

        try:
            df_schema = pl.scan_parquet(sample_file).collect_schema()
            df_schema = {col.lower(): dtype for col, dtype in df_schema.items()}
            df_schema = pl.Schema(df_schema)
        except Exception as e:
            errors.append(f"Table '{table_name}': Error reading schema: {e}")
            continue

        pre_join_columns: Dict[str, pl.DataType] = {}
        for tc in configs:
            for spec in tc.get("pre_join", []):
                join_table = spec.get("table", "")
                join_dir = omop_dir / join_table
                join_sample = None
                if join_dir.is_dir():
                    join_parquets = list(join_dir.glob("*.parquet"))
                    if join_parquets:
                        join_sample = join_parquets[0]
                elif (omop_dir / f"{join_table}.parquet").exists():
                    join_sample = omop_dir / f"{join_table}.parquet"
                if join_sample:
                    try:
                        join_schema = pl.scan_parquet(join_sample).collect_schema()
                        join_schema = {col.lower(): dtype for col, dtype in join_schema.items()}
                        select_cols = spec.get("select", [])
                        for col_name in select_cols:
                            col_lower = col_name.lower()
                            if col_lower in join_schema:
                                pre_join_columns[col_lower] = join_schema[col_lower]
                    except Exception:
                        pass
        if pre_join_columns:
            merged = dict(df_schema)
            merged.update(pre_join_columns)
            df_schema = pl.Schema(merged)

        for table_config in configs:
            subject_id_field = table_config.get("subject_id_field", config.get("primary_key", "person_id"))
            if subject_id_field not in df_schema.names():
                errors.append(
                    f"Table '{table_name}': subject_id_field '{subject_id_field}' not found. "
                    f"Available: {df_schema.names()}"
                )

            time_start_field = (
                table_config.get("time_start") or table_config.get("time_field") or table_config.get("datetime_field")
            )
            if time_start_field:
                if any(marker in time_start_field for marker in ["@", "$", "{", "||"]):
                    column_names = extract_column_names_from_template(time_start_field)
                    for col_name in column_names:
                        if col_name not in df_schema.names():
                            errors.append(
                                f"Table '{table_name}': time_start template references column '{col_name}' which was not found"
                            )
                else:
                    if time_start_field not in df_schema.names():
                        errors.append(f"Table '{table_name}': time_start field '{time_start_field}' not found")

            time_start_fallbacks = table_config.get("time_start_fallbacks") or table_config.get("time_fallbacks", [])
            for fallback in time_start_fallbacks:
                if fallback and fallback not in df_schema.names():
                    errors.append(f"Table '{table_name}': time_start_fallback '{fallback}' not found")

            time_end_field = table_config.get("time_end") or table_config.get("time_end_field")
            if time_end_field:
                if any(marker in time_end_field for marker in ["@", "$", "{", "||"]):
                    column_names = extract_column_names_from_template(time_end_field)
                    for col_name in column_names:
                        if col_name not in df_schema.names():
                            errors.append(
                                f"Table '{table_name}': time_end template references column '{col_name}' which was not found"
                            )
                else:
                    if time_end_field not in df_schema.names():
                        errors.append(f"Table '{table_name}': time_end field '{time_end_field}' not found")

            time_end_fallbacks = table_config.get("time_end_fallbacks", [])
            for fallback in time_end_fallbacks:
                if fallback and fallback not in df_schema.names():
                    errors.append(f"Table '{table_name}': time_end_fallback '{fallback}' not found")

            code_mappings = table_config.get("code_mappings", {})
            is_canonical = "code" in table_config

            if not is_canonical and not code_mappings:
                errors.append(f"Table '{table_name}': No code mappings defined in config.")

            if "source_value" in code_mappings:
                code_field = code_mappings["source_value"].get("field")
                if code_field and code_field not in df_schema.names():
                    errors.append(f"Table '{table_name}': source_value field '{code_field}' not found")

            if "concept_id" in code_mappings:
                concept_id_field = code_mappings["concept_id"].get("concept_id_field")
                if concept_id_field and concept_id_field not in df_schema.names():
                    errors.append(f"Table '{table_name}': concept_id_field '{concept_id_field}' not found")

                source_concept_id_field = code_mappings["concept_id"].get("source_concept_id_field")
                if source_concept_id_field and source_concept_id_field not in df_schema.names():
                    errors.append(
                        f"Table '{table_name}': source_concept_id_field '{source_concept_id_field}' not found"
                    )

            for mapping_type in ["source_value", "concept_id"]:
                if mapping_type in code_mappings:
                    mapping_config = code_mappings[mapping_type]
                    if isinstance(mapping_config, dict) and "template" in mapping_config:
                        template = mapping_config["template"]
                        field_refs = re.findall(r"\{(\w+)\}", template)
                        for field in field_refs:
                            if field not in df_schema.names():
                                errors.append(
                                    f"Table '{table_name}': {mapping_type} template field '{field}' not found in data"
                                )

            for meta_spec in table_config.get("metadata", []) + table_config.get("properties", []):
                if "value" in meta_spec:
                    value = meta_spec["value"]
                    if isinstance(value, str) and any(marker in value for marker in ["@", "$", "{", "||"]):
                        column_names = extract_column_names_from_template(value)
                        for col_name in column_names:
                            if col_name not in df_schema.names():
                                errors.append(
                                    f"Table '{table_name}': property '{meta_spec.get('name')}' template references column '{col_name}' which was not found"
                                )
                elif "literal" not in meta_spec:
                    if "concept_lookup_field" in meta_spec:
                        source_col = meta_spec["concept_lookup_field"]
                        if source_col not in df_schema.names():
                            errors.append(
                                f"Table '{table_name}': property '{meta_spec.get('name')}' concept lookup field '{source_col}' not found"
                            )
                    elif "source" in meta_spec:
                        source_col = meta_spec["source"]
                        if source_col not in df_schema.names():
                            errors.append(
                                f"Table '{table_name}': property '{meta_spec.get('name')}' source column '{source_col}' not found"
                            )
                    else:
                        meta_name = meta_spec.get("name")
                        if meta_name and meta_name not in df_schema.names():
                            errors.append(f"Table '{table_name}': property column '{meta_name}' not found")

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

    # Check text_value / numeric_value concept lookups
    if table_config.get("text_value_field_concept_lookup") or table_config.get("numeric_value_field_concept_lookup"):
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


def _get_omop_vocab_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize the vocabulary.$omop config to object form."""
    vocab = config.get("vocabulary", {})
    omop = vocab.get("$omop", [])
    if isinstance(omop, list):
        return {"sources": omop}
    if isinstance(omop, dict):
        return omop
    return {}


def config_requires_relationship_mapping(config: Dict[str, Any]) -> bool:
    """Check whether the config uses concept_relationship for $omop: resolution."""
    return "concept_relationship" in _get_omop_vocab_config(config).get("sources", [])


def config_requires_standard_only(config: Dict[str, Any]) -> List[str]:
    """Return the list of allowed standard_concept values for $omop: filtering.

    Returns an empty list if no filtering is requested.
    Accepts ``standard_only: true`` (shorthand for ``["S"]``) or an explicit
    list like ``["S", "C"]``.
    """
    val = _get_omop_vocab_config(config).get("standard_only", False)
    if val is True:
        return ["S"]
    if isinstance(val, list):
        return val
    return []


def describe_code_mapping(
    table_config: Dict[str, Any],
    is_canonical: bool,
    fixed_code: Optional[str],
) -> str:
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
# CONCEPT MAP BUILDING
# ============================================================================


def build_concept_map(omop_dir: Path, verbose: bool = False) -> Tuple[pl.DataFrame, Dict[int, Any]]:
    """
    Build concept ID → concept code mapping from OMOP concept table.

    Returns:
        concept_df: Polars DataFrame with concept columns
        code_metadata: Dict[code -> metadata] for custom concepts
    """
    if verbose:
        print("\n" + "=" * 70)
        print("BUILDING CONCEPT MAP")
        print("=" * 70)

    code_metadata: Dict[str, Any] = {}
    concept_dfs = []

    concept_dir = omop_dir / "concept"
    concept_files: List[Path] = []
    if concept_dir.is_dir():
        concept_files = list(concept_dir.glob("*.parquet"))
    elif (omop_dir / "concept.parquet").exists():
        concept_files = [omop_dir / "concept.parquet"]

    if not concept_files:
        print("⚠️  No concept files found - concept mapping will be unavailable")
        return (
            pl.DataFrame(
                schema={
                    "concept_id": pl.Int64,
                    "code": pl.Utf8,
                    "concept_code": pl.Utf8,
                    "concept_name": pl.Utf8,
                    "vocabulary_id": pl.Utf8,
                    "domain_id": pl.Utf8,
                    "concept_class_id": pl.Utf8,
                    "standard_concept": pl.Utf8,
                }
            ),
            {},
        )

    for concept_file in tqdm(concept_files, desc="Loading concepts"):
        df = pl.read_parquet(concept_file)
        df = df.rename({col: col.lower() for col in df.columns})

        concept_df = df.select(
            concept_id=pl.col("concept_id").cast(pl.Int64),
            code=pl.col("vocabulary_id") + "/" + pl.col("concept_code"),
            concept_code=pl.col("concept_code"),
            concept_name=pl.col("concept_name"),
            vocabulary_id=pl.col("vocabulary_id"),
            domain_id=pl.col("domain_id"),
            concept_class_id=pl.col("concept_class_id"),
            **({"standard_concept": pl.col("standard_concept")} if "standard_concept" in df.columns else {}),
        )

        concept_dfs.append(concept_df)

        custom_df = concept_df.filter(pl.col("concept_id") > 2_000_000_000)
        for row in custom_df.iter_rows(named=True):
            code_metadata[row["code"]] = {
                "code": row["code"],
                "description": row["concept_name"],
                "parent_codes": [],
            }

    concept_df_combined = pl.concat(concept_dfs, rechunk=True)

    rel_dir = omop_dir / "concept_relationship"
    rel_files: List[Path] = []
    if rel_dir.is_dir():
        rel_files = list(rel_dir.glob("*.parquet"))
    elif (omop_dir / "concept_relationship.parquet").exists():
        rel_files = [omop_dir / "concept_relationship.parquet"]

    if rel_files:
        concept_id_to_code = dict(
            zip(concept_df_combined["concept_id"].to_list(), concept_df_combined["code"].to_list(), strict=False)
        )

        for rel_file in tqdm(rel_files, desc="Loading concept relationships"):
            df = pl.read_parquet(rel_file)
            df = df.rename({col: col.lower() for col in df.columns})

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

    return concept_df_combined, code_metadata


def build_relationship_resolution_map(
    omop_dir: Path,
    concept_df: pl.DataFrame,
    verbose: bool = False,
) -> Optional[pl.DataFrame]:
    """Build a source_concept_id → resolved_code map from concept_relationship "Maps to" chains.

    Resolves any non-standard source concept to a standard target concept. Loads the
    concept_relationship table, filters to ``relationship_id = "Maps to"`` with
    non-self-referencing pairs, joins the target concept_id against the concept table
    to get the resolved code, and keeps only rows where the target is a standard concept
    (``standard_concept = 'S'``).

    For source concepts that map to multiple standard targets, the first is kept
    (deterministic via sort on target concept_id).

    Returns:
        DataFrame with columns [source_concept_id: Int64, resolved_code: Utf8],
        or None if no concept_relationship files are found.
    """
    rel_dir = omop_dir / "concept_relationship"
    rel_files: List[Path] = []
    if rel_dir.is_dir():
        rel_files = list(rel_dir.glob("*.parquet"))
    elif (omop_dir / "concept_relationship.parquet").exists():
        rel_files = [omop_dir / "concept_relationship.parquet"]

    if not rel_files:
        if verbose:
            print("  ⚠️  No concept_relationship files found — skipping relationship resolution")
        return None

    if verbose:
        print(f"\n  Building relationship resolution map from {len(rel_files)} file(s)...")

    rel_dfs = []
    for rel_file in tqdm(rel_files, desc="Loading concept_relationship", disable=not verbose):
        df = pl.read_parquet(rel_file)
        df = df.rename({col: col.lower() for col in df.columns})

        maps_to = df.filter(
            (pl.col("relationship_id") == "Maps to")
            & (pl.col("concept_id_1") != pl.col("concept_id_2"))
            & (pl.col("concept_id_2") > 0)
        ).select(
            source_concept_id=pl.col("concept_id_1").cast(pl.Int64),
            target_concept_id=pl.col("concept_id_2").cast(pl.Int64),
        )
        rel_dfs.append(maps_to)

    if not rel_dfs:
        return None

    all_rels = pl.concat(rel_dfs, rechunk=True)

    # Join target concept_id against the concept table to get code + standard_concept flag
    has_standard = "standard_concept" in concept_df.columns
    if has_standard:
        target_info = concept_df.select(
            pl.col("concept_id").alias("target_concept_id"),
            pl.col("code").alias("resolved_code"),
            pl.col("standard_concept"),
        )
    else:
        target_info = concept_df.select(
            pl.col("concept_id").alias("target_concept_id"),
            pl.col("code").alias("resolved_code"),
        )

    resolved = all_rels.join(target_info, on="target_concept_id", how="inner")

    if has_standard:
        resolved = resolved.filter(pl.col("standard_concept") == "S").drop("standard_concept")

    # Deduplicate: for source concepts with multiple standard targets, keep the first
    resolved = (
        resolved.sort("target_concept_id")
        .group_by("source_concept_id")
        .first()
        .select("source_concept_id", "resolved_code")
    )

    if verbose:
        print(f"  ✅ Built resolution map: {len(resolved):,} source → standard mappings")

    return resolved


def find_concept_id_columns_for_prescan(table_name: str, config: Dict) -> List[str]:
    """Find all concept_id columns for a table based on config (for pre-scanning)."""
    columns: List[str] = []

    configs_to_scan: List[Dict] = []

    if table_name in config.get("tables", {}):
        configs_to_scan.append(config["tables"][table_name])

    for _event_name, event_config in config.get("canonical_events", {}).items():
        if event_config.get("table") == table_name:
            configs_to_scan.append(event_config)

    if not configs_to_scan:
        return columns

    for table_config in configs_to_scan:
        code_mappings = table_config.get("code_mappings", {})
        concept_id_config = code_mappings.get("concept_id", {})

        if concept_id_config:
            concept_id_field = concept_id_config.get("concept_id_field")
            if concept_id_field and concept_id_field not in columns:
                columns.append(concept_id_field)

            source_concept_id_field = concept_id_config.get("source_concept_id_field")
            if source_concept_id_field and source_concept_id_field not in columns:
                columns.append(source_concept_id_field)

        text_value_lookup = table_config.get("text_value_field_concept_lookup")
        if text_value_lookup and text_value_lookup not in columns:
            columns.append(text_value_lookup)

        properties = table_config.get("properties") or table_config.get("metadata", [])
        for prop in properties:
            field = prop.get("concept_lookup_field")
            if field and field not in columns:
                columns.append(field)

    return columns


def validate_parquet_file(file_path: Path) -> bool:
    """Quick validation check for parquet file health."""
    try:
        pq.read_schema(file_path)
        return True
    except Exception:
        return False


def fast_scan_file_for_concept_ids(file_path: Path, concept_id_columns: List[str]) -> Tuple[Set[int], Optional[str]]:
    """
    Fast scan of a single file to extract unique concept_ids.
    Uses Polars lazy evaluation to only read specified columns.
    """
    concept_ids: Set[int] = set()

    try:
        if not validate_parquet_file(file_path):
            return concept_ids, f"File validation failed: {file_path.name}"

        lazy_df = pl.scan_parquet(file_path)
        schema = lazy_df.collect_schema()
        col_mapping = {col: col.lower() for col in schema.names()}
        lazy_df = lazy_df.rename(col_mapping)

        concept_id_columns_lower = [col.lower() for col in concept_id_columns]
        schema_lower = lazy_df.collect_schema()
        existing_cols = [col for col in concept_id_columns_lower if col in schema_lower.names()]

        if not existing_cols:
            return concept_ids, None

        for col in existing_cols:
            unique_values = lazy_df.select(pl.col(col).cast(pl.Int64)).unique().collect()
            for row in unique_values.iter_rows():
                concept_id = row[0]
                if concept_id is not None and concept_id > 0:
                    concept_ids.add(concept_id)

        return concept_ids, None

    except Exception as e:
        return concept_ids, f"{file_path.name}: {str(e)}"


def prescan_worker(args: Tuple) -> Dict[str, Any]:
    """Worker process to scan a batch of files for concept_ids."""
    worker_id, file_batch = args

    worker_concept_ids: Set[int] = set()
    files_processed = 0
    errors: List[str] = []

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
    timeout_per_worker: int = 600,
    process_method: str = "spawn",
) -> Set[int]:
    """
    Fast parallel pre-scan to collect all unique concept_ids used in OMOP data.
    Uses greedy load balancing for optimal worker distribution.
    """
    files_to_scan: List[Tuple[Path, List[str]]] = []

    tables_scanned: set = set()
    for table_name, table_config in config.get("tables", {}).items():
        code_mappings = table_config.get("code_mappings", {})
        if "concept_id" not in code_mappings:
            continue

        concept_id_columns = find_concept_id_columns_for_prescan(table_name, config)
        if not concept_id_columns:
            continue

        table_files = find_omop_table_files(omop_dir, table_name)
        for file_path in table_files:
            files_to_scan.append((file_path, concept_id_columns))
        tables_scanned.add(table_name)

    for _event_name, event_config in config.get("canonical_events", {}).items():
        event_table = event_config.get("table")
        if not event_table or event_table in tables_scanned:
            continue
        concept_id_columns = find_concept_id_columns_for_prescan(event_table, config)
        if not concept_id_columns:
            continue
        table_files = find_omop_table_files(omop_dir, event_table)
        for file_path in table_files:
            files_to_scan.append((file_path, concept_id_columns))
        tables_scanned.add(event_table)

    if not files_to_scan:
        if verbose:
            print("  No files to scan for concept optimization")
        return set()

    print(f"  Pre-scanning {len(files_to_scan)} files...")

    file_info = []
    for file_tuple in files_to_scan:
        file_path = file_tuple[0]
        try:
            file_size = file_path.stat().st_size
        except OSError:
            file_size = 0
        file_info.append((file_tuple, file_size))

    file_info.sort(key=lambda x: x[1], reverse=True)

    worker_loads: List[List] = [[] for _ in range(num_workers)]
    worker_sizes = [0] * num_workers

    for file_tuple, size in file_info:
        min_worker = worker_sizes.index(min(worker_sizes))
        worker_loads[min_worker].append(file_tuple)
        worker_sizes[min_worker] += size

    worker_args = list(enumerate(worker_loads))

    all_concept_ids: Set[int] = set()
    total_errors: List[str] = []

    try:
        with mp.get_context(process_method).Pool(processes=num_workers) as pool:
            async_result = pool.map_async(prescan_worker, worker_args)

            try:
                worker_results = async_result.get(timeout=timeout_per_worker)

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
                        for error in total_errors[:5]:
                            print(f"     - {error}")
                        if len(total_errors) > 5:
                            print(f"     ... and {len(total_errors) - 5} more")

            except mp.TimeoutError:
                print(f"  ⚠️  WARNING: Pre-scan timed out after {timeout_per_worker}s")
                print("  ⚠️  Continuing without concept optimization (will use full concept table)")
                pool.terminate()
                pool.join()
                return set()

    except Exception as e:
        print(f"  ⚠️  WARNING: Pre-scan failed: {e}")
        print("  ⚠️  Continuing without concept optimization (will use full concept table)")
        return set()

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
# TRANSFORMATION UTILITIES
# ============================================================================


def _build_condition_expr(cond: Dict[str, Any], columns: set) -> Optional[pl.Expr]:
    """Build a Polars filter expression from a single condition dict."""
    field = cond["field"]
    op = cond["op"]

    if field not in columns:
        return None

    if op == "is_not_null":
        return pl.col(field).is_not_null()
    elif op == "is_null":
        return pl.col(field).is_null()
    elif op == "in":
        return pl.col(field).is_in(cond["value"])
    elif op == "!=":
        return pl.col(field) != cond["value"]
    elif op == "==":
        return pl.col(field) == cond["value"]
    elif op == ">":
        return pl.col(field) > cond["value"]
    elif op == "<":
        return pl.col(field) < cond["value"]
    elif op == ">=":
        return pl.col(field) >= cond["value"]
    elif op == "<=":
        return pl.col(field) <= cond["value"]
    return None


def apply_filter_conditions(df: pl.DataFrame, conditions: List[Any]) -> pl.DataFrame:
    """Apply compiled filter conditions to a DataFrame.

    Accepts two formats:
    - OR-of-AND groups: ``[[cond, cond], [cond]]`` — groups are ORed, conditions
      within each group are ANDed.
    - Legacy flat list: ``[cond, cond]`` — all conditions ANDed (backwards compatible).
    """
    if not conditions:
        return df

    columns = set(df.columns)

    # Detect format: if first element is a list, it's OR-of-AND groups
    if isinstance(conditions[0], list):
        or_exprs: List[pl.Expr] = []
        for group in conditions:
            and_exprs = [_build_condition_expr(c, columns) for c in group]
            and_exprs = [e for e in and_exprs if e is not None]
            if and_exprs:
                combined = and_exprs[0]
                for e in and_exprs[1:]:
                    combined = combined & e
                or_exprs.append(combined)
        if or_exprs:
            final = or_exprs[0]
            for e in or_exprs[1:]:
                final = final | e
            df = df.filter(final)
    else:
        # Legacy flat list: all conditions ANDed
        for cond in conditions:
            expr = _build_condition_expr(cond, columns)
            if expr is not None:
                df = df.filter(expr)

    return df


def apply_transforms(expr: pl.Expr, transforms: list) -> pl.Expr:
    """
    Apply a list of transformations to a Polars expression.

    Supported transforms:
    - {"type": "replace", "pattern": "X", "replacement": "Y"}
    - {"type": "regex_replace", "pattern": "X", "replacement": "Y"}
    - {"type": "lower"} / {"type": "upper"} / {"type": "strip"}
    - {"type": "strip_chars", "characters": "X"}
    - {"type": "split", "delimiter": "X", "index": N, "default": "Y"}
    """
    for transform in transforms:
        transform_type = transform.get("type")

        if transform_type == "replace":
            pattern = transform.get("pattern", "")
            replacement = transform.get("replacement", "")
            expr = expr.str.replace_all(pattern, replacement, literal=True)

        elif transform_type == "regex_replace":
            pattern = transform.get("pattern", "")
            replacement = transform.get("replacement", "")
            expr = expr.str.replace_all(pattern, replacement, literal=False)

        elif transform_type == "lower":
            expr = expr.str.to_lowercase()

        elif transform_type == "upper":
            expr = expr.str.to_uppercase()

        elif transform_type == "strip":
            expr = expr.str.strip_chars()

        elif transform_type == "strip_chars":
            characters = transform.get("characters", "")
            expr = expr.str.strip_chars(characters)

        elif transform_type == "split":
            delimiter = transform.get("delimiter", "")
            index = transform.get("index")
            default = transform.get("default")
            split_expr = expr.str.split(delimiter)
            if index is not None:
                expr = split_expr.list.get(int(index), null_on_oob=True)
                if default is not None:
                    expr = pl.when((expr.is_null()) | (expr == "")).then(pl.lit(default)).otherwise(expr)
            else:
                expr = split_expr

    return expr


def build_template_expression(
    template: str,
    transforms: Optional[List[Dict[str, Any]]] = None,
    column_transforms: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> pl.Expr:
    """Build a Polars expression from a template string with optional transforms."""
    parts = re.split(r"(\{[^}]+\})", template)

    exprs = []
    for part in parts:
        if part.startswith("{") and part.endswith("}"):
            field = part[1:-1]
            col_expr = pl.col(field).cast(pl.Utf8).fill_null("")

            if column_transforms and field in column_transforms:
                col_expr = apply_transforms(col_expr, column_transforms[field])

            exprs.append(col_expr)
        elif part:
            exprs.append(pl.lit(part))

    code_expr = pl.concat_str(exprs)

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
    relationship_map_df: Optional[pl.DataFrame] = None,
    standard_only: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Transform OMOP DataFrame to MEDS Unsorted format.

    Fast transformation using vectorized operations, no sorting.
    Uses DataFrame joins for concept mapping.
    Produces output compatible with meds_etl_cpp or unsorted.sort().

    Args:
        relationship_map_df: Optional DataFrame with [source_concept_id, resolved_code]
            from concept_relationship "Maps to" chains. When provided, source concept IDs
            are resolved through this map before falling back to the regular concept lookup.
        standard_only: List of allowed standard_concept values (e.g., ["S"] or ["S", "C"]).
            When non-empty, the primary concept table join only matches concepts whose
            standard_concept is in this list. Empty list or None means no filtering.
    """
    # 0. Apply row-level filter (if configured)
    compiled_filter = table_config.get("_compiled_filter")
    if compiled_filter:
        df = apply_filter_conditions(df, compiled_filter)
        if len(df) == 0:
            schema = dict(meds_schema.items())
            return pl.DataFrame(schema=schema)

    # 1. subject_id
    subject_id_field = table_config.get("subject_id_field", primary_key)
    subject_id = pl.col(subject_id_field).cast(pl.Int64).alias("subject_id")

    # 2. time
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

    # 3. Code mappings
    code_mappings = table_config.get("code_mappings", {})
    base_exprs = [subject_id, time]

    code_candidates: List[str] = []
    needs_concept_join = False
    concept_join_alias = None
    concept_join_field = "code"
    code_is_fixed = False
    _single_join_is_standard_anchor = False
    _resolve_only_single = False
    concept_config = None

    if fixed_code:
        code_is_fixed = True
        base_exprs.append(pl.lit(fixed_code).alias("code"))
    else:
        if not code_mappings:
            raise ValueError("No code mappings defined for table")

        if "concept_id" in code_mappings:
            concept_config = code_mappings["concept_id"]

            if "template" in concept_config:
                alias = "code_concept_template"
                concept_fields_to_map = concept_config.get("concept_fields", [])

                if concept_fields_to_map and concept_df is not None and len(concept_df) > 0:
                    temp_df = df
                    for i, concept_field_name in enumerate(concept_fields_to_map):
                        code_alias = f"_concept_code_{i}_{concept_field_name}"
                        temp_df = temp_df.join(
                            concept_df.select(
                                [pl.col("concept_id").alias(f"_join_id_{i}"), pl.col("code").alias(code_alias)]
                            ),
                            left_on=concept_field_name,
                            right_on=f"_join_id_{i}",
                            how="left",
                        )

                    t_parts = re.split(r"(\{[^}]+\})", concept_config["template"])
                    t_exprs = []
                    for part in t_parts:
                        if part.startswith("{") and part.endswith("}"):
                            field = part[1:-1]
                            if field in concept_fields_to_map:
                                idx = concept_fields_to_map.index(field)
                                code_alias = f"_concept_code_{idx}_{field}"
                                t_exprs.append(pl.col(code_alias).cast(pl.Utf8).fill_null(f"UNKNOWN_{field.upper()}"))
                            else:
                                t_exprs.append(pl.col(field).cast(pl.Utf8).fill_null(""))
                        elif part:
                            t_exprs.append(pl.lit(part))

                    code_expr = pl.concat_str(t_exprs)
                    t_transforms = concept_config.get("transforms", [])
                    code_expr = apply_transforms(code_expr, t_transforms)
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

                concept_id_op = concept_config.get("_concept_id_field_operation")
                source_concept_id_op = concept_config.get("_source_concept_id_field_operation")

                if concept_df is None or len(concept_df) == 0:
                    raise ValueError("concept_id mapping requested but concept_df not provided")

                if not concept_id_field and not source_concept_id_field and fallback_concept_id is None:
                    raise ValueError(
                        "concept_id mapping requested but no concept_id_field, "
                        "source_concept_id_field, fallback_concept_id, or template defined"
                    )

                needs_concept_join = True
                concept_join_alias = "code_concept_lookup"
                concept_join_field = concept_config.get("concept_field", "code")
                _single_join_is_standard_anchor = False

                # When both source_concept_id and concept_id are present, keep them
                # as separate columns so each gets its own concept table join.
                # This avoids losing the standard concept_id when the custom
                # source_concept_id coalesces over it.
                if source_concept_id_field and concept_id_field:
                    base_exprs.append(pl.col(source_concept_id_field).cast(pl.Int64).alias("_source_concept_id"))
                    concept_expr = pl.col(concept_id_field).cast(pl.Int64)
                    if fallback_concept_id is not None:
                        concept_expr = (
                            pl.when((concept_expr.is_not_null()) & (concept_expr != 0))
                            .then(concept_expr)
                            .otherwise(pl.lit(fallback_concept_id).cast(pl.Int64))
                        )
                        base_exprs.append(concept_expr.alias("concept_id"))
                        base_exprs.append(pl.lit(fallback_concept_id).cast(pl.Int64).alias("_fallback_concept_id"))
                    else:
                        base_exprs.append(concept_expr.alias("concept_id"))
                elif source_concept_id_field:
                    col_expr = pl.col(source_concept_id_field).cast(pl.Int64)
                    if fallback_concept_id is not None:
                        base_exprs.append(
                            pl.when((col_expr.is_not_null()) & (col_expr != 0))
                            .then(col_expr)
                            .otherwise(pl.lit(fallback_concept_id).cast(pl.Int64))
                            .alias("concept_id")
                        )
                    else:
                        base_exprs.append(col_expr.alias("concept_id"))
                elif concept_id_field:
                    _single_join_is_standard_anchor = True
                    col_expr = pl.col(concept_id_field).cast(pl.Int64)
                    if fallback_concept_id is not None:
                        base_exprs.append(
                            pl.when((col_expr.is_not_null()) & (col_expr != 0))
                            .then(col_expr)
                            .otherwise(pl.lit(fallback_concept_id).cast(pl.Int64))
                            .alias("concept_id")
                        )
                    else:
                        base_exprs.append(col_expr.alias("concept_id"))

                    if concept_id_op == "resolve":
                        base_exprs.append(
                            pl.col(concept_id_field).cast(pl.Int64).alias("_source_concept_id_for_resolution")
                        )
                    elif concept_id_op != "lookup" and relationship_map_df is not None:
                        # Auto-detect companion source_concept_id column for
                        # relationship resolution.  OMOP convention:
                        #   observation_concept_id → observation_source_concept_id
                        #   condition_concept_id  → condition_source_concept_id
                        companion = concept_id_field.replace("_concept_id", "_source_concept_id")
                        if companion != concept_id_field and companion in df.columns:
                            base_exprs.append(
                                pl.col(companion).cast(pl.Int64).alias("_source_concept_id_for_resolution")
                            )
                else:
                    base_exprs.append(pl.lit(fallback_concept_id).cast(pl.Int64).alias("concept_id"))

                # For resolve-only single-field, the concept table join result is
                # not a code candidate — only relationship resolution provides the code.
                _resolve_only_single = False
                if source_concept_id_field and not concept_id_field and source_concept_id_op == "resolve":
                    _resolve_only_single = True
                elif concept_id_field and not source_concept_id_field and concept_id_op == "resolve":
                    _resolve_only_single = True

                if not _resolve_only_single:
                    code_candidates.append(concept_join_alias)

                # Track source_concept_id separately for relationship resolution
                # (skip when explicit "lookup" — no relationship resolution wanted)
                if relationship_map_df is not None and source_concept_id_field and source_concept_id_op != "lookup":
                    if not (source_concept_id_field and concept_id_field):
                        base_exprs.append(
                            pl.col(source_concept_id_field).cast(pl.Int64).alias("_source_concept_id_for_resolution")
                        )

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

        if not code_is_fixed and not code_candidates and not _resolve_only_single:
            raise ValueError(f"No code mappings defined for table '{table_config.get('table', 'unknown')}'.")

    # 4. numeric_value and text_value (with concept lookup + fallback support)
    numeric_value_type = meds_schema.get("numeric_value", pl.Float32)
    concept_value_requests: List[Dict[str, Any]] = []

    numeric_value_field = table_config.get("numeric_value_field")
    if numeric_value_field:
        nv_fallbacks = table_config.get("numeric_value_field_fallbacks", [])
        nv_options = [pl.col(numeric_value_field).cast(numeric_value_type)]
        for fb in nv_fallbacks:
            nv_options.append(pl.col(fb).cast(numeric_value_type))
        base_exprs.append((pl.coalesce(nv_options) if len(nv_options) > 1 else nv_options[0]).alias("numeric_value"))
    else:
        base_exprs.append(pl.lit(None, dtype=numeric_value_type).alias("numeric_value"))

    text_value_field = table_config.get("text_value_field")
    text_value_concept_lookup = table_config.get("text_value_field_concept_lookup")
    if text_value_field:
        tv_fallbacks = table_config.get("text_value_field_fallbacks", [])
        tv_options = [pl.col(text_value_field).cast(pl.Utf8)]
        for fb in tv_fallbacks:
            tv_options.append(pl.col(fb).cast(pl.Utf8))
        base_exprs.append((pl.coalesce(tv_options) if len(tv_options) > 1 else tv_options[0]).alias("text_value"))
    elif text_value_concept_lookup:
        temp_col = "__text_value_concept_id"
        temp_alias = "__text_value_concept_code"
        base_exprs.append(pl.col(text_value_concept_lookup).cast(pl.Int64).alias(temp_col))
        concept_value_requests.append(
            {
                "concept_col": temp_col,
                "code_alias": temp_alias,
                "output_name": "text_value",
                "type": pl.Utf8,
            }
        )
    else:
        base_exprs.append(pl.lit(None, dtype=pl.Utf8).alias("text_value"))

    # 5. end time (with fallbacks)
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

    # 6. Property columns
    properties = table_config.get("properties") or table_config.get("metadata", [])
    concept_property_requests: List[Dict[str, Any]] = []

    for prop_spec in properties:
        prop_name = prop_spec.get("name")
        if prop_name is None:
            continue
        prop_type = prop_spec.get("type", "string")
        prop_polars_type = config_type_to_polars(prop_type)
        output_name = prop_spec.get("alias", prop_name)

        if "literal" in prop_spec:
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
            source_field = prop_spec.get("source", prop_name)
            base_exprs.append(pl.col(source_field).cast(prop_polars_type).alias(output_name))

    # Build base DataFrame
    result = df.select(base_exprs)

    # Perform concept join for code (only select needed columns to avoid leaking extras)
    _deferred_source_alias = None
    _deferred_fallback_alias = None
    primary_join_alias = None
    if needs_concept_join:
        join_alias = concept_join_alias or "code"
        if concept_join_field not in concept_df.columns:
            raise ValueError(
                f"Concept field '{concept_join_field}' not found in concept DataFrame. "
                f"Available columns: {concept_df.columns}."
            )

        if "_source_concept_id" in result.columns and "concept_id" in result.columns:
            # Separate joins: concept_id and source_concept_id each get their own lookup.
            # They become separate code candidates so relationship resolution can
            # be inserted between them in priority order.
            primary_join_alias = f"{join_alias}_primary"
            source_join_alias = f"{join_alias}_source"

            # When standard_only, filter primary join to allowed standard_concept values
            primary_concept_df = concept_df
            if standard_only and "standard_concept" in concept_df.columns:
                primary_concept_df = concept_df.filter(pl.col("standard_concept").is_in(standard_only))

            primary_join_df = primary_concept_df.select(
                pl.col("concept_id"),
                pl.col(concept_join_field).alias(primary_join_alias),
            )
            result = result.join(primary_join_df, on="concept_id", how="left")

            # Unfiltered lookup for exempt_codes rescue in dual-column path
            exempt_codes = table_config.get("exempt_codes", [])
            if standard_only and exempt_codes and primary_concept_df is not concept_df:
                unfiltered_join_df = concept_df.select(
                    pl.col("concept_id").alias("_exempt_join_id"),
                    pl.col(concept_join_field).alias("_exempt_code_candidate"),
                )
                result = result.join(
                    unfiltered_join_df,
                    left_on="concept_id",
                    right_on="_exempt_join_id",
                    how="left",
                )

            source_join_df = concept_df.select(
                pl.col("concept_id").alias("_src_join_id"),
                pl.col(concept_join_field).alias(source_join_alias),
            )
            result = result.join(
                source_join_df,
                left_on="_source_concept_id",
                right_on="_src_join_id",
                how="left",
            )

            if "_fallback_concept_id" in result.columns:
                fb_join_alias = f"{join_alias}_fallback"
                fb_join_df = concept_df.select(
                    pl.col("concept_id").alias("_fb_join_id"),
                    pl.col(concept_join_field).alias(fb_join_alias),
                )
                result = result.join(
                    fb_join_df,
                    left_on="_fallback_concept_id",
                    right_on="_fb_join_id",
                    how="left",
                )
                _deferred_fallback_alias = fb_join_alias

            # Rename _source_concept_id for relationship resolution (only when
            # the source field operation is not an explicit "lookup")
            source_concept_id_op = concept_config.get("_source_concept_id_field_operation")
            if source_concept_id_op != "lookup":
                result = result.rename({"_source_concept_id": "_source_concept_id_for_resolution"})
            elif "_source_concept_id" in result.columns:
                result = result.drop("_source_concept_id")

            # When the source field is an explicit lookup, its raw code is a
            # candidate (just like concept_id).  Otherwise it's suppressed.
            if source_concept_id_op == "lookup":
                _deferred_source_alias = source_join_alias

            # Replace the single code_concept_lookup candidate with ordered candidates:
            # Priority: 1) primary (concept_id), 2) relationship (inserted later)
            # Raw source concept code is only added when source field is an explicit lookup.
            if join_alias in code_candidates:
                code_candidates.remove(join_alias)
            code_candidates.append(primary_join_alias)

            drop_cols = ["concept_id"]
            if "_fallback_concept_id" in result.columns:
                drop_cols.append("_fallback_concept_id")
            result = result.drop([c for c in drop_cols if c in result.columns])
        else:
            # Single-column join path
            single_concept_df = concept_df
            if standard_only and _single_join_is_standard_anchor and "standard_concept" in concept_df.columns:
                single_concept_df = concept_df.filter(pl.col("standard_concept").is_in(standard_only))

            join_df = single_concept_df.select(
                [
                    pl.col("concept_id"),
                    pl.col(concept_join_field).alias(join_alias),
                ]
            )
            result = result.join(join_df, on="concept_id", how="left")

            # When standard_only is filtering and exempt_codes are configured,
            # also do an unfiltered lookup to rescue exempt codes.
            exempt_codes = table_config.get("exempt_codes", [])
            if (
                standard_only
                and _single_join_is_standard_anchor
                and exempt_codes
                and single_concept_df is not concept_df
            ):
                unfiltered_join_df = concept_df.select(
                    pl.col("concept_id").alias("_exempt_join_id"),
                    pl.col(concept_join_field).alias("_exempt_code_candidate"),
                )
                result = result.join(
                    unfiltered_join_df,
                    left_on="concept_id",
                    right_on="_exempt_join_id",
                    how="left",
                )

            result = result.drop("concept_id")

    # Perform relationship resolution (source_concept_id → "Maps to" → standard code)
    if relationship_map_df is not None and "_source_concept_id_for_resolution" in result.columns:
        rel_alias = "code_relationship_resolved"
        result = result.join(
            relationship_map_df,
            left_on="_source_concept_id_for_resolution",
            right_on="source_concept_id",
            how="left",
        )
        if "resolved_code" in result.columns:
            result = result.rename({"resolved_code": rel_alias})
        result = result.drop("_source_concept_id_for_resolution")
        if "source_concept_id" in result.columns:
            result = result.drop("source_concept_id")
        if rel_alias in result.columns:
            code_candidates.append(rel_alias)

            # Deprecation warning for implicit resolution via legacy $omop: syntax
            concept_id_op = concept_config.get("_concept_id_field_operation") if concept_config else None
            source_concept_id_op = concept_config.get("_source_concept_id_field_operation") if concept_config else None
            if not concept_id_op and not source_concept_id_op:
                import warnings

                warnings.warn(
                    "$omop:@field with implicit concept_relationship resolution is deprecated. "
                    "Use $omop.resolve:@field for explicit resolution or $omop.lookup:@field "
                    "for direct lookup.",
                    DeprecationWarning,
                    stacklevel=2,
                )

            if (
                concept_config.get("_source_first")
                and primary_join_alias is not None
                and rel_alias in code_candidates
                and primary_join_alias in code_candidates
            ):
                code_candidates.remove(rel_alias)
                idx = code_candidates.index(primary_join_alias)
                code_candidates.insert(idx, rel_alias)

    # Add deferred source and fallback concept codes as lowest-priority fallbacks.
    if _deferred_source_alias is not None and _deferred_source_alias in result.columns:
        code_candidates.append(_deferred_source_alias)
    if _deferred_fallback_alias is not None and _deferred_fallback_alias in result.columns:
        code_candidates.append(_deferred_fallback_alias)

    # Reorder deferred source alias for _source_first (handles lookup-lookup case
    # where relationship resolution is skipped and the source alias is deferred)
    if (
        concept_config
        and concept_config.get("_source_first")
        and primary_join_alias is not None
        and _deferred_source_alias is not None
        and _deferred_source_alias in code_candidates
        and primary_join_alias in code_candidates
    ):
        code_candidates.remove(_deferred_source_alias)
        idx = code_candidates.index(primary_join_alias)
        code_candidates.insert(idx, _deferred_source_alias)

    # Perform concept lookups for properties and text_value/numeric_value
    all_concept_requests = concept_value_requests + concept_property_requests
    if all_concept_requests:
        if concept_df is None or len(concept_df) == 0:
            raise ValueError("Concept lookup requested but concept_df not provided")
        for req in all_concept_requests:
            concept_col = req["concept_col"]
            code_alias = req["code_alias"]
            output_name = req["output_name"]
            prop_type = req["type"]

            join_df = concept_df.select(
                [
                    pl.col("concept_id"),
                    pl.col("code").alias(code_alias),
                ]
            )
            result = result.join(join_df, left_on=concept_col, right_on="concept_id", how="left")

            if concept_col in result.columns:
                result = result.drop(concept_col)
            if "concept_id" in result.columns:
                result = result.drop("concept_id")

            if code_alias in result.columns:
                result = result.rename({code_alias: output_name})
            result = result.with_columns(pl.col(output_name).cast(prop_type))

    # Combine candidate code columns (fallback order)
    if not code_is_fixed:
        available_candidates = [alias for alias in code_candidates if alias in result.columns]
        if not available_candidates:
            raise ValueError("No code mapping expressions materialized for table")

        result = result.with_columns(pl.coalesce([pl.col(alias) for alias in available_candidates]).alias("code"))
        drop_cols = [alias for alias in available_candidates if alias != "code"]
        if drop_cols:
            result = result.drop(drop_cols)

    # Rescue exempt codes: for rows where code is null but the unfiltered
    # concept lookup produced a code in the exempt_codes set, use that code.
    exempt_codes = table_config.get("exempt_codes", [])
    if exempt_codes and "_exempt_code_candidate" in result.columns:
        exempt_set = set(exempt_codes)
        result = result.with_columns(
            pl.when(pl.col("code").is_null() & pl.col("_exempt_code_candidate").is_in(exempt_set))
            .then(pl.col("_exempt_code_candidate"))
            .otherwise(pl.col("code"))
            .alias("code")
        )
        result = result.drop("_exempt_code_candidate")

    # Filter out rows with null codes
    result = result.filter(pl.col("code").is_not_null())

    # 7. Enforce global schema: add missing columns, reorder, and cast
    existing_cols = set(result.columns)
    final_select = []

    for col_name, col_type in meds_schema.items():
        if col_name in existing_cols:
            final_select.append(pl.col(col_name).cast(col_type))
        else:
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
    Accepts a tuple of 10, 11, or 12 elements.
    Falls back to "zstd" compression, no relationship map, and standard_only=None for shorter tuples.
    """
    relationship_map_data = None
    standard_only = None

    if len(args) == 12:
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
            compression,
            relationship_map_data,
            standard_only,
        ) = args
    elif len(args) == 11:
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
            compression,
            relationship_map_data,
        ) = args
    elif len(args) == 10:
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
            compression,
        ) = args
    else:
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
        compression = "zstd"

    concept_df = pickle.loads(concept_df_data) if concept_df_data else None
    concept_df_size = len(concept_df) if concept_df is not None else 0
    relationship_map_df = pickle.loads(relationship_map_data) if relationship_map_data else None

    try:
        df = pl.read_parquet(file_path)
        df = df.rename({col: col.lower() for col in df.columns})

        # Apply pre_join: enrich DataFrame with columns from reference tables
        pre_join_data = table_config.get("_pre_join_data", [])
        for join_spec in pre_join_data:
            join_df = pickle.loads(join_spec["df_bytes"])
            on_col = join_spec["on"]
            if on_col in df.columns:
                df = df.join(join_df, on=on_col, how="left")

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

        meds_df = transform_to_meds_unsorted(
            df=df,
            table_config=table_config,
            primary_key=primary_key,
            meds_schema=meds_schema,
            concept_df=concept_df,
            fixed_code=fixed_code,
            relationship_map_df=relationship_map_df,
            standard_only=standard_only,
        )

        output_rows = len(meds_df)
        filtered_rows = input_rows - output_rows

        output_file = output_dir / f"{table_name}_{uuid.uuid4()}.parquet"
        if compression == "zstd":
            meds_df.write_parquet(output_file, compression="zstd", compression_level=1)
        else:
            meds_df.write_parquet(output_file, compression=compression)

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
# STAGE 1 TASK COLLECTION
# ============================================================================


def collect_stage1_tasks(
    config: Dict,
    omop_dir: Path,
    primary_key: str,
    unsorted_dir: Path,
    meds_schema: Dict[str, type],
    concept_df_data: Optional[bytes],
    compression: str = "zstd",
    relationship_map_data: Optional[bytes] = None,
    standard_only: Optional[List[str]] = None,
) -> List[Tuple]:
    """
    Collect all Stage 1 processing tasks from config.

    Correctly handles canonical events with both fixed codes and template/code_mapping codes.
    Pre-loads any pre_join reference tables and embeds them in the table config.
    """
    tasks = []

    def _prepare_config_with_pre_joins(table_config: Dict) -> Dict:
        """Load pre_join tables from omop_dir and embed as _pre_join_data."""
        pre_joins = table_config.get("pre_join")
        if not pre_joins:
            return table_config
        prepared = table_config.copy()
        join_data = []
        for spec in pre_joins:
            join_table = spec["table"]
            join_files = find_omop_table_files(omop_dir, join_table)
            if not join_files:
                print(f"  ⚠️  pre_join table '{join_table}' not found in {omop_dir}")
                continue
            dfs = [pl.read_parquet(f) for f in join_files]
            join_df = pl.concat(dfs, rechunk=True) if len(dfs) > 1 else dfs[0]
            join_df = join_df.rename({col: col.lower() for col in join_df.columns})
            on_col = spec["on"].lower()
            select_cols = [c.lower() for c in spec.get("select", [])]
            keep_cols = [on_col] + [c for c in select_cols if c != on_col]
            available = [c for c in keep_cols if c in join_df.columns]
            if on_col not in join_df.columns:
                print(f"  ⚠️  pre_join column '{on_col}' not found in '{join_table}'")
                continue
            join_df = join_df.select(available).unique(subset=[on_col])
            join_data.append({"on": on_col, "select": select_cols, "df_bytes": pickle.dumps(join_df)})
        if join_data:
            prepared["_pre_join_data"] = join_data
        return prepared

    for event_name, event_config in config.get("canonical_events", {}).items():
        table_name = event_config["table"]
        files = find_omop_table_files(omop_dir, table_name)

        fixed_code = event_config.get("code")
        has_code_mappings = "code_mappings" in event_config

        if not fixed_code and not has_code_mappings:
            raise ValueError(
                f"Canonical event '{event_name}' must define either 'code' (for fixed codes) "
                f"or 'code_mappings' (for template-based codes). Found neither in config."
            )

        is_canonical = fixed_code is not None
        prepared_config = _prepare_config_with_pre_joins(event_config)

        for file_path in files:
            tasks.append(
                (
                    file_path,
                    table_name,
                    prepared_config,
                    primary_key,
                    unsorted_dir,
                    meds_schema,
                    concept_df_data,
                    is_canonical,
                    fixed_code,
                    compression,
                    relationship_map_data,
                    standard_only,
                )
            )

    for table_name, table_config in config.get("tables", {}).items():
        files = find_omop_table_files(omop_dir, table_name)
        prepared_config = _prepare_config_with_pre_joins(table_config)

        for file_path in files:
            tasks.append(
                (
                    file_path,
                    table_name,
                    prepared_config,
                    primary_key,
                    unsorted_dir,
                    meds_schema,
                    concept_df_data,
                    False,
                    None,
                    compression,
                    relationship_map_data,
                    standard_only,
                )
            )

    return tasks
