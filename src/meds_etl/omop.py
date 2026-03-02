"""
Refactored OMOP to MEDS ETL pipeline.

Architecture:
  Stage 1: OMOP → MEDS Unsorted (fast, parallel, no sorting)
  Stage 2: MEDS Unsorted → MEDS (via meds_etl_cpp or unsorted.sort())

Stage 1 logic lives in omop_common.py (shared with omop_streaming.py).
This module provides Stage 2 sort backends and the CLI.
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
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import meds
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

import meds_etl
import meds_etl.unsorted

# Re-export for any external consumers
from meds_etl.omop_common import (  # noqa: F401
    apply_transforms,
    build_concept_map,
    build_template_expression,
    collect_stage1_tasks,
    config_requires_concept_lookup,
    config_type_to_polars,
    describe_code_mapping,
    find_concept_id_columns_for_prescan,
    find_omop_table_files,
    get_meds_schema_from_config,
    get_property_column_info,
    prescan_concept_ids,
    process_omop_file_worker,
    table_requires_concept_lookup,
    transform_to_meds_unsorted,
    validate_config_against_data,
)

# ============================================================================
# SEQUENTIAL LOW-MEMORY SORT
# ============================================================================


def process_single_shard(args: Tuple) -> Dict:
    """Worker function to process a single shard (filter, sort, write)."""
    shard_id, unsorted_files, output_dir, num_shards, meds_schema = args

    try:
        shard_data = []

        for unsorted_file in unsorted_files:
            try:
                df = pl.scan_parquet(unsorted_file).filter((pl.col("subject_id") % num_shards) == shard_id).collect()
                if len(df) > 0:
                    shard_data.append(df)
            except Exception:
                continue

        if not shard_data:
            return {"shard_id": shard_id, "rows": 0, "success": True}

        shard_df = pl.concat(shard_data, rechunk=True)
        row_count = len(shard_df)

        cast_exprs = [pl.col(col).cast(dtype) for col, dtype in meds_schema.items() if col in shard_df.columns]
        shard_df = shard_df.select(cast_exprs)
        shard_df = shard_df.sort(["subject_id", "time"])

        output_file = output_dir / f"{shard_id}.parquet"
        shard_df.write_parquet(output_file, compression="zstd")

        del shard_df
        del shard_data

        return {"shard_id": shard_id, "rows": row_count, "success": True}

    except Exception as e:
        return {"shard_id": shard_id, "rows": 0, "success": False, "error": str(e)}


def parallel_shard_sort(
    unsorted_dir: Path,
    output_dir: Path,
    num_shards: int,
    num_workers: int,
    meds_schema: Dict[str, type],
    process_method: str = "spawn",
    verbose: bool = False,
) -> None:
    """
    Parallel shard-based external sort.

    Each worker processes one complete shard at a time:
    load data → filter → sort → write → next shard.

    Writes to output_dir/result/data/ to match other backends.
    """
    print("\n✅ Using parallel shard-based sort...")
    print(f"   Workers: {num_workers}")
    print(f"   Shards: {num_shards}")
    print(f"   Memory usage: ~{num_workers} shards in RAM at once")

    result_dir = output_dir / "result"
    final_data_dir = result_dir / "data"
    final_data_dir.mkdir(parents=True, exist_ok=True)

    unsorted_files = list(unsorted_dir.glob("*.parquet"))
    if not unsorted_files:
        print(f"   ⚠️  No unsorted files found in {unsorted_dir}")
        return

    print(f"\n   Found {len(unsorted_files)} unsorted files")

    tasks = [(shard_id, unsorted_files, final_data_dir, num_shards, meds_schema) for shard_id in range(num_shards)]

    if num_workers > 1:
        with mp.get_context(process_method).Pool(processes=num_workers) as pool:
            results = list(
                tqdm(pool.imap_unordered(process_single_shard, tasks), total=num_shards, desc="Processing shards")
            )
    else:
        results = []
        for task in tqdm(tasks, desc="Processing shards"):
            results.append(process_single_shard(task))

    successes = [r for r in results if r["success"]]
    failures = [r for r in results if not r["success"]]
    total_rows = sum(r["rows"] for r in successes)

    print("\n   ✅ Parallel shard sort complete")
    print(f"   Shards processed: {len(successes)}/{num_shards}")
    print(f"   Total rows: {total_rows:,}")

    if failures and verbose:
        print(f"   ⚠️  {len(failures)} shards failed:")
        for f in failures[:5]:
            print(f"      Shard {f['shard_id']}: {f.get('error', 'Unknown error')}")


def sequential_shard_sort(
    unsorted_dir: Path,
    output_dir: Path,
    num_shards: int,
    meds_schema: Dict[str, type],
    verbose: bool = False,
) -> None:
    """
    Sequential low-memory external sort.

    Processes one shard at a time to minimize memory usage.
    Writes to output_dir/result/data/ to match other backends.
    """
    print("\n✅ Using sequential low-memory sort...")
    print(f"   Processing {num_shards} shards one at a time")
    print("   Memory usage: ~1 shard in RAM at a time")

    result_dir = output_dir / "result"
    final_data_dir = result_dir / "data"
    final_data_dir.mkdir(parents=True, exist_ok=True)

    unsorted_files = list(unsorted_dir.glob("*.parquet"))
    if not unsorted_files:
        print(f"   ⚠️  No unsorted files found in {unsorted_dir}")
        return

    print(f"\n   Found {len(unsorted_files)} unsorted files")

    for shard_id in tqdm(range(num_shards), desc="Processing shards"):
        shard_data = []

        for unsorted_file in unsorted_files:
            try:
                df = pl.scan_parquet(unsorted_file).filter((pl.col("subject_id") % num_shards) == shard_id).collect()
                if len(df) > 0:
                    shard_data.append(df)
            except Exception as e:
                if verbose:
                    print(f"\n   ⚠️  Error reading {unsorted_file}: {e}")
                continue

        if not shard_data:
            continue

        shard_df = pl.concat(shard_data, rechunk=True)

        cast_exprs = [pl.col(col).cast(dtype) for col, dtype in meds_schema.items() if col in shard_df.columns]
        shard_df = shard_df.select(cast_exprs)
        shard_df = shard_df.sort(["subject_id", "time"])

        output_file = final_data_dir / f"{shard_id}.parquet"
        shard_df.write_parquet(output_file, compression="zstd")

        del shard_df
        del shard_data

    print("\n   ✅ Sequential sort complete")
    print(f"   Output: {final_data_dir}")


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
    verbose: bool = False,
    low_memory: bool = False,
    parallel_shards: bool = False,
    process_method: str = "spawn",
    force_refresh: bool = False,
):
    """
    Run OMOP to MEDS ETL pipeline.

    Stage 1: OMOP → MEDS Unsorted (Python/Polars)
    Stage 2: External Sort (meds_etl_cpp or Python fallback)
    """
    print("\n" + "=" * 70)
    print("OMOP → MEDS ETL PIPELINE")
    print("=" * 70)

    with open(config_path, "r") as f:
        config = json.load(f)

    from meds_etl.config_compiler import compile_config
    from meds_etl.config_schema import validate_config_schema

    schema_errors = validate_config_schema(config)
    if schema_errors:
        print("\n❌ CONFIG VALIDATION FAILED\n")
        for err in schema_errors:
            print(f"  ✗ {err}")
        print()
        sys.exit(1)

    config = compile_config(config)

    primary_key = config.get("primary_key", "person_id")

    output_dir = Path(output_dir)
    temp_dir = output_dir / "temp"
    unsorted_dir = temp_dir / "unsorted_data"
    metadata_dir = temp_dir / "metadata"
    final_dir = output_dir / "data"

    if output_dir.exists():
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

    output_dir.mkdir(parents=True, exist_ok=True)
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)
    unsorted_dir.mkdir()
    metadata_dir.mkdir()

    validate_config_against_data(omop_dir, config, verbose=verbose)

    concept_lookup_needed = config_requires_concept_lookup(config)

    concept_df: Optional[pl.DataFrame] = None
    code_metadata: Dict[int, Any] = {}

    if concept_lookup_needed:
        print(f"\n{'=' * 70}")
        print("BUILDING CONCEPT MAP")
        print(f"{'=' * 70}")

        concept_df, code_metadata = build_concept_map(omop_dir, verbose=verbose)

        if len(concept_df) == 0:
            print("\n❌ ERROR: concept_id mappings requested but concept table not found!")
            print("   Update the config to avoid concept lookups or provide the concept table.")
            sys.exit(1)

        print(f"  ✅ Loaded {len(concept_df):,} concepts")

        if verbose:
            print("\n📊 Optimizing concept map (pre-scanning data to find used concepts)...")

        original_size = len(concept_df)

        used_concept_ids = prescan_concept_ids(
            omop_dir, config, num_workers, verbose=verbose, process_method=process_method
        )

        if used_concept_ids:
            concept_df = concept_df.filter(pl.col("concept_id").is_in(list(used_concept_ids)))
            filtered_size = len(concept_df)

            if verbose:
                reduction_pct = 100 * (1 - filtered_size / original_size) if original_size > 0 else 0
                print(
                    f"  ✅ Optimized: {original_size:,} → {filtered_size:,} concepts ({reduction_pct:.1f}% reduction)"
                )
    else:
        print("\n📋 Config does not require concept lookups - skipping concept map build")

    concept_df_data = pickle.dumps(concept_df) if concept_df is not None else None

    dataset_metadata = {
        "dataset_name": "OMOP",
        "dataset_version": time.strftime("%Y-%m-%d"),
        "etl_name": "meds_etl.omop",
        "etl_version": meds_etl.__version__,
        "meds_version": meds.__version__,
    }

    with open(metadata_dir / "dataset.json", "w") as f:
        json.dump(dataset_metadata, f, indent=2)

    if code_metadata:
        table = pa.Table.from_pylist(list(code_metadata.values()), meds.code_metadata_schema())
        pq.write_table(table, metadata_dir / "codes.parquet")

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

    meds_schema = get_meds_schema_from_config(config)
    if verbose:
        print("\n📋 Global MEDS schema:")
        print("  Core columns: subject_id, time, code, numeric_value, text_value, end")
        print(f"  Property columns: {len(meds_schema) - 6}")

    tasks = collect_stage1_tasks(
        config=config,
        omop_dir=omop_dir,
        primary_key=primary_key,
        unsorted_dir=unsorted_dir,
        meds_schema=meds_schema,
        concept_df_data=concept_df_data,
        compression="zstd",
    )

    print(f"\nProcessing {len(tasks)} files with {num_workers} workers...")

    if verbose and len(tasks) > 0:
        print("\n🔍 DEBUG: Processing first file to test...")
        test_result = process_omop_file_worker(tasks[0])
        print(f"   Test result: {test_result}")
        if test_result["success"]:
            print("   ✅ First file processed successfully")
            print(f"   Input: {test_result['input_rows']:,} rows")
            print(f"   Output: {test_result['output_rows']:,} rows")
        else:
            print(f"   ❌ First file failed: {test_result.get('error', 'Unknown')}")
        print()

    stage1_start = time.time()

    if num_workers > 1:
        os.environ["POLARS_MAX_THREADS"] = "1"
        with mp.get_context(process_method).Pool(processes=num_workers) as pool:
            results = list(
                tqdm(
                    pool.imap_unordered(process_omop_file_worker, tasks),
                    total=len(tasks),
                    desc="Processing OMOP files",
                )
            )
    else:
        results = []
        for task in tqdm(tasks, desc="Processing OMOP files"):
            results.append(process_omop_file_worker(task))

    stage1_elapsed = time.time() - stage1_start

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

    print(f"Time:             {stage1_elapsed:.2f}s ({stage1_elapsed/60:.2f}m)")

    print("\n📊 Per-table breakdown:")
    by_table: Dict[str, Dict] = {}
    for r in successes:
        table = r.get("table", "unknown")
        if table not in by_table:
            by_table[table] = {
                "files": 0,
                "input": 0,
                "output": 0,
                "filtered": 0,
                "code_mapping_used": r.get("code_mapping_used", "unknown"),
            }
        by_table[table]["files"] += 1
        by_table[table]["input"] += r.get("input_rows", 0)
        by_table[table]["output"] += r.get("output_rows", 0)
        by_table[table]["filtered"] += r.get("filtered_rows", 0)

    concept_df_size = successes[0].get("concept_df_size", 0) if successes else 0
    print(f"   Concept DataFrame size: {concept_df_size:,} concepts\n")

    for table, stats in sorted(by_table.items(), key=lambda x: x[1]["input"], reverse=True):
        retention = 100 * stats["output"] / stats["input"] if stats["input"] > 0 else 0
        mapping_used = stats["code_mapping_used"]
        print(
            f"   {table:25s} [{mapping_used:12s}]  {stats['files']:3d} files  "
            f"{stats['input']:10,} → {stats['output']:10,} rows ({retention:5.1f}%)"
        )

    if failures:
        print(f"\n⚠️  {len(failures)} files failed:")
        for f in failures[:10]:
            print(f"   - {f.get('table', 'unknown'):20s} / {f['file']}: {f.get('error', 'Unknown error')}")
            if "traceback" in f and verbose:
                print(f"     {f['traceback']}")

    written_files = list(unsorted_dir.glob("*.parquet"))
    print(f"\n💾 Files written to {unsorted_dir}:")
    print(f"   Total files: {len(written_files)}")
    if len(written_files) > 0:
        total_size = sum(f.stat().st_size for f in written_files) / 1024 / 1024
        print(f"   Total size:  {total_size:.1f} MB")
    else:
        print("   ⚠️  NO FILES WRITTEN!")

    # ========================================================================
    # STAGE 2: External sort
    # ========================================================================

    print("\n" + "=" * 70)
    print("STAGE 2: EXTERNAL SORT (partition + sort)")
    print("=" * 70)

    stage2_start = time.time()

    if num_shards is None:
        num_shards = config.get("num_shards", 100)

    if verbose:
        print(f"\n📊 Shards: {num_shards}, Workers: {num_workers}")

    if low_memory:
        print("\n🔧 Low-memory mode enabled")
        sequential_shard_sort(unsorted_dir, output_dir, num_shards, meds_schema, verbose=verbose)
    elif parallel_shards:
        print("\n🔧 Parallel shard mode enabled")
        parallel_shard_sort(
            unsorted_dir, output_dir, num_shards, num_workers, meds_schema, process_method, verbose=verbose
        )
    else:
        if backend == "cpp":
            try:
                import meds_etl_cpp  # noqa: F401

                print("\n✅ Using meds_etl_cpp (C++) for external sort...")
            except ImportError:
                print("\n❌ ERROR: meds_etl_cpp not available but --backend cpp was specified")
                sys.exit(1)
        elif backend == "auto":
            try:
                import meds_etl_cpp  # noqa: F401

                print("\n✅ Using meds_etl_cpp (C++) for external sort...")
            except ImportError:
                print("\n⚠️  meds_etl_cpp not available, using Python fallback...")
                backend = "polars"
        else:
            print("\n⚠️  Using Python/Polars for sorting...")

        meds_etl.unsorted.sort(
            source_unsorted_path=str(temp_dir),
            target_meds_path=str(output_dir / "result"),
            num_shards=num_shards,
            num_proc=num_workers,
            backend=backend,
        )

        print("   ✅ External sort complete")

    result_data_dir = output_dir / "result" / "data"
    if result_data_dir.exists():
        if final_dir.exists():
            shutil.rmtree(final_dir)
        shutil.move(str(result_data_dir), str(final_dir))
        shutil.rmtree(output_dir / "result")

    stage2_elapsed = time.time() - stage2_start
    print(f"\n✅ Stage 2 complete: {stage2_elapsed:.2f}s ({stage2_elapsed/60:.2f}m)")

    print("\nCleaning up temporary directory...")
    shutil.rmtree(temp_dir)

    total_elapsed = stage1_elapsed + stage2_elapsed

    print("\n" + "=" * 70)
    print("ETL COMPLETE")
    print("=" * 70)
    print(f"Total time:   {total_elapsed:.2f}s ({total_elapsed/60:.2f}m)")
    print(f"Total rows:   {total_output_rows:,}")
    if total_output_rows > 0:
        print(f"Throughput:   {total_output_rows/total_elapsed:,.0f} rows/s")
    print(f"Output dir:   {final_dir}")
    print("=" * 70)


# ============================================================================
# CLI
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="OMOP to MEDS ETL Pipeline")

    parser.add_argument("--omop_dir", required=True, help="Path to OMOP data directory (Parquet files)")
    parser.add_argument("--output_dir", required=True, help="Output directory for MEDS data")
    parser.add_argument("--config", required=True, help="Path to ETL config JSON file")
    parser.add_argument(
        "--workers", type=int, default=mp.cpu_count(), help="Number of worker processes (default: all CPUs)"
    )
    parser.add_argument(
        "--shards", type=int, default=None, help="Number of output shards (default: from config or 100)"
    )
    parser.add_argument(
        "--backend",
        choices=["cpp", "polars", "auto"],
        default="auto",
        help="Stage 2 backend: 'auto' (try cpp, fallback polars), 'cpp', 'polars'. Default: auto",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--low-memory", action="store_true", help="Use sequential shard processing (minimal memory)")
    parser.add_argument(
        "--parallel-shards", action="store_true", help="Use parallel shard processing (moderate memory)"
    )
    parser.add_argument(
        "--process_method",
        choices=["spawn", "fork"],
        default="spawn",
        help="Multiprocessing start method. Default: spawn",
    )
    parser.add_argument("--force-refresh", action="store_true", help="Delete existing output directory if it exists")

    args = parser.parse_args()

    run_omop_to_meds_etl(
        omop_dir=Path(args.omop_dir),
        output_dir=Path(args.output_dir),
        config_path=Path(args.config),
        num_workers=args.workers,
        num_shards=args.shards,
        backend=args.backend,
        verbose=args.verbose,
        low_memory=args.low_memory,
        parallel_shards=args.parallel_shards,
        process_method=args.process_method,
        force_refresh=args.force_refresh,
    )


if __name__ == "__main__":
    main()
