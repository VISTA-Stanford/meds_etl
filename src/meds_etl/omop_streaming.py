"""
OMOP to MEDS ETL with Polars Streaming External Sort

Stage 1: OMOP → MEDS Unsorted (shared with omop.py via omop_common.py)
Stage 2: Streaming external sort using Polars lazy evaluation + sink_parquet

Key advantage: Polars' native k-way merge is faster than manual implementations,
and uses bounded memory through streaming.
"""

import gc
import json
import multiprocessing as mp
import os
import pickle
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl
from tqdm import tqdm

# Re-export everything tests and external consumers might import from this module.
# This maintains backwards compatibility after moving code to omop_common.
from meds_etl.omop_common import (  # noqa: F401
    apply_transforms,
    build_concept_map,
    build_relationship_resolution_map,
    build_template_expression,
    collect_stage1_tasks,
    config_requires_concept_lookup,
    config_requires_relationship_mapping,
    config_type_to_polars,
    describe_code_mapping,
    fast_scan_file_for_concept_ids,
    find_concept_id_columns_for_prescan,
    find_omop_table_files,
    get_meds_schema_from_config,
    get_metadata_column_info,
    get_property_column_info,
    prescan_concept_ids,
    prescan_worker,
    process_omop_file_worker,
    table_requires_concept_lookup,
    transform_to_meds_unsorted,
    validate_config_against_data,
    validate_parquet_file,
)

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
        df = pl.read_parquet(unsorted_file, low_memory=low_memory)

        if len(df) == 0:
            return run_counts

        df = df.with_columns([(pl.col("subject_id") % num_shards).alias("_shard_id")])

        for shard_id in range(num_shards):
            shard_data = df.filter(pl.col("_shard_id") == shard_id).drop("_shard_id")

            if len(shard_data) == 0:
                continue

            num_rows = len(shard_data)
            num_chunks = (num_rows + chunk_rows - 1) // chunk_rows

            for chunk_idx in range(num_chunks):
                start_idx = chunk_idx * chunk_rows
                end_idx = min((chunk_idx + 1) * chunk_rows, num_rows)

                chunk = shard_data[start_idx:end_idx]
                sorted_chunk = chunk.sort(["subject_id", "time"])

                shard_dir = runs_dir / f"shard_{shard_id}"
                run_file = shard_dir / f"run_{file_idx:05d}_{chunk_idx:05d}.parquet"
                sorted_chunk.write_parquet(run_file, compression=compression)

                run_counts[shard_id] += 1

            del shard_data

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
    Stage 2.1: Partition unsorted data into sorted runs.

    Reads each unsorted file ONCE and partitions to all shards.
    """
    print("\n[Stage 2.1] Creating sorted runs (optimized + parallel)...")
    print(f"  Shards: {num_shards}")
    print(f"  Workers: {num_workers}")
    print(f"  Chunk size: {chunk_rows:,} rows")
    print("  Strategy: Read-once partitioning")

    runs_dir.mkdir(parents=True, exist_ok=True)

    for shard_id in range(num_shards):
        shard_dir = runs_dir / f"shard_{shard_id}"
        shard_dir.mkdir(exist_ok=True)

    unsorted_files = sorted(unsorted_dir.glob("*.parquet"))

    if not unsorted_files:
        print("  WARNING: No unsorted files found")
        return

    print(f"  Unsorted files: {len(unsorted_files)}")

    worker_args = [
        (file_idx, unsorted_file, runs_dir, num_shards, chunk_rows, compression, verbose, low_memory)
        for file_idx, unsorted_file in enumerate(unsorted_files)
    ]

    if num_workers == 1:
        all_run_counts = []
        for args in tqdm(worker_args, desc="Partitioning files"):
            run_counts = _partition_file_worker(args)
            all_run_counts.append(run_counts)
    else:
        with mp.Pool(processes=num_workers) as pool:
            all_run_counts = list(
                tqdm(
                    pool.imap_unordered(_partition_file_worker, worker_args),
                    total=len(worker_args),
                    desc="Partitioning files",
                )
            )

    total_run_counts = [0] * num_shards
    for run_counts in all_run_counts:
        for shard_id in range(num_shards):
            total_run_counts[shard_id] += run_counts[shard_id]

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

    Uses Polars' lazy evaluation + streaming engine for memory-bounded k-way merge.
    """
    run_files = sorted(shard_dir.glob("run_*.parquet"))

    if not run_files:
        return 0

    scans = [pl.scan_parquet(str(f), low_memory=low_memory) for f in run_files]

    (
        pl.concat(scans, how="vertical")
        .sort(["subject_id", "time"])
        .sink_parquet(
            str(output_file),
            compression=compression,
            maintain_order=True,
            row_group_size=row_group_size,
        )
    )

    row_count = pl.scan_parquet(output_file).select(pl.len()).collect().item()

    gc.collect()

    return row_count


def _merge_shard_worker(args):
    """Worker function for parallel shard merging (module-level for pickling)."""
    shard_dir, output_file, compression, low_memory, row_group_size = args

    rows = streaming_merge_shard(
        shard_dir,
        output_file,
        compression=compression,
        verbose=False,
        low_memory=low_memory,
        row_group_size=row_group_size,
    )

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
    """
    print("\n" + "=" * 70)
    print("STAGE 2: STREAMING EXTERNAL SORT")
    print("=" * 70)

    stage2_start = time.time()

    runs_dir = output_dir / "runs_temp"
    final_dir = output_dir / "data"
    final_dir.mkdir(parents=True, exist_ok=True)

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
        if not runs_dir.exists():
            print(f"ERROR: No runs found at {runs_dir}")
            print("  Run with --pipeline partition first!")
            sys.exit(1)

    total_rows = 0

    if run_merge:
        merge_tasks = []
        for shard_id in range(num_shards):
            shard_dir = runs_dir / f"shard_{shard_id}"
            if shard_dir.exists():
                output_file = final_dir / f"{shard_id}.parquet"
                merge_tasks.append((shard_dir, output_file, final_compression, low_memory, row_group_size))

        if merge_workers == 0:
            num_merge_workers = max(1, mp.cpu_count() // 2)
        else:
            num_merge_workers = merge_workers

        if num_merge_workers == 1:
            print("\n[Stage 2.2] Streaming merge (sequential)...")
            row_counts = []
            for task in tqdm(merge_tasks, desc="Merging shards"):
                rows = _merge_shard_worker(task)
                row_counts.append(rows)
        else:
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

        if run_partition:
            shutil.rmtree(runs_dir)
    else:
        print("\nSkipping Stage 2.2 (merge)")

    stage2_elapsed = time.time() - stage2_start

    print("\n" + "=" * 70)
    print("STAGE 2 COMPLETE: STREAMING SORT")
    print("=" * 70)
    print(f"Total rows:    {total_rows:,}")
    print(f"Shards:        {num_shards}")
    print(f"Time:          {stage2_elapsed:.2f}s ({stage2_elapsed/60:.2f}m)")
    if stage2_elapsed > 0:
        print(f"Throughput:    {total_rows / stage2_elapsed / 1_000_000:.2f}M rows/sec")

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
    chunk_rows: int = 10_000_000,
    run_compression: str = "lz4",
    final_compression: str = "zstd",
    merge_workers: int = 0,
    pipeline_stages: str = "all",
    low_memory: bool = False,
    row_group_size: int = 100_000,
    polars_threads: Optional[int] = None,
    rayon_threads: Optional[int] = None,
    process_method: str = "spawn",
    force_refresh: bool = False,
):
    """
    Run OMOP to MEDS ETL with streaming external sort.

    Same Stage 1 as omop.py but uses Polars streaming for Stage 2.
    """
    # Configure multiprocessing (must be before any pool creation)
    try:
        mp.set_start_method(process_method, force=True)
        if verbose:
            print(f"Multiprocessing method: '{process_method}'")
    except RuntimeError:
        current_method = mp.get_start_method()
        if current_method != process_method and verbose:
            print(f"Warning: multiprocessing method already set to '{current_method}' (requested '{process_method}')")

    if polars_threads is not None:
        os.environ["POLARS_MAX_THREADS"] = str(polars_threads)
    elif "POLARS_MAX_THREADS" not in os.environ:
        os.environ["POLARS_MAX_THREADS"] = "1"

    if rayon_threads is not None:
        os.environ["RAYON_NUM_THREADS"] = str(rayon_threads)
    elif "RAYON_NUM_THREADS" not in os.environ:
        os.environ["RAYON_NUM_THREADS"] = "1"

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

    if num_shards is None:
        num_shards = config.get("num_shards", 100)

    output_dir = Path(output_dir)
    temp_dir = output_dir / "temp"
    unsorted_dir = temp_dir / "unsorted_data"

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

    print("\n" + "=" * 70)
    print("STREAMING ETL MODE")
    print("=" * 70)
    print(f"Pipeline stages: {', '.join(sorted(stages_to_run))}")
    print(f"Run compression: {run_compression}")
    print(f"Final compression: {final_compression}")
    print("Using Polars 1.x streaming for Stage 2")

    import meds
    import pyarrow as pa
    import pyarrow.parquet as pq

    import meds_etl

    primary_key = config.get("primary_key", "person_id")

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)
    unsorted_dir.mkdir()
    metadata_dir = temp_dir / "metadata"
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
            print("\nERROR: concept_id mappings requested but concept table not found!")
            sys.exit(1)

        print(f"  Loaded {len(concept_df):,} concepts")

        original_size = len(concept_df)

        if verbose:
            print("\nOptimizing concept map (pre-scanning data to find used concepts)...")

        used_concept_ids = prescan_concept_ids(
            omop_dir, config, num_workers, verbose=verbose, process_method=process_method
        )

        if used_concept_ids:
            concept_df = concept_df.filter(pl.col("concept_id").is_in(list(used_concept_ids)))
            if verbose:
                print(f"  Optimized: {original_size:,} -> {len(concept_df):,} concepts")
    else:
        print("\n📋 Config does not require concept lookups - skipping concept map build")

    # Build relationship resolution map if any table uses concept_relationship mapping
    relationship_map_df: Optional[pl.DataFrame] = None

    if config_requires_relationship_mapping(config) and concept_df is not None and len(concept_df) > 0:
        print("\n  Building concept_relationship resolution map...")
        relationship_map_df = build_relationship_resolution_map(omop_dir, concept_df, verbose=verbose)
        if relationship_map_df is not None:
            print(f"  ✅ {len(relationship_map_df):,} source → standard concept mappings loaded")
        else:
            print("  ⚠️  No concept_relationship table found — falling back to direct concept lookups")

    concept_df_data = pickle.dumps(concept_df) if concept_df is not None else None
    relationship_map_data = pickle.dumps(relationship_map_df) if relationship_map_df is not None else None

    dataset_metadata = {
        "dataset_name": "OMOP",
        "dataset_version": time.strftime("%Y-%m-%d"),
        "etl_name": "meds_etl.omop_streaming",
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

    stage1_elapsed = 0
    total_output_rows = 0

    if "transform" in stages_to_run:
        print("\n" + "=" * 70)
        print("STAGE 1: OMOP → MEDS UNSORTED")
        print("=" * 70)

        meds_schema = get_meds_schema_from_config(config)

        tasks = collect_stage1_tasks(
            config=config,
            omop_dir=omop_dir,
            primary_key=primary_key,
            unsorted_dir=unsorted_dir,
            meds_schema=meds_schema,
            concept_df_data=concept_df_data,
            compression="lz4",
            relationship_map_data=relationship_map_data,
        )

        print(f"\nProcessing {len(tasks)} files with {num_workers} workers...")

        stage1_start = time.time()

        if num_workers > 1:
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

        print("\nPer-table breakdown:")
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
                f"   {table:30s} [{mapping_used:20s}] {stats['files']:4d} files  "
                f"{stats['input']:>13,} -> {stats['output']:>13,} rows ({retention:5.1f}%)"
            )

        if failures:
            print(f"\nWARNING: {len(failures)} files failed:")
            for f in failures[:10]:
                print(f"  - {f.get('table', 'unknown'):20s} / {f['file']}: {f.get('error', 'Unknown error')}")

        written_files = list(unsorted_dir.glob("*.parquet"))
        print(f"\nFiles written to {unsorted_dir}:")
        print(f"  Total files: {len(written_files)}")
        if len(written_files) > 0:
            total_size = sum(f.stat().st_size for f in written_files) / 1024 / 1024
            print(f"  Total size:  {total_size:.1f} MB")
        print(f"{'=' * 70}")
    else:
        print("\nSkipping Stage 1 (transform)")
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

        run_partition = "partition" in stages_to_run
        run_merge = "merge" in stages_to_run

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

    print("\nCleaning up temporary directory...")
    shutil.rmtree(temp_dir)
    print(f"  ✓ Removed: {temp_dir}")

    total_elapsed = stage1_elapsed + stage2_elapsed

    print("\n" + "=" * 70)
    print("ETL COMPLETE")
    print("=" * 70)
    print(f"Stage 1 time: {stage1_elapsed:.2f}s ({stage1_elapsed/60:.2f}m)")
    print(f"Stage 2 time: {stage2_elapsed:.2f}s ({stage2_elapsed/60:.2f}m)")
    print(f"Total time:   {total_elapsed:.2f}s ({total_elapsed/60:.2f}m)")
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

    memory_group = parser.add_argument_group("Memory Configuration", "Control memory usage (important for laptops)")
    memory_group.add_argument("--low_memory", action="store_true", help="Enable Polars low_memory mode")
    memory_group.add_argument(
        "--row_group_size", type=int, default=100_000, help="Rows per Parquet row group (default: 100_000)"
    )
    memory_group.add_argument("--polars_threads", type=int, default=None, help="Max threads for Polars")
    memory_group.add_argument("--rayon_threads", type=int, default=None, help="Max threads for Rayon")
    memory_group.add_argument(
        "--process_method",
        choices=["spawn", "fork"],
        default="spawn",
        help="Multiprocessing start method (default: spawn)",
    )

    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--force-refresh", action="store_true", help="Delete existing output directory if it exists")

    args = parser.parse_args()

    run_omop_to_meds_streaming(
        omop_dir=Path(args.omop_dir),
        output_dir=Path(args.output_dir),
        config_path=Path(args.config),
        num_workers=args.workers,
        num_shards=args.shards,
        verbose=args.verbose,
        chunk_rows=args.chunk_rows,
        run_compression=args.run_compression,
        final_compression=args.final_compression,
        merge_workers=args.merge_workers,
        pipeline_stages=args.pipeline,
        low_memory=args.low_memory,
        row_group_size=args.row_group_size,
        polars_threads=args.polars_threads,
        rayon_threads=args.rayon_threads,
        process_method=args.process_method,
        force_refresh=args.force_refresh,
    )


if __name__ == "__main__":
    main()
