"""
OMOP to MEDS ETL with Polars Streaming External Sort

This version uses Polars 1.x streaming engine for Stage 2 (external sort):
- Stage 1: OMOP → MEDS Unsorted (same as omop_refactor.py)
- Stage 2: Streaming external sort using Polars lazy evaluation + sink_parquet

Key advantage: Polars' native k-way merge is faster than manual implementations,
and uses bounded memory through streaming.

Based on external_sort.py approach.
"""

from pathlib import Path
import shutil
from typing import Optional
import polars as pl
import os
import gc

# Import Stage 1 from omop_refactor.py
import sys

sys.path.insert(0, str(Path(__file__).parent))

from omop_refactor import (
    run_omop_to_meds_etl,
    get_meds_schema_from_config,
    find_omop_table_files,
    process_omop_file_worker,
)
import json
import time
from tqdm import tqdm


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
    print(f"\n[Stage 2.1] Creating sorted runs (optimized + parallel)...")
    print(f"  Shards: {num_shards}")
    print(f"  Workers: {num_workers}")
    print(f"  Chunk size: {chunk_rows:,} rows")
    print(f"  Strategy: Read-once partitioning")

    runs_dir.mkdir(parents=True, exist_ok=True)

    # Create shard directories
    for shard_id in range(num_shards):
        shard_dir = runs_dir / f"shard_{shard_id}"
        shard_dir.mkdir(exist_ok=True)

    # Find all unsorted files
    unsorted_files = sorted(unsorted_dir.glob("*.parquet"))

    if not unsorted_files:
        print(f"  WARNING: No unsorted files found")
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
    print(f"\n  ✓ Partitioning complete")
    print(f"  Total runs created: {sum(total_run_counts)}")
    print(f"  Runs per shard (avg): {sum(total_run_counts) / num_shards:.1f}")

    if verbose:
        print(f"\n  Detailed runs per shard:")
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
    print(f"\n" + "=" * 70)
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
            print(f"  Run with --pipeline partition first!")
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
            print(f"\n[Stage 2.2] Streaming merge (sequential)...")
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

    print(f"\n" + "=" * 70)
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
        print(f"\nPer-shard breakdown:")
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
    code_mapping_mode: str = "auto",
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
            print(f"Set POLARS_MAX_THREADS=1 (default for multiprocessing)")
    
    # Configure Rayon threading (used by Polars internally)
    if rayon_threads is not None:
        os.environ["RAYON_NUM_THREADS"] = str(rayon_threads)
        if verbose:
            print(f"Set RAYON_NUM_THREADS={rayon_threads} (overriding env var)")
    elif "RAYON_NUM_THREADS" not in os.environ:
        # Default: single-threaded per worker
        os.environ["RAYON_NUM_THREADS"] = "1"
        if verbose:
            print(f"Set RAYON_NUM_THREADS=1 (default for multiprocessing)")
    
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

    # Determine num_shards
    if num_shards is None:
        num_shards = config.get("num_shards", 100)

    # Setup directories
    output_dir = Path(output_dir)
    temp_dir = output_dir / "temp"
    unsorted_dir = temp_dir / "unsorted_data"

    # Run Stage 1 using original implementation
    # This creates unsorted MEDS files
    print("\n" + "=" * 70)
    print("STREAMING ETL MODE")
    print("=" * 70)
    print(f"Pipeline stages: {', '.join(sorted(stages_to_run))}")
    print(f"Run compression: {run_compression}")
    print(f"Final compression: {final_compression}")
    print(f"Using Polars 1.x streaming for Stage 2")

    from omop_refactor import (
        validate_config_against_data,
        build_concept_map,
        prescan_concept_ids,
        get_meds_schema_from_config,
        find_omop_table_files,
    )
    import meds
    import meds_etl
    import pyarrow as pa
    import pyarrow.parquet as pq
    import multiprocessing as mp
    import pickle

    primary_key = config.get("primary_key", "person_id")

    # Clean and create directories
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)
    unsorted_dir.mkdir()
    metadata_dir = temp_dir / "metadata"
    metadata_dir.mkdir()

    # Determine code mapping
    if code_mapping_mode == "auto":
        code_mapping_choice = "concept_id"
    else:
        code_mapping_choice = code_mapping_mode

    # Validate config
    validate_config_against_data(omop_dir, config, code_mapping_choice, verbose=verbose)

    # Build concept map if needed
    concept_df = pl.DataFrame(schema={"concept_id": pl.Int64, "code": pl.Utf8})
    code_metadata = {}

    if code_mapping_choice == "concept_id":
        print(f"\n{'=' * 70}")
        print("BUILDING CONCEPT MAP")
        print(f"{'=' * 70}")

        concept_df, code_metadata = build_concept_map(omop_dir, verbose=verbose)

        if len(concept_df) == 0:
            print(f"\nERROR: concept_id mapping requested but concept table not found!")
            sys.exit(1)

        print(f"  Loaded {len(concept_df):,} concepts")

        # Optimize if requested
        if optimize_concepts:
            original_size = len(concept_df)
            
            if verbose:
                print(f"\nOptimizing concept map (pre-scanning data to find used concepts)...")
                print(f"  Using {num_workers} workers to scan OMOP files")
                print(f"  (If this hangs on Linux, ensure --process_method spawn is set)")
            
            used_concept_ids = prescan_concept_ids(omop_dir, config, num_workers, verbose=verbose)

            if used_concept_ids:
                concept_df = concept_df.filter(pl.col("concept_id").is_in(list(used_concept_ids)))
                if verbose:
                    print(f"  Optimized: {original_size:,} -> {len(concept_df):,} concepts")

    concept_df_data = pickle.dumps(concept_df)

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
                        code_mapping_choice,
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
                        code_mapping_choice,
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
        print(f"\nPer-table breakdown:")
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
            print(f"  Run with --pipeline transform first!")
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
    print(f"\nCleaning up temporary directory...")
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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="OMOP to MEDS ETL with Polars Streaming")

    parser.add_argument("--omop_dir", required=True, help="OMOP data directory")
    parser.add_argument("--output_dir", required=True, help="Output directory")
    parser.add_argument("--config", required=True, help="ETL config JSON")
    parser.add_argument("--workers", type=int, default=8, help="Stage 1 workers")
    parser.add_argument("--shards", type=int, default=None, help="Number of shards")
    parser.add_argument("--chunk_rows", type=int, default=10_000_000, help="Rows per sorted run")
    parser.add_argument("--code_mapping", choices=["auto", "concept_id", "source_value"], default="auto")
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
    memory_group = parser.add_argument_group("Memory Configuration", 
                                              "Control memory usage and profiling (important for laptops)")
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

    args = parser.parse_args()

    run_omop_to_meds_streaming(
        omop_dir=Path(args.omop_dir),
        output_dir=Path(args.output_dir),
        config_path=Path(args.config),
        num_workers=args.workers,
        num_shards=args.shards,
        code_mapping_mode=args.code_mapping,
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
    )
