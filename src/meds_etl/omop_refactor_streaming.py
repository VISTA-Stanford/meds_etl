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


def partition_to_sorted_runs(
    unsorted_dir: Path,
    runs_dir: Path,
    num_shards: int,
    chunk_rows: int = 10_000_000,
    verbose: bool = False,
) -> None:
    """
    Stage 2.1: Partition unsorted data into sorted runs.
    
    For each shard:
    - Read all unsorted files in chunks
    - Filter for this shard (subject_id % num_shards == shard_id)
    - Sort chunk by (subject_id, time)
    - Write as sorted run
    
    This creates many small sorted files that will be merged in Stage 2.2
    """
    print(f"\n[Stage 2.1] Creating sorted runs...")
    print(f"  Shards: {num_shards}")
    print(f"  Chunk size: {chunk_rows:,} rows")
    print(f"  Strategy: Polars lazy + streaming")
    
    runs_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all unsorted files
    unsorted_files = sorted(unsorted_dir.glob("*.parquet"))
    
    if not unsorted_files:
        print(f"  ⚠️  No unsorted files found")
        return
    
    print(f"  Unsorted files: {len(unsorted_files)}")
    
    # For each shard, create sorted runs
    for shard_id in tqdm(range(num_shards), desc="Creating runs"):
        shard_dir = runs_dir / f"shard_{shard_id}"
        shard_dir.mkdir(exist_ok=True)
        
        run_count = 0
        
        # Process each unsorted file
        for file_idx, unsorted_file in enumerate(unsorted_files):
            try:
                # Lazy scan and filter for this shard
                lazy_df = pl.scan_parquet(unsorted_file).filter(
                    (pl.col("subject_id") % num_shards) == shard_id
                )
                
                # Process in chunks (streaming)
                chunk_idx = 0
                offset = 0
                
                while True:
                    # Fetch chunk
                    chunk = lazy_df.slice(offset, chunk_rows).collect()
                    
                    if len(chunk) == 0:
                        break
                    
                    # Sort chunk
                    sorted_chunk = chunk.sort(["subject_id", "time"])
                    
                    # Write as run
                    run_file = shard_dir / f"run_{file_idx:05d}_{chunk_idx:05d}.parquet"
                    sorted_chunk.write_parquet(run_file, compression="zstd")
                    
                    run_count += 1
                    chunk_idx += 1
                    offset += chunk_rows
                    
            except Exception as e:
                if verbose:
                    print(f"\n  ⚠️  Error processing {unsorted_file.name}: {e}")
                continue
        
        if verbose and run_count > 0:
            print(f"  Shard {shard_id}: {run_count} runs")


def streaming_merge_shard(
    shard_dir: Path,
    output_file: Path,
    verbose: bool = False,
) -> int:
    """
    Stage 2.2: Streaming merge of sorted runs using Polars.
    
    Uses Polars' lazy evaluation + streaming engine:
    - Lazy scan all run files
    - Concat them
    - Sort (triggers streaming k-way merge)
    - Sink to output (streaming write)
    
    This is memory-bounded and efficient (native Rust implementation).
    """
    run_files = sorted(shard_dir.glob("run_*.parquet"))
    
    if not run_files:
        return 0
    
    # Lazy scan all runs
    scans = [pl.scan_parquet(str(f)) for f in run_files]
    
    # Concat + sort + streaming sink
    # Polars automatically uses streaming k-way merge here!
    (
        pl.concat(scans, how="vertical")
        .sort(["subject_id", "time"])
        .sink_parquet(
            str(output_file),
            compression="zstd",
            maintain_order=True  # Important for sorted output
        )
    )
    
    # Get row count
    row_count = pl.scan_parquet(output_file).select(pl.len()).collect().item()
    
    return row_count


def streaming_external_sort(
    unsorted_dir: Path,
    output_dir: Path,
    num_shards: int,
    chunk_rows: int = 10_000_000,
    verbose: bool = False,
) -> None:
    """
    Complete streaming external sort using Polars 1.x.
    
    Two stages:
    1. Partition unsorted data into sorted runs (chunked processing)
    2. Streaming k-way merge of runs (Polars native)
    
    Memory-bounded, fast, and simple.
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
    partition_to_sorted_runs(
        unsorted_dir=unsorted_dir,
        runs_dir=runs_dir,
        num_shards=num_shards,
        chunk_rows=chunk_rows,
        verbose=verbose
    )
    
    # Stage 2.2: Streaming merge per shard
    print(f"\n[Stage 2.2] Streaming merge...")
    
    total_rows = 0
    for shard_id in tqdm(range(num_shards), desc="Merging shards"):
        shard_dir = runs_dir / f"shard_{shard_id}"
        
        if not shard_dir.exists():
            continue
        
        output_file = final_dir / f"{shard_id}.parquet"
        
        rows = streaming_merge_shard(
            shard_dir=shard_dir,
            output_file=output_file,
            verbose=verbose
        )
        
        total_rows += rows
    
    # Cleanup runs
    shutil.rmtree(runs_dir)
    
    stage2_elapsed = time.time() - stage2_start
    
    print(f"\n" + "=" * 70)
    print("STREAMING SORT COMPLETE")
    print("=" * 70)
    print(f"Total rows:    {total_rows:,}")
    print(f"Shards:        {num_shards}")
    print(f"Time:          {stage2_elapsed:.2f}s")
    if stage2_elapsed > 0:
        print(f"Throughput:    {total_rows / stage2_elapsed / 1_000_000:.2f}M rows/sec")
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
):
    """
    Run OMOP to MEDS ETL with streaming external sort.
    
    Same as omop_refactor.py but uses Polars streaming for Stage 2.
    
    Args:
        chunk_rows: Rows per sorted run (default 10M - adjust based on RAM)
    """
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
            print(f"\n❌ ERROR: concept_id mapping requested but concept table not found!")
            sys.exit(1)
        
        print(f"  ✅ Loaded {len(concept_df):,} concepts")
        
        # Optimize if requested
        if optimize_concepts:
            original_size = len(concept_df)
            used_concept_ids = prescan_concept_ids(omop_dir, config, num_workers, verbose=verbose)
            
            if used_concept_ids:
                concept_df = concept_df.filter(pl.col("concept_id").is_in(list(used_concept_ids)))
                if verbose:
                    print(f"  ✅ Optimized: {original_size:,} → {len(concept_df):,} concepts")
    
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
            tasks.append((
                file_path, table_name, event_config, primary_key,
                code_mapping_choice, unsorted_dir, meds_schema,
                concept_df_data, True, fixed_code
            ))
    
    # Regular tables
    for table_name, table_config in config.get("tables", {}).items():
        files = find_omop_table_files(omop_dir, table_name)
        
        for file_path in files:
            tasks.append((
                file_path, table_name, table_config, primary_key,
                code_mapping_choice, unsorted_dir, meds_schema,
                concept_df_data, False, None
            ))
    
    print(f"\nProcessing {len(tasks)} files with {num_workers} workers...")
    
    stage1_start = time.time()
    
    if num_workers > 1:
        import os
        os.environ["POLARS_MAX_THREADS"] = "1"
        with mp.Pool(processes=num_workers) as pool:
            results = list(
                tqdm(
                    pool.imap_unordered(process_omop_file_worker, tasks),
                    total=len(tasks),
                    desc="Processing OMOP files"
                )
            )
    else:
        results = [process_omop_file_worker(task) for task in tqdm(tasks, desc="Processing")]
    
    stage1_elapsed = time.time() - stage1_start
    
    # Report Stage 1 results
    successes = [r for r in results if r["success"]]
    total_output_rows = sum(r.get("output_rows", 0) for r in successes)
    
    print(f"\n✅ Stage 1 complete: {stage1_elapsed:.2f}s")
    print(f"   Output rows: {total_output_rows:,}")
    
    # ========================================================================
    # STAGE 2: Streaming External Sort (Polars 1.x)
    # ========================================================================
    
    streaming_external_sort(
        unsorted_dir=unsorted_dir,
        output_dir=output_dir,
        num_shards=num_shards,
        chunk_rows=chunk_rows,
        verbose=verbose
    )
    
    # Cleanup temp
    shutil.rmtree(temp_dir)
    
    total_elapsed = stage1_elapsed + (time.time() - stage1_start - stage1_elapsed)
    
    print("\n" + "=" * 70)
    print("ETL COMPLETE")
    print("=" * 70)
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
    parser.add_argument("--code_mapping", choices=["auto", "concept_id", "source_value"], 
                       default="auto")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--no-optimize-concepts", dest="optimize_concepts", 
                       action="store_false")
    
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
    )

