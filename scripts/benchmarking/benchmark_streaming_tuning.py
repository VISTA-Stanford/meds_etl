#!/usr/bin/env python
"""
Tuning Experiments for omop_streaming.py

This script runs a grid search over key parameters to find optimal configurations:
- Number of workers (--workers)
- Number of shards (--shards)
- Chunk rows (--chunk_rows)
- Merge workers (--merge_workers)
- Compression settings (--run_compression, --final_compression)
- Row group size (--row_group_size)
- Low memory mode (--low_memory)

Usage:
    python benchmark_streaming_tuning.py \
        --omop_dir /path/to/omop/data \
        --base_output_dir /path/to/tuning/outputs \
        --config /path/to/config.json

    # Or run specific experiments
    python benchmark_streaming_tuning.py \
        --omop_dir /path/to/omop/data \
        --base_output_dir /path/to/tuning/outputs \
        --config /path/to/config.json \
        --experiment workers  # Only test different worker counts

Output:
    Creates timestamped tuning log: tuning_streaming_YYYYMMDD_HHMMSS.log
    Creates JSON results file for analysis
"""

import argparse
import json
import multiprocessing
import os
import subprocess
import sys
import time
import itertools
from datetime import datetime
from pathlib import Path


class TuningLogger:
    """Logger that writes to both console and file"""

    def __init__(self, log_file):
        self.log_file = log_file
        self.file_handle = open(log_file, "w")

    def log(self, message):
        """Write message to both console and file"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_message = f"[{timestamp}] {message}"
        print(full_message)
        self.file_handle.write(full_message + "\n")
        self.file_handle.flush()

    def close(self):
        self.file_handle.close()


def run_streaming_experiment(omop_dir, output_dir, config, params, logger, experiment_id):
    """Run a single omop_streaming experiment with given parameters"""

    logger.log("=" * 80)
    logger.log(f"EXPERIMENT {experiment_id}")
    logger.log("=" * 80)
    logger.log("Parameters:")
    for key, value in params.items():
        logger.log(f"  {key}: {value}")
    logger.log("")

    # Build command
    cmd = [
        sys.executable,
        "-m",
        "meds_etl.omop_streaming",
        "--omop_dir",
        omop_dir,
        "--output_dir",
        output_dir,
        "--config",
        config,
        "--force-refresh",  # Always overwrite for benchmarking
    ]

    # Add parameters
    for key, value in params.items():
        if key == "low_memory":
            if value:
                cmd.append("--low_memory")
        elif key == "verbose":
            if value:
                cmd.append("--verbose")
        else:
            cmd.append(f"--{key}")
            cmd.append(str(value))

    logger.log(f"Command: {' '.join(cmd)}")
    logger.log("")

    start_time = time.time()

    try:
        # Run command
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

        # Capture output
        output_lines = []
        for line in process.stdout:
            stripped = line.rstrip()
            output_lines.append(stripped)
            # Only log important lines to keep log manageable
            if any(
                keyword in stripped.lower()
                for keyword in ["stage", "complete", "error", "warning", "✓", "✗", "time:", "throughput"]
            ):
                logger.log(f"  {stripped}")

        process.wait()
        elapsed = time.time() - start_time

        result = {
            "experiment_id": experiment_id,
            "params": params,
            "elapsed_seconds": elapsed,
            "elapsed_minutes": elapsed / 60,
            "success": process.returncode == 0,
            "exit_code": process.returncode,
            "output_dir": output_dir,
        }

        # Try to extract stage timings from output
        stage_timings = extract_stage_timings(output_lines)
        if stage_timings:
            result["stage_timings"] = stage_timings

        # Get output stats
        stats = get_output_stats(output_dir, logger)
        if stats:
            result["stats"] = stats

        if result["success"]:
            logger.log("")
            logger.log(f"✓ SUCCESS - Experiment {experiment_id}")
            logger.log(f"  Total time: {elapsed:.2f}s ({elapsed/60:.2f}m)")
            if stage_timings:
                logger.log("  Stage timings:")
                for stage, timing in stage_timings.items():
                    logger.log(f"    {stage}: {timing:.2f}s")
        else:
            logger.log("")
            logger.log(f"✗ FAILED - Experiment {experiment_id} (exit code: {process.returncode})")
            logger.log(f"  Time: {elapsed:.2f}s")

        return result

    except Exception as e:
        elapsed = time.time() - start_time
        logger.log(f"✗ ERROR - Experiment {experiment_id}")
        logger.log(f"  {str(e)}")

        return {
            "experiment_id": experiment_id,
            "params": params,
            "elapsed_seconds": elapsed,
            "elapsed_minutes": elapsed / 60,
            "success": False,
            "error": str(e),
            "output_dir": output_dir,
        }


def extract_stage_timings(output_lines):
    """Extract stage timings from omop_streaming output"""
    timings = {}

    for line in output_lines:
        # Look for patterns like "Stage 1 time: 123.45s"
        if "stage 1 time:" in line.lower():
            try:
                time_str = line.split(":")[-1].strip().rstrip("s")
                timings["stage1"] = float(time_str)
            except:
                pass
        elif "stage 2 time:" in line.lower():
            try:
                time_str = line.split(":")[-1].strip().rstrip("s")
                timings["stage2"] = float(time_str)
            except:
                pass
        elif "total time:" in line.lower() and "stage" not in line.lower():
            try:
                time_str = line.split(":")[-1].strip().rstrip("s")
                timings["total"] = float(time_str)
            except:
                pass

    return timings if timings else None


def get_output_stats(output_dir, logger):
    """Get statistics about the output data"""
    data_dir = Path(output_dir) / "data"

    if not data_dir.exists():
        return None

    stats = {"num_files": 0, "total_size_gb": 0.0}

    try:
        for file_path in data_dir.glob("*.parquet"):
            size_bytes = file_path.stat().st_size
            size_gb = size_bytes / (1024**3)
            stats["num_files"] += 1
            stats["total_size_gb"] += size_gb

        return stats
    except:
        return None


def generate_experiments(experiment_type, baseline_params, max_cpus=None):
    """Generate list of parameter combinations to test

    Args:
        experiment_type: Type of experiment to run
        baseline_params: Default parameters for experiments
        max_cpus: Maximum number of CPUs available (auto-detected if None)
    """

    if max_cpus is None:
        max_cpus = multiprocessing.cpu_count()

    experiments = []

    if experiment_type == "workers":
        # Test different worker counts, capped by available CPUs
        # Use powers of 2 up to max_cpus, plus max_cpus itself
        worker_options = [1]
        current = 2
        while current <= max_cpus:
            worker_options.append(current)
            current *= 2
        # Add max_cpus if not already included
        if max_cpus not in worker_options and max_cpus > 1:
            worker_options.append(max_cpus)
        worker_options.sort()

        for workers in worker_options:
            params = baseline_params.copy()
            params["workers"] = workers
            experiments.append(params)

    elif experiment_type == "shards":
        # Test different shard counts
        for shards in [5, 10, 20, 50, 100]:
            params = baseline_params.copy()
            params["shards"] = shards
            experiments.append(params)

    elif experiment_type == "chunk_rows":
        # Test different chunk sizes
        for chunk_rows in [1_000_000, 5_000_000, 10_000_000, 20_000_000]:
            params = baseline_params.copy()
            params["chunk_rows"] = chunk_rows
            experiments.append(params)

    elif experiment_type == "compression":
        # Test different compression combinations
        compressions = ["lz4", "zstd", "snappy"]
        for run_comp, final_comp in itertools.product(compressions, compressions):
            params = baseline_params.copy()
            params["run_compression"] = run_comp
            params["final_compression"] = final_comp
            experiments.append(params)

    elif experiment_type == "memory":
        # Test memory configurations
        configs = [
            {"low_memory": False, "row_group_size": 100_000},
            {"low_memory": True, "row_group_size": 50_000},
            {"low_memory": True, "row_group_size": 100_000},
            {"low_memory": False, "row_group_size": 200_000},
        ]
        for config in configs:
            params = baseline_params.copy()
            params.update(config)
            experiments.append(params)

    elif experiment_type == "workers_and_shards":
        # Test combinations of workers and shards, capped by available CPUs
        worker_counts = [w for w in [4, 8, 16, 32] if w <= max_cpus]
        # Always include max_cpus if it's not in the list and > 1
        if max_cpus not in worker_counts and max_cpus > 1:
            worker_counts.append(max_cpus)
        worker_counts.sort()

        shard_counts = [10, 20, 50]
        for workers, shards in itertools.product(worker_counts, shard_counts):
            params = baseline_params.copy()
            params["workers"] = workers
            params["shards"] = shards
            experiments.append(params)

    elif experiment_type == "full":
        # Full grid search (warning: can be many experiments!)
        # Cap worker counts by available CPUs
        worker_counts = [w for w in [4, 8, 16, 32] if w <= max_cpus]
        if max_cpus not in worker_counts and max_cpus > 1:
            worker_counts.append(max_cpus)
        worker_counts.sort()

        shard_counts = [10, 20]
        chunk_rows_options = [5_000_000, 10_000_000]

        for workers, shards, chunk_rows in itertools.product(worker_counts, shard_counts, chunk_rows_options):
            params = baseline_params.copy()
            params["workers"] = workers
            params["shards"] = shards
            params["chunk_rows"] = chunk_rows
            experiments.append(params)

    else:
        raise ValueError(f"Unknown experiment type: {experiment_type}")

    return experiments


def main():
    parser = argparse.ArgumentParser(description="Tuning experiments for omop_streaming.py")

    # Required arguments
    parser.add_argument("--omop_dir", required=True, help="Path to OMOP data directory")
    parser.add_argument("--base_output_dir", required=True, help="Base directory for experiment outputs")
    parser.add_argument("--config", required=True, help="Path to ETL config JSON")

    # Experiment selection
    parser.add_argument(
        "--experiment",
        choices=["workers", "shards", "chunk_rows", "compression", "memory", "workers_and_shards", "full"],
        default="workers_and_shards",
        help="Type of experiment to run (default: workers_and_shards)",
    )

    # Baseline parameters (used as defaults for experiments)
    parser.add_argument("--baseline_workers", type=int, default=8, help="Baseline number of workers (default: 8)")
    parser.add_argument("--baseline_shards", type=int, default=10, help="Baseline number of shards (default: 10)")
    parser.add_argument(
        "--baseline_chunk_rows", type=int, default=10_000_000, help="Baseline chunk rows (default: 10,000,000)"
    )

    # Options
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output in experiments")
    parser.add_argument("--dry_run", action="store_true", help="Print experiments without running them")

    args = parser.parse_args()

    # Detect system resources
    max_cpus = multiprocessing.cpu_count()

    # Create base output directory
    base_output = Path(args.base_output_dir)
    base_output.mkdir(parents=True, exist_ok=True)

    # Create timestamped log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = base_output / f"tuning_streaming_{timestamp}.log"

    logger = TuningLogger(log_file)

    # Log configuration
    logger.log("=" * 80)
    logger.log("TUNING EXPERIMENTS: omop_streaming.py")
    logger.log("=" * 80)
    logger.log(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.log(f"System CPUs: {max_cpus}")
    logger.log(f"OMOP data: {args.omop_dir}")
    logger.log(f"Config: {args.config}")
    logger.log(f"Output base: {args.base_output_dir}")
    logger.log(f"Experiment type: {args.experiment}")
    logger.log(f"Log file: {log_file}")
    logger.log("")
    logger.log(f"Note: Worker counts will be capped at {max_cpus} (available CPUs)")
    logger.log("")

    # Baseline parameters - cap baseline workers at available CPUs
    baseline_workers = min(args.baseline_workers, max_cpus)
    if baseline_workers < args.baseline_workers:
        logger.log(
            f"Note: Baseline workers reduced from {args.baseline_workers} to {baseline_workers} (max available CPUs)"
        )
        logger.log("")

    baseline_params = {
        "workers": baseline_workers,
        "shards": args.baseline_shards,
        "chunk_rows": args.baseline_chunk_rows,
        "code_mapping": "auto",
        "run_compression": "lz4",
        "final_compression": "zstd",
        "merge_workers": 0,  # auto
        "low_memory": False,
        "row_group_size": 100_000,
        "verbose": args.verbose,
    }

    # Generate experiments (pass max_cpus to cap worker counts)
    experiments = generate_experiments(args.experiment, baseline_params, max_cpus=max_cpus)

    logger.log(f"Generated {len(experiments)} experiments")
    logger.log("")

    if args.dry_run:
        logger.log("DRY RUN - Listing experiments without running:")
        logger.log("")
        for i, params in enumerate(experiments, 1):
            logger.log(f"Experiment {i}:")
            for key, value in params.items():
                logger.log(f"  {key}: {value}")
            logger.log("")
        logger.close()
        return

    # Run experiments
    results = []

    for i, params in enumerate(experiments, 1):
        experiment_id = f"{args.experiment}_{i}"
        output_dir = str(base_output / experiment_id)

        result = run_streaming_experiment(args.omop_dir, output_dir, args.config, params, logger, experiment_id)

        results.append(result)
        logger.log("")

    # ========================================================================
    # Summary
    # ========================================================================
    logger.log("=" * 80)
    logger.log("TUNING SUMMARY")
    logger.log("=" * 80)
    logger.log("")

    # Sort by elapsed time (successful runs only)
    successful_results = [r for r in results if r["success"]]
    failed_results = [r for r in results if not r["success"]]

    if successful_results:
        successful_results.sort(key=lambda x: x["elapsed_seconds"])

        logger.log(f"Successful experiments: {len(successful_results)}")
        logger.log(f"Failed experiments: {len(failed_results)}")
        logger.log("")
        logger.log("Top 5 fastest configurations:")
        logger.log("")

        for i, result in enumerate(successful_results[:5], 1):
            logger.log(f"{i}. Experiment {result['experiment_id']}")
            logger.log(f"   Time: {result['elapsed_seconds']:.2f}s ({result['elapsed_minutes']:.2f}m)")
            logger.log("   Parameters:")
            for key, value in result["params"].items():
                if key != "verbose":  # Skip verbose in summary
                    logger.log(f"     {key}: {value}")
            if "stats" in result:
                logger.log(
                    f"   Output: {result['stats']['num_files']} files, " f"{result['stats']['total_size_gb']:.2f} GB"
                )
            logger.log("")

        # Best configuration
        best = successful_results[0]
        logger.log("=" * 80)
        logger.log("BEST CONFIGURATION:")
        logger.log("=" * 80)
        logger.log(f"Experiment: {best['experiment_id']}")
        logger.log(f"Time: {best['elapsed_seconds']:.2f}s ({best['elapsed_minutes']:.2f}m)")
        logger.log("")
        logger.log("Parameters:")
        for key, value in best["params"].items():
            if key != "verbose":
                logger.log(f"  {key}: {value}")
        logger.log("")

        # Create command for best configuration
        cmd_parts = [
            "python -m meds_etl.omop_streaming",
            f"--omop_dir {args.omop_dir}",
            f"--output_dir OUTPUT_DIR",
            f"--config {args.config}",
        ]
        for key, value in best["params"].items():
            if key == "low_memory" and value:
                cmd_parts.append("--low_memory")
            elif key == "verbose" and value:
                cmd_parts.append("--verbose")
            elif key != "verbose":
                cmd_parts.append(f"--{key} {value}")

        logger.log("Command to use best configuration:")
        logger.log(" \\\n  ".join(cmd_parts))
        logger.log("")
    else:
        logger.log("No successful experiments!")
        logger.log("")

    if failed_results:
        logger.log("Failed experiments:")
        for result in failed_results:
            logger.log(f"  - {result['experiment_id']}")
        logger.log("")

    logger.log(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.log(f"Results saved to: {log_file}")

    # Save JSON results
    json_file = base_output / f"tuning_streaming_{timestamp}.json"
    with open(json_file, "w") as f:
        json.dump(
            {
                "experiment_type": args.experiment,
                "timestamp": timestamp,
                "num_experiments": len(experiments),
                "num_successful": len(successful_results),
                "num_failed": len(failed_results),
                "results": results,
            },
            f,
            indent=2,
        )
    logger.log(f"JSON results saved to: {json_file}")

    logger.close()

    print(f"\n✓ Tuning experiments complete! See {log_file}")


if __name__ == "__main__":
    main()
