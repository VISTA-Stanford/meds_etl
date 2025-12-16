#!/usr/bin/env python
"""
Benchmark Bake-off: Compare OMOP → MEDS ETL methods

This script compares omop_legacy.py and omop.py (recommended)
with both C++ and Polars backends:
- omop_legacy.py [cpp, polars]
- omop.py [cpp, polars]

Usage:
    python benchmark_methods_bakeoff.py \
        --omop_dir /path/to/omop/data \
        --base_output_dir /path/to/benchmark/outputs \
        --config /path/to/config.json \
        --num_shards 10 \
        --num_workers 8

Output:
    Creates timestamped benchmark log: benchmark_bakeoff_YYYYMMDD_HHMMSS.log
    Creates separate output directories for each method
"""

import argparse
import importlib.util
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


class BenchmarkLogger:
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


def check_cpp_backend_available():
    """Check if meds_etl_cpp is installed"""
    return importlib.util.find_spec("meds_etl_cpp") is not None


def run_command(cmd, logger, method_name):
    """Run a command and capture timing"""
    logger.log("=" * 80)
    logger.log(f"STARTING: {method_name}")
    logger.log("=" * 80)
    logger.log(f"Command: {' '.join(cmd)}")
    logger.log("")

    start_time = time.time()

    try:
        # Run command and capture output
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

        # Stream output to logger
        for line in process.stdout:
            logger.log(f"  {line.rstrip()}")

        process.wait()
        elapsed = time.time() - start_time

        if process.returncode == 0:
            logger.log("")
            logger.log(f"✓ SUCCESS: {method_name}")
            logger.log(f"  Time: {elapsed:.2f}s ({elapsed/60:.2f}m)")
            return True, elapsed
        else:
            logger.log("")
            logger.log(f"✗ FAILED: {method_name} (exit code: {process.returncode})")
            logger.log(f"  Time: {elapsed:.2f}s ({elapsed/60:.2f}m)")
            return False, elapsed

    except Exception as e:
        elapsed = time.time() - start_time
        logger.log(f"✗ ERROR: {method_name}")
        logger.log(f"  {str(e)}")
        logger.log(f"  Time: {elapsed:.2f}s ({elapsed/60:.2f}m)")
        return False, elapsed


def get_output_stats(output_dir, logger):
    """Get statistics about the output data"""
    data_dir = Path(output_dir) / "data"

    if not data_dir.exists():
        logger.log("  Warning: data directory not found")
        return None

    stats = {"num_files": 0, "total_size_gb": 0.0, "files": []}

    for file_path in data_dir.glob("*.parquet"):
        size_bytes = file_path.stat().st_size
        size_gb = size_bytes / (1024**3)
        stats["num_files"] += 1
        stats["total_size_gb"] += size_gb
        stats["files"].append({"name": file_path.name, "size_gb": size_gb})

    return stats


def main():
    parser = argparse.ArgumentParser(description="Benchmark bake-off: Compare all OMOP ETL methods")

    # Input/Output
    parser.add_argument("--omop_dir", required=True, help="Path to OMOP data directory")
    parser.add_argument(
        "--base_output_dir", required=True, help="Base directory for benchmark outputs (subdirectories will be created)"
    )
    parser.add_argument(
        "--config", required=True, help="Path to ETL config JSON (for omop_streaming and omop_refactor)"
    )

    # Parameters
    parser.add_argument("--num_shards", type=int, default=10, help="Number of output shards (default: 10)")
    parser.add_argument(
        "--num_workers", type=int, default=8, help="Number of workers for parallel processing (default: 8)"
    )
    parser.add_argument("--omop_version", type=str, default="5.4", help="OMOP version for omop.py (default: 5.4)")

    # Method selection
    parser.add_argument(
        "--methods",
        nargs="+",
        choices=["omop", "refactor", "all"],
        default=["all"],
        help="Which methods to benchmark (default: all)",
    )

    # Backend selection
    parser.add_argument(
        "--backends",
        nargs="+",
        choices=["cpp", "polars", "all"],
        default=["all"],
        help="Which backends to benchmark (default: all)",
    )

    # Options
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    args = parser.parse_args()

    # Determine which methods to run
    if "all" in args.methods:
        methods_to_run = ["omop", "refactor"]
    else:
        methods_to_run = args.methods

    # Determine which backends to run
    if "all" in args.backends:
        backends_to_run = ["cpp", "polars"]
    else:
        backends_to_run = args.backends

    # Check if cpp backend is available
    cpp_available = check_cpp_backend_available()
    if "cpp" in backends_to_run and not cpp_available:
        print("WARNING: cpp backend requested but meds_etl_cpp is not installed!")
        print("\nTo install cpp backend:")
        print("  pip install meds_etl[cpp]")
        print("  # or")
        print("  uv pip install meds_etl[cpp]")
        print("\nSkipping cpp backend tests...")
        backends_to_run = [b for b in backends_to_run if b != "cpp"]

        if not backends_to_run:
            print("ERROR: No backends available to test!")
            sys.exit(1)

    # Create base output directory
    base_output = Path(args.base_output_dir)
    base_output.mkdir(parents=True, exist_ok=True)

    # Create timestamped log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = base_output / f"benchmark_bakeoff_{timestamp}.log"

    logger = BenchmarkLogger(log_file)

    # Log configuration
    logger.log("=" * 80)
    logger.log("BENCHMARK BAKE-OFF: OMOP ETL METHODS COMPARISON")
    logger.log("=" * 80)
    logger.log(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.log(f"OMOP data: {args.omop_dir}")
    logger.log(f"Config: {args.config}")
    logger.log(f"Output base: {args.base_output_dir}")
    logger.log(f"Shards: {args.num_shards}")
    logger.log(f"Workers: {args.num_workers}")
    logger.log(f"Methods: {', '.join(methods_to_run)}")
    logger.log(f"Backends: {', '.join(backends_to_run)}")
    logger.log(f"meds_etl_cpp available: {cpp_available}")
    logger.log("Multiprocessing: spawn (forced for Linux stability)")
    logger.log(f"Log file: {log_file}")
    logger.log("")

    results = {}

    # ========================================================================
    # 1. omop_legacy.py - Test both backends
    # ========================================================================
    if "omop" in methods_to_run:
        for backend in backends_to_run:
            method_name = f"omop_legacy.py ({backend})"
            output_dir = base_output / f"omop_legacy_output_{backend}"

            cmd = [
                sys.executable,
                "-m",
                "meds_etl.omop_legacy",
                args.omop_dir,
                str(output_dir),
                "--num_shards",
                str(args.num_shards),
                "--num_proc",
                str(args.num_workers),
                "--backend",
                backend,
                "--omop_version",
                args.omop_version,
                "--force_refresh",
            ]

            if args.verbose:
                cmd.append("--verbose")
                cmd.append("1")

            success, elapsed = run_command(cmd, logger, method_name)
            stats = get_output_stats(output_dir, logger) if success else None

            results[f"legacy_{backend}"] = {
                "success": success,
                "elapsed_seconds": elapsed,
                "elapsed_minutes": elapsed / 60,
                "output_dir": str(output_dir),
                "backend": backend,
                "method": "omop_legacy.py",
                "stats": stats,
            }

            logger.log("")

    # ========================================================================
    # 2. omop.py (recommended) - Test both backends
    # ========================================================================
    if "refactor" in methods_to_run:
        for backend in backends_to_run:
            method_name = f"omop.py (recommended - {backend})"
            output_dir = base_output / f"omop_output_{backend}"

            # Map backend for omop.py (it uses 'auto', 'cpp', or 'polars')
            backend_arg = backend if backend in ["cpp", "polars"] else "auto"

            cmd = [
                sys.executable,
                "-m",
                "meds_etl.omop",
                "--omop_dir",
                args.omop_dir,
                "--output_dir",
                str(output_dir),
                "--config",
                args.config,
                "--shards",
                str(args.num_shards),
                "--workers",
                str(args.num_workers),
                "--backend",
                backend_arg,
                "--process_method",
                "spawn",  # Force spawn for Linux stability
                "--force-refresh",  # Always overwrite for benchmarking
            ]

            if args.verbose:
                cmd.append("--verbose")

            success, elapsed = run_command(cmd, logger, method_name)
            stats = get_output_stats(output_dir, logger) if success else None

            results[f"omop_{backend}"] = {
                "success": success,
                "elapsed_seconds": elapsed,
                "elapsed_minutes": elapsed / 60,
                "output_dir": str(output_dir),
                "backend": backend,
                "method": "omop.py",
                "stats": stats,
            }

            logger.log("")

    # ========================================================================
    # Summary
    # ========================================================================
    logger.log("=" * 80)
    logger.log("BENCHMARK SUMMARY")
    logger.log("=" * 80)
    logger.log("")

    # Print results table
    logger.log(f"{'Method':<30} {'Status':<10} {'Time (s)':<12} {'Time (m)':<12} {'Files':<8} {'Size (GB)':<12}")
    logger.log("-" * 95)

    for method, result in results.items():
        status = "✓ SUCCESS" if result["success"] else "✗ FAILED"
        time_s = f"{result['elapsed_seconds']:.2f}"
        time_m = f"{result['elapsed_minutes']:.2f}"

        if result["stats"]:
            num_files = str(result["stats"]["num_files"])
            total_size = f"{result['stats']['total_size_gb']:.2f}"
        else:
            num_files = "N/A"
            total_size = "N/A"

        logger.log(f"{method:<30} {status:<10} {time_s:<12} {time_m:<12} {num_files:<8} {total_size:<12}")

    logger.log("")
    logger.log("=" * 80)

    # Calculate speedups
    logger.log("")
    logger.log("SPEEDUP ANALYSIS")
    logger.log("-" * 95)

    # Compare C++ vs Polars for each method
    for method in methods_to_run:
        cpp_key = f"{method}_cpp"
        polars_key = f"{method}_polars"

        if cpp_key in results and polars_key in results:
            if results[cpp_key]["success"] and results[polars_key]["success"]:
                speedup = results[polars_key]["elapsed_seconds"] / results[cpp_key]["elapsed_seconds"]
                logger.log(f"Speedup ({method}.py: polars → cpp): {speedup:.2f}x")

    # Compare omop_legacy.py vs omop.py for each backend
    for backend in backends_to_run:
        legacy_key = f"legacy_{backend}"
        omop_key = f"omop_{backend}"

        if legacy_key in results and omop_key in results:
            if results[legacy_key]["success"] and results[omop_key]["success"]:
                speedup = results[legacy_key]["elapsed_seconds"] / results[omop_key]["elapsed_seconds"]
                logger.log(f"Speedup ({backend} backend: omop_legacy.py → omop.py): {speedup:.2f}x")

    logger.log("")
    logger.log(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.log(f"Results saved to: {log_file}")

    # Save JSON results
    json_file = base_output / f"benchmark_bakeoff_{timestamp}.json"
    with open(json_file, "w") as f:
        json.dump(results, f, indent=2)
    logger.log(f"JSON results saved to: {json_file}")

    logger.close()

    print(f"\n✓ Benchmark complete! See {log_file}")


if __name__ == "__main__":
    main()
