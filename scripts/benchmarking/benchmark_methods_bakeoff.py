#!/usr/bin/env python
"""
Benchmark Bake-off: Compare all OMOP → MEDS ETL methods

This script runs all three OMOP ETL methods sequentially and captures timing logs:
- omop.py (legacy)
- omop_streaming.py (streaming with external sort)
- omop_refactor.py (refactored with config)

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
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
import importlib.util


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
    parser.add_argument(
        "--backend",
        type=str,
        choices=["polars", "cpp"],
        default="polars",
        help="Backend for omop.py legacy method (default: polars)",
    )
    
    # Method selection
    parser.add_argument(
        "--methods",
        nargs="+",
        choices=["omop", "streaming", "refactor", "all"],
        default=["all"],
        help="Which methods to benchmark (default: all)",
    )
    
    # Options
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    
    args = parser.parse_args()
    
    # Check if cpp backend is requested and available
    if args.backend == "cpp" and "omop" in args.methods:
        if not check_cpp_backend_available():
            print("ERROR: cpp backend requested but meds_etl_cpp is not installed!")
            print("\nTo install cpp backend:")
            print("  pip install meds_etl[cpp]")
            print("  # or")
            print("  uv pip install meds_etl[cpp]")
            print("\nFalling back to polars backend...")
            args.backend = "polars"
    
    # Determine which methods to run
    if "all" in args.methods:
        methods_to_run = ["omop", "streaming", "refactor"]
    else:
        methods_to_run = args.methods
    
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
    logger.log(f"Backend (omop.py): {args.backend}")
    if args.backend == "cpp":
        cpp_available = check_cpp_backend_available()
        logger.log(f"meds_etl_cpp available: {cpp_available}")
    logger.log(f"Methods: {', '.join(methods_to_run)}")
    logger.log(f"Multiprocessing: spawn (forced for Linux stability)")
    logger.log(f"Log file: {log_file}")
    logger.log("")
    
    results = {}
    
    # ========================================================================
    # 1. omop.py (legacy)
    # ========================================================================
    if "omop" in methods_to_run:
        method_name = f"omop.py (legacy - {args.backend})"
        output_dir = base_output / f"omop_output_{args.backend}"
        
        cmd = [
            sys.executable,
            "-m",
            "meds_etl.omop",
            args.omop_dir,
            str(output_dir),
            "--num_shards",
            str(args.num_shards),
            "--num_proc",
            str(args.num_workers),
            "--backend",
            args.backend,
            "--omop_version",
            args.omop_version,
            "--force_refresh",
        ]
        
        if args.verbose:
            cmd.append("--verbose")
            cmd.append("1")
        
        success, elapsed = run_command(cmd, logger, method_name)
        stats = get_output_stats(output_dir, logger) if success else None
        
        results[f"omop_{args.backend}"] = {
            "success": success,
            "elapsed_seconds": elapsed,
            "elapsed_minutes": elapsed / 60,
            "output_dir": str(output_dir),
            "backend": args.backend,
            "stats": stats,
        }
        
        logger.log("")
    
    # ========================================================================
    # 2. omop_streaming.py (streaming with external sort)
    # ========================================================================
    if "streaming" in methods_to_run:
        method_name = "omop_streaming.py (streaming)"
        output_dir = base_output / "streaming_output"
        
        cmd = [
            sys.executable,
            "-m",
            "meds_etl.omop_streaming",
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
            "--code_mapping",
            "auto",
            "--process_method",
            "spawn",  # Force spawn for Linux stability
            "--force-refresh",  # Always overwrite for benchmarking
        ]
        
        if args.verbose:
            cmd.append("--verbose")
        
        success, elapsed = run_command(cmd, logger, method_name)
        stats = get_output_stats(output_dir, logger) if success else None
        
        results["streaming"] = {
            "success": success,
            "elapsed_seconds": elapsed,
            "elapsed_minutes": elapsed / 60,
            "output_dir": str(output_dir),
            "stats": stats,
        }
        
        logger.log("")
    
    # ========================================================================
    # 3. omop_refactor.py (refactored with config)
    # ========================================================================
    if "refactor" in methods_to_run:
        method_name = "omop_refactor.py (refactored)"
        output_dir = base_output / "refactor_output"
        
        cmd = [
            sys.executable,
            "-m",
            "meds_etl.omop_refactor",
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
            "auto",
            "--code_mapping",
            "auto",
            "--process_method",
            "spawn",  # Force spawn for Linux stability
            "--force-refresh",  # Always overwrite for benchmarking
        ]
        
        if args.verbose:
            cmd.append("--verbose")
        
        success, elapsed = run_command(cmd, logger, method_name)
        stats = get_output_stats(output_dir, logger) if success else None
        
        results["refactor"] = {
            "success": success,
            "elapsed_seconds": elapsed,
            "elapsed_minutes": elapsed / 60,
            "output_dir": str(output_dir),
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
    
    # Calculate speedup
    omop_key = f"omop_{args.backend}"
    if omop_key in results and "streaming" in results:
        if results[omop_key]["success"] and results["streaming"]["success"]:
            speedup = results[omop_key]["elapsed_seconds"] / results["streaming"]["elapsed_seconds"]
            logger.log(f"Speedup (omop[{args.backend}] → streaming): {speedup:.2f}x")
    
    if "streaming" in results and "refactor" in results:
        if results["streaming"]["success"] and results["refactor"]["success"]:
            speedup = results["streaming"]["elapsed_seconds"] / results["refactor"]["elapsed_seconds"]
            logger.log(f"Speedup (streaming → refactor): {speedup:.2f}x")
    
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
