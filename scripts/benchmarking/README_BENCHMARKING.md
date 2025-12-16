# Benchmarking Scripts

This directory contains scripts for benchmarking and tuning MEDS ETL methods.

## Scripts

### 1. `benchmark_methods_bakeoff.py`

Compares OMOP → MEDS ETL methods sequentially:
- `omop_legacy.py` (legacy API with positional args)
- `omop.py` (recommended, config-driven)

**Usage:**

```bash
# Basic usage
python examples/benchmark_methods_bakeoff.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir /path/to/benchmark/outputs \
  --config examples/omop_etl_extended_config.json \
  --num_shards 10 \
  --num_workers 8

# Test only specific methods
python examples/benchmark_methods_bakeoff.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir /path/to/benchmark/outputs \
  --config examples/omop_etl_extended_config.json \
  --methods streaming refactor  # Skip omop.py

# Verbose output
python examples/benchmark_methods_bakeoff.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir /path/to/benchmark/outputs \
  --config examples/omop_etl_extended_config.json \
  --verbose
```

**Output:**

- Timestamped log file: `benchmark_bakeoff_YYYYMMDD_HHMMSS.log`
- JSON results file: `benchmark_bakeoff_YYYYMMDD_HHMMSS.json`
- Separate output directories for each method:
  - `omop_legacy_output_{backend}/`
  - `omop_output_{backend}/`

**Log Contents:**

- Full command output from each method
- Stage-by-stage timing information
- Summary table comparing all methods
- Speedup calculations
- Output statistics (file count, size)

---

### 2. `benchmark_streaming_tuning.py`

Runs parameter tuning experiments for `omop_streaming.py` to find optimal configurations.

**Experiment Types:**

- `workers` - Test different worker counts (1, 2, 4, 8, 16)
- `shards` - Test different shard counts (5, 10, 20, 50, 100)
- `chunk_rows` - Test different chunk sizes (1M, 5M, 10M, 20M)
- `compression` - Test compression combinations (lz4, zstd, snappy)
- `memory` - Test memory configurations (low_memory, row_group_size)
- `workers_and_shards` - Test combinations of workers and shards (default)
- `full` - Full grid search (warning: many experiments!)

**Usage:**

```bash
# Default: test workers and shards combinations
python examples/benchmark_streaming_tuning.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir /path/to/tuning/outputs \
  --config examples/omop_etl_extended_config.json

# Test specific parameter
python examples/benchmark_streaming_tuning.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir /path/to/tuning/outputs \
  --config examples/omop_etl_extended_config.json \
  --experiment workers  # Only test worker counts

# Test compression settings
python examples/benchmark_streaming_tuning.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir /path/to/tuning/outputs \
  --config examples/omop_etl_extended_config.json \
  --experiment compression

# Dry run (see experiments without running)
python examples/benchmark_streaming_tuning.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir /path/to/tuning/outputs \
  --config examples/omop_etl_extended_config.json \
  --experiment full \
  --dry_run

# Custom baseline parameters
python examples/benchmark_streaming_tuning.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir /path/to/tuning/outputs \
  --config examples/omop_etl_extended_config.json \
  --experiment shards \
  --baseline_workers 16 \
  --baseline_chunk_rows 20000000
```

**Output:**

- Timestamped log file: `tuning_streaming_YYYYMMDD_HHMMSS.log`
- JSON results file: `tuning_streaming_YYYYMMDD_HHMMSS.json`
- Separate output directory for each experiment: `{experiment_type}_{N}/`

**Log Contents:**

- Parameters for each experiment
- Stage timing information extracted from output
- Summary of all experiments
- Top 5 fastest configurations
- Best configuration with command to reproduce it

---

## Example Workflows

### Quick Comparison

Compare all methods on a small dataset:

```bash
python examples/benchmark_methods_bakeoff.py \
  --omop_dir data/omop_sample \
  --base_output_dir benchmark_results \
  --config examples/omop_etl_base_config.json \
  --num_shards 5 \
  --num_workers 4
```

### Production Tuning

Find optimal configuration for production dataset:

```bash
# Step 1: Quick worker count test
python examples/benchmark_streaming_tuning.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir tuning_results \
  --config examples/omop_etl_extended_config.json \
  --experiment workers

# Step 2: Test shards with best worker count from step 1
python examples/benchmark_streaming_tuning.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir tuning_results \
  --config examples/omop_etl_extended_config.json \
  --experiment shards \
  --baseline_workers 16  # Use best from step 1

# Step 3: Fine-tune compression and memory settings
python examples/benchmark_streaming_tuning.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir tuning_results \
  --config examples/omop_etl_extended_config.json \
  --experiment memory \
  --baseline_workers 16 \
  --baseline_shards 20  # Use best from steps 1 & 2
```

### Full Grid Search

⚠️ **Warning:** This can generate many experiments!

```bash
# First, do a dry run to see how many experiments will run
python examples/benchmark_streaming_tuning.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir tuning_results \
  --config examples/omop_etl_extended_config.json \
  --experiment full \
  --dry_run

# If reasonable, run the full grid search
python examples/benchmark_streaming_tuning.py \
  --omop_dir /path/to/omop/data \
  --base_output_dir tuning_results \
  --config examples/omop_etl_extended_config.json \
  --experiment full
```

---

## Analyzing Results

### Log Files

Both scripts create detailed log files with:
- Timestamped entries
- Full command output
- Stage timings
- Summary tables
- Best configurations

### JSON Results

JSON files contain structured data for analysis:

```python
import json
import pandas as pd

# Load bakeoff results
with open('benchmark_results/benchmark_bakeoff_20241122_120000.json') as f:
    bakeoff = json.load(f)

# Compare methods
for method, result in bakeoff.items():
    print(f"{method}: {result['elapsed_minutes']:.2f}m")

# Load tuning results
with open('tuning_results/tuning_streaming_20241122_120000.json') as f:
    tuning = json.load(f)

# Convert to DataFrame for analysis
df = pd.DataFrame(tuning['results'])
df = df[df['success']].sort_values('elapsed_seconds')

# Find best worker count
worker_results = df[df['params'].apply(lambda p: 'workers' in p)]
print(worker_results[['experiment_id', 'elapsed_seconds', 'params']])
```

---

## Tips

### For Small Datasets (< 1GB)
- Use fewer workers: `--num_workers 2`
- Use fewer shards: `--num_shards 5`
- Single experiments are fast, so `--experiment full` is feasible

### For Medium Datasets (1-10GB)
- Test workers first: `--experiment workers`
- Then test shards: `--experiment shards`
- Use baseline results between experiments

### For Large Datasets (> 10GB)
- Start with `--experiment workers` only
- Use `--dry_run` before running full experiments
- Consider testing on a sample first
- Use `--low_memory` if RAM is limited

### For Production
- Run `benchmark_methods_bakeoff.py` to establish baseline
- Use `benchmark_streaming_tuning.py` to optimize
- Test on representative data size
- Re-run periodically as data grows

---

## Notes

- All scripts run methods **sequentially** (not in parallel)
- Each experiment gets its own output directory
- Scripts don't modify any source code
- Logs capture both stdout and stderr
- Failed experiments are recorded but don't stop the script
- Output directories can be large - ensure sufficient disk space

---

## Troubleshooting

### Script fails immediately
- Check that OMOP data directory exists
- Verify config file is valid JSON
- Ensure output directory is writable

### Method fails in bakeoff
- Check the log file for error messages
- The script continues with remaining methods
- Failed method will show in summary table

### Tuning takes too long
- Use `--dry_run` to preview experiments
- Start with simpler experiment types (workers, shards)
- Reduce baseline parameters
- Test on smaller data sample first

### Out of memory errors
- Use `--experiment memory` to test low-memory configs
- Reduce baseline worker count
- Reduce baseline chunk_rows
- Use fewer shards initially

