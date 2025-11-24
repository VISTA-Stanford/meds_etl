# Cleanup: Removed Subprocess Isolation Workaround

## Summary

We've **reverted to the clean, direct call** to `meds_etl.unsorted.sort()` for the C++ backend. The subprocess isolation was a workaround for what appeared to be semaphore leaks, but the real issues were:

1. **Compression mismatch** (LZ4 vs Zstd) → Fixed
2. **Schema type mismatch** (Float64 vs Float32) → Fixed

With these root causes resolved, the subprocess workaround is no longer needed.

## What Changed

### Before (Subprocess Isolation - Lines 1729-1769)
```python
# Use the unified sort interface
# For C++ backend, use subprocess isolation to avoid semaphore conflicts
if backend == "cpp":
    print("\n   Using subprocess isolation for C++ backend (prevents semaphore conflicts)")
    
    # Call C++ backend in a completely fresh subprocess
    # This eliminates ALL semaphore conflicts from Stage 1
    import subprocess
    
    subprocess_code = f"""
import meds_etl.unsorted
meds_etl.unsorted.sort(
    source_unsorted_path="{temp_dir}",
    target_meds_path="{output_dir / 'result'}",
    num_shards={num_shards},
    num_proc={num_workers},
    backend="cpp"
)
"""
    result = subprocess.run(
        [sys.executable, "-c", subprocess_code],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        print(f"\n❌ C++ backend failed with return code {result.returncode}")
        print(f"STDOUT:\n{result.stdout}")
        print(f"STDERR:\n{result.stderr}")
        sys.exit(1)
    
    if verbose and result.stdout:
        print(result.stdout)
else:
    # Polars backend can run directly (no subprocess needed)
    meds_etl.unsorted.sort(
        source_unsorted_path=str(temp_dir),
        target_meds_path=str(output_dir / "result"),
        num_shards=num_shards,
        num_proc=num_workers,
        backend=backend,
    )
```

### After (Clean Direct Call - Lines 1729-1736)
```python
# Use the unified sort interface (same as omop.py)
meds_etl.unsorted.sort(
    source_unsorted_path=str(temp_dir),
    target_meds_path=str(output_dir / "result"),
    num_shards=num_shards,
    num_proc=num_workers,
    backend=backend,
)
```

## Benefits of Reverting

1. **Simpler code:** 8 lines instead of 40+
2. **Better error messages:** Errors propagate directly instead of through subprocess stderr
3. **Easier debugging:** No subprocess boundary to cross
4. **Consistent with `omop.py`:** Uses the same calling pattern
5. **No output capture issues:** Direct stdout/stderr instead of subprocess capture

## Why the Workaround Seemed Necessary

When we first encountered the crashes, we saw:
- `resource_tracker: There appear to be 6-7 leaked semaphore objects`
- `Aborted (core dumped)`

This looked like a multiprocessing conflict, so we added subprocess isolation. However, the **real problem** was that the C++ backend was crashing due to:
1. Unexpected compression format (LZ4 instead of Zstd)
2. Unexpected data type (Float64 instead of Float32)

The semaphore warnings were a **side effect** of the hard crash, not the root cause.

## Testing

The C++ backend now works reliably with direct calls because:
- ✅ Temp files use Zstd compression (matches expectation)
- ✅ `numeric_value` uses Float32 (matches MEDS schema)
- ✅ All other schema fields match the MEDS standard

## Files Modified

- `src/meds_etl/omop_refactor.py`:
  - Lines 1729-1736: Simplified to direct `meds_etl.unsorted.sort()` call
  - Line 1815: Updated help text to remove "subprocess isolation" mention

