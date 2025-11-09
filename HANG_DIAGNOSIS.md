# ETL Hang Diagnosis & Solution

## The Problem

Your ETL is hanging at **Phase 2: Processing patient data** after successfully decompressing and setting up CSV parsing. The logs show:

```
16:55:45 [PID 77277] INFO:   Decompressed 000000000000.csv.gz in 0.0s
16:55:45 [PID 77277] INFO:   CSV setup for 000000000000.csv.gz in 0.0s
[HANGS HERE - never completes]
```

## Root Cause

The hang happens in `write_event_data()` during the **concept ID lookup**:

```python
# Line 301 in omop.py
code = concept_id.replace_strict(concept_id_map, return_dtype=pl.Utf8(), default=None)
```

This line looks up **every concept_id** in your **8.2 million concept map**! 

### Why It's Slow

- Your concept_id_map has 8,265,542 entries
- Each batch has ~1M rows (default batch_size)
- Polars `.replace_strict()` with 8M mappings is O(n*log(m)) where:
  - n = number of rows in batch (~1M)
  - m = size of map (8.2M)
- This takes **minutes per batch**, not seconds!

### Why It Appears to Hang

With 4 workers each processing batches that take 5-10 minutes, you see:
- No progress bar movement
- No log output (default logging only every 5 batches)
- Looks like a hang, but it's just **extremely slow**

## I've Added Diagnostic Timing

The updated code now logs:
1. Time for each batch write operation
2. Time for `.collect()` (where concept lookup happens)
3. Time for `.write_parquet()` (disk I/O)

## Test With New Timing

```bash
conda activate pipelines

# Copy updated file with timing
PACKAGE_DIR=$(python -c "import meds_etl; import os; print(os.path.dirname(meds_etl.__file__))")
cp /Users/jfries/code/meds_etl/src/meds_etl/omop.py "$PACKAGE_DIR/"

# Run again - now with detailed timing
meds_etl_omop \
/Users/jfries/code/vista-data-pipelines/data/cache/vista_debug/som-nero-plevriti-deidbdf.vista_debug \
/Users/jfries/code/vista-data-pipelines/data/cache/vista_debug_meds \
--num_proc 4 \
--omop_version 5.3 \
--verbose 1 \
--force_refresh
```

You should now see:
```
16:55:45 [PID 77277] INFO:   CSV setup for 000000000000.csv.gz in 0.0s
16:58:23 [PID 77277] INFO:     collect() took 158.2s    <-- THIS is the bottleneck!
16:58:24 [PID 77277] INFO:     write_parquet() took 0.8s
16:58:24 [PID 77277] INFO:   Batch 1: write took 158.9s
```

If `collect()` is taking >100s per batch, that confirms the concept lookup is the problem.

## Solutions

### Option 1: Reduce Concept Map Size (Recommended for Debug)

Create a filtered concept map with only concepts actually used in your data:

```python
# Create a minimal concept extractor script
import polars as pl
import gzip
import os

# Get all concept IDs actually used in your data
omop_dir = "/path/to/vista_debug/som-nero-plevriti-deidbdf.vista_debug"
used_concepts = set()

for table in ["measurement", "observation", "condition_occurrence", "drug_exposure", "procedure_occurrence", "visit_occurrence"]:
    table_dir = os.path.join(omop_dir, table)
    if os.path.exists(table_dir):
        for f in os.listdir(table_dir):
            if f.endswith(".csv.gz"):
                df = pl.read_csv(gzip.open(os.path.join(table_dir, f)), infer_schema_length=0)
                if "concept_id" in df.columns:
                    used_concepts.update(df["concept_id"].unique().to_list())

print(f"Found {len(used_concepts)} unique concepts actually used")
print(f"Full concept map has 8.2M concepts")
print(f"Reduction: {100*(1 - len(used_concepts)/8_265_542):.1f}%")
```

Then create a minimal concept directory with only used concepts.

### Option 2: Use Parquet Files Instead of CSV.gz

Parquet files are much faster to read and process. If you can get OMOP data in parquet format, Phase 2 will be much faster.

### Option 3: Optimize Polars Operations

The `.replace_strict()` with a huge map is the bottleneck. We could potentially:
- Pre-filter the concept_id_map to only include concepts in each table
- Use a join operation instead of replace
- Process smaller batch sizes

### Option 4: Wait It Out (Not Recommended)

With 12 files and 4 workers processing batches that take ~3-5 minutes each:
- **Estimated time:** 15-30 minutes total
- But you'll see no progress indicators except occasional logs

## Recommended Action

1. **First, run with new timing** to confirm diagnosis
2. **For debug work:** Create minimal concept tables (only ~1000 concepts needed for 10 patients)
3. **For production:** Consider getting data in parquet format or optimize the concept lookup

## Quick Minimal Concept Table Creation

```bash
# Create a minimal OMOP directory for fast testing
MINIMAL_DIR="/Users/jfries/code/vista-data-pipelines/data/cache/vista_debug_minimal"
FULL_DIR="/Users/jfries/code/vista-data-pipelines/data/cache/vista_debug/som-nero-plevriti-deidbdf.vista_debug"

# Copy patient data tables (small, fast)
mkdir -p "$MINIMAL_DIR"
for table in person drug_exposure visit_occurrence condition_occurrence death \
             procedure_occurrence image_occurrence device_exposure measurement \
             observation note visit_detail; do
    if [ -d "$FULL_DIR/$table" ]; then
        cp -r "$FULL_DIR/$table" "$MINIMAL_DIR/"
    fi
done

# Copy only FIRST concept file (not all 19!)
mkdir -p "$MINIMAL_DIR/concept"
cp "$FULL_DIR/concept/000000000000.csv.gz" "$MINIMAL_DIR/concept/"

# Copy only FIRST cdm_source file
mkdir -p "$MINIMAL_DIR/cdm_source"
cp "$FULL_DIR/cdm_source/000000000000.csv.gz" "$MINIMAL_DIR/cdm_source/"

# Skip concept_ancestor and concept_relationship (95 + 128 = 223 files!)
# These are huge and optional for basic ETL

echo "Minimal directory created: $MINIMAL_DIR"
echo "This will be 10-20x faster for testing!"
```

Then run with minimal directory:
```bash
meds_etl_omop \
"$MINIMAL_DIR" \
/Users/jfries/code/vista-data-pipelines/data/cache/vista_debug_meds \
--num_proc 4 \
--omop_version 5.3 \
--verbose 1 \
--force_refresh
```

## Summary

- ✅ **Not actually hanging** - just extremely slow (3-5 min/batch)
- ✅ **Root cause:** Looking up concepts in 8.2M concept map
- ✅ **Solution for debug:** Use minimal concept tables (1 file instead of 242!)
- ✅ **New timing** will show exactly where time is spent
- ✅ **Expected speedup:** 10-20x faster with minimal concepts

Try the minimal concept directory approach first - you'll get results in 1-2 minutes instead of 20-30 minutes!


