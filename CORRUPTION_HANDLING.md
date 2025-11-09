# Gzip File Corruption Handling

## Problem
Corrupted `.csv.gz` files can cause the ETL pipeline to hang silently, making it difficult to diagnose issues. This is especially problematic when processing large datasets with hundreds of files.

## Solution
Added comprehensive corruption detection and graceful error handling throughout the pipeline.

### Key Features

1. **Fast Gzip Validation** (`validate_gzip_file()`)
   - Reads first 1MB of file to check decompression
   - Catches most corruption without full decompression
   - ~10-100ms per file depending on size

2. **Automatic File Skipping**
   - Corrupted files are logged and skipped
   - Pipeline continues processing good files
   - Summary of skipped files at end of each phase

3. **Enhanced Error Reporting**
   - Shows which specific file is corrupted
   - Includes error type and message
   - Process-safe logging with PID

## Implementation Details

### Validation Function
```python
def validate_gzip_file(filepath: str, max_bytes_to_check: int = 1024 * 1024) -> bool:
    """
    Fast validation of gzip file integrity.
    Reads the beginning to check if it can be decompressed.
    """
    try:
        with gzip.open(filepath, 'rb') as f:
            f.read(max_bytes_to_check)  # Try reading 1MB
        return True
    except (gzip.BadGzipFile, EOFError, OSError) as e:
        logger.warning(f"Corrupted: {filepath} - {e}")
        return False
```

### Enhanced `load_file()`
```python
def load_file(path_to_decompressed_dir: str, fname: str):
    if fname.endswith(".gz"):
        # Validate before decompressing
        if not validate_gzip_file(fname):
            raise ValueError(f"Corrupted gzip file: {fname}")
        
        # Decompress and check return code
        result = subprocess.run(["gunzip", "-c", fname], 
                                stdout=file, stderr=subprocess.PIPE)
        if result.returncode != 0:
            raise ValueError(f"Failed to decompress: {fname}")
    ...
```

### Protected File Processing
All file processing is now wrapped with try-except blocks:

**Metadata Extraction:**
```python
for concept_file in tqdm(concept_files):
    try:
        f = load_file(path_to_decompressed_dir, concept_file)
    except (ValueError, Exception) as e:
        logger.warning(f"⚠ Skipping corrupted file: {e}")
        skipped_concept_files += 1
        continue
    
    try:
        with f:
            # Process file...
    except Exception as e:
        logger.warning(f"⚠ Error processing: {e}")
        skipped_concept_files += 1
```

**Patient Data Processing:**
```python
def process_table_csv(args):
    try:
        temp_f = load_file(path_to_decompressed_dir, table_file)
    except ValueError as e:
        logger.error(f"⚠ Skipping corrupted file: {e}")
        return  # Skip this file
    
    try:
        with temp_f:
            # Process batches...
    except Exception as e:
        logger.error(f"⚠ Error processing: {e}")
        # Continue with other files
```

## Sample Output

### Normal Run (No Corruption)
```
======================================================================
Phase 1: Extracting metadata from OMOP concept tables
======================================================================
Generating metadata from OMOP `concept` table: 100%|████| 19/19
✓ Metadata extraction completed in 45.2s
  - Loaded 5,234,891 concepts
```

### With Corrupted Files
```
======================================================================
Phase 1: Extracting metadata from OMOP concept tables
======================================================================
14:23:15 [PID 12346] WARNING: Corrupted or invalid gzip file: concept/000000000005.csv.gz - BadGzipFile: Not a gzipped file
14:23:15 [PID 12346] WARNING: ⚠ Skipping corrupted concept file 000000000005.csv.gz: Corrupted or invalid gzip file: ...
14:23:20 [PID 12346] WARNING: ⚠ Error processing concept file 000000000012.csv.gz: Unexpected end of data
Generating metadata from OMOP `concept` table: 100%|████| 19/19
⚠ Skipped 2 corrupted concept files
✓ Metadata extraction completed in 45.2s
  - Loaded 5,234,891 concepts (from 17 good files)
```

### Patient Data Processing
```
======================================================================
Phase 2: Processing patient data tables
======================================================================
14:25:30 [PID 12347] ERROR: ⚠ Skipping corrupted file person/000000000000.csv.gz: Corrupted or invalid gzip file
14:25:31 [PID 12348] INFO: Processing measurement: 000000000000.csv.gz
14:25:31 [PID 12348] INFO:   Decompressed 000000000000.csv.gz in 0.1s
Mapping CSV OMOP tables -> MEDS format: 100%|████| 11/12 [00:08<00:00, 1.4it/s]
```

## Error Types Caught

1. **Corrupted Headers:**
   - `gzip.BadGzipFile`: Not a valid gzip file
   - `OSError`: File truncated or damaged

2. **Incomplete Files:**
   - `EOFError`: Unexpected end of file
   - `subprocess` errors: Gunzip failed to decompress

3. **Processing Errors:**
   - CSV parsing errors
   - Schema mismatches
   - Memory errors

## Testing

### Simulate Corruption for Testing
```bash
# Create a fake gzip file
echo "not gzipped" > test_bad.csv.gz

# Truncate a real gzip file
head -c 100 good_file.csv.gz > truncated.csv.gz

# Run ETL - should skip these files gracefully
meds_etl_omop /path/to/omop /path/to/output --verbose 1
```

## Performance Impact

- **Validation cost:** ~10-50ms per file (negligible)
- **Error handling:** No overhead when files are valid
- **Benefits:**
  - No more silent hangs
  - Pipeline completes despite some corrupted files
  - Clear error messages for debugging

## Best Practices

1. **Monitor Logs:**
   ```bash
   meds_etl_omop ... --verbose 1 2>&1 | tee etl.log
   grep "⚠ Skipped" etl.log
   ```

2. **Check for Skipped Files:**
   - Look for warning messages in logs
   - Investigate why files are corrupted
   - Re-download or regenerate corrupted files

3. **Validate Before Processing:**
   ```bash
   # Quick check of all .gz files
   for f in data/**/*.csv.gz; do
       gzip -t "$f" 2>&1 || echo "BAD: $f"
   done
   ```

## Limitations

- **Partial corruption** in the middle of large files may not be caught by header validation
- **Silent data corruption** (valid format, wrong data) cannot be detected
- **Extremely large files:** Only checks first 1MB for speed

For critical production runs, consider running full `gzip -t` validation on all files before starting the ETL.

## Summary

✅ **Fast validation** - 1MB header check catches most issues  
✅ **Graceful skipping** - Pipeline continues despite errors  
✅ **Clear logging** - Know exactly which files failed  
✅ **Process-safe** - Works correctly with multiprocessing  
✅ **Three phases protected** - Metadata, patient data, and sorting  
✅ **Summary reporting** - Total skipped files logged  


