"""
Compare MEDS outputs from omop.py (baseline) and omop_streaming.py (memory-efficient version)
"""

import json
from pathlib import Path

import polars as pl


def compare_meds_directories(dir1: Path, dir2: Path, name1: str = "baseline", name2: str = "streaming"):
    """
    Compare two MEDS output directories (memory-efficient).
    """
    print("=" * 80)
    print(f"COMPARING MEDS OUTPUTS: {name1} vs {name2}")
    print("=" * 80)

    # ========================================================================
    # 1. METADATA COMPARISON
    # ========================================================================
    print("\n[1] METADATA COMPARISON")
    print("-" * 80)

    # Compare dataset.json
    dataset1 = json.load(open(dir1 / "metadata" / "dataset.json"))
    dataset2 = json.load(open(dir2 / "metadata" / "dataset.json"))

    print(f"\n{name1} metadata:")
    for key, value in dataset1.items():
        print(f"  {key}: {value}")

    print(f"\n{name2} metadata:")
    for key, value in dataset2.items():
        print(f"  {key}: {value}")

    if dataset1 != dataset2:
        print("\n⚠️  Metadata differs!")
        for key in set(dataset1.keys()) | set(dataset2.keys()):
            v1 = dataset1.get(key)
            v2 = dataset2.get(key)
            if v1 != v2:
                print(f"  {key}: {v1} vs {v2}")
    else:
        print("\n✅ Metadata identical")

    # Compare codes.parquet
    codes1 = pl.read_parquet(dir1 / "metadata" / "codes.parquet")
    codes2 = pl.read_parquet(dir2 / "metadata" / "codes.parquet")

    print(f"\n{name1} codes: {len(codes1):,} codes")
    print(f"{name2} codes: {len(codes2):,} codes")

    if len(codes1) != len(codes2):
        print(f"⚠️  Code count differs: {len(codes1):,} vs {len(codes2):,}")
    else:
        print("✅ Same number of codes")

    # ========================================================================
    # 2. FILE STRUCTURE COMPARISON
    # ========================================================================
    print("\n[2] FILE STRUCTURE COMPARISON")
    print("-" * 80)

    data_dir1 = dir1 / "data"
    data_dir2 = dir2 / "data"

    files1 = sorted(data_dir1.glob("*.parquet"))
    files2 = sorted(data_dir2.glob("*.parquet"))

    print(f"\n{name1}: {len(files1)} parquet files")
    print(f"{name2}: {len(files2)} parquet files")

    if len(files1) != len(files2):
        print(f"⚠️  Different number of shards: {len(files1)} vs {len(files2)}")
    else:
        print(f"✅ Same number of shards: {len(files1)}")

    # File sizes
    print("\nFile sizes:")
    total_size1 = 0
    total_size2 = 0
    for f1, f2 in zip(files1, files2, strict=False):
        size1 = f1.stat().st_size / 1024 / 1024  # MB
        size2 = f2.stat().st_size / 1024 / 1024  # MB
        total_size1 += size1
        total_size2 += size2
        print(f"  Shard {files1.index(f1)}: {size1:.1f} MB ({name1}) vs {size2:.1f} MB ({name2})")

    print(f"\n  Total: {total_size1:.1f} MB ({name1}) vs {total_size2:.1f} MB ({name2})")

    # ========================================================================
    # 3. SCHEMA COMPARISON
    # ========================================================================
    print("\n[3] SCHEMA COMPARISON")
    print("-" * 80)

    # Read first shard from each (just schema)
    schema1 = pl.read_parquet_schema(files1[0])
    schema2 = pl.read_parquet_schema(files2[0])

    print(f"\n{name1} schema ({len(schema1)} columns):")
    for col, dtype in schema1.items():
        print(f"  {col}: {dtype}")

    print(f"\n{name2} schema ({len(schema2)} columns):")
    for col, dtype in schema2.items():
        print(f"  {col}: {dtype}")

    # Check for schema differences
    cols1 = set(schema1.keys())
    cols2 = set(schema2.keys())

    if cols1 != cols2:
        print("\n⚠️  SCHEMA DIFFERS!")
        only_in_1 = cols1 - cols2
        only_in_2 = cols2 - cols1

        if only_in_1:
            print(f"  Only in {name1}: {only_in_1}")
        if only_in_2:
            print(f"  Only in {name2}: {only_in_2}")
    else:
        # Check if types match
        type_diffs = []
        for col in cols1:
            if schema1[col] != schema2[col]:
                type_diffs.append((col, schema1[col], schema2[col]))

        if type_diffs:
            print("\n⚠️  Column types differ:")
            for col, type1, type2 in type_diffs:
                print(f"  {col}: {type1} vs {type2}")
        else:
            print("\n✅ Schemas identical")

    # ========================================================================
    # 4. ROW COUNT COMPARISON (Lazy - no memory load)
    # ========================================================================
    print("\n[4] ROW COUNT COMPARISON")
    print("-" * 80)

    total_rows1 = 0
    total_rows2 = 0

    print("\nCounting rows per shard (using lazy API)...")
    for i, (f1, f2) in enumerate(zip(files1, files2, strict=False)):
        # Use lazy scanning to avoid loading data
        count1 = pl.scan_parquet(f1).select(pl.len()).collect().item()
        count2 = pl.scan_parquet(f2).select(pl.len()).collect().item()
        total_rows1 += count1
        total_rows2 += count2
        print(f"  Shard {i}: {count1:,} ({name1}) vs {count2:,} ({name2})")

    print(f"\n{name1} total: {total_rows1:,} rows")
    print(f"{name2} total: {total_rows2:,} rows")

    if total_rows1 != total_rows2:
        diff = abs(total_rows1 - total_rows2)
        pct = (diff / max(total_rows1, total_rows2)) * 100
        print(f"⚠️  ROW COUNT DIFFERS: {diff:,} rows ({pct:.2f}%)")
    else:
        print("✅ Same total row count")

    # ========================================================================
    # 5. SAMPLE COMPARISON (first shard only)
    # ========================================================================
    print("\n[5] SAMPLE DATA COMPARISON (First Shard Only)")
    print("-" * 80)

    # Read just the first shard
    df1 = pl.read_parquet(files1[0])
    df2 = pl.read_parquet(files2[0])

    print("\nFirst shard:")
    print(f"  {name1}: {len(df1):,} rows")
    print(f"  {name2}: {len(df2):,} rows")

    # Get unique codes in first shard
    codes1 = df1["code"].unique().sort()
    codes2 = df2["code"].unique().sort()

    print(f"\n  {name1}: {len(codes1):,} unique codes")
    print(f"  {name2}: {len(codes2):,} unique codes")

    # Get unique subjects
    subjects1 = df1["subject_id"].unique().sort()
    subjects2 = df2["subject_id"].unique().sort()

    print(f"\n  {name1}: {len(subjects1):,} unique subjects")
    print(f"  {name2}: {len(subjects2):,} unique subjects")

    # Top codes in first shard
    print(f"\nTop 10 codes in first shard ({name1}):")
    top_codes1 = df1.group_by("code").len().sort("len", descending=True).head(10)
    for row in top_codes1.iter_rows(named=True):
        print(f"  {row['code']}: {row['len']:,}")

    print(f"\nTop 10 codes in first shard ({name2}):")
    top_codes2 = df2.group_by("code").len().sort("len", descending=True).head(10)
    for row in top_codes2.iter_rows(named=True):
        print(f"  {row['code']}: {row['len']:,}")

    # Sample patient
    sample_subject = subjects1[0]
    sample1 = df1.filter(pl.col("subject_id") == sample_subject).sort(["time", "code"])
    sample2 = df2.filter(pl.col("subject_id") == sample_subject).sort(["time", "code"])

    print(f"\nSample subject {sample_subject} (first shard only):")
    print(f"  {name1}: {len(sample1)} events")
    print(f"  {name2}: {len(sample2)} events")

    if len(sample1) > 0:
        print(f"\nFirst 5 events ({name1}):")
        print(sample1.head(5))

    if len(sample2) > 0:
        print(f"\nFirst 5 events ({name2}):")
        print(sample2.head(5))

    # ========================================================================
    # 6. AGGREGATE STATISTICS (using lazy)
    # ========================================================================
    print("\n[6] AGGREGATE STATISTICS (Lazy Computation)")
    print("-" * 80)

    print(f"\nComputing aggregates for {name1}...")
    lazy1 = pl.scan_parquet(str(data_dir1 / "*.parquet"))
    stats1 = lazy1.select(
        [
            pl.col("subject_id").n_unique().alias("n_subjects"),
            pl.col("code").n_unique().alias("n_codes"),
            pl.len().alias("n_rows"),
        ]
    ).collect()

    print(f"Computing aggregates for {name2}...")
    lazy2 = pl.scan_parquet(str(data_dir2 / "*.parquet"))
    stats2 = lazy2.select(
        [
            pl.col("subject_id").n_unique().alias("n_subjects"),
            pl.col("code").n_unique().alias("n_codes"),
            pl.len().alias("n_rows"),
        ]
    ).collect()

    print(f"\n{name1} statistics:")
    print(f"  Total rows: {stats1['n_rows'][0]:,}")
    print(f"  Unique subjects: {stats1['n_subjects'][0]:,}")
    print(f"  Unique codes: {stats1['n_codes'][0]:,}")

    print(f"\n{name2} statistics:")
    print(f"  Total rows: {stats2['n_rows'][0]:,}")
    print(f"  Unique subjects: {stats2['n_subjects'][0]:,}")
    print(f"  Unique codes: {stats2['n_codes'][0]:,}")

    # ========================================================================
    # 7. SUMMARY
    # ========================================================================
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    issues = []
    if stats1["n_rows"][0] != stats2["n_rows"][0]:
        issues.append(f"Row count differs: {stats1['n_rows'][0]:,} vs {stats2['n_rows'][0]:,}")
    if cols1 != cols2:
        issues.append("Schema columns differ")
    if stats1["n_codes"][0] != stats2["n_codes"][0]:
        issues.append(f"Code count differs: {stats1['n_codes'][0]:,} vs {stats2['n_codes'][0]:,}")
    if stats1["n_subjects"][0] != stats2["n_subjects"][0]:
        issues.append(f"Subject count differs: {stats1['n_subjects'][0]:,} vs {stats2['n_subjects'][0]:,}")

    if issues:
        print("\n⚠️  DIFFERENCES FOUND:")
        for issue in issues:
            print(f"  • {issue}")
    else:
        print("\n✅ Outputs appear identical!")

    print()


if __name__ == "__main__":
    baseline_dir = Path("data/meds_omop_baseline")
    streaming_dir = Path("data/meds_omop_streams")

    compare_meds_directories(baseline_dir, streaming_dir, name1="omop.py", name2="omop_streaming.py")
