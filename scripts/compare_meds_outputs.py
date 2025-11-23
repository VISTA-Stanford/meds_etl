"""
Compare MEDS outputs from omop.py (baseline) and omop_streaming.py
"""

import polars as pl
import json
from pathlib import Path
from collections import defaultdict

def compare_meds_directories(dir1: Path, dir2: Path, name1: str = "baseline", name2: str = "streaming"):
    """
    Compare two MEDS output directories.
    
    Args:
        dir1: First MEDS directory (e.g., meds_omop_baseline)
        dir2: Second MEDS directory (e.g., meds_omop_streams)
        name1: Label for first directory
        name2: Label for second directory
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
    
    # ========================================================================
    # 3. SCHEMA COMPARISON
    # ========================================================================
    print("\n[3] SCHEMA COMPARISON")
    print("-" * 80)
    
    # Read first shard from each
    df1 = pl.read_parquet(files1[0])
    df2 = pl.read_parquet(files2[0])
    
    schema1 = df1.schema
    schema2 = df2.schema
    
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
    # 4. ROW COUNT COMPARISON
    # ========================================================================
    print("\n[4] ROW COUNT COMPARISON")
    print("-" * 80)
    
    total_rows1 = 0
    total_rows2 = 0
    shard_counts1 = []
    shard_counts2 = []
    
    print(f"\nCounting rows per shard...")
    for f1, f2 in zip(files1, files2):
        count1 = len(pl.read_parquet(f1))
        count2 = len(pl.read_parquet(f2))
        shard_counts1.append(count1)
        shard_counts2.append(count2)
        total_rows1 += count1
        total_rows2 += count2
        print(f"  Shard {files1.index(f1)}: {count1:,} ({name1}) vs {count2:,} ({name2})")
    
    print(f"\n{name1} total: {total_rows1:,} rows")
    print(f"{name2} total: {total_rows2:,} rows")
    
    if total_rows1 != total_rows2:
        diff = abs(total_rows1 - total_rows2)
        pct = (diff / max(total_rows1, total_rows2)) * 100
        print(f"⚠️  ROW COUNT DIFFERS: {diff:,} rows ({pct:.2f}%)")
    else:
        print("✅ Same total row count")
    
    # ========================================================================
    # 5. CODE DISTRIBUTION COMPARISON
    # ========================================================================
    print("\n[5] CODE DISTRIBUTION COMPARISON")
    print("-" * 80)
    
    print(f"\nReading all data from {name1}...")
    df1_all = pl.concat([pl.read_parquet(f) for f in files1])
    
    print(f"Reading all data from {name2}...")
    df2_all = pl.concat([pl.read_parquet(f) for f in files2])
    
    # Code counts
    codes1_counts = df1_all.group_by("code").len().sort("len", descending=True)
    codes2_counts = df2_all.group_by("code").len().sort("len", descending=True)
    
    print(f"\n{name1}: {len(codes1_counts)} unique codes")
    print(f"{name2}: {len(codes2_counts)} unique codes")
    
    print(f"\nTop 10 codes in {name1}:")
    for row in codes1_counts.head(10).iter_rows(named=True):
        print(f"  {row['code']}: {row['len']:,} occurrences")
    
    print(f"\nTop 10 codes in {name2}:")
    for row in codes2_counts.head(10).iter_rows(named=True):
        print(f"  {row['code']}: {row['len']:,} occurrences")
    
    # Compare code sets
    codes1_set = set(df1_all["code"].unique().to_list())
    codes2_set = set(df2_all["code"].unique().to_list())
    
    only_in_1 = codes1_set - codes2_set
    only_in_2 = codes2_set - codes1_set
    
    if only_in_1 or only_in_2:
        print("\n⚠️  CODE SETS DIFFER!")
        if only_in_1:
            print(f"  Only in {name1}: {len(only_in_1)} codes")
            print(f"    Examples: {list(only_in_1)[:5]}")
        if only_in_2:
            print(f"  Only in {name2}: {len(only_in_2)} codes")
            print(f"    Examples: {list(only_in_2)[:5]}")
    else:
        print("\n✅ Same set of codes")
    
    # ========================================================================
    # 6. SUBJECT_ID DISTRIBUTION
    # ========================================================================
    print("\n[6] SUBJECT_ID DISTRIBUTION")
    print("-" * 80)
    
    subjects1 = df1_all["subject_id"].unique().sort()
    subjects2 = df2_all["subject_id"].unique().sort()
    
    print(f"\n{name1}: {len(subjects1):,} unique subjects")
    print(f"{name2}: {len(subjects2):,} unique subjects")
    
    if len(subjects1) != len(subjects2):
        print(f"⚠️  Different number of subjects")
    else:
        print("✅ Same number of subjects")
    
    # Check if same subjects
    if not subjects1.equals(subjects2):
        subjects1_set = set(subjects1.to_list())
        subjects2_set = set(subjects2.to_list())
        only_in_1 = subjects1_set - subjects2_set
        only_in_2 = subjects2_set - subjects1_set
        
        if only_in_1 or only_in_2:
            print(f"⚠️  Different subjects!")
            if only_in_1:
                print(f"  Only in {name1}: {len(only_in_1)} subjects")
                print(f"    Examples: {list(only_in_1)[:5]}")
            if only_in_2:
                print(f"  Only in {name2}: {len(only_in_2)} subjects")
                print(f"    Examples: {list(only_in_2)[:5]}")
    else:
        print("✅ Same subjects")
    
    # ========================================================================
    # 7. SAMPLE DATA COMPARISON
    # ========================================================================
    print("\n[7] SAMPLE DATA COMPARISON")
    print("-" * 80)
    
    # Pick a random subject that exists in both
    common_subjects = set(subjects1.to_list()) & set(subjects2.to_list())
    if common_subjects:
        sample_subject = sorted(common_subjects)[0]
        
        sample1 = df1_all.filter(pl.col("subject_id") == sample_subject).sort(["time", "code"])
        sample2 = df2_all.filter(pl.col("subject_id") == sample_subject).sort(["time", "code"])
        
        print(f"\nSample subject: {sample_subject}")
        print(f"  {name1}: {len(sample1)} events")
        print(f"  {name2}: {len(sample2)} events")
        
        print(f"\nFirst 5 events in {name1}:")
        print(sample1.head(5))
        
        print(f"\nFirst 5 events in {name2}:")
        print(sample2.head(5))
    
    # ========================================================================
    # 8. SUMMARY
    # ========================================================================
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    issues = []
    if total_rows1 != total_rows2:
        issues.append(f"Row count differs: {total_rows1:,} vs {total_rows2:,}")
    if cols1 != cols2:
        issues.append("Schema columns differ")
    if only_in_1 or only_in_2:
        issues.append("Code sets differ")
    if len(subjects1) != len(subjects2):
        issues.append("Subject count differs")
    
    if issues:
        print("\n⚠️  DIFFERENCES FOUND:")
        for issue in issues:
            print(f"  • {issue}")
    else:
        print("\n✅ No significant differences found!")
    
    print()


if __name__ == "__main__":
    baseline_dir = Path("data/meds_omop_baseline")
    streaming_dir = Path("data/meds_omop_streams")
    
    compare_meds_directories(
        baseline_dir,
        streaming_dir,
        name1="omop.py",
        name2="omop_streaming.py"
    )

