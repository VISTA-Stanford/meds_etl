"""
Detailed comparison of MEDS outputs to find specific row differences
"""

import polars as pl
from pathlib import Path
from collections import defaultdict

def detailed_row_comparison(dir1: Path, dir2: Path, name1: str = "omop.py", name2: str = "omop_streaming.py"):
    """
    Find and enumerate specific rows that differ between two MEDS outputs.
    """
    print("=" * 80)
    print(f"DETAILED ROW COMPARISON: {name1} vs {name2}")
    print("=" * 80)
    
    data_dir1 = dir1 / "data"
    data_dir2 = dir2 / "data"
    
    # ========================================================================
    # 1. CODE COMPARISON - Which codes are different?
    # ========================================================================
    print("\n[1] CODE SET COMPARISON")
    print("-" * 80)
    
    print(f"Loading unique codes from {name1}...")
    codes1 = pl.scan_parquet(str(data_dir1 / "*.parquet")).select("code").unique().collect()
    codes1_set = set(codes1["code"].to_list())
    
    print(f"Loading unique codes from {name2}...")
    codes2 = pl.scan_parquet(str(data_dir2 / "*.parquet")).select("code").unique().collect()
    codes2_set = set(codes2["code"].to_list())
    
    only_in_1 = codes1_set - codes2_set
    only_in_2 = codes2_set - codes1_set
    
    print(f"\n{name1}: {len(codes1_set):,} unique codes")
    print(f"{name2}: {len(codes2_set):,} unique codes")
    print(f"Only in {name1}: {len(only_in_1):,} codes")
    print(f"Only in {name2}: {len(only_in_2):,} codes")
    
    if only_in_1:
        print(f"\nSample codes only in {name1} (first 20):")
        for code in sorted(only_in_1)[:20]:
            print(f"  {code}")
    
    if only_in_2:
        print(f"\nSample codes only in {name2} (first 20):")
        for code in sorted(only_in_2)[:20]:
            print(f"  {code}")
    
    # ========================================================================
    # 2. CODE FREQUENCY COMPARISON
    # ========================================================================
    print("\n[2] CODE FREQUENCY COMPARISON")
    print("-" * 80)
    
    if only_in_1:
        print(f"\nCounting occurrences of codes only in {name1}...")
        df1 = pl.scan_parquet(str(data_dir1 / "*.parquet"))
        codes_only_1_freq = df1.filter(
            pl.col("code").is_in(list(only_in_1))
        ).group_by("code").len().sort("len", descending=True).collect()
        
        print(f"Total rows with codes only in {name1}: {codes_only_1_freq['len'].sum():,}")
        print(f"\nTop 10 codes only in {name1}:")
        for row in codes_only_1_freq.head(10).iter_rows(named=True):
            print(f"  {row['code']}: {row['len']:,} occurrences")
    
    if only_in_2:
        print(f"\nCounting occurrences of codes only in {name2}...")
        df2 = pl.scan_parquet(str(data_dir2 / "*.parquet"))
        codes_only_2_freq = df2.filter(
            pl.col("code").is_in(list(only_in_2))
        ).group_by("code").len().sort("len", descending=True).collect()
        
        print(f"Total rows with codes only in {name2}: {codes_only_2_freq['len'].sum():,}")
        print(f"\nTop 10 codes only in {name2}:")
        for row in codes_only_2_freq.head(10).iter_rows(named=True):
            print(f"  {row['code']}: {row['len']:,} occurrences")
    
    # ========================================================================
    # 3. COMMON CODE FREQUENCY DIFFERENCES
    # ========================================================================
    print("\n[3] COMMON CODE FREQUENCY DIFFERENCES")
    print("-" * 80)
    
    common_codes = codes1_set & codes2_set
    print(f"\nAnalyzing {len(common_codes):,} common codes...")
    
    print(f"Getting frequencies from {name1}...")
    freq1 = pl.scan_parquet(str(data_dir1 / "*.parquet")).group_by("code").len().collect()
    freq1 = freq1.rename({"len": "count_1"})
    
    print(f"Getting frequencies from {name2}...")
    freq2 = pl.scan_parquet(str(data_dir2 / "*.parquet")).group_by("code").len().collect()
    freq2 = freq2.rename({"len": "count_2"})
    
    # Join and compute differences
    freq_comparison = freq1.join(freq2, on="code", how="outer_coalesce").with_columns([
        pl.col("count_1").fill_null(0),
        pl.col("count_2").fill_null(0),
    ]).with_columns([
        (pl.col("count_1") - pl.col("count_2")).alias("diff"),
        (pl.col("count_1") - pl.col("count_2")).abs().alias("abs_diff"),
    ]).sort("abs_diff", descending=True)
    
    print(f"\nTop 20 codes with biggest frequency differences:")
    print(f"{'Code':<50} {name1:>15} {name2:>15} {'Difference':>15}")
    print("-" * 100)
    
    for row in freq_comparison.head(20).iter_rows(named=True):
        code = row['code'][:45] if row['code'] else 'null'
        count1 = row['count_1']
        count2 = row['count_2']
        diff = row['diff']
        print(f"{code:<50} {count1:>15,} {count2:>15,} {diff:>+15,}")
    
    # ========================================================================
    # 4. ANALYZE SPECIFIC PATIENTS WITH DIFFERENCES
    # ========================================================================
    print("\n[4] PATIENT-LEVEL COMPARISON")
    print("-" * 80)
    
    # Get subjects with different row counts
    print(f"\nAnalyzing per-subject row counts...")
    
    subj_counts1 = pl.scan_parquet(str(data_dir1 / "*.parquet")).group_by("subject_id").len().collect()
    subj_counts1 = subj_counts1.rename({"len": "count_1"})
    
    subj_counts2 = pl.scan_parquet(str(data_dir2 / "*.parquet")).group_by("subject_id").len().collect()
    subj_counts2 = subj_counts2.rename({"len": "count_2"})
    
    subj_comparison = subj_counts1.join(subj_counts2, on="subject_id", how="outer_coalesce").with_columns([
        pl.col("count_1").fill_null(0),
        pl.col("count_2").fill_null(0),
    ]).with_columns([
        (pl.col("count_1") - pl.col("count_2")).alias("diff"),
        (pl.col("count_1") - pl.col("count_2")).abs().alias("abs_diff"),
    ]).sort("abs_diff", descending=True)
    
    print(f"\nTop 10 subjects with biggest row count differences:")
    print(f"{'Subject ID':<15} {name1:>15} {name2:>15} {'Difference':>15}")
    print("-" * 65)
    
    for row in subj_comparison.head(10).iter_rows(named=True):
        subj_id = row['subject_id']
        count1 = row['count_1']
        count2 = row['count_2']
        diff = row['diff']
        print(f"{subj_id:<15} {count1:>15,} {count2:>15,} {diff:>+15,}")
    
    # ========================================================================
    # 5. SAMPLE SPECIFIC ROWS - Pick a subject with differences
    # ========================================================================
    print("\n[5] DETAILED EXAMPLE - Subject with Differences")
    print("-" * 80)
    
    # Pick a subject with a moderate difference (not 0, not too huge)
    example_subjects = subj_comparison.filter(
        (pl.col("abs_diff") > 0) & (pl.col("abs_diff") < 1000)
    ).head(1)
    
    if len(example_subjects) > 0:
        example_subj = example_subjects["subject_id"][0]
        
        print(f"\nAnalyzing subject {example_subj}:")
        
        # Load data for this subject from both
        df1_subj = pl.scan_parquet(str(data_dir1 / "*.parquet")).filter(
            pl.col("subject_id") == example_subj
        ).collect()
        
        df2_subj = pl.scan_parquet(str(data_dir2 / "*.parquet")).filter(
            pl.col("subject_id") == example_subj
        ).collect()
        
        print(f"  {name1}: {len(df1_subj):,} rows")
        print(f"  {name2}: {len(df2_subj):,} rows")
        
        # Get common columns for comparison
        common_cols = ["subject_id", "time", "code"]
        
        # Check which codes differ
        codes1_subj = set(df1_subj["code"].unique().to_list())
        codes2_subj = set(df2_subj["code"].unique().to_list())
        
        only_in_1_subj = codes1_subj - codes2_subj
        only_in_2_subj = codes2_subj - codes1_subj
        
        if only_in_1_subj:
            print(f"\n  Codes only in {name1} for this subject: {len(only_in_1_subj)}")
            print(f"  Examples: {list(only_in_1_subj)[:5]}")
            
            # Show sample rows
            sample1 = df1_subj.filter(pl.col("code").is_in(list(only_in_1_subj))).head(5)
            print(f"\n  Sample rows only in {name1}:")
            print(sample1.select(["subject_id", "time", "code", "numeric_value"]))
        
        if only_in_2_subj:
            print(f"\n  Codes only in {name2} for this subject: {len(only_in_2_subj)}")
            print(f"  Examples: {list(only_in_2_subj)[:5]}")
            
            # Show sample rows
            sample2 = df2_subj.filter(pl.col("code").is_in(list(only_in_2_subj))).head(5)
            print(f"\n  Sample rows only in {name2}:")
            print(sample2.select(["subject_id", "time", "code", "numeric_value"]))
        
        # Check for duplicate counts
        code_counts1 = df1_subj.group_by("code").len().sort("code")
        code_counts2 = df2_subj.group_by("code").len().sort("code")
        
        code_count_comp = code_counts1.join(code_counts2, on="code", how="outer_coalesce", suffix="_2").with_columns([
            pl.col("len").fill_null(0).alias("count_1"),
            pl.col("len_2").fill_null(0).alias("count_2"),
        ]).with_columns([
            (pl.col("count_1") - pl.col("count_2")).alias("diff")
        ]).filter(pl.col("diff") != 0).sort(pl.col("diff").abs(), descending=True)
        
        if len(code_count_comp) > 0:
            print(f"\n  Codes with different frequencies for this subject:")
            print(f"  {'Code':<40} {name1:>10} {name2:>10} {'Diff':>10}")
            print("  " + "-" * 75)
            for row in code_count_comp.head(10).iter_rows(named=True):
                code = row['code'][:35] if row['code'] else 'null'
                c1 = row['count_1']
                c2 = row['count_2']
                diff = row['diff']
                print(f"  {code:<40} {c1:>10,} {c2:>10,} {diff:>+10,}")
    
    # ========================================================================
    # 6. SCHEMA-BASED ANALYSIS
    # ========================================================================
    print("\n[6] SCHEMA-BASED FILTERING ANALYSIS")
    print("-" * 80)
    
    # Check which columns might be causing filtering differences
    schema1 = pl.read_parquet_schema(list(data_dir1.glob("*.parquet"))[0])
    schema2 = pl.read_parquet_schema(list(data_dir2.glob("*.parquet"))[0])
    
    # Columns in omop.py that aren't in omop_streaming
    cols_only_1 = set(schema1.keys()) - set(schema2.keys())
    cols_only_2 = set(schema2.keys()) - set(schema1.keys())
    
    print(f"\nColumns only in {name1}: {cols_only_1}")
    print(f"Columns only in {name2}: {cols_only_2}")
    
    print(f"\nThis suggests:")
    print(f"  - {name1} uses hardcoded OMOP schema with custom fields")
    print(f"  - {name2} uses config-driven schema with more OMOP metadata")
    
    # ========================================================================
    # 7. NUMERIC VALUE COMPARISON
    # ========================================================================
    print("\n[7] NUMERIC VALUE COMPARISON")
    print("-" * 80)
    
    # Check if numeric_value handling differs
    print(f"\nChecking numeric_value statistics...")
    
    nv_stats1 = pl.scan_parquet(str(data_dir1 / "*.parquet")).select([
        pl.col("numeric_value").is_not_null().sum().alias("non_null"),
        pl.col("numeric_value").is_null().sum().alias("null_count"),
        pl.len().alias("total"),
    ]).collect()
    
    nv_stats2 = pl.scan_parquet(str(data_dir2 / "*.parquet")).select([
        pl.col("numeric_value").is_not_null().sum().alias("non_null"),
        pl.col("numeric_value").is_null().sum().alias("null_count"),
        pl.len().alias("total"),
    ]).collect()
    
    print(f"\n{name1} numeric_value:")
    print(f"  Non-null: {nv_stats1['non_null'][0]:,} ({nv_stats1['non_null'][0]/nv_stats1['total'][0]*100:.1f}%)")
    print(f"  Null:     {nv_stats1['null_count'][0]:,} ({nv_stats1['null_count'][0]/nv_stats1['total'][0]*100:.1f}%)")
    print(f"  Total:    {nv_stats1['total'][0]:,}")
    
    print(f"\n{name2} numeric_value:")
    print(f"  Non-null: {nv_stats2['non_null'][0]:,} ({nv_stats2['non_null'][0]/nv_stats2['total'][0]*100:.1f}%)")
    print(f"  Null:     {nv_stats2['null_count'][0]:,} ({nv_stats2['null_count'][0]/nv_stats2['total'][0]*100:.1f}%)")
    print(f"  Total:    {nv_stats2['total'][0]:,}")
    
    # ========================================================================
    # 8. SUMMARY OF FINDINGS
    # ========================================================================
    print("\n" + "=" * 80)
    print("SUMMARY OF DIFFERENCES")
    print("=" * 80)
    
    print(f"\n1. Row Count Difference: {abs(nv_stats1['total'][0] - nv_stats2['total'][0]):,} rows")
    print(f"   ({name1}: {nv_stats1['total'][0]:,} vs {name2}: {nv_stats2['total'][0]:,})")
    
    print(f"\n2. Code Set Differences:")
    print(f"   - Codes only in {name1}: {len(only_in_1):,}")
    if only_in_1:
        print(f"     Total rows: {codes_only_1_freq['len'].sum():,}")
    print(f"   - Codes only in {name2}: {len(only_in_2):,}")
    if only_in_2:
        print(f"     Total rows: {codes_only_2_freq['len'].sum():,}")
    
    print(f"\n3. Schema Differences:")
    print(f"   - {name1}: {len(schema1)} columns")
    print(f"   - {name2}: {len(schema2)} columns")
    print(f"   - Unique to {name1}: {len(cols_only_1)} columns")
    print(f"   - Unique to {name2}: {len(cols_only_2)} columns")
    
    print(f"\n4. Likely Causes:")
    if len(only_in_1) > len(only_in_2):
        print(f"   ⚠️  {name1} generates MORE unique codes (possibly including intermediate IDs)")
    if len(only_in_2) > len(only_in_1):
        print(f"   ⚠️  {name2} generates MORE unique codes (better code mapping?)")
    
    print(f"\n5. Recommendations:")
    print(f"   - Review code mapping logic in both implementations")
    print(f"   - Check for null/empty row filtering differences")
    print(f"   - Verify concept ID mapping behavior")
    print()


if __name__ == "__main__":
    baseline_dir = Path("data/meds_omop_baseline")
    streaming_dir = Path("data/meds_omop_streams")
    
    detailed_row_comparison(
        baseline_dir,
        streaming_dir,
        name1="omop.py",
        name2="omop_streaming.py"
    )

