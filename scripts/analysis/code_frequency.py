#!/usr/bin/env python
"""
MEDS Code Frequency Analysis

Generates a code frequency report from a MEDS dataset:
1. Vocabulary summary table (total events, unique codes per vocabulary)
2. Example codes for each vocabulary (2-3 samples)
3. Frequency distribution breakdown (how many codes appear N times)

Usage:
    python code_frequency.py /path/to/meds/data
    python code_frequency.py /path/to/meds/data --output report.csv
"""

import argparse
import sys
from pathlib import Path

import polars as pl


def load_meds_dataset(meds_path: Path) -> pl.LazyFrame:
    """Load MEDS dataset from parquet files."""
    meds_path = Path(meds_path)

    # Find data directory
    if (meds_path / "data").exists():
        data_dir = meds_path / "data"
    elif (meds_path / "result" / "data").exists():
        data_dir = meds_path / "result" / "data"
    else:
        data_dir = meds_path

    parquet_files = list(data_dir.glob("*.parquet"))
    if not parquet_files:
        parquet_files = list(data_dir.glob("**/*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {meds_path}")

    print(f"Found {len(parquet_files)} parquet files in {data_dir}")
    return pl.scan_parquet(parquet_files)


def extract_vocabulary(code_col: pl.Expr) -> pl.Expr:
    """Extract vocabulary prefix from code (e.g., 'SNOMED/12345' -> 'SNOMED')."""
    return pl.when(code_col.str.contains("/")).then(code_col.str.split("/").list.first()).otherwise(pl.lit("OTHER"))


def format_table(df: pl.DataFrame, title: str = "") -> str:
    """Format DataFrame as a simple text table."""
    lines = []
    if title:
        lines.append(f"\n{'=' * 60}")
        lines.append(title)
        lines.append("=" * 60)

    # Compute column widths
    headers = df.columns
    widths = []
    for col in headers:
        formatted_vals = []
        for v in df[col].to_list():
            if isinstance(v, float):
                formatted_vals.append(f"{v:.2f}")
            elif isinstance(v, int):
                formatted_vals.append(f"{v:,}")
            else:
                formatted_vals.append(str(v))
        max_val_width = max(len(v) for v in formatted_vals) if formatted_vals else 0
        widths.append(max(len(col), max_val_width))

    # Header
    header_line = "  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=False))
    lines.append(header_line)
    lines.append("-" * len(header_line))

    # Rows
    for row in df.iter_rows():
        formatted = []
        for val, width in zip(row, widths, strict=False):
            if isinstance(val, float):
                formatted.append(f"{val:.2f}".ljust(width))
            elif isinstance(val, int):
                formatted.append(f"{val:,}".ljust(width))
            else:
                formatted.append(str(val).ljust(width))
        lines.append("  ".join(formatted))

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate code frequency analysis from MEDS dataset",
    )
    parser.add_argument("meds_path", type=Path, help="Path to MEDS dataset directory")
    parser.add_argument("--output", "-o", type=Path, help="Save full code frequencies to CSV")

    args = parser.parse_args()

    # Load dataset
    print(f"Loading MEDS dataset from {args.meds_path}...")
    try:
        df = load_meds_dataset(args.meds_path)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Compute code frequencies
    print("Computing code frequencies...")
    code_counts = (
        df.select("code")
        .group_by("code")
        .agg(pl.len().alias("count"))
        .with_columns(extract_vocabulary(pl.col("code")).alias("vocabulary"))
        .collect()
    )

    total_events = code_counts["count"].sum()
    total_codes = len(code_counts)

    print(f"\nDataset: {total_events:,} total events, {total_codes:,} unique codes")

    # =========================================================================
    # 1. VOCABULARY SUMMARY TABLE
    # =========================================================================
    vocab_summary = (
        code_counts.group_by("vocabulary")
        .agg(
            [
                pl.len().alias("unique_codes"),
                pl.col("count").sum().alias("total_events"),
            ]
        )
        .with_columns((pl.col("total_events") * 100.0 / total_events).alias("pct"))
        .sort("total_events", descending=True)
    )

    print(format_table(vocab_summary, "VOCABULARY SUMMARY"))

    # =========================================================================
    # 2. EXAMPLE CODES PER VOCABULARY
    # =========================================================================
    print(f"\n{'=' * 60}")
    print("EXAMPLE CODES (top 3 per vocabulary)")
    print("=" * 60)

    for vocab in vocab_summary["vocabulary"].to_list():
        vocab_codes = code_counts.filter(pl.col("vocabulary") == vocab).sort("count", descending=True).head(3)
        examples = []
        for row in vocab_codes.iter_rows(named=True):
            examples.append(f"{row['code']} ({row['count']:,})")

        print(f"\n{vocab}:")
        for ex in examples:
            print(f"  - {ex}")

    # =========================================================================
    # 3. FREQUENCY DISTRIBUTION BREAKDOWN
    # =========================================================================
    # Bucket codes by their frequency
    buckets = [
        (1, 1, "1"),
        (2, 10, "2-10"),
        (11, 100, "11-100"),
        (101, 1000, "101-1K"),
        (1001, 10000, "1K-10K"),
        (10001, 100000, "10K-100K"),
        (100001, 1000000, "100K-1M"),
        (1000001, None, ">1M"),
    ]

    freq_dist = []
    for low, high, label in buckets:
        if high is None:
            bucket_codes = code_counts.filter(pl.col("count") >= low)
        else:
            bucket_codes = code_counts.filter((pl.col("count") >= low) & (pl.col("count") <= high))

        n_codes = len(bucket_codes)
        n_events = bucket_codes["count"].sum() if n_codes > 0 else 0
        pct_codes = 100.0 * n_codes / total_codes if total_codes > 0 else 0
        pct_events = 100.0 * n_events / total_events if total_events > 0 else 0

        freq_dist.append(
            {
                "frequency": label,
                "codes": n_codes,
                "pct_codes": pct_codes,
                "events": n_events,
                "pct_events": pct_events,
            }
        )

    freq_df = pl.DataFrame(freq_dist)
    print(format_table(freq_df, "CODE FREQUENCY DISTRIBUTION"))

    # =========================================================================
    # OPTIONAL: Save full results
    # =========================================================================
    if args.output:
        full_results = (
            code_counts.with_columns((pl.col("count") * 100.0 / total_events).alias("pct"))
            .sort(["vocabulary", "count"], descending=[False, True])
            .select(["vocabulary", "code", "count", "pct"])
        )
        full_results.write_csv(args.output)
        print(f"\nFull code frequencies saved to {args.output}")


if __name__ == "__main__":
    main()
