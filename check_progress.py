#!/usr/bin/env python3
"""
Quick script to check ETL progress by monitoring output directory
"""
import os
import sys
import time
from pathlib import Path

def check_progress(output_dir):
    """Monitor the MEDS ETL output directory for progress"""
    output_path = Path(output_dir)
    
    if not output_path.exists():
        print(f"❌ Output directory doesn't exist yet: {output_dir}")
        return
    
    print(f"Monitoring: {output_dir}\n")
    
    # Check temp/unsorted_data for files being created
    unsorted_data = output_path / "temp" / "unsorted_data"
    if unsorted_data.exists():
        files = list(unsorted_data.glob("*.parquet"))
        print(f"✓ MEDS Unsorted files: {len(files)} files")
        if files:
            total_size = sum(f.stat().st_size for f in files) / (1024**2)
            print(f"  Total size: {total_size:.1f} MB")
            print(f"  Latest: {max(files, key=lambda f: f.stat().st_mtime).name}")
    else:
        print("⏳ Waiting for unsorted_data directory...")
    
    # Check data directory
    data_dir = output_path / "data"
    if data_dir.exists():
        files = list(data_dir.glob("*.parquet"))
        print(f"\n✓ Final MEDS files: {len(files)} files")
        if files:
            total_size = sum(f.stat().st_size for f in files) / (1024**2)
            print(f"  Total size: {total_size:.1f} MB")
    
    # Check decompressed directory (should be empty or removed when done)
    decompressed = output_path / "decompressed"
    if decompressed.exists():
        files = list(decompressed.glob("*"))
        print(f"\n⏳ Decompressed temp files: {len(files)}")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_progress.py <output_directory>")
        print("Example: python check_progress.py /path/to/vista_debug_meds")
        sys.exit(1)
    
    output_dir = sys.argv[1]
    
    try:
        while True:
            os.system('clear' if os.name != 'nt' else 'cls')
            print(f"MEDS ETL Progress Monitor")
            print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*60 + "\n")
            check_progress(output_dir)
            print("\nPress Ctrl+C to stop monitoring...")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")


