#!/bin/bash
# Monitor ETL progress by watching file creation

OUTPUT_DIR="${1:-/Users/jfries/code/vista-data-pipelines/data/cache/vista_debug_meds}"

echo "Monitoring ETL progress in: $OUTPUT_DIR"
echo "Press Ctrl+C to stop"
echo ""

LAST_COUNT=0
while true; do
    clear
    echo "==================================================="
    echo "MEDS ETL Progress Monitor - $(date '+%H:%M:%S')"
    echo "==================================================="
    echo ""
    
    # Count unsorted files
    if [ -d "$OUTPUT_DIR/temp/unsorted_data" ]; then
        UNSORTED=$(find "$OUTPUT_DIR/temp/unsorted_data" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
        echo "✓ MEDS Unsorted files: $UNSORTED / ~12 expected"
        
        if [ "$UNSORTED" -gt "$LAST_COUNT" ]; then
            echo "  → Processing... (+$((UNSORTED - LAST_COUNT)) new files)"
            LAST_COUNT=$UNSORTED
        fi
        
        # Show most recent file
        LATEST=$(find "$OUTPUT_DIR/temp/unsorted_data" -name "*.parquet" -type f 2>/dev/null | xargs ls -t 2>/dev/null | head -1)
        if [ -n "$LATEST" ]; then
            AGE=$(( $(date +%s) - $(stat -f %m "$LATEST" 2>/dev/null || stat -c %Y "$LATEST" 2>/dev/null) ))
            echo "  Latest: $(basename "$LATEST") (${AGE}s ago)"
        fi
        
        # Show total size
        SIZE=$(du -sh "$OUTPUT_DIR/temp/unsorted_data" 2>/dev/null | cut -f1)
        echo "  Size: $SIZE"
    else
        echo "⏳ Waiting for unsorted_data directory..."
    fi
    
    echo ""
    
    # Check for final data files
    if [ -d "$OUTPUT_DIR/data" ]; then
        DATA_FILES=$(find "$OUTPUT_DIR/data" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
        echo "✓ Final MEDS files: $DATA_FILES"
        if [ "$DATA_FILES" -gt 0 ]; then
            echo "  🎉 ETL COMPLETE!"
            break
        fi
    fi
    
    # Check if process is still running
    if pgrep -f "meds_etl_omop" > /dev/null; then
        echo ""
        echo "✓ ETL process is running"
        echo "  CPU: $(ps aux | grep meds_etl_omop | grep -v grep | awk '{print $3}')%"
        echo "  Memory: $(ps aux | grep meds_etl_omop | grep -v grep | awk '{print $4}')%"
    else
        echo ""
        echo "⚠ No ETL process detected (may have finished or crashed)"
    fi
    
    echo ""
    echo "==================================================="
    echo "Refreshing every 5 seconds..."
    sleep 5
done


