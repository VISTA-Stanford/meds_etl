# omop.py (recommended - uses meds_etl_cpp for sorting)
uv run python -m meds_etl.omop \
  --omop_dir ../vista-data-pipelines/local/som-nero-plevriti-deidbdf.vista_debug_large/ \
  --output_dir data/vista_debug_large/ \
  --config examples/omop_etl_vista_raw_codes.json \
  --workers 10 \
  --shards 10 \
  --backend cpp \
  --verbose \
  --force-refresh

# omop_streaming.py (alternative - uses Polars streaming for sorting)
uv run python -m meds_etl.omop_streaming \
  --omop_dir ../vista-data-pipelines/local/som-nero-plevriti-deidbdf.vista_debug_large/ \
  --output_dir data/vista_debug_large_streaming/ \
  --config examples/omop_etl_vista_raw_codes.json \
  --workers 10 \
  --shards 10 \
  --chunk_rows 10_000_000 \
  --merge_workers 4 \
  --verbose \
  --force-refresh
