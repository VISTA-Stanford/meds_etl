
uv run python -m meds_etl.omop_refactor \
  --omop_dir /Users/jfries/code/vista-data-pipelines/local/som-nero-plevriti-deidbdf.vista_debug_large/ \
  --output_dir data/meds_omop_refactor/ \
  --config examples/omop_etl_vista_config.json \
  --workers 8 \
  --shards 10 \
  --backend cpp \
  --code_mapping concept_id \
  --verbose


uv run python -m meds_etl.omop_refactor_streaming \
  --omop_dir /Users/jfries/code/vista-data-pipelines/local/som-nero-plevriti-deidbdf.vista_debug_large/ \
  --output_dir data/meds_omop_streams \
  --config omop_etl_vista_config.json \
  --workers 8 \
  --shards 10 \
  --row_group_size 200_000 \
  --polars_threads 1 \
  --process_method spawn \
  --merge_workers 4 \
  --run_compression uncompressed \
  --chunk_rows 10_000_000


