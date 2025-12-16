uv run python -m meds_etl.omop \
  --omop_dir ../vista-data-pipelines/local/som-nero-plevriti-deidbdf.vista_debug_large/ \
  --output_dir data/meds_omop/ \
  --config examples/omop_etl_vista_config.json \
  --workers 12 \
  --shards 10 \
  --backend cpp \
  --verbose \
  --force-refresh
