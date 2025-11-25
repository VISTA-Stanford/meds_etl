python -m meds_etl.omop \
  /shahlab/projects/tumorboard/code/vista-data-pipelines/local/som-nero-plevriti-deidbdf.oncology_omop_confidential_irb76049_aug2025/ \
  data/vista_aug2025_legacy/ \
  --num_shards 128 \
  --num_proc 64 \
  --backend cpp \
  --omop_version 5.3


uv run python -m meds_etl.omop_refactor_streaming \
--omop_dir /shahlab/projects/tumorboard/code/vista-data-pipelines/local/som-nero-plevriti-deidbdf.vista_debug_large/ \
--output_dir data/meds_omop_streams \
--config omop_etl_vista_config.json \
--workers 8 \
--shards 8 \
--row_group_size 200_000 \
--polars_threads 4 \
--process_method spawn \
--merge_workers 4 \
--run_compression uncompressed \
--chunk_rows 20_000_000