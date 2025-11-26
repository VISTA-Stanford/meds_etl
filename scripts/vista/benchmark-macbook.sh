# streaming tuning
uv run python scripts/benchmarking/benchmark_streaming_tuning.py \
--omop_dir ../vista-data-pipelines/local/som-nero-plevriti-deidbdf.vista_debug_large/ \
--base_output_dir data/benchmarking/full/ \
--config examples/omop_etl_vista_config.json \
--experiment full

# compare versions
uv run python scripts/benchmarking/benchmark_methods_bakeoff.py \
--omop_dir ../vista-data-pipelines/local/som-nero-plevriti-deidbdf.vista_debug_large/ \
--base_output_dir data/benchmarking/bakeoff/ \
--config examples/omop_etl_vista_config.json \
--num_shards 10 \
--num_workers 8