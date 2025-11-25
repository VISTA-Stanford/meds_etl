uv run python scripts/benchmarking/benchmark_streaming_tuning.py \
--omop_dir /shahlab/projects/tumorboard/code/vista-data-pipelines/local/som-nero-plevriti-deidbdf.vista_debug_large/ \
--base_output_dir data/benchmarking/full/ \
--config examples/omop_etl_vista_config.json \
--experiment full


uv runpython scripts/benchmarking/benchmark_methods_bakeoff.py \
--omop_dir /shahlab/projects/tumorboard/code/vista-data-pipelines/local/som-nero-plevriti-deidbdf.vista_debug_large/ \
--base_output_dir data/benchmarking/bakeoff/ \
--config examples/omop_etl_vista_config.json \
--backend cpp
