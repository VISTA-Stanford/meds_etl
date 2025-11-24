python -m meds_etl.omop \
  /shahlab/projects/tumorboard/code/vista-data-pipelines/local/som-nero-plevriti-deidbdf.oncology_omop_confidential_irb76049_aug2025/ \
  data/vista_aug2025_legacy/ \
  --num_shards 128 \
  --num_proc 64 \
  --backend cpp \
  --omop_version 5.3