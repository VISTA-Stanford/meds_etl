# MEDS ETL Performance Benchmarking Report

Generated: 2025-11-22 14:34:24

---

## Executive Summary

**Best Configuration:** full_12 from full experiment
- **Time:** 1.27 minutes
- **Workers:** 16
- **Shards:** 20
- **Stage 1:** 12.2s
- **Stage 2:** 56.0s

---

## Experiment Results

### 1. Worker Count Optimization

*Experiment not yet run. Run with: `--experiment workers`*

---

### 2. Shard Count Optimization

*Experiment not yet run. Run with: `--experiment shards`*

---

### 3. Chunk Size Optimization

*Experiment not yet run. Run with: `--experiment chunk_rows`*

---

### 4. Compression Strategy Optimization

*Experiment not yet run. Run with: `--experiment compression`*

---

### 5. Memory Configuration Optimization

*Experiment not yet run. Run with: `--experiment memory`*

---

### 6. Combined Workers × Shards Optimization

*Experiment not yet run. Run with: `--experiment workers_and_shards`*

---

### 7. Full Grid Search

Comprehensive grid search across multiple parameters simultaneously.

### Full Experiment

| Rank | Experiment | Time (min) | Stage 1 (s) | Stage 2 (s) | Size (GB) |
|------|------------|------------|-------------|-------------|----------|
| 1 | full_12 | 1.27 | 12.2 | 56.0 | 3.69 |
| 2 | full_11 | 1.31 | 12.3 | 57.8 | 3.69 |
| 3 | full_9 | 1.45 | 12.3 | 66.3 | 3.69 |
| 4 | full_10 | 1.48 | 12.6 | 67.9 | 3.69 |
| 5 | full_8 | 1.82 | 23.8 | 76.8 | 3.69 |
| 6 | full_7 | 1.83 | 23.7 | 77.6 | 3.69 |
| 7 | full_5 | 1.93 | 23.8 | 83.7 | 3.69 |
| 8 | full_6 | 1.96 | 23.7 | 85.3 | 3.69 |
| 9 | full_3 | 2.83 | 46.2 | 114.9 | 3.69 |
| 10 | full_1 | 2.85 | 47.0 | 115.2 | 3.69 |

#### Top 5 Configurations

| Rank | Workers | Shards | Chunk Rows | Compression (run/final) | Time (min) |
|------|---------|--------|------------|------------------------|------------|
| 1 | 16 | 20 | 10.0M | lz4/zstd | 1.27 |
| 2 | 16 | 20 | 5.0M | lz4/zstd | 1.31 |
| 3 | 16 | 10 | 5.0M | lz4/zstd | 1.45 |
| 4 | 16 | 10 | 10.0M | lz4/zstd | 1.48 |
| 5 | 8 | 20 | 10.0M | lz4/zstd | 1.82 |

---

## Recommendations

Based on benchmark results, the recommended configuration is:

```bash
python -m meds_etl.omop_streaming \
  --omop_dir INPUT_DIR \
  --output_dir OUTPUT_DIR \
  --config CONFIG.json \
  --workers 16 \
  --shards 20 \
  --chunk_rows 10000000 \
  --run_compression lz4 \
  --final_compression zstd
```

---

*Note: Performance may vary based on dataset size, system hardware, and available memory.*
