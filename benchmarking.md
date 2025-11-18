# MEDS ETL Benchmarking Results

## Overview
This document tracks performance benchmarks for the MEDS ETL pipeline across different configurations, data sizes, and hardware setups.

## System Information

### Test Environment 1
- **OS**: 
- **CPU**: 
- **RAM**: 
- **Storage Type**: 
- **Python Version**: 
- **Key Dependencies**: 
- **Date**: 

### Test Environment 2 (Optional)
- **OS**: 
- **CPU**: 
- **RAM**: 
- **Storage Type**: 
- **Python Version**: 
- **Key Dependencies**: 
- **Date**: 

---

## Benchmark Results

### Full Pipeline Performance

| Dataset | Records | File Size | Total Time | Throughput | Peak Memory | Config |
|---------|---------|-----------|------------|------------|-------------|--------|
| Example 1 | | | | | | |
| Example 2 | | | | | | |
| OMOP EHRSHOT | | | | | | |

### Stage-by-Stage Breakdown

#### Data Loading
| Dataset | Format | File Count | Read Time | Records/sec | Notes |
|---------|--------|------------|-----------|-------------|-------|
| | | | | | |
| | | | | | |

#### Transformation/Processing
| Dataset | Operation | Time | Records/sec | Memory Usage | Notes |
|---------|-----------|------|-------------|--------------|-------|
| | Merge | | | | |
| | Partition | | | | |
| | Filter | | | | |

#### Data Writing
| Dataset | Format | File Count | Write Time | Records/sec | Output Size | Notes |
|---------|--------|------------|------------|-------------|-------------|-------|
| | Parquet | | | | | |
| | CSV | | | | | |

### Scalability Tests

#### Varying Data Sizes
| Input Size | Processing Time | Time/GB | Peak Memory | Throughput | Notes |
|------------|----------------|---------|-------------|------------|-------|
| 1 GB | | | | | |
| 10 GB | | | | | |
| 100 GB | | | | | |
| 1 TB | | | | | |

#### Varying Worker Counts
| Workers | Dataset | Processing Time | Speedup | Efficiency | Notes |
|---------|---------|----------------|---------|------------|-------|
| 1 | | | 1.0x | 100% | Baseline |
| 2 | | | | | |
| 4 | | | | | |
| 8 | | | | | |
| 16 | | | | | |

### Configuration Comparisons

#### Python vs C++ Implementation
| Implementation | Dataset | Time | Speedup | Memory | Notes |
|----------------|---------|------|---------|--------|-------|
| Python | | | 1.0x | | Baseline |
| C++ | | | | | |

#### Compression Methods
| Method | Compression Time | Decompression Time | Compressed Size | Ratio | Notes |
|--------|------------------|--------------------|--------------------|-------|-------|
| None | | | | 1.0x | |
| gzip | | | | | |
| snappy | | | | | |
| zstd | | | | | |

---

## Detailed Run Logs

### Run 1: [Date]
**Configuration:**
- Dataset: 
- Command: `...`
- Config file: 

**Results:**
```
[Paste detailed output here]
```

**Notes:**
- 
- 

---

### Run 2: [Date]
**Configuration:**
- Dataset: 
- Command: `...`
- Config file: 

**Results:**
```
[Paste detailed output here]
```

**Notes:**
- 
- 

---

## Performance Analysis

### Bottlenecks Identified
1. 
2. 
3. 

### Optimization Opportunities
1. 
2. 
3. 

### Comparative Insights
- 
- 
- 

---

## Testing Checklist

- [ ] Baseline performance established
- [ ] Scalability tests completed
- [ ] Memory profiling done
- [ ] I/O bottlenecks identified
- [ ] Multi-threading/processing benchmarked
- [ ] Different data formats tested
- [ ] Compression impact measured
- [ ] Edge cases tested (very large files, many small files)
- [ ] Production hardware tested
- [ ] Reproducibility verified

---

## Notes & Observations
- 
- 
- 

---

*Last Updated: [Date]*

