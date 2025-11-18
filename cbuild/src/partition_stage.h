#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <atomic>
#include <thread>
#include <unistd.h>
#include <chrono>
#include <iomanip>

#include "config.h"
#include "event_row.h"
#include "csv_reader.h"
#include "arrow_writer.h"
#include "time_utils.h"
#include "string_utils.h"
#include "file_utils.h"
#include "logger.h"

/**
 * Stage 1: Partition source CSV files into sorted runs, sharded by patient ID.
 * 
 * This stage:
 * 1. Reads each source CSV file
 * 2. Extracts events (patient_id, timestamps, all fields as JSON)
 * 3. Hashes patient_id to assign to a shard
 * 4. Buffers events in memory
 * 5. When buffer is full, sorts and writes to disk as a "run" file
 * 6. Each run file is stored in: tmp/shard=N/run-THREAD_ID-SEQ.csv
 */
namespace etl {

// Global sequence number for unique run file names
static std::atomic<uint64_t> g_run_sequence{0};

/**
 * Parameters for the partition stage.
 */
struct PartitionParams {
    std::string primary_key_column;  // Column name for patient ID
    size_t num_shards;               // Number of shards to partition into
    size_t rows_per_run;             // Max rows to buffer before writing a run
    std::filesystem::path temp_dir;  // Temporary directory for intermediate files
};

/**
 * Write a sorted run of events to disk in Arrow IPC format.
 * 
 * Format: Arrow IPC with schema: patient_id, timestamp_start_min, timestamp_end_min,
 *         event_type_index, code, metadata_json
 */
inline void write_run_file_arrow(const std::filesystem::path& output_path, 
                                  const std::vector<EventRow>& events) {
    file_utils::create_directories(output_path.parent_path());
    
    try {
        ArrowWriter writer(output_path.string());
        writer.write_batch(events);
        writer.close();
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to write Arrow run file: " + 
                               output_path.string() + " - " + e.what());
    }
}

/**
 * Persistent buffers for a worker thread that accumulate rows across CSV files.
 * Buffers are flushed when they reach rows_per_run, minimizing file creation.
 */
class PartitionBuffers {
public:
    explicit PartitionBuffers(size_t num_shards) 
        : shard_buffers_(num_shards) {}
    
    /**
     * Add an event to the appropriate shard buffer.
     * Returns true if the buffer was flushed (reached rows_per_run).
     */
    bool add_event(size_t shard_id, EventRow event, 
                   size_t rows_per_run,
                   const std::filesystem::path& temp_dir) {
        auto& buffer = shard_buffers_[shard_id];
        buffer.push_back(std::move(event));
        
        if (buffer.size() >= rows_per_run) {
            flush_shard(shard_id, temp_dir);
            return true;
        }
        return false;
    }
    
    /**
     * Get total memory usage estimate (in bytes) across all buffers.
     */
    size_t total_memory_usage() const {
        size_t total = 0;
        for (const auto& buffer : shard_buffers_) {
            // Rough estimate: each EventRow is ~250 bytes
            total += buffer.size() * 250;
        }
        return total;
    }
    
    /**
     * Flush all buffers if total memory usage exceeds threshold.
     */
    void check_and_flush_if_needed(size_t max_memory_bytes, 
                                   const std::filesystem::path& temp_dir) {
        if (total_memory_usage() > max_memory_bytes) {
            flush_all(temp_dir);
        }
    }
    
    /**
     * Flush a specific shard buffer if it has data.
     */
    void flush_shard(size_t shard_id, const std::filesystem::path& temp_dir) {
        auto& buffer = shard_buffers_[shard_id];
        if (buffer.empty()) {
            return;
        }
        
        std::sort(buffer.begin(), buffer.end());
        
        // Use thread ID + sequence for unique filenames
        std::ostringstream filename;
        filename << "run-" << std::this_thread::get_id() << "-" 
                 << g_run_sequence.fetch_add(1) << ".arrow";
        
        std::filesystem::path output_path = 
            temp_dir / 
            ("shard=" + std::to_string(shard_id)) /
            filename.str();
        
        write_run_file_arrow(output_path, buffer);
        buffer.clear();
        buffer.shrink_to_fit();
    }
    
    /**
     * Flush all remaining buffers (call at end of worker processing).
     */
    void flush_all(const std::filesystem::path& temp_dir) {
        for (size_t shard_id = 0; shard_id < shard_buffers_.size(); ++shard_id) {
            flush_shard(shard_id, temp_dir);
        }
    }
    
    /**
     * Get the current size of a shard buffer.
     */
    size_t buffer_size(size_t shard_id) const {
        return shard_buffers_[shard_id].size();
    }

private:
    std::vector<std::vector<EventRow>> shard_buffers_;
};

/**
 * Convert a CSV row to a selective JSON payload string.
 * Only includes specified metadata columns, not all columns.
 * 
 * Creates: {"metadata_col1":"val1","metadata_col2":"val2",...}
 */
/**
 * Build compact indexed metadata string.
 * 
 * Instead of: {"provider_id":"123","visit_occurrence_id":"456"}
 * We generate: 1:123,2:456
 * 
 * Format: index:value,index:value,...
 * This significantly reduces metadata size (no JSON escaping overhead).
 * 
 * OPTIMIZED: Avoids ostringstream, avoids string copies, direct char manipulation
 */
inline std::string row_to_indexed_metadata(const std::vector<std::string>& headers,
                                           const std::vector<std::string>& row,
                                           const std::vector<std::string>& metadata_cols,
                                           const Config& config) {
    if (metadata_cols.empty()) {
        return "";  // Empty string if no metadata
    }
    
    std::string result;
    result.reserve(64);  // Pre-allocate typical size
    bool first = true;
    
    for (const auto& col_name : metadata_cols) {
        size_t col_idx = find_column_index(headers, col_name);
        if (col_idx != SIZE_MAX && col_idx < row.size()) {
            size_t col_index = config.get_column_index(col_name);
            if (col_index > 0) {  // 0 means not found
                if (!first) {
                    result += ',';
                }
                
                // Append col_index as string
                result += std::to_string(col_index);
                result += ':';
                
                // Append value, escaping commas/colons in-place
                const std::string& value = row[col_idx];
                for (char c : value) {
                    if (c == ',' || c == ':' || c == '\n' || c == '\r') {
                        result += ' ';  // Replace with space
                    } else {
                        result += c;
                    }
                }
                
                first = false;
            }
        }
    }
    
    return result;
}

/**
 * Process a single CSV file: extract events and partition into shards.
 * 
 * Events are added to persistent buffers that accumulate across CSV files.
 * Buffers are flushed automatically when they reach rows_per_run.
 * 
 * @param table_name Name of the table (e.g., "lab_results")
 * @param table_config Configuration for this table
 * @param csv_file_path Path to the CSV file to process
 * @param params Partition parameters
 * @param config Global configuration (for indexing)
 * @param buffers Persistent buffers that accumulate rows across CSV files
 * @param verbose If true, log detailed timing information
 * @return Number of rows processed from this file
 */
inline uint64_t partition_csv_file(const std::string& table_name,
                                   const TableConfig& table_config,
                                   const std::string& csv_file_path,
                                   const PartitionParams& params,
                                   const Config& config,
                                   PartitionBuffers& buffers,
                                   bool verbose = false) {
    
    auto file_start = std::chrono::steady_clock::now();
    
    LOG_DEBUG("Processing: " + csv_file_path);
    
    // Open CSV file
    CsvReader reader(csv_file_path, true);
    const auto& headers = reader.headers();
    
    // Find required columns
    size_t pk_col_idx = find_column_index(headers, params.primary_key_column);
    if (pk_col_idx == SIZE_MAX) {
        LOG_WARNING("Skipping " + csv_file_path + " (missing primary key column: " + 
                   params.primary_key_column + ")");
        return 0;
    }
    
    size_t start_col_idx = find_column_index(headers, table_config.ts_start_col);
    if (start_col_idx == SIZE_MAX) {
        LOG_WARNING("Skipping " + csv_file_path + " (missing start timestamp column: " + 
                   table_config.ts_start_col + ")");
        return 0;
    }
    
    size_t end_col_idx = SIZE_MAX;
    if (table_config.has_end_timestamp()) {
        end_col_idx = find_column_index(headers, table_config.ts_end_col);
    }
    
    // Find code column (optional)
    size_t code_col_idx = SIZE_MAX;
    if (table_config.has_code()) {
        code_col_idx = find_column_index(headers, table_config.code_col);
    }
    
    // Process each row
    std::vector<std::string> row;
    uint64_t rows_processed = 0;
    uint64_t rows_skipped = 0;
    
    // Profiling counters
    int64_t csv_parse_time_ms = 0;
    int64_t timestamp_parse_time_ms = 0;
    int64_t metadata_build_time_ms = 0;
    int64_t buffer_add_time_ms = 0;
    
    while (true) {
        auto row_start = std::chrono::steady_clock::now();
        if (!reader.next_row(row)) break;
        auto after_csv = std::chrono::steady_clock::now();
        csv_parse_time_ms += std::chrono::duration_cast<std::chrono::milliseconds>(after_csv - row_start).count();
        
        // Extract patient ID
        if (pk_col_idx >= row.size() || row[pk_col_idx].empty()) {
            ++rows_skipped;
            continue;
        }
        const std::string& patient_id = row[pk_col_idx];
        
        // Extract start timestamp
        if (start_col_idx >= row.size()) {
            ++rows_skipped;
            continue;
        }
        auto before_ts = std::chrono::steady_clock::now();
        auto start_ts = time_utils::parse_timestamp_ms(row[start_col_idx]);
        if (!start_ts) {
            ++rows_skipped;
            continue;  // Must have valid start timestamp
        }
        
        // Extract end timestamp (if configured)
        int64_t end_ts = 0;
        if (end_col_idx != SIZE_MAX && end_col_idx < row.size()) {
            auto end_ts_opt = time_utils::parse_timestamp_ms(row[end_col_idx]);
            if (end_ts_opt) {
                end_ts = *end_ts_opt;
            }
        }
        auto after_ts = std::chrono::steady_clock::now();
        timestamp_parse_time_ms += std::chrono::duration_cast<std::chrono::milliseconds>(after_ts - before_ts).count();
        
        // Extract code (if configured)
        std::string code;
        if (code_col_idx != SIZE_MAX && code_col_idx < row.size()) {
            code = row[code_col_idx];
        }
        
        // Create event
        auto before_metadata = std::chrono::steady_clock::now();
        EventRow event;
        event.patient_id = patient_id;
        // Convert milliseconds to minutes for space efficiency
        event.timestamp_start_min = *start_ts / 60000;
        event.timestamp_end_min = end_ts / 60000;
        event.event_type_index = config.get_table_index(table_name);
        event.code = code;
        event.metadata_json = row_to_indexed_metadata(headers, row, table_config.metadata_cols, config);
        auto after_metadata = std::chrono::steady_clock::now();
        metadata_build_time_ms += std::chrono::duration_cast<std::chrono::milliseconds>(after_metadata - before_metadata).count();
        
        // Assign to shard and add to persistent buffer
        // Buffer will auto-flush when it reaches rows_per_run
        auto before_buffer = std::chrono::steady_clock::now();
        size_t shard_id = string_utils::hash_string_64(patient_id) % params.num_shards;
        buffers.add_event(shard_id, std::move(event), params.rows_per_run, params.temp_dir);
        auto after_buffer = std::chrono::steady_clock::now();
        buffer_add_time_ms += std::chrono::duration_cast<std::chrono::milliseconds>(after_buffer - before_buffer).count();
        
        ++rows_processed;
    }
    
    // Note: We do NOT flush buffers here - they persist across CSV files
    // Buffers will be flushed when they reach rows_per_run, or at the end
    // of all processing by the worker thread via flush_all()
    
    auto file_end = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(file_end - file_start).count();
    
    if (verbose) {
        double elapsed_sec = elapsed_ms / 1000.0;
        double rows_per_sec = (elapsed_sec > 0) ? (rows_processed / elapsed_sec) : 0;
        
        int64_t total_profiled = csv_parse_time_ms + timestamp_parse_time_ms + 
                                 metadata_build_time_ms + buffer_add_time_ms;
        
        std::ostringstream msg;
        msg << "  [" << std::this_thread::get_id() << "] " 
            << csv_file_path << "\n"
            << "    Rows: " << rows_processed 
            << " | Total: " << std::fixed << std::setprecision(2) << elapsed_sec << "s"
            << " | Rate: " << std::fixed << std::setprecision(0) << rows_per_sec << " rows/s\n"
            << "    Breakdown: CSV=" << csv_parse_time_ms << "ms"
            << " | Timestamp=" << timestamp_parse_time_ms << "ms"
            << " | Metadata=" << metadata_build_time_ms << "ms"
            << " | Buffer=" << buffer_add_time_ms << "ms"
            << " | Other=" << (elapsed_ms - total_profiled) << "ms";
        LOG_INFO(msg.str());
    } else {
        LOG_DEBUG("Completed: " + csv_file_path + 
                  " (processed: " + std::to_string(rows_processed) + 
                 ", skipped: " + std::to_string(rows_skipped) + ")");
    }
    
    return rows_processed;
}

} // namespace etl

