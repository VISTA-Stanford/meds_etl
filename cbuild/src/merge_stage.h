#pragma once

#include <string>
#include <vector>
#include <queue>
#include <memory>
#include <fstream>
#include <sstream>
#include <filesystem>

#include "event_row.h"
#include "arrow_reader.h"
#include "parquet_writer.h"
#include "time_utils.h"
#include "file_utils.h"
#include "logger.h"

/**
 * Stage 2: Merge sorted runs within each shard to produce final output in Parquet format.
 */
namespace etl {

// Global sequence for intermediate merge files
static std::atomic<uint64_t> g_merge_sequence{0};

/**
 * Reader for Arrow IPC run files produced by the partition stage.
 */
class RunFileReader {
public:
    explicit RunFileReader(const std::string& path) 
        : reader_(path), path_(path) {}
    
    bool next(EventRow& event) {
        return reader_.next(event);
    }
    
    const std::string& path() const { return path_; }

private:
    ArrowReader reader_;
    std::string path_;
};

/**
 * Heap item for k-way merge.
 */
struct HeapItem {
    EventRow event;
    size_t reader_index;
    
    bool operator<(const HeapItem& other) const {
        return other.event < event;  // Min-heap
    }
};

/**
 * Perform k-way merge of Arrow IPC run files and write to Parquet.
 */
inline void merge_run_files_to_parquet(const std::vector<std::string>& input_files,
                                        const std::string& output_file,
                                        const Config& config,
                                        size_t batch_size = 5000,
                                        parquet::Compression::type compression = parquet::Compression::SNAPPY,
                                        bool profile = false) {
    
    if (input_files.empty()) {
        return;
    }
    
    auto profile_start = std::chrono::steady_clock::now();
    int64_t time_opening = 0, time_heap_init = 0, time_reading = 0;
    int64_t time_heap_ops = 0, time_writing = 0;
    
    // Open all input files
    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::unique_ptr<RunFileReader>> readers;
    for (const auto& path : input_files) {
        readers.emplace_back(std::make_unique<RunFileReader>(path));
    }
    time_opening = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    
    // Open Parquet writer with configurable compression
    ParquetWriter parquet_writer(output_file, &config, compression);
    
    // Initialize heap
    auto t1 = std::chrono::steady_clock::now();
    std::priority_queue<HeapItem> heap;
    for (size_t i = 0; i < readers.size(); ++i) {
        EventRow event;
        if (readers[i]->next(event)) {
            heap.push(HeapItem{event, i});
        }
    }
    time_heap_init = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t1).count();
    
    // K-way merge with batch writing
    uint64_t events_written = 0;
    std::vector<EventRow> batch;
    batch.reserve(batch_size);
    
    while (!heap.empty()) {
        auto t_pop = std::chrono::steady_clock::now();
        HeapItem item = heap.top();
        heap.pop();
        time_heap_ops += std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t_pop).count();
        
        batch.push_back(item.event);
        ++events_written;
        
        // Write batch when full
        if (batch.size() >= batch_size) {
            auto t_write = std::chrono::steady_clock::now();
            parquet_writer.write_batch(batch);
            time_writing += std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t_write).count();
            batch.clear();
        }
        
        // Read next event
        auto t_read = std::chrono::steady_clock::now();
        EventRow next_event;
        if (readers[item.reader_index]->next(next_event)) {
            time_reading += std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t_read).count();
            
            auto t_push = std::chrono::steady_clock::now();
            heap.push(HeapItem{next_event, item.reader_index});
            time_heap_ops += std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t_push).count();
        }
    }
    
    // Write remaining batch
    if (!batch.empty()) {
        auto t_write = std::chrono::steady_clock::now();
        parquet_writer.write_batch(batch);
        time_writing += std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t_write).count();
    }
    
    parquet_writer.close();
    
    if (profile) {
        int64_t total = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - profile_start).count();
        
        std::ostringstream msg;
        msg << "  Merge profile for " << input_files.size() << " files (" << events_written << " rows, " << total << "ms):\n"
            << "    Opening files:   " << time_opening << "ms\n"
            << "    Heap init:       " << time_heap_init << "ms\n"
            << "    Reading Arrow:   " << time_reading << "ms\n"
            << "    Heap ops:        " << time_heap_ops << "ms\n"
            << "    Writing Parquet: " << time_writing << "ms\n"
            << "    Other:           " << (total - time_opening - time_heap_init - time_reading - time_heap_ops - time_writing) << "ms";
        LOG_INFO(msg.str());
    }
}

/**
 * Merge all runs for a single shard to produce final Parquet output.
 */
inline void merge_shard(const std::filesystem::path& temp_root,
                       const std::filesystem::path& output_root,
                       size_t shard_id,
                       size_t max_open,
                       const Config& config,
                       size_t batch_size = 5000,
                       parquet::Compression::type compression = parquet::Compression::SNAPPY,
                       bool /* no_gzip unused */ = false,
                       bool profile = false) {
    
    namespace fs = std::filesystem;
    
    fs::path shard_dir = temp_root / ("shard=" + std::to_string(shard_id));
    if (!fs::exists(shard_dir)) {
        return;
    }
    
    // Find all Arrow run files
    std::vector<std::string> run_files;
    for (const auto& entry : fs::directory_iterator(shard_dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".arrow") {
            run_files.push_back(entry.path().string());
        }
    }
    
    if (run_files.empty()) {
        LOG_WARNING("No run files found for shard " + std::to_string(shard_id));
        return;
    }
    
    LOG_INFO("Merging shard " + std::to_string(shard_id) + 
             " (" + std::to_string(run_files.size()) + " run files)");
    
    // Multi-level merge if too many files
    // (Note: For intermediate merges, we'd write Arrow; for now simplified)
    if (run_files.size() > max_open) {
        LOG_WARNING("Shard " + std::to_string(shard_id) + " has " + 
                   std::to_string(run_files.size()) + " files (max " + 
                   std::to_string(max_open) + "). Consider increasing max_open or rows_per_run.");
    }
    
    // Final merge to Parquet
    fs::path in_progress_dir = output_root / ("shard=" + std::to_string(shard_id) + ".inprogress");
    std::error_code ec;
    fs::remove_all(in_progress_dir, ec);
    fs::create_directories(in_progress_dir);
    
    fs::path final_output = in_progress_dir / "part-0000.parquet";
    merge_run_files_to_parquet(run_files, final_output.string(), config, batch_size, compression, profile);
    
    // Atomically rename
    fs::path final_dir = output_root / ("shard=" + std::to_string(shard_id));
    fs::remove_all(final_dir, ec);
    fs::rename(in_progress_dir, final_dir);
    
    LOG_INFO("Completed shard " + std::to_string(shard_id));
}

} // namespace etl
