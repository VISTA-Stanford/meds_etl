/**
 * Sharded On-Disk Join ETL Pipeline
 * 
 * This program performs a scalable, multi-threaded ETL (Extract, Transform, Load)
 * process for medical event data. It:
 * 
 * 1. Reads configuration defining tables and their timestamp columns
 * 2. Discovers CSV/CSV.GZ files under a root directory
 * 3. Partitions events by patient ID into shards (for parallelism)
 * 4. Merges events from multiple tables into unified patient timelines
 * 5. Outputs sorted, timestamped event sequences per patient
 * 
 * Key Features:
 * - Handles arbitrarily large datasets (streaming, on-disk merge)
 * - Parallel processing with configurable worker threads
 * - Supports both plain CSV and gzip-compressed files
 * - Deterministic output (stable sorting with tie-breakers)
 * 
 * Build:
 *   g++ -O3 -std=c++17 -pthread -o sharded_join sharded_join_refactored.cpp -lz
 * 
 * Usage:
 *   ./sharded_join --config config.json --root /data --shards 256 --workers 16
 */

#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <parquet/properties.h>

#include "config.h"
#include "event_row.h"
#include "partition_stage.h"
#include "merge_stage.h"
#include "file_utils.h"
#include "logger.h"

namespace fs = std::filesystem;

/**
 * Command-line arguments for the ETL pipeline.
 */
struct Arguments {
    std::string config_file;           // Path to JSON config file
    fs::path data_root;                // Root directory containing table subdirectories
    size_t num_shards = 256;           // Number of shards for partitioning
    fs::path temp_dir = "tmp";         // Temporary directory for intermediate files
    fs::path output_dir = "final";     // Final output directory
    size_t num_workers = std::thread::hardware_concurrency();  // Parallel workers
    size_t rows_per_run = 50000;       // Rows to buffer before writing a run
    size_t max_open_files = 1024;      // Max files to merge at once
    size_t partition_buffer_mb = 500;  // Max memory per partition worker (MB)
    size_t merge_batch_size = 5000;    // Batch size for merge writes (rows)
    std::string parquet_compression = "snappy";  // Parquet compression: none, snappy, gzip, lz4, zstd
    bool verbose = false;              // Enable debug logging
    bool no_gzip = false;              // Skip gzip compression for final output (faster)
    bool profile = false;              // Enable detailed profiling of merge operations
};

/**
 * Print usage information.
 */
void print_usage(const char* program_name) {
    std::cerr << "Usage: " << program_name << " [OPTIONS]\n\n"
              << "Required:\n"
              << "  --config FILE             Path to JSON configuration file\n"
              << "  --root DIR                Root directory containing table subdirectories\n"
              << "\n"
              << "Optional:\n"
              << "  --shards N                Number of shards (default: 256)\n"
              << "  --tmp DIR                 Temporary directory (default: tmp)\n"
              << "  --out DIR                 Output directory (default: final)\n"
              << "  --workers N               Number of worker threads (default: CPU count)\n"
              << "  --rows_per_run N          Rows per run file (default: 50000)\n"
              << "  --max_open N              Max files to merge at once (default: 1024)\n"
              << "  --partition_buffer_mb N   Max memory per partition worker in MB (default: 500)\n"
              << "  --merge_batch_size N      Batch size for merge writes in rows (default: 5000)\n"
              << "  --parquet_compression STR Compression for Parquet: none, snappy, gzip, lz4, zstd (default: snappy)\n"
              << "  --no-gzip                 Skip gzip compression (faster, larger files)\n"
              << "  --profile                 Enable detailed profiling of merge operations\n"
              << "  --verbose                 Enable debug logging\n"
              << "\n"
              << "Memory tuning tips:\n"
              << "  Total partition memory ≈ workers × partition_buffer_mb\n"
              << "  Total merge memory ≈ workers × merge_batch_size × 250 bytes\n"
              << "\n"
              << "Compression comparison (speed vs size):\n"
              << "  none   - Fastest, largest files\n"
              << "  lz4    - Very fast, good compression\n"
              << "  snappy - Fast, good compression (default)\n"
              << "  gzip   - Slower, better compression\n"
              << "  zstd   - Slowest, best compression\n"
              << "\n"
              << "Example:\n"
              << "  " << program_name << " --config config.json --root /data/omop \\\n"
              << "    --shards 512 --workers 32 --partition_buffer_mb 300 \\\n"
              << "    --merge_batch_size 2000 --parquet_compression lz4 --out /output/meds\n";
}

/**
 * Convert compression string to Parquet compression type.
 */
parquet::Compression::type parse_compression(const std::string& str) {
    std::string lower = str;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    
    if (lower == "none" || lower == "uncompressed") {
        return parquet::Compression::UNCOMPRESSED;
    } else if (lower == "snappy") {
        return parquet::Compression::SNAPPY;
    } else if (lower == "gzip" || lower == "gz") {
        return parquet::Compression::GZIP;
    } else if (lower == "lz4") {
        return parquet::Compression::LZ4;
    } else if (lower == "zstd") {
        return parquet::Compression::ZSTD;
    } else {
        throw std::runtime_error("Unknown compression type: " + str + 
                               " (valid: none, snappy, gzip, lz4, zstd)");
    }
}

/**
 * Parse command-line arguments.
 */
Arguments parse_arguments(int argc, char** argv) {
    Arguments args;
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        auto get_next_arg = [&]() -> std::string {
            if (i + 1 >= argc) {
                throw std::runtime_error("Missing value for argument: " + arg);
            }
            return argv[++i];
        };
        
        if (arg == "--config") {
            args.config_file = get_next_arg();
        } else if (arg == "--root") {
            args.data_root = get_next_arg();
        } else if (arg == "--shards") {
            args.num_shards = std::stoul(get_next_arg());
        } else if (arg == "--tmp") {
            args.temp_dir = get_next_arg();
        } else if (arg == "--out") {
            args.output_dir = get_next_arg();
        } else if (arg == "--workers") {
            args.num_workers = std::stoul(get_next_arg());
        } else if (arg == "--rows_per_run") {
            args.rows_per_run = std::stoul(get_next_arg());
        } else if (arg == "--max_open") {
            args.max_open_files = std::stoul(get_next_arg());
        } else if (arg == "--partition_buffer_mb") {
            args.partition_buffer_mb = std::stoul(get_next_arg());
        } else if (arg == "--merge_batch_size") {
            args.merge_batch_size = std::stoul(get_next_arg());
        } else if (arg == "--parquet_compression") {
            args.parquet_compression = get_next_arg();
        } else if (arg == "--no-gzip") {
            args.no_gzip = true;
        } else if (arg == "--profile") {
            args.profile = true;
        } else if (arg == "--verbose") {
            args.verbose = true;
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            std::exit(0);
        } else {
            throw std::runtime_error("Unknown argument: " + arg);
        }
    }
    
    // Validate required arguments
    if (args.config_file.empty()) {
        throw std::runtime_error("Missing required argument: --config");
    }
    if (args.data_root.empty()) {
        throw std::runtime_error("Missing required argument: --root");
    }
    
    return args;
}

/**
 * A single processing job for Stage 1 (partition).
 */
struct PartitionJob {
    std::string table_name;
    etl::TableConfig table_config;
    std::string csv_file_path;
    size_t source_index;
};

/**
 * Stage 1: Partition all CSV files into sharded, sorted runs.
 */
void run_partition_stage(const etl::Config& config,
                        const Arguments& args) {
    
    LOG_INFO("=== STAGE 1: PARTITIONING ===");
    LOG_INFO("Discovering CSV files under: " + args.data_root.string());
    
    // Discover all CSV files, organized by table
    std::vector<PartitionJob> jobs;
    
    for (const auto& [table_name, table_config] : config.tables()) {
        fs::path table_dir = args.data_root / table_name;
        
        auto csv_files = etl::file_utils::find_csv_files(table_dir);
        
        if (csv_files.empty()) {
            LOG_WARNING("No CSV files found for table: " + table_name + 
                       " (searched: " + table_dir.string() + ")");
            continue;
        }
        
        LOG_INFO("Found " + std::to_string(csv_files.size()) + 
                 " file(s) for table: " + table_name);
        
        for (size_t i = 0; i < csv_files.size(); ++i) {
            jobs.push_back(PartitionJob{
                table_name,
                table_config,
                csv_files[i],
                i
            });
        }
    }
    
    if (jobs.empty()) {
        throw std::runtime_error("No CSV files found to process!");
    }
    
    LOG_INFO("Total files to process: " + std::to_string(jobs.size()));
    LOG_INFO("Worker threads: " + std::to_string(args.num_workers));
    LOG_INFO("Shards: " + std::to_string(args.num_shards));
    
    // Prepare partition parameters
    etl::PartitionParams params;
    params.primary_key_column = config.primary_key();
    params.num_shards = args.num_shards;
    params.rows_per_run = args.rows_per_run;
    params.temp_dir = args.temp_dir;
    
    // Create temp directory
    etl::file_utils::create_directories(args.temp_dir);
    
    // Process files in parallel with progress tracking
    std::atomic<size_t> next_job{0};
    std::atomic<size_t> completed_jobs{0};
    std::atomic<uint64_t> total_rows{0};  // Track total rows processed
    std::vector<std::thread> workers;
    
    auto start_time = std::chrono::steady_clock::now();
    
    for (size_t w = 0; w < args.num_workers; ++w) {
        workers.emplace_back([&]() {
            // Create persistent buffers for this worker thread
            // These accumulate rows across CSV files until rows_per_run is reached
            etl::PartitionBuffers buffers(params.num_shards);
            
            // Memory limit per worker (configurable)
            const size_t MAX_MEMORY_PER_WORKER = args.partition_buffer_mb * 1024 * 1024;
            size_t files_processed_since_last_check = 0;
            
            while (true) {
                size_t job_idx = next_job.fetch_add(1);
                if (job_idx >= jobs.size()) {
                    break;
                }
                
                const auto& job = jobs[job_idx];
                
                try {
                    uint64_t rows = etl::partition_csv_file(
                        job.table_name,
                        job.table_config,
                        job.csv_file_path,
                        params,
                        config,
                        buffers,  // Pass persistent buffers
                        args.verbose  // Pass verbose flag for per-file timing
                    );
                    total_rows.fetch_add(rows);
                    completed_jobs.fetch_add(1);
                    
                    // Periodically check memory usage and flush if needed
                    files_processed_since_last_check++;
                    if (files_processed_since_last_check >= 10) {
                        buffers.check_and_flush_if_needed(MAX_MEMORY_PER_WORKER, params.temp_dir);
                        files_processed_since_last_check = 0;
                    }
                    
                } catch (const std::exception& e) {
                    LOG_ERROR("Failed to process " + job.csv_file_path + ": " + e.what());
                    completed_jobs.fetch_add(1);
                }
            }
            
            // Flush all remaining buffers at the end of worker processing
            buffers.flush_all(params.temp_dir);
        });
    }
    
    // Progress monitoring thread
    std::atomic<bool> monitoring{true};
    std::thread monitor([&]() {
        while (monitoring) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            
            size_t completed = completed_jobs.load();
            if (completed == 0) continue;
            
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
            if (elapsed == 0) continue;  // Avoid division by zero
            
            uint64_t rows = total_rows.load();
            double progress = (100.0 * completed) / jobs.size();
            double rows_per_sec = static_cast<double>(rows) / elapsed;
            
            // Estimate remaining rows based on average rows per file
            double avg_rows_per_file = static_cast<double>(rows) / completed;
            size_t remaining_files = jobs.size() - completed;
            uint64_t estimated_remaining_rows = static_cast<uint64_t>(avg_rows_per_file * remaining_files);
            size_t eta_sec = (rows_per_sec > 0) ? static_cast<size_t>(estimated_remaining_rows / rows_per_sec) : 0;
            
            std::ostringstream msg;
            msg << "Progress: " << completed << "/" << jobs.size() 
                << " (" << std::fixed << std::setprecision(1) << progress << "%) | "
                << "Elapsed: " << (elapsed / 60) << "m " << (elapsed % 60) << "s | "
                << "Rows: " << rows << " | "
                << "Rate: " << std::fixed << std::setprecision(0) << rows_per_sec << " rows/sec | "
                << "ETA: " << (eta_sec / 60) << "m " << (eta_sec % 60) << "s";
            LOG_INFO(msg.str());
            
            // Stop monitoring once all files are processed
            // (workers may still be flushing buffers, but we don't need updates)
            if (completed >= jobs.size()) {
                LOG_INFO("All files processed, flushing buffers...");
                break;
            }
        }
    });
    
    // Wait for all workers to complete
    for (auto& worker : workers) {
        worker.join();
    }
    
    // Stop monitoring
    monitoring = false;
    monitor.join();
    
    // Final summary
    auto end_time = std::chrono::steady_clock::now();
    auto total_sec = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();
    uint64_t final_rows = total_rows.load();
    double final_rows_per_sec = (total_sec > 0) ? (static_cast<double>(final_rows) / total_sec) : 0;
    
    LOG_INFO("Stage 1 completed: " + std::to_string(jobs.size()) + " files, " +
             std::to_string(final_rows) + " rows in " + 
             std::to_string(total_sec) + "s (" + 
             std::to_string(static_cast<size_t>(final_rows_per_sec)) + " rows/sec)");
    
    LOG_INFO("Stage 1 complete: All files partitioned");
}

/**
 * Stage 2: Merge sorted runs within each shard.
 */
void run_merge_stage(const etl::Config& config, const Arguments& args) {
    LOG_INFO("=== STAGE 2: MERGING ===");
    LOG_INFO("Merging " + std::to_string(args.num_shards) + " shards");
    
    // Create output directory
    etl::file_utils::create_directories(args.output_dir);
    
    // Merge shards in parallel
    // Use same number of workers for merge (it's fast with 10 shards)
    size_t merge_workers = std::min(args.num_workers, args.num_shards);
    LOG_INFO("Merge workers: " + std::to_string(merge_workers));
    
    std::atomic<size_t> next_shard{0};
    std::vector<std::thread> workers;
    
    for (size_t w = 0; w < merge_workers; ++w) {
        workers.emplace_back([&]() {
            while (true) {
                size_t shard_id = next_shard.fetch_add(1);
                if (shard_id >= args.num_shards) {
                    break;
                }
                
                try {
                    parquet::Compression::type compression = parse_compression(args.parquet_compression);
                    etl::merge_shard(
                        args.temp_dir,
                        args.output_dir,
                        shard_id,
                        args.max_open_files,
                        config,
                        args.merge_batch_size,
                        compression,
                        args.no_gzip,
                        args.profile
                    );
                } catch (const std::exception& e) {
                    LOG_ERROR("Failed to merge shard " + std::to_string(shard_id) + 
                             ": " + e.what());
                }
            }
        });
    }
    
    // Wait for all workers to complete
    for (auto& worker : workers) {
        worker.join();
    }
    
    LOG_INFO("Stage 2 complete: All shards merged");
}

/**
 * Main entry point.
 */
int main(int argc, char** argv) {
    try {
        // Parse arguments
        Arguments args = parse_arguments(argc, argv);
        
        // Configure logging
        if (args.verbose) {
            etl::Logger::instance().set_level(etl::LogLevel::DEBUG);
        } else {
            etl::Logger::instance().set_level(etl::LogLevel::INFO);
        }
        
        LOG_INFO("=== SHARDED JOIN ETL PIPELINE ===");
        LOG_INFO("Config: " + args.config_file);
        LOG_INFO("Data root: " + args.data_root.string());
        LOG_INFO("Output: " + args.output_dir.string());
        
        // Load configuration
        etl::Config config = etl::Config::load_from_file(args.config_file);
        LOG_INFO("Loaded config: " + std::to_string(config.tables().size()) + " tables");
        LOG_INFO("Primary key column: " + config.primary_key());
        
        // Start total timer
        auto pipeline_start = std::chrono::steady_clock::now();
        
        // Stage 1: Partition
        run_partition_stage(config, args);
        
        // Stage 2: Merge
        run_merge_stage(config, args);
        
        // Calculate total elapsed time
        auto pipeline_end = std::chrono::steady_clock::now();
        auto total_seconds = std::chrono::duration_cast<std::chrono::seconds>(pipeline_end - pipeline_start).count();
        auto minutes = total_seconds / 60;
        auto seconds = total_seconds % 60;
        
        LOG_INFO("=== PIPELINE COMPLETE ===");
        std::ostringstream time_msg;
        time_msg << "Total time: " << total_seconds << "s";
        if (minutes > 0) {
            time_msg << " (" << minutes << "m " << seconds << "s)";
        }
        LOG_INFO(time_msg.str());
        LOG_INFO("Output written to: " + args.output_dir.string());
        
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "FATAL ERROR: " << e.what() << "\n";
        return 1;
    }
}

