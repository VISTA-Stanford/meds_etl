#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <memory>
#include <string>
#include <vector>

#include "event_row.h"
#include "logger.h"

namespace etl {

/**
 * Write EventRows to Parquet format.
 * 
 * Parquet is a columnar format optimized for analytics:
 * - Better compression than CSV.gz (~40% smaller)
 * - Much faster to read downstream
 * - Industry standard
 */
class ParquetWriter {
public:
    /**
     * Open a Parquet file for writing.
     * 
     * @param path Output file path (will add .parquet extension)
     * @param config Optional config for table/column names
     * @param compression Compression codec (NONE, SNAPPY, GZIP, LZ4, ZSTD)
     */
    explicit ParquetWriter(const std::string& path, 
                          const Config* config = nullptr,
                          parquet::Compression::type compression = parquet::Compression::SNAPPY) 
        : path_(path), rows_written_(0), config_(config) {
        
        // Define schema - use native timestamp type for performance
        schema_ = arrow::schema({
            arrow::field("primary_key", arrow::utf8()),
            arrow::field("start_datetime", arrow::timestamp(arrow::TimeUnit::MILLI)),  // Native timestamp
            arrow::field("end_datetime", arrow::timestamp(arrow::TimeUnit::MILLI)),
            arrow::field("event_type", arrow::utf8()),  // Human-readable table names
            arrow::field("code", arrow::utf8()),
            arrow::field("metadata", arrow::utf8())
        });
        
        // Open file for writing
        auto maybe_file = arrow::io::FileOutputStream::Open(path);
        if (!maybe_file.ok()) {
            throw std::runtime_error("Failed to open Parquet file for writing: " + path + 
                                   " - " + maybe_file.status().ToString());
        }
        file_ = *maybe_file;
        
        // Configure Parquet writer
        parquet::WriterProperties::Builder props_builder;
        props_builder.compression(compression);
        props_builder.version(parquet::ParquetVersion::PARQUET_2_6);
        
        auto props = props_builder.build();
        
        // Create Parquet writer
        auto status_writer = parquet::arrow::FileWriter::Open(
            *schema_, 
            arrow::default_memory_pool(), 
            file_,
            props
        );
        
        if (!status_writer.ok()) {
            throw std::runtime_error("Failed to create Parquet writer: " + 
                                   status_writer.status().ToString());
        }
        writer_ = std::move(*status_writer);
    }
    
    /**
     * Write a batch of EventRows to the Parquet file.
     * Converts timestamps to RFC3339 format and table indices to names.
     */
    void write_batch(const std::vector<EventRow>& events) {
        if (events.empty()) {
            return;
        }
        
        // Create Arrow builders
        arrow::StringBuilder primary_key_builder;
        arrow::TimestampBuilder start_datetime_builder(arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
        arrow::TimestampBuilder end_datetime_builder(arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
        arrow::StringBuilder event_type_builder;
        arrow::StringBuilder code_builder;
        arrow::StringBuilder metadata_builder;
        
        // Append data with conversions
        for (const auto& event : events) {
            // Patient ID
            auto status = primary_key_builder.Append(event.patient_id);
            if (!status.ok()) {
                throw std::runtime_error("Parquet append failed: " + status.ToString());
            }
            
            // Start datetime - convert minutes to milliseconds (no string conversion!)
            int64_t start_ms = event.timestamp_start_min * 60000;
            status = start_datetime_builder.Append(start_ms);
            if (!status.ok()) throw std::runtime_error("Parquet append failed");
            
            // End datetime - append null if not present
            if (event.timestamp_end_min > 0) {
                int64_t end_ms = event.timestamp_end_min * 60000;
                status = end_datetime_builder.Append(end_ms);
            } else {
                status = end_datetime_builder.AppendNull();
            }
            if (!status.ok()) throw std::runtime_error("Parquet append failed");
            
            // Event type - convert index to table name
            std::string table_name;
            if (config_ != nullptr) {
                table_name = config_->get_table_name(event.event_type_index);
            } else {
                table_name = std::to_string(event.event_type_index);
            }
            status = event_type_builder.Append(table_name);
            if (!status.ok()) throw std::runtime_error("Parquet append failed");
            
            // Code
            status = code_builder.Append(event.code);
            if (!status.ok()) throw std::runtime_error("Parquet append failed");
            
            // Metadata
            status = metadata_builder.Append(event.metadata_json);
            if (!status.ok()) throw std::runtime_error("Parquet append failed");
        }
        
        // Finish builders to create arrays
        auto maybe_primary_key = primary_key_builder.Finish();
        auto maybe_start_datetime = start_datetime_builder.Finish();
        auto maybe_end_datetime = end_datetime_builder.Finish();
        auto maybe_event_type = event_type_builder.Finish();
        auto maybe_code = code_builder.Finish();
        auto maybe_metadata = metadata_builder.Finish();
        
        if (!maybe_primary_key.ok() || !maybe_start_datetime.ok() || 
            !maybe_end_datetime.ok() || !maybe_event_type.ok() ||
            !maybe_code.ok() || !maybe_metadata.ok()) {
            throw std::runtime_error("Failed to finish Parquet builders");
        }
        
        // Create record batch
        auto batch = arrow::RecordBatch::Make(
            schema_,
            events.size(),
            {
                *maybe_primary_key,
                *maybe_start_datetime,
                *maybe_end_datetime,
                *maybe_event_type,
                *maybe_code,
                *maybe_metadata
            }
        );
        
        // Write batch to Parquet
        auto status = writer_->WriteRecordBatch(*batch);
        if (!status.ok()) {
            throw std::runtime_error("Failed to write Parquet batch: " + status.ToString());
        }
        
        rows_written_ += events.size();
    }
    
    /**
     * Close the Parquet file and finalize.
     */
    void close() {
        if (writer_) {
            auto status = writer_->Close();
            if (!status.ok()) {
                LOG_WARNING("Failed to close Parquet writer: " + status.ToString());
            }
            writer_.reset();
        }
        
        if (file_) {
            auto status = file_->Close();
            if (!status.ok()) {
                LOG_WARNING("Failed to close Parquet file: " + status.ToString());
            }
            file_.reset();
        }
        
        LOG_DEBUG("Wrote " + std::to_string(rows_written_) + " rows to Parquet: " + path_);
    }
    
    ~ParquetWriter() {
        close();
    }
    
    // No copying
    ParquetWriter(const ParquetWriter&) = delete;
    ParquetWriter& operator=(const ParquetWriter&) = delete;

private:
    std::string path_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::io::FileOutputStream> file_;
    std::unique_ptr<parquet::arrow::FileWriter> writer_;
    uint64_t rows_written_;
    const Config* config_;
};

} // namespace etl

