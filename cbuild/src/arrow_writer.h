#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <memory>
#include <string>
#include <vector>

#include "event_row.h"
#include "logger.h"

namespace etl {

/**
 * Write EventRow batches to Arrow IPC format.
 * 
 * Arrow IPC (Feather v2) is a fast, memory-mappable binary format
 * optimized for inter-process communication. Much faster than CSV.
 */
class ArrowWriter {
public:
    /**
     * Open an Arrow IPC file for writing.
     * 
     * @param path Output file path
     */
    explicit ArrowWriter(const std::string& path) : path_(path), rows_written_(0) {
        // Define schema for EventRow
        schema_ = arrow::schema({
            arrow::field("patient_id", arrow::utf8()),
            arrow::field("timestamp_start_min", arrow::int64()),
            arrow::field("timestamp_end_min", arrow::int64()),
            arrow::field("event_type_index", arrow::uint64()),
            arrow::field("code", arrow::utf8()),
            arrow::field("metadata_json", arrow::utf8())
        });
        
        // Open file for writing
        auto maybe_file = arrow::io::FileOutputStream::Open(path);
        if (!maybe_file.ok()) {
            throw std::runtime_error("Failed to open Arrow file for writing: " + path + 
                                   " - " + maybe_file.status().ToString());
        }
        file_ = *maybe_file;
        
        // Create IPC writer with LZ4 compression for space savings
        // LZ4 is very fast (~500 MB/s compression) with good compression ratio
        auto ipc_options = arrow::ipc::IpcWriteOptions::Defaults();
        ipc_options.codec = arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME).ValueOrDie();
        
        auto maybe_writer = arrow::ipc::MakeFileWriter(file_, schema_, ipc_options);
        if (!maybe_writer.ok()) {
            throw std::runtime_error("Failed to create Arrow writer: " + 
                                   maybe_writer.status().ToString());
        }
        writer_ = *maybe_writer;
    }
    
    /**
     * Write a batch of EventRows to the file.
     * 
     * @param events Vector of events to write
     */
    void write_batch(const std::vector<EventRow>& events) {
        if (events.empty()) {
            return;
        }
        
        // Create Arrow builders
        arrow::StringBuilder patient_id_builder;
        arrow::Int64Builder timestamp_start_builder;
        arrow::Int64Builder timestamp_end_builder;
        arrow::UInt64Builder event_type_builder;
        arrow::StringBuilder code_builder;
        arrow::StringBuilder metadata_builder;
        
        // Append data
        for (const auto& event : events) {
            auto status = patient_id_builder.Append(event.patient_id);
            if (!status.ok()) {
                throw std::runtime_error("Arrow append failed: " + status.ToString());
            }
            
            status = timestamp_start_builder.Append(event.timestamp_start_min);
            if (!status.ok()) throw std::runtime_error("Arrow append failed");
            
            status = timestamp_end_builder.Append(event.timestamp_end_min);
            if (!status.ok()) throw std::runtime_error("Arrow append failed");
            
            status = event_type_builder.Append(event.event_type_index);
            if (!status.ok()) throw std::runtime_error("Arrow append failed");
            
            status = code_builder.Append(event.code);
            if (!status.ok()) throw std::runtime_error("Arrow append failed");
            
            status = metadata_builder.Append(event.metadata_json);
            if (!status.ok()) throw std::runtime_error("Arrow append failed");
        }
        
        // Finish builders to create arrays
        auto maybe_patient_id = patient_id_builder.Finish();
        auto maybe_timestamp_start = timestamp_start_builder.Finish();
        auto maybe_timestamp_end = timestamp_end_builder.Finish();
        auto maybe_event_type = event_type_builder.Finish();
        auto maybe_code = code_builder.Finish();
        auto maybe_metadata = metadata_builder.Finish();
        
        if (!maybe_patient_id.ok() || !maybe_timestamp_start.ok() || 
            !maybe_timestamp_end.ok() || !maybe_event_type.ok() ||
            !maybe_code.ok() || !maybe_metadata.ok()) {
            throw std::runtime_error("Failed to finish Arrow builders");
        }
        
        // Create record batch
        auto batch = arrow::RecordBatch::Make(
            schema_,
            events.size(),
            {
                *maybe_patient_id,
                *maybe_timestamp_start,
                *maybe_timestamp_end,
                *maybe_event_type,
                *maybe_code,
                *maybe_metadata
            }
        );
        
        // Write batch
        auto status = writer_->WriteRecordBatch(*batch);
        if (!status.ok()) {
            throw std::runtime_error("Failed to write Arrow batch: " + status.ToString());
        }
        
        rows_written_ += events.size();
    }
    
    /**
     * Close the file and finalize.
     */
    void close() {
        if (writer_) {
            auto status = writer_->Close();
            if (!status.ok()) {
                LOG_WARNING("Failed to close Arrow writer: " + status.ToString());
            }
            writer_.reset();
        }
        
        if (file_) {
            auto status = file_->Close();
            if (!status.ok()) {
                LOG_WARNING("Failed to close Arrow file: " + status.ToString());
            }
            file_.reset();
        }
        
        LOG_DEBUG("Wrote " + std::to_string(rows_written_) + " rows to " + path_);
    }
    
    ~ArrowWriter() {
        close();
    }
    
    // No copying
    ArrowWriter(const ArrowWriter&) = delete;
    ArrowWriter& operator=(const ArrowWriter&) = delete;

private:
    std::string path_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::io::FileOutputStream> file_;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer_;
    uint64_t rows_written_;
};

} // namespace etl

