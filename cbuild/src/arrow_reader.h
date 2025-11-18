#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <memory>
#include <string>

#include "event_row.h"
#include "logger.h"

namespace etl {

/**
 * Read EventRow batches from Arrow IPC files.
 * 
 * Provides much faster reading than CSV parsing.
 */
class ArrowReader {
public:
    /**
     * Open an Arrow IPC file for reading.
     * 
     * @param path Input file path
     */
    explicit ArrowReader(const std::string& path) 
        : path_(path), current_batch_idx_(0), current_row_idx_(0) {
        
        // Open file for reading
        auto maybe_file = arrow::io::ReadableFile::Open(path);
        if (!maybe_file.ok()) {
            throw std::runtime_error("Failed to open Arrow file for reading: " + path +
                                   " - " + maybe_file.status().ToString());
        }
        file_ = *maybe_file;
        
        // Open IPC reader
        auto maybe_reader = arrow::ipc::RecordBatchFileReader::Open(file_);
        if (!maybe_reader.ok()) {
            throw std::runtime_error("Failed to open Arrow reader: " +
                                   maybe_reader.status().ToString());
        }
        reader_ = *maybe_reader;
        
        num_batches_ = reader_->num_record_batches();
        
        // Load first batch
        if (num_batches_ > 0) {
            load_batch(0);
        }
    }
    
    /**
     * Read the next event from the file.
     * 
     * @param event Output event
     * @return true if event was read, false if EOF
     */
    bool next(EventRow& event) {
        // Check if we've exhausted current batch
        while (current_batch_idx_ < num_batches_) {
            if (current_row_idx_ < current_batch_->num_rows()) {
                // Read row from current batch
                event = read_row(current_row_idx_);
                current_row_idx_++;
                return true;
            }
            
            // Move to next batch
            current_batch_idx_++;
            current_row_idx_ = 0;
            
            if (current_batch_idx_ < num_batches_) {
                load_batch(current_batch_idx_);
            }
        }
        
        return false;  // EOF
    }
    
    const std::string& path() const { return path_; }
    
    ~ArrowReader() {
        if (file_) {
            auto status = file_->Close();
            (void)status;  // Ignore result in destructor
        }
    }
    
    // No copying
    ArrowReader(const ArrowReader&) = delete;
    ArrowReader& operator=(const ArrowReader&) = delete;

private:
    void load_batch(int batch_idx) {
        auto maybe_batch = reader_->ReadRecordBatch(batch_idx);
        if (!maybe_batch.ok()) {
            throw std::runtime_error("Failed to read Arrow batch " + 
                                   std::to_string(batch_idx) + ": " +
                                   maybe_batch.status().ToString());
        }
        current_batch_ = *maybe_batch;
    }
    
    EventRow read_row(int64_t row_idx) {
        EventRow event;
        
        // Extract columns (cast to concrete types)
        auto patient_id_array = std::static_pointer_cast<arrow::StringArray>(
            current_batch_->column(0));
        auto timestamp_start_array = std::static_pointer_cast<arrow::Int64Array>(
            current_batch_->column(1));
        auto timestamp_end_array = std::static_pointer_cast<arrow::Int64Array>(
            current_batch_->column(2));
        auto event_type_array = std::static_pointer_cast<arrow::UInt64Array>(
            current_batch_->column(3));
        auto code_array = std::static_pointer_cast<arrow::StringArray>(
            current_batch_->column(4));
        auto metadata_array = std::static_pointer_cast<arrow::StringArray>(
            current_batch_->column(5));
        
        // Read values
        event.patient_id = patient_id_array->GetString(row_idx);
        event.timestamp_start_min = timestamp_start_array->Value(row_idx);
        event.timestamp_end_min = timestamp_end_array->Value(row_idx);
        event.event_type_index = event_type_array->Value(row_idx);
        event.code = code_array->GetString(row_idx);
        event.metadata_json = metadata_array->GetString(row_idx);
        
        return event;
    }
    
    std::string path_;
    std::shared_ptr<arrow::io::ReadableFile> file_;
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader_;
    std::shared_ptr<arrow::RecordBatch> current_batch_;
    int num_batches_;
    int current_batch_idx_;
    int64_t current_row_idx_;
};

} // namespace etl

