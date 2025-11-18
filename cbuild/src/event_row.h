#pragma once

#include <string>
#include <cstdint>

/**
 * Core data structure representing a single event in the ETL pipeline.
 */
namespace etl {

/**
 * EventRow represents a single patient event extracted from source data.
 * 
 * This is the fundamental unit of data that flows through the pipeline:
 * 1. Extracted from source CSV files
 * 2. Partitioned into shards by patient ID
 * 3. Sorted within each shard
 * 4. Merged across tables
 * 5. Written to final output
 */
struct EventRow {
    // Patient/subject identifier (e.g., "PATIENT_12345")
    std::string patient_id;
    
    // Event start time in minutes since Unix epoch
    int64_t timestamp_start_min = 0;
    
    // Event end time in minutes since Unix epoch (0 if not applicable)
    int64_t timestamp_end_min = 0;
    
    // Event type/table index (numeric index instead of full table name)
    size_t event_type_index = 0;
    
    // Event code/value (e.g., condition code, drug code)
    std::string code;
    
    // JSON payload containing only selected metadata fields
    std::string metadata_json;
    
    /**
     * Comparison operator for sorting events.
     * 
     * Sort order:
     * 1. By patient_id (ascending)
     * 2. By timestamp_start_ms (ascending)
     * 
     * Note: Events with identical patient_id and timestamp will be in undefined order.
     * This is acceptable for most use cases (ML, analytics, etc.)
     */
    bool operator<(const EventRow& other) const {
        if (patient_id != other.patient_id) {
            return patient_id < other.patient_id;
        }
        return timestamp_start_min < other.timestamp_start_min;
    }
    
    /**
     * Equality comparison (for testing).
     */
    bool operator==(const EventRow& other) const {
        return patient_id == other.patient_id &&
               timestamp_start_min == other.timestamp_start_min &&
               timestamp_end_min == other.timestamp_end_min &&
               event_type_index == other.event_type_index &&
               code == other.code &&
               metadata_json == other.metadata_json;
    }
};

} // namespace etl

