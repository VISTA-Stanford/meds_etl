#pragma once

#include <string>
#include <map>
#include <set>
#include <vector>
#include <stdexcept>

#include "json_parser.h"

/**
 * Configuration structures for the ETL pipeline.
 * 
 * The config file defines:
 * - primary_key: The column name for patient/subject ID
 * - tables: Array of table definitions, each with:
 *   - name: Table name (matches directory name in data root)
 *   - ts_start: Column name for event start timestamp
 *   - ts_end: (Optional) Column name for event end timestamp
 */
namespace etl {

/**
 * Configuration for a single table in the ETL.
 */
struct TableConfig {
    std::string name;          // Table name (e.g., "lab_results")
    std::string ts_start_col;  // Start timestamp column (e.g., "result_datetime")
    std::string ts_end_col;    // End timestamp column (optional, empty if not used)
    std::string code_col;      // Code/value column (e.g., "condition_source_value")
    std::vector<std::string> metadata_cols;  // Additional columns to export
    
    TableConfig() = default;
    
    TableConfig(const std::string& name_, 
                const std::string& ts_start_, 
                const std::string& ts_end_ = "",
                const std::string& code_ = "")
        : name(name_), ts_start_col(ts_start_), ts_end_col(ts_end_), code_col(code_) {}
    
    /**
     * Check if this table has an end timestamp column.
     */
    bool has_end_timestamp() const {
        return !ts_end_col.empty();
    }
    
    /**
     * Check if this table has a code column.
     */
    bool has_code() const {
        return !code_col.empty();
    }
};

/**
 * Complete ETL configuration loaded from JSON.
 * 
 * This class also builds a global column index mapping to reduce metadata size.
 * Instead of storing {"provider_id":"123","visit_occurrence_id":"456"}, 
 * we store {"1":"123","2":"456"} where 1 and 2 are column indices.
 */
class Config {
public:
    /**
     * Load configuration from a JSON file.
     * 
     * Expected format:
     * {
     *   "primary_key": "patient_id",
     *   "tables": [
     *     {
     *       "name": "lab_results",
     *       "ts_start": "result_datetime",
     *       "ts_end": ""
     *     },
     *     ...
     *   ]
     * }
     */
    static Config load_from_file(const std::string& config_path) {
        json::Value root = json::parse_file(config_path);
        
        if (root.type != json::Value::OBJECT) {
            throw std::runtime_error("Config: root must be a JSON object");
        }
        
        Config config;
        
        // Parse primary_key (with default)
        if (root.has_key("primary_key") && root["primary_key"].type == json::Value::STRING) {
            config.primary_key_ = root["primary_key"].string;
        } else {
            config.primary_key_ = "primary_key";  // Default
        }
        
        // Parse tables array
        if (!root.has_key("tables")) {
            throw std::runtime_error("Config: 'tables' array is required");
        }
        
        const json::Value& tables_array = root["tables"];
        if (tables_array.type != json::Value::ARRAY) {
            throw std::runtime_error("Config: 'tables' must be an array");
        }
        
        for (const json::Value& table_val : tables_array.array) {
            if (table_val.type != json::Value::OBJECT) {
                continue;  // Skip non-object entries
            }
            
            TableConfig table;
            
            // Extract table name
            if (table_val.has_key("name") && table_val["name"].type == json::Value::STRING) {
                table.name = table_val["name"].string;
            }
            
            // Extract start timestamp column
            if (table_val.has_key("ts_start") && table_val["ts_start"].type == json::Value::STRING) {
                table.ts_start_col = table_val["ts_start"].string;
            }
            
            // Extract end timestamp column (optional)
            if (table_val.has_key("ts_end") && table_val["ts_end"].type == json::Value::STRING) {
                table.ts_end_col = table_val["ts_end"].string;
            }
            
            // Extract code column (optional)
            if (table_val.has_key("code") && table_val["code"].type == json::Value::STRING) {
                table.code_col = table_val["code"].string;
            }
            
            // Extract metadata columns (optional array)
            if (table_val.has_key("metadata") && table_val["metadata"].type == json::Value::ARRAY) {
                for (const auto& col_val : table_val["metadata"].array) {
                    if (col_val.type == json::Value::STRING) {
                        table.metadata_cols.push_back(col_val.string);
                    }
                }
            }
            
            // Validate required fields
            if (table.name.empty() || table.ts_start_col.empty()) {
                throw std::runtime_error(
                    "Config: each table must have 'name' and 'ts_start'"
                );
            }
            
            config.tables_[table.name] = table;
        }
        
        if (config.tables_.empty()) {
            throw std::runtime_error("Config: no valid tables defined");
        }
        
        // Build global column index mapping
        config.build_column_index();
        
        return config;
    }
    
    /**
     * Get the primary key column name (e.g., "patient_id").
     */
    const std::string& primary_key() const {
        return primary_key_;
    }
    
    /**
     * Get configuration for a specific table by name.
     * Throws if table not found.
     */
    const TableConfig& get_table(const std::string& table_name) const {
        auto it = tables_.find(table_name);
        if (it == tables_.end()) {
            throw std::runtime_error("Config: table not found: " + table_name);
        }
        return it->second;
    }
    
    /**
     * Check if a table is configured.
     */
    bool has_table(const std::string& table_name) const {
        return tables_.find(table_name) != tables_.end();
    }
    
    /**
     * Get all configured tables.
     */
    const std::map<std::string, TableConfig>& tables() const {
        return tables_;
    }
    
    /**
     * Get a list of all table names.
     */
    std::vector<std::string> table_names() const {
        std::vector<std::string> names;
        names.reserve(tables_.size());
        for (const auto& pair : tables_) {
            names.push_back(pair.first);
        }
        return names;
    }
    
    /**
     * Get the column index for a given column name.
     * Returns 0 if column not found (0 is reserved for unknown columns).
     */
    size_t get_column_index(const std::string& column_name) const {
        auto it = column_to_index_.find(column_name);
        if (it == column_to_index_.end()) {
            return 0;  // Unknown column
        }
        return it->second;
    }
    
    /**
     * Get the column name for a given index.
     * Returns empty string if index not found.
     */
    std::string get_column_name(size_t index) const {
        auto it = index_to_column_.find(index);
        if (it == index_to_column_.end()) {
            return "";
        }
        return it->second;
    }
    
    /**
     * Get all column mappings (for writing header/legend).
     */
    const std::map<size_t, std::string>& column_index_map() const {
        return index_to_column_;
    }
    
    /**
     * Get the table index for a given table name.
     * Returns 0 if table not found (0 is reserved for unknown tables).
     */
    size_t get_table_index(const std::string& table_name) const {
        auto it = table_to_index_.find(table_name);
        if (it == table_to_index_.end()) {
            return 0;  // Unknown table
        }
        return it->second;
    }
    
    /**
     * Get the table name for a given index.
     * Returns empty string if index not found.
     */
    std::string get_table_name(size_t index) const {
        auto it = index_to_table_.find(index);
        if (it == index_to_table_.end()) {
            return "";
        }
        return it->second;
    }
    
    /**
     * Get all table mappings (for writing header/legend).
     */
    const std::map<size_t, std::string>& table_index_map() const {
        return index_to_table_;
    }

private:
    /**
     * Build global index mappings for tables and columns.
     * This allows us to use numeric indices instead of full names.
     */
    void build_column_index() {
        // Build table index mapping
        size_t table_index = 1;  // 0 is reserved for unknown
        for (const auto& pair : tables_) {
            const std::string& table_name = pair.first;
            table_to_index_[table_name] = table_index;
            index_to_table_[table_index] = table_name;
            ++table_index;
        }
        
        // Collect all unique metadata column names
        std::set<std::string> all_columns;
        for (const auto& pair : tables_) {
            const TableConfig& table = pair.second;
            for (const std::string& col : table.metadata_cols) {
                all_columns.insert(col);
            }
        }
        
        // Assign column indices starting from 1 (0 is reserved for unknown)
        size_t col_index = 1;
        for (const std::string& col : all_columns) {
            column_to_index_[col] = col_index;
            index_to_column_[col_index] = col;
            ++col_index;
        }
    }
    
    std::string primary_key_;
    std::map<std::string, TableConfig> tables_;
    
    // Column index mappings
    std::map<std::string, size_t> column_to_index_;  // column_name -> index
    std::map<size_t, std::string> index_to_column_;  // index -> column_name
    
    // Table index mappings
    std::map<std::string, size_t> table_to_index_;   // table_name -> index
    std::map<size_t, std::string> index_to_table_;   // index -> table_name
};

} // namespace etl

