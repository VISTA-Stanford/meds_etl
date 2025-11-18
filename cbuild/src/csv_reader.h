#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <cstring>
#include <memory>
#include <zlib.h>

#include "string_utils.h"

/**
 * CSV file reader with support for both plain and gzip-compressed files.
 * Automatically detects .gz extension and uses appropriate reader.
 */
namespace etl {

/**
 * Buffered gzip reader for high-performance line reading.
 * Uses large block reads instead of line-by-line gzgets() for 5-10x speedup.
 */
class GzLineReader {
public:
    explicit GzLineReader(gzFile gz) : gz_(gz), buffer_pos_(0), buffer_end_(0), eof_(false) {
        buffer_.resize(BUFFER_SIZE);
    }
    
    /**
     * Read the next line from gzipped file.
     * Returns false at EOF, true if a line was read.
     */
    bool read_line(std::string& out) {
        out.clear();
        if (!gz_ || eof_) {
            return false;
        }
        
        while (true) {
            // Search for newline in current buffer
            for (size_t i = buffer_pos_; i < buffer_end_; ++i) {
                if (buffer_[i] == '\n') {
                    // Found newline - extract line
                    out.append(&buffer_[buffer_pos_], i - buffer_pos_);
                    buffer_pos_ = i + 1;
                    
                    // Remove trailing \r if present
                    if (!out.empty() && out.back() == '\r') {
                        out.pop_back();
                    }
                    return true;
                }
            }
            
            // No newline found - append buffer and read more
            out.append(&buffer_[buffer_pos_], buffer_end_ - buffer_pos_);
            
            // Read next block
            int bytes_read = gzread(gz_, buffer_.data(), BUFFER_SIZE);
            if (bytes_read <= 0) {
                eof_ = true;
                // Return true if we accumulated any data (last line without newline)
                return !out.empty();
            }
            
            buffer_pos_ = 0;
            buffer_end_ = static_cast<size_t>(bytes_read);
        }
    }

private:
    static constexpr size_t BUFFER_SIZE = 16 * 1024 * 1024;  // 16MB blocks for fast decompression
    gzFile gz_;
    std::vector<char> buffer_;
    size_t buffer_pos_;
    size_t buffer_end_;
    bool eof_;
};

/**
 * CSV reader that handles both .csv and .csv.gz files.
 * 
 * Usage:
 *   CsvReader reader("data.csv.gz");
 *   std::vector<std::string> row;
 *   while (reader.next_row(row)) {
 *       // Process row
 *   }
 */
class CsvReader {
public:
    /**
     * Open a CSV file (plain or gzipped).
     * 
     * @param path Path to the CSV file
     * @param has_header If true, first row is read as headers
     */
    explicit CsvReader(const std::string& path, bool has_header = true) 
        : is_gzipped_(string_utils::ends_with(path, ".gz")) {
        
        if (is_gzipped_) {
            gz_file_ = gzopen(path.c_str(), "rb");
            if (!gz_file_) {
                throw std::runtime_error("Failed to open gzipped file: " + path);
            }
            // Set large buffer for better decompression performance
            gzbuffer(gz_file_, 16 * 1024 * 1024);
            // Create buffered line reader
            gz_reader_ = std::make_unique<GzLineReader>(gz_file_);
        } else {
            plain_file_.open(path);
            if (!plain_file_) {
                throw std::runtime_error("Failed to open file: " + path);
            }
        }
        
        // Read header row if requested
        if (has_header) {
            std::string line;
            if (!read_line(line)) {
                throw std::runtime_error("Empty CSV file: " + path);
            }
            headers_ = parse_csv_line(line);
        }
    }
    
    ~CsvReader() {
        gz_reader_.reset();  // Destroy reader before closing file
        if (gz_file_) {
            gzclose(gz_file_);
        }
        if (plain_file_.is_open()) {
            plain_file_.close();
        }
    }
    
    // No copying
    CsvReader(const CsvReader&) = delete;
    CsvReader& operator=(const CsvReader&) = delete;
    
    /**
     * Get the header row (column names).
     * Empty if file was opened without headers.
     */
    const std::vector<std::string>& headers() const {
        return headers_;
    }
    
    /**
     * Read the next row from the CSV file.
     * Returns false at EOF.
     * 
     * Note: Reuses the row vector to minimize allocations.
     */
    bool next_row(std::vector<std::string>& row) {
        std::string line;
        if (!read_line(line)) {
            return false;
        }
        parse_csv_line_into(line, row);
        return true;
    }
    
    /**
     * Parse a single CSV line, handling quoted fields.
     * Supports RFC 4180 CSV format:
     * - Fields separated by commas
     * - Fields with commas/newlines must be quoted
     * - Quotes inside quoted fields are escaped as ""
     */
    static std::vector<std::string> parse_csv_line(const std::string& line) {
        std::vector<std::string> fields;
        parse_csv_line_into(line, fields);
        return fields;
    }
    
    /**
     * Parse CSV line into existing vector to reduce allocations.
     * Clears and reuses the fields vector.
     */
    static void parse_csv_line_into(const std::string& line, std::vector<std::string>& fields) {
        fields.clear();
        
        if (line.empty()) {
            fields.push_back("");
            return;
        }
        
        std::string current_field;
        current_field.reserve(64);  // Pre-allocate for typical field size
        bool in_quotes = false;
        
        for (size_t i = 0; i < line.size(); ++i) {
            char c = line[i];
            
            if (in_quotes) {
                if (c == '"') {
                    // Check for escaped quote ("")
                    if (i + 1 < line.size() && line[i + 1] == '"') {
                        current_field.push_back('"');
                        ++i;  // Skip next quote
                    } else {
                        in_quotes = false;
                    }
                } else {
                    current_field.push_back(c);
                }
            } else {
                if (c == ',') {
                    fields.push_back(std::move(current_field));
                    current_field.clear();
                } else if (c == '"') {
                    in_quotes = true;
                } else {
                    current_field.push_back(c);
                }
            }
        }
        
        fields.push_back(std::move(current_field));
    }

private:
    bool read_line(std::string& line) {
        if (is_gzipped_) {
            return gz_reader_->read_line(line);
        } else {
            if (!std::getline(plain_file_, line)) {
                return false;
            }
            // Remove trailing \r if present
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            return true;
        }
    }
    
    bool is_gzipped_;
    std::ifstream plain_file_;
    gzFile gz_file_ = nullptr;
    std::unique_ptr<GzLineReader> gz_reader_;
    std::vector<std::string> headers_;
};

/**
 * Find the index of a column by name in the header row.
 * Returns SIZE_MAX if not found.
 */
inline size_t find_column_index(const std::vector<std::string>& headers, 
                                 const std::string& column_name) {
    for (size_t i = 0; i < headers.size(); ++i) {
        if (headers[i] == column_name) {
            return i;
        }
    }
    return SIZE_MAX;
}

/**
 * Escape a string for CSV output (RFC 4180).
 * 
 * Rules:
 * - If field contains comma, quote, or newline: wrap in quotes
 * - If field contains quotes: double them and wrap in quotes
 * - Otherwise: output as-is
 * 
 * Examples:
 *   "hello" -> hello
 *   "hello,world" -> "hello,world"
 *   "say \"hi\"" -> "say ""hi"""
 */
inline std::string escape_csv_field(const std::string& field) {
    bool needs_quoting = false;
    
    // Check if field needs quoting
    for (char c : field) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            needs_quoting = true;
            break;
        }
    }
    
    if (!needs_quoting) {
        return field;
    }
    
    // Build quoted field with escaped quotes
    std::string result = "\"";
    for (char c : field) {
        if (c == '"') {
            result += "\"\"";  // Double the quote
        } else {
            result += c;
        }
    }
    result += "\"";
    
    return result;
}

} // namespace etl

