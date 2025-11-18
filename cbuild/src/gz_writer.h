#pragma once

#include <string>
#include <filesystem>
#include <stdexcept>
#include <zlib.h>

/**
 * Simple gzip writer for compressed CSV output.
 */
namespace etl {

/**
 * RAII wrapper for gzipped file writing.
 * 
 * Usage:
 *   GzWriter writer("output.csv.gz");
 *   writer.write("header1,header2\n");
 *   writer.write("value1,value2\n");
 *   // Automatically closes on destruction
 */
class GzWriter {
public:
    /**
     * Open a gzipped file for writing.
     * 
     * @param path Output file path (should end with .gz)
     * @param compression_level 0-9, where 9 is maximum compression (default: 6)
     */
    explicit GzWriter(const std::string& path, int compression_level = 6) 
        : path_(path), gz_(nullptr) {
        
        // Construct mode string: "wb" + compression level
        std::string mode = "wb" + std::to_string(compression_level);
        
        gz_ = gzopen(path.c_str(), mode.c_str());
        if (!gz_) {
            throw std::runtime_error("Failed to open gzip file for writing: " + path);
        }
    }
    
    /**
     * Close the file (automatically called by destructor).
     */
    ~GzWriter() {
        close();
    }
    
    // Disable copy
    GzWriter(const GzWriter&) = delete;
    GzWriter& operator=(const GzWriter&) = delete;
    
    // Enable move
    GzWriter(GzWriter&& other) noexcept 
        : path_(std::move(other.path_)), gz_(other.gz_) {
        other.gz_ = nullptr;
    }
    
    GzWriter& operator=(GzWriter&& other) noexcept {
        if (this != &other) {
            close();
            path_ = std::move(other.path_);
            gz_ = other.gz_;
            other.gz_ = nullptr;
        }
        return *this;
    }
    
    /**
     * Write a string to the gzipped file.
     * 
     * @param data String to write
     * @throws std::runtime_error if write fails
     */
    void write(const std::string& data) {
        if (!gz_) {
            throw std::runtime_error("Cannot write to closed gzip file: " + path_);
        }
        
        int written = gzwrite(gz_, data.c_str(), static_cast<unsigned>(data.size()));
        if (written <= 0) {
            throw std::runtime_error("Failed to write to gzip file: " + path_);
        }
    }
    
    /**
     * Write a C-string to the gzipped file.
     */
    void write(const char* data, size_t len) {
        if (!gz_) {
            throw std::runtime_error("Cannot write to closed gzip file: " + path_);
        }
        
        int written = gzwrite(gz_, data, static_cast<unsigned>(len));
        if (written <= 0) {
            throw std::runtime_error("Failed to write to gzip file: " + path_);
        }
    }
    
    /**
     * Flush buffered data to disk.
     */
    void flush() {
        if (gz_) {
            gzflush(gz_, Z_SYNC_FLUSH);
        }
    }
    
    /**
     * Close the file.
     */
    void close() {
        if (gz_) {
            gzclose(gz_);
            gz_ = nullptr;
        }
    }
    
    /**
     * Check if the file is open.
     */
    bool is_open() const {
        return gz_ != nullptr;
    }
    
    /**
     * Get the file path.
     */
    const std::string& path() const {
        return path_;
    }

private:
    std::string path_;
    gzFile gz_;
};

} // namespace etl

