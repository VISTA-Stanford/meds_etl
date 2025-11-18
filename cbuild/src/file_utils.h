#pragma once

#include <filesystem>
#include <vector>
#include <string>

#include "string_utils.h"

namespace fs = std::filesystem;

/**
 * File system utilities for discovering and managing data files.
 */
namespace etl {
namespace file_utils {

/**
 * Recursively find all CSV and CSV.GZ files under a directory.
 * 
 * @param root_dir Directory to search
 * @return Vector of absolute file paths
 */
inline std::vector<std::string> find_csv_files(const fs::path& root_dir) {
    std::vector<std::string> files;
    
    if (!fs::exists(root_dir)) {
        return files;
    }
    
    // Recursively iterate through directory
    for (const auto& entry : fs::recursive_directory_iterator(
            root_dir, 
            fs::directory_options::follow_directory_symlink)) {
        
        if (!entry.is_regular_file()) {
            continue;
        }
        
        std::string path = entry.path().string();
        
        // Check if it's a CSV file (plain or gzipped)
        if (string_utils::ends_with(path, ".csv") || 
            string_utils::ends_with(path, ".csv.gz")) {
            files.push_back(std::move(path));
        }
    }
    
    return files;
}

/**
 * Create a directory and all parent directories if they don't exist.
 * Similar to `mkdir -p`.
 */
inline void create_directories(const fs::path& path) {
    fs::create_directories(path);
}

/**
 * Remove a directory and all its contents.
 * Similar to `rm -rf`.
 */
inline void remove_directory(const fs::path& path) {
    std::error_code ec;
    fs::remove_all(path, ec);
    // Ignore errors - directory might not exist
}

} // namespace file_utils
} // namespace etl

