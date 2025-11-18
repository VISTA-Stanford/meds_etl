#pragma once

#include <string>
#include <algorithm>

/**
 * String utility functions for the ETL pipeline.
 * These are simple, reusable string operations.
 */
namespace etl {
namespace string_utils {

/**
 * Check if a string ends with a given suffix.
 * Example: ends_with("file.csv.gz", ".gz") returns true
 */
inline bool ends_with(const std::string& str, const std::string& suffix) {
    if (str.size() < suffix.size()) {
        return false;
    }
    return std::equal(suffix.begin(), suffix.end(), str.end() - suffix.size());
}

/**
 * Check if a string contains only digits (0-9).
 * Used to distinguish epoch timestamps from ISO8601 strings.
 */
inline bool is_all_digits(const std::string& str) {
    if (str.empty()) {
        return false;
    }
    for (char c : str) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    return true;
}

/**
 * FNV-1a hash function for 64-bit hashing.
 * Used for consistent patient ID sharding across the pipeline.
 */
inline uint64_t hash_string_64(const std::string& str) {
    constexpr uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
    constexpr uint64_t FNV_PRIME = 1099511628211ULL;
    
    uint64_t hash = FNV_OFFSET_BASIS;
    for (unsigned char c : str) {
        hash ^= c;
        hash *= FNV_PRIME;
    }
    return hash;
}

/**
 * Escape a string for JSON output.
 * Handles: quotes, backslashes, control characters, etc.
 */
inline std::string json_escape(const std::string& str) {
    std::string result;
    result.reserve(str.size() + 8);  // Reserve extra space for escapes
    
    for (unsigned char uc : str) {
        char c = static_cast<char>(uc);
        switch (c) {
            case '\"': result += "\\\""; break;
            case '\\': result += "\\\\"; break;
            case '\b': result += "\\b"; break;
            case '\f': result += "\\f"; break;
            case '\n': result += "\\n"; break;
            case '\r': result += "\\r"; break;
            case '\t': result += "\\t"; break;
            default:
                if (uc < 0x20) {
                    // Control character - encode as \uXXXX
                    char buf[7];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", uc);
                    result += buf;
                } else {
                    result += c;
                }
        }
    }
    return result;
}

} // namespace string_utils
} // namespace etl

