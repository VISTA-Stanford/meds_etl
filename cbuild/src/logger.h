#pragma once

#include <iostream>
#include <sstream>
#include <mutex>
#include <chrono>
#include <iomanip>

/**
 * Thread-safe logging utilities for the ETL pipeline.
 * Provides different log levels and automatic timestamping.
 */
namespace etl {

/**
 * Log levels in increasing order of severity.
 */
enum class LogLevel {
    DEBUG,    // Detailed debugging information
    INFO,     // General informational messages
    WARNING,  // Warning messages (non-critical issues)
    ERROR     // Error messages (critical issues)
};

/**
 * Thread-safe logger with configurable log level.
 */
class Logger {
public:
    /**
     * Get the singleton logger instance.
     */
    static Logger& instance() {
        static Logger logger;
        return logger;
    }
    
    /**
     * Set the minimum log level. Messages below this level are ignored.
     */
    void set_level(LogLevel level) {
        std::lock_guard<std::mutex> lock(mutex_);
        min_level_ = level;
    }
    
    /**
     * Log a message at the specified level.
     */
    void log(LogLevel level, const std::string& message) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (level < min_level_) {
            return;  // Message below minimum level
        }
        
        // Get current timestamp
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        std::tm tm;
        localtime_r(&time_t_now, &tm);
        
        // Format: [YYYY-MM-DD HH:MM:SS] [LEVEL] message
        std::cerr << "[";
        std::cerr << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
        std::cerr << "] [" << level_to_string(level) << "] ";
        std::cerr << message << "\n";
    }
    
    /**
     * Log a debug message.
     */
    void debug(const std::string& message) {
        log(LogLevel::DEBUG, message);
    }
    
    /**
     * Log an info message.
     */
    void info(const std::string& message) {
        log(LogLevel::INFO, message);
    }
    
    /**
     * Log a warning message.
     */
    void warning(const std::string& message) {
        log(LogLevel::WARNING, message);
    }
    
    /**
     * Log an error message.
     */
    void error(const std::string& message) {
        log(LogLevel::ERROR, message);
    }

private:
    Logger() : min_level_(LogLevel::INFO) {}
    
    std::string level_to_string(LogLevel level) const {
        switch (level) {
            case LogLevel::DEBUG:   return "DEBUG";
            case LogLevel::INFO:    return "INFO";
            case LogLevel::WARNING: return "WARN";
            case LogLevel::ERROR:   return "ERROR";
            default:                return "UNKNOWN";
        }
    }
    
    std::mutex mutex_;
    LogLevel min_level_;
};

// Convenience macros for logging
#define LOG_DEBUG(msg) etl::Logger::instance().debug(msg)
#define LOG_INFO(msg) etl::Logger::instance().info(msg)
#define LOG_WARNING(msg) etl::Logger::instance().warning(msg)
#define LOG_ERROR(msg) etl::Logger::instance().error(msg)

} // namespace etl

