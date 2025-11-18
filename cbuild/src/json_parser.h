#pragma once

#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <stdexcept>
#include <sstream>

/**
 * Minimal JSON parser for configuration files.
 * Supports: objects, arrays, strings, numbers, booleans, null.
 * 
 * This is intentionally minimal - for production use, consider nlohmann/json.
 */
namespace etl {
namespace json {

/**
 * JSON value type - represents any JSON data structure.
 */
struct Value {
    enum Type { OBJECT, ARRAY, STRING, NUMBER, BOOLEAN, NULL_VALUE };
    
    Type type = NULL_VALUE;
    
    // Storage for different types
    std::map<std::string, Value> object;
    std::vector<Value> array;
    std::string string;
    double number = 0.0;
    bool boolean = false;
    
    /**
     * Check if this value is an object and has a specific key.
     */
    bool has_key(const std::string& key) const {
        return type == OBJECT && object.find(key) != object.end();
    }
    
    /**
     * Get a value by key (for objects). Throws if not an object or key missing.
     */
    const Value& operator[](const std::string& key) const {
        if (type != OBJECT) {
            throw std::runtime_error("JSON value is not an object");
        }
        auto it = object.find(key);
        if (it == object.end()) {
            throw std::runtime_error("JSON key not found: " + key);
        }
        return it->second;
    }
    
    /**
     * Get a value by index (for arrays). Throws if not an array or out of bounds.
     */
    const Value& operator[](size_t index) const {
        if (type != ARRAY) {
            throw std::runtime_error("JSON value is not an array");
        }
        if (index >= array.size()) {
            throw std::runtime_error("JSON array index out of bounds");
        }
        return array[index];
    }
};

/**
 * JSON tokenizer/parser.
 */
class Parser {
public:
    explicit Parser(const std::string& json_string) : json_(json_string), pos_(0) {}
    
    /**
     * Parse the JSON string and return the root value.
     */
    Value parse() {
        Value result = parse_value();
        skip_whitespace();
        if (pos_ != json_.size()) {
            throw std::runtime_error("JSON: trailing characters after value");
        }
        return result;
    }

private:
    const std::string& json_;
    size_t pos_;
    
    void skip_whitespace() {
        while (pos_ < json_.size() && 
               (json_[pos_] == ' ' || json_[pos_] == '\n' || 
                json_[pos_] == '\t' || json_[pos_] == '\r')) {
            ++pos_;
        }
    }
    
    bool try_consume(char c) {
        skip_whitespace();
        if (pos_ < json_.size() && json_[pos_] == c) {
            ++pos_;
            return true;
        }
        return false;
    }
    
    std::string parse_string() {
        skip_whitespace();
        if (pos_ >= json_.size() || json_[pos_] != '"') {
            throw std::runtime_error("JSON: expected string");
        }
        ++pos_;
        
        std::string result;
        while (pos_ < json_.size()) {
            char c = json_[pos_++];
            
            if (c == '"') {
                return result;
            }
            
            if (c == '\\') {
                if (pos_ >= json_.size()) {
                    throw std::runtime_error("JSON: incomplete escape sequence");
                }
                char escape = json_[pos_++];
                switch (escape) {
                    case '"':  result.push_back('"'); break;
                    case '\\': result.push_back('\\'); break;
                    case '/':  result.push_back('/'); break;
                    case 'b':  result.push_back('\b'); break;
                    case 'f':  result.push_back('\f'); break;
                    case 'n':  result.push_back('\n'); break;
                    case 'r':  result.push_back('\r'); break;
                    case 't':  result.push_back('\t'); break;
                    case 'u':
                        // Unicode escape - simplified handling
                        if (pos_ + 4 > json_.size()) {
                            throw std::runtime_error("JSON: incomplete \\u escape");
                        }
                        result += "\\u";
                        result.append(json_.substr(pos_, 4));
                        pos_ += 4;
                        break;
                    default:
                        throw std::runtime_error("JSON: invalid escape sequence");
                }
            } else {
                result.push_back(c);
            }
        }
        
        throw std::runtime_error("JSON: unterminated string");
    }
    
    Value parse_value() {
        skip_whitespace();
        if (pos_ >= json_.size()) {
            throw std::runtime_error("JSON: unexpected end of input");
        }
        
        char c = json_[pos_];
        
        // Object
        if (c == '{') {
            return parse_object();
        }
        
        // Array
        if (c == '[') {
            return parse_array();
        }
        
        // String
        if (c == '"') {
            Value v;
            v.type = Value::STRING;
            v.string = parse_string();
            return v;
        }
        
        // Boolean true
        if (json_.compare(pos_, 4, "true") == 0) {
            pos_ += 4;
            Value v;
            v.type = Value::BOOLEAN;
            v.boolean = true;
            return v;
        }
        
        // Boolean false
        if (json_.compare(pos_, 5, "false") == 0) {
            pos_ += 5;
            Value v;
            v.type = Value::BOOLEAN;
            v.boolean = false;
            return v;
        }
        
        // Null
        if (json_.compare(pos_, 4, "null") == 0) {
            pos_ += 4;
            Value v;
            v.type = Value::NULL_VALUE;
            return v;
        }
        
        // Number
        size_t start = pos_;
        if (json_[pos_] == '-') {
            ++pos_;
        }
        while (pos_ < json_.size() && 
               ((json_[pos_] >= '0' && json_[pos_] <= '9') || 
                json_[pos_] == '.' || json_[pos_] == 'e' || 
                json_[pos_] == 'E' || json_[pos_] == '+' || json_[pos_] == '-')) {
            ++pos_;
        }
        
        if (pos_ > start) {
            Value v;
            v.type = Value::NUMBER;
            v.number = std::stod(json_.substr(start, pos_ - start));
            return v;
        }
        
        throw std::runtime_error("JSON: unexpected character");
    }
    
    Value parse_object() {
        Value v;
        v.type = Value::OBJECT;
        
        if (!try_consume('{')) {
            throw std::runtime_error("JSON: expected '{'");
        }
        
        // Empty object
        if (try_consume('}')) {
            return v;
        }
        
        while (true) {
            std::string key = parse_string();
            
            if (!try_consume(':')) {
                throw std::runtime_error("JSON: expected ':'");
            }
            
            Value value = parse_value();
            v.object.emplace(std::move(key), std::move(value));
            
            if (try_consume('}')) {
                break;
            }
            
            if (!try_consume(',')) {
                throw std::runtime_error("JSON: expected ',' or '}'");
            }
        }
        
        return v;
    }
    
    Value parse_array() {
        Value v;
        v.type = Value::ARRAY;
        
        if (!try_consume('[')) {
            throw std::runtime_error("JSON: expected '['");
        }
        
        // Empty array
        if (try_consume(']')) {
            return v;
        }
        
        while (true) {
            v.array.push_back(parse_value());
            
            if (try_consume(']')) {
                break;
            }
            
            if (!try_consume(',')) {
                throw std::runtime_error("JSON: expected ',' or ']'");
            }
        }
        
        return v;
    }
};

/**
 * Parse a JSON file and return the root value.
 */
inline Value parse_file(const std::string& filepath) {
    std::ifstream file(filepath);
    if (!file) {
        throw std::runtime_error("Failed to open JSON file: " + filepath);
    }
    
    std::string content(
        (std::istreambuf_iterator<char>(file)),
        std::istreambuf_iterator<char>()
    );
    
    Parser parser(content);
    return parser.parse();
}

} // namespace json
} // namespace etl

