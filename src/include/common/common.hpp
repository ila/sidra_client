#pragma once

#include "duckdb.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

// Protocol message types for client-server communication
enum class ClientMessage : int32_t {
	CLOSE_CONNECTION = 0,
	NEW_CLIENT = 1,
	NEW_RESULT = 2,
	NEW_STATISTICS = 3,
	NEW_FILE = 4,
	ERROR_NON_EXISTING_CLIENT = 5,
	OK = 6,
	UPDATE_TIMESTAMP_CLIENT = 7,
	FLUSH = 8,
	UPDATE_WINDOW = 9,
};

string ClientMessageToString(ClientMessage msg);

// Config file parsing
unordered_map<string, string> ParseConfig(const string &path, const string &config_name);

// Validate that required config keys are present
void ValidateConfig(const unordered_map<string, string> &config, const vector<string> &required_keys);

// Execute SQL statements from a file, one per line
void ExecuteSQLFile(const string &file_path, Connection &con);

} // namespace duckdb
