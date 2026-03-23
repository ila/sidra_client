#include "common/common.hpp"
#include "debug.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"

#include <fstream>
#include <sstream>

namespace duckdb {

string ClientMessageToString(ClientMessage msg) {
	switch (msg) {
	case ClientMessage::CLOSE_CONNECTION:
		return "Close connection";
	case ClientMessage::NEW_CLIENT:
		return "New client";
	case ClientMessage::NEW_RESULT:
		return "New result";
	case ClientMessage::NEW_STATISTICS:
		return "New statistics";
	case ClientMessage::NEW_FILE:
		return "New file";
	case ClientMessage::ERROR_NON_EXISTING_CLIENT:
		return "Error non existing client";
	case ClientMessage::OK:
		return "Ok";
	case ClientMessage::UPDATE_TIMESTAMP_CLIENT:
		return "Update timestamp client";
	case ClientMessage::FLUSH:
		return "Flush";
	case ClientMessage::UPDATE_WINDOW:
		return "Update window";
	default:
		return "Unknown message";
	}
}

static string TrimWhitespace(const string &str) {
	auto start = str.find_first_not_of(" \t\r\n");
	if (start == string::npos) {
		return "";
	}
	auto end = str.find_last_not_of(" \t\r\n");
	return str.substr(start, end - start + 1);
}

unordered_map<string, string> ParseConfig(const string &path, const string &config_name) {
	SIDRA_CLIENT_DEBUG_PRINT("ParseConfig: trying " + path + config_name);

	unordered_map<string, string> config;
	std::ifstream config_file(path + config_name);
	if (!config_file.is_open()) {
		config_file.open(config_name);
		if (!config_file.is_open()) {
			throw IOException("Config file not found: %s (searched in '%s' and current directory)", config_name, path);
		}
	}

	string line;
	idx_t line_number = 0;
	while (std::getline(config_file, line)) {
		line_number++;
		line = TrimWhitespace(line);
		if (line.empty() || line[0] == '#') {
			continue;
		}
		auto eq_pos = line.find('=');
		if (eq_pos == string::npos) {
			throw IOException("Invalid config format at line %llu: missing '=' separator", line_number);
		}
		string key = TrimWhitespace(line.substr(0, eq_pos));
		string value = TrimWhitespace(line.substr(eq_pos + 1));
		if (key.empty()) {
			throw IOException("Invalid config format at line %llu: empty key", line_number);
		}
		config[key] = value;
	}

	SIDRA_CLIENT_DEBUG_PRINT("ParseConfig: loaded " + to_string(config.size()) + " entries");
	return config;
}

void ValidateConfig(const unordered_map<string, string> &config, const vector<string> &required_keys) {
	for (auto &key : required_keys) {
		if (config.find(key) == config.end()) {
			throw IOException("Missing required config key: '%s'", key);
		}
	}
}

void ExecuteSQLFile(const string &file_path, Connection &con) {
	SIDRA_CLIENT_DEBUG_PRINT("ExecuteSQLFile: " + file_path);

	std::ifstream input_file(file_path);
	if (!input_file.is_open()) {
		throw IOException("SQL file not found: %s", file_path);
	}

	string line;
	while (std::getline(input_file, line)) {
		line = TrimWhitespace(line);
		if (line.empty() || line[0] == '-') {
			continue;
		}
		auto result = con.Query(line);
		if (result->HasError()) {
			throw IOException("Error executing SQL from '%s': %s\nQuery: %s", file_path, result->GetError(), line);
		}
	}
}

} // namespace duckdb
