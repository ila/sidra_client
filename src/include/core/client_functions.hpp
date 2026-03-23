#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Refreshes a materialized view via IVM and returns the refresh timestamp
timestamp_t RefreshMaterializedView(const string &view_name, Connection &con);

// Sends refreshed view results to the server
// Returns the socket fd (caller must close via CloseConnection)
int32_t SendResults(const string &view_name, timestamp_t timestamp, Connection &con, const string &config_path);

// Registers a new client with the server and executes initialization queries
// Returns the socket fd (caller must close via CloseConnection)
int32_t GenerateClientInformation(Connection &con, unordered_map<string, string> &config);

} // namespace duckdb
