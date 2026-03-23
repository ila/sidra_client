#pragma once

#include "duckdb.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

// Establishes a TCP connection to the server using config parameters
// Throws IOException on failure
int32_t ConnectToServer(const unordered_map<string, string> &config);

// Closes the connection by sending a CLOSE_CONNECTION message and shutting down the socket
void CloseConnection(int32_t sock);

// Serializes and sends DataChunks from a query result over the socket
void SendChunks(MaterializedQueryResult &result, int32_t sock);

// Sends raw bytes over a socket, handling partial writes
void SendAll(int32_t sock, const void *data, size_t len);

// Receives raw bytes from a socket, handling partial reads
void RecvAll(int32_t sock, void *data, size_t len);

} // namespace duckdb
