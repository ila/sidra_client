#include "network/network.hpp"
#include "common/common.hpp"
#include "debug.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace duckdb {

void SendAll(int32_t sock, const void *data, size_t len) {
	auto ptr = static_cast<const char *>(data);
	size_t sent = 0;
	while (sent < len) {
		auto n = send(sock, ptr + sent, len - sent, 0);
		if (n < 0) {
			throw IOException("Failed to send data: %s", strerror(errno));
		}
		sent += static_cast<size_t>(n);
	}
}

void RecvAll(int32_t sock, void *data, size_t len) {
	auto ptr = static_cast<char *>(data);
	size_t received = 0;
	while (received < len) {
		auto n = read(sock, ptr + received, len - received);
		if (n < 0) {
			throw IOException("Failed to receive data: %s", strerror(errno));
		}
		if (n == 0) {
			throw IOException("Connection closed unexpectedly (received %llu of %llu bytes)", (uint64_t)received,
			                  (uint64_t)len);
		}
		received += static_cast<size_t>(n);
	}
}

int32_t ConnectToServer(const unordered_map<string, string> &config) {
	ValidateConfig(config, {"server_addr", "server_port"});

	auto addr_it = config.find("server_addr");
	auto port_it = config.find("server_port");
	auto &server_addr = addr_it->second;
	auto &server_port = port_it->second;

	SIDRA_CLIENT_DEBUG_PRINT("ConnectToServer: " + server_addr + ":" + server_port);

	int32_t sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		throw IOException("Socket creation failed: %s", strerror(errno));
	}

	struct hostent *host = gethostbyname(server_addr.c_str());
	if (!host) {
		close(sock);
		throw IOException("Unknown host: %s", server_addr);
	}

	struct sockaddr_in serv_addr;
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	memcpy(&serv_addr.sin_addr.s_addr, host->h_addr_list[0], host->h_length);
	serv_addr.sin_port = htons(static_cast<uint16_t>(std::stoi(server_port)));

	if (connect(sock, reinterpret_cast<struct sockaddr *>(&serv_addr), sizeof(serv_addr)) < 0) {
		close(sock);
		throw IOException("Connection to %s:%s failed: %s", server_addr, server_port, strerror(errno));
	}

	SIDRA_CLIENT_DEBUG_PRINT("ConnectToServer: connected successfully");
	return sock;
}

void CloseConnection(int32_t sock) {
	SIDRA_CLIENT_DEBUG_PRINT("CloseConnection: closing socket");
	auto message = ClientMessage::CLOSE_CONNECTION;
	// Best-effort send - don't throw if the connection is already broken
	send(sock, &message, sizeof(int32_t), 0);
	shutdown(sock, SHUT_RDWR);
	close(sock);
}

void SendChunks(MaterializedQueryResult &result, int32_t sock) {
	auto &collection = result.Collection();
	idx_t num_chunks = collection.ChunkCount();

	SIDRA_CLIENT_DEBUG_PRINT("SendChunks: sending " + to_string(num_chunks) + " chunks");

	SendAll(sock, &num_chunks, sizeof(idx_t));

	for (auto &chunk : collection.Chunks()) {
		MemoryStream target;
		BinarySerializer serializer(target);
		serializer.Begin();
		chunk.Serialize(serializer);
		serializer.End();

		auto data = target.GetData();
		idx_t len = target.GetPosition();

		SendAll(sock, &len, sizeof(idx_t));
		SendAll(sock, data, len);
	}
}

} // namespace duckdb
