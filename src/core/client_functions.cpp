#include "core/client_functions.hpp"
#include "common/common.hpp"
#include "network/network.hpp"
#include "debug.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

timestamp_t RefreshMaterializedView(const string &view_name, Connection &con) {
	SIDRA_CLIENT_DEBUG_PRINT("RefreshMaterializedView: " + view_name);

	auto result = con.Query("PRAGMA ivm('" + view_name + "');");
	if (result->HasError()) {
		throw IOException("Error refreshing materialized view '%s': %s", view_name, result->GetError());
	}

	return Timestamp::GetCurrentTimestamp();
}

int32_t SendResults(const string &view_name, timestamp_t timestamp, Connection &con, const string &config_path) {
	SIDRA_CLIENT_DEBUG_PRINT("SendResults: view=" + view_name);

	string config_file = "client.config";
	auto config = ParseConfig(config_path, config_file);

	auto chunks = con.Query("SELECT * FROM " + KeywordHelper::WriteOptionallyQuoted(view_name) + ";");
	if (chunks->HasError()) {
		throw IOException("Error fetching view data for '%s': %s", view_name, chunks->GetError());
	}
	if (chunks->Collection().Count() == 0) {
		throw IOException("No data to send for view '%s'", view_name);
	}

	int32_t sock = ConnectToServer(config);

	try {
		auto message = ClientMessage::NEW_RESULT;
		SendAll(sock, &message, sizeof(int32_t));

		// Send client id
		auto client = con.Query("SELECT id FROM client_information;");
		if (client->HasError() || client->Collection().Count() == 0) {
			throw IOException("Failed to retrieve client information");
		}
		auto client_id = client->Fetch()->GetValue(0, 0).GetValue<uint64_t>();
		SendAll(sock, &client_id, sizeof(uint64_t));

		// Receive acknowledgment
		int32_t response;
		RecvAll(sock, &response, sizeof(int32_t));
		if (response != static_cast<int32_t>(ClientMessage::OK)) {
			throw IOException("Server rejected client: not initialized (response=%d)", response);
		}

		// Send view name
		auto name_size = view_name.size();
		SendAll(sock, &name_size, sizeof(size_t));
		SendAll(sock, view_name.c_str(), name_size);

		// Update local refresh metadata
		string timestamp_string = Timestamp::ToString(timestamp);
		auto update_result = con.Query("INSERT INTO client_refreshes VALUES ('" + view_name + "', '" +
		                               timestamp_string + "') ON CONFLICT DO UPDATE SET last_result = '" +
		                               timestamp_string + "' WHERE view_name = '" + view_name + "';");
		if (update_result->HasError()) {
			throw IOException("Error updating client refresh metadata: %s", update_result->GetError());
		}

		// Send timestamp
		auto timestamp_size = timestamp_string.size();
		SendAll(sock, &timestamp_size, sizeof(size_t));
		SendAll(sock, timestamp_string.c_str(), timestamp_size);

		// Send data chunks
		SendChunks(*chunks, sock);

		SIDRA_CLIENT_DEBUG_PRINT("SendResults: data sent successfully");
	} catch (...) {
		CloseConnection(sock);
		throw;
	}

	return sock;
}

static void InsertClientRecord(Connection &con, const unordered_map<string, string> &config, uint64_t id,
                               const string &timestamp_string) {
	string table_name = "client_information";
	auto schema_it = config.find("schema_name");
	if (schema_it != config.end() && schema_it->second != "main") {
		table_name = schema_it->second + ".client_information";
	}
	string query =
	    "INSERT OR IGNORE INTO " + table_name + " VALUES (" + to_string(id) + ", '" + timestamp_string + "', NULL);";
	auto result = con.Query(query);
	if (result->HasError()) {
		throw IOException("Error inserting client information: %s", result->GetError());
	}
}

int32_t GenerateClientInformation(Connection &con, unordered_map<string, string> &config) {
	SIDRA_CLIENT_DEBUG_PRINT("GenerateClientInformation: starting");

	uint64_t id;
	string timestamp_string;

	auto result = con.Query("SELECT * FROM client_information;");
	if (result->HasError()) {
		throw IOException("Error querying client_information: %s", result->GetError());
	}

	if (result->RowCount() > 0) {
		// Client already registered (e.g., after a failed previous connection)
		id = result->GetValue(0, 0).GetValue<uint64_t>();
		timestamp_string = result->GetValue(1, 0).ToString();
		SIDRA_CLIENT_DEBUG_PRINT("GenerateClientInformation: existing client id=" + to_string(id));
	} else {
		RandomEngine engine;
		id = engine.NextRandomInteger64();
		timestamp_t timestamp = Timestamp::GetCurrentTimestamp();
		timestamp_string = Timestamp::ToString(timestamp);
		InsertClientRecord(con, config, id, timestamp_string);
		SIDRA_CLIENT_DEBUG_PRINT("GenerateClientInformation: new client id=" + to_string(id));
	}

	int32_t sock = ConnectToServer(config);

	try {
		auto message = ClientMessage::NEW_CLIENT;
		auto timestamp_size = timestamp_string.size();

		SendAll(sock, &message, sizeof(int32_t));
		SendAll(sock, &id, sizeof(uint64_t));
		SendAll(sock, &timestamp_size, sizeof(size_t));
		SendAll(sock, timestamp_string.c_str(), timestamp_size);

		// Receive initialization queries from server
		size_t size;
		RecvAll(sock, &size, sizeof(size_t));

		vector<char> buffer(size);
		RecvAll(sock, buffer.data(), size);
		string queries(buffer.data(), size);

		auto query_result = con.Query(queries);
		if (query_result->HasError()) {
			Printer::Print("Warning: error executing initialization queries: " + query_result->GetError());
			Printer::Print("Attempting to continue...");
		}

		// Update last_update timestamp
		auto update =
		    "UPDATE client_information SET last_update = '" + timestamp_string + "' WHERE id = " + to_string(id) + ";";
		auto update_result = con.Query(update);
		if (update_result->HasError()) {
			throw IOException("Error updating client information: %s", update_result->GetError());
		}
	} catch (...) {
		CloseConnection(sock);
		throw;
	}

	return sock;
}

} // namespace duckdb
