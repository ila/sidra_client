#define DUCKDB_EXTENSION_MAIN

#include "sidra_client_extension.hpp"
#include "core/client_functions.hpp"
#include "common/common.hpp"
#include "network/network.hpp"
#include "debug.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

static string GetConfigPath(ClientContext &context) {
	Value config_path_value;
	context.TryGetCurrentSetting("sidra_client_config_path", config_path_value);
	if (config_path_value.IsNull()) {
		return "./";
	}
	return StringValue::Get(config_path_value);
}

static void RefreshPragma(ClientContext &context, const FunctionParameters &parameters) {
	SIDRA_CLIENT_DEBUG_PRINT("RefreshPragma: called");

	auto view_name = StringValue::Get(parameters.values[0]);
	auto con = Connection(*context.db);
	auto timestamp = RefreshMaterializedView(view_name, con);

	auto config_path = GetConfigPath(context);
	auto sock = SendResults(view_name, timestamp, con, config_path);
	CloseConnection(sock);
}

static void InitializeClientPragma(ClientContext &context, const FunctionParameters &parameters) {
	SIDRA_CLIENT_DEBUG_PRINT("InitializeClientPragma: called");

	auto config_path = GetConfigPath(context);
	string config_file = "client.config";
	auto config = ParseConfig(config_path, config_file);

	ValidateConfig(config, {"db_name"});
	DuckDB db(config["db_name"]);
	Connection con(db);

	auto tables_path = config_path + "tables.sql";
	ExecuteSQLFile(tables_path, con);

	auto sock = GenerateClientInformation(con, config);
	CloseConnection(sock);
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &db_config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	db_config.AddExtensionOption("sidra_client_config_path", "Path to sidra client configuration directory",
	                             LogicalType::VARCHAR, Value("./"));

	auto initialize_client = PragmaFunction::PragmaCall("initialize_client", InitializeClientPragma, {});
	loader.RegisterFunction(initialize_client);

	auto refresh = PragmaFunction::PragmaCall("refresh", RefreshPragma, {LogicalType::VARCHAR});
	loader.RegisterFunction(refresh);
}

void SidraClientExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string SidraClientExtension::Name() {
	return "sidra_client";
}

std::string SidraClientExtension::Version() const {
#ifdef EXT_VERSION_SIDRA_CLIENT
	return EXT_VERSION_SIDRA_CLIENT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(sidra_client, loader) {
	duckdb::LoadInternal(loader);
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
