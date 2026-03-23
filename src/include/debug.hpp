#pragma once

// Set to 1 to enable debug output for tracing code paths
#define SIDRA_CLIENT_DEBUG 0

#if SIDRA_CLIENT_DEBUG
#include "duckdb/common/printer.hpp"
#define SIDRA_CLIENT_DEBUG_PRINT(x) duckdb::Printer::Print("[SIDRA_CLIENT] " + std::string(x))
#else
#define SIDRA_CLIENT_DEBUG_PRINT(x) ((void)0)
#endif
