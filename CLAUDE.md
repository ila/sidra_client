# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Standalone DuckDB extension (`sidra_client`) providing the client-side component of the SIDRA decentralized data architecture. Clients initialize with a remote server, refresh local materialized views via IVM (OpenIVM), and send aggregated results over TCP. Originally ported from `duckdb/extension/client` on the `sidra-dev` branch; now targets DuckDB v1.5 as standalone.

Sister extensions for reference: `pac`, `lpts`, `openivm` at `/home/ila/Code/`.

## Build & test

```bash
GEN=ninja make                    # release build (preferred, avoid debug)
make format-fix                   # MUST run before every commit
make test                         # run SQL logic tests in test/sql/
```

Always run `make format-fix` before committing. If a test breaks, fix the bug — never delete the test.

## Code style

Follows DuckDB's `.clang-tidy` and `.clang-format` (LLVM-based, 120 col, tabs):

| Element | Convention |
|---------|-----------|
| Classes, functions | `CamelCase` |
| Variables, parameters, members | `lower_case` |
| Macros, static constants, enum values | `UPPER_CASE` |
| Typedefs | `lower_case_t` |
| Namespaces | `lower_case` |

All code lives in the `duckdb` namespace.

## Source layout

```
src/
  core/
    client_extension.cpp    # Extension entry point, pragma registration (LoadInternal)
    client_functions.cpp    # RefreshMaterializedView, SendResults, GenerateClientInformation
  common/
    common.cpp              # ParseConfig, ExecuteSQLFile, ClientMessage enum
  network/
    network.cpp             # ConnectToServer, CloseConnection, SendAll/RecvAll, SendChunks
  include/
    sidra_client_extension.hpp   # SidraClientExtension class (must match extension name)
    debug.hpp                    # SIDRA_CLIENT_DEBUG toggle macro
    core/client_functions.hpp
    common/common.hpp
    network/network.hpp
```

New source files must be added to `EXTENSION_SOURCES` in `CMakeLists.txt`.

## Extension entry point pattern (DuckDB v1.5)

```cpp
static void LoadInternal(ExtensionLoader &loader) {
    auto &db_config = DBConfig::GetConfig(loader.GetDatabaseInstance());
    db_config.AddExtensionOption(...);  // register settings
    loader.RegisterFunction(...);       // register pragmas/functions
}

DUCKDB_CPP_EXTENSION_ENTRY(sidra_client, loader) {
    duckdb::LoadInternal(loader);
}
```

Key v1.5 changes: `ExtensionUtil` is removed — use `loader.RegisterFunction()`. Settings are read via `context.TryGetCurrentSetting()`. The extension header filename must be `{extension_name}_extension.hpp`.

## Debug prints

Toggle `SIDRA_CLIENT_DEBUG` to `1` in `src/include/debug.hpp` to enable trace output. Add `SIDRA_CLIENT_DEBUG_PRINT("message")` at key code paths.

## Dependencies

- If LPTS is needed, add it as a **git submodule** — do not vendor/copy source files.
- The word "rdda" must never appear anywhere — it is an old name; use "sidra".

## Benchmarks

The root contains Python benchmark scripts, R plotting scripts, Ansible playbooks, and SQL scripts from the SIDRA paper. See `README.md` for benchmark setup and configuration details.
