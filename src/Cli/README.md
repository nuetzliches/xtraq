# Xtraq CLI command surface

This document describes the command and option surface provided by the `xtraq` global tool together with the hosting architecture under `src/Cli`. It reflects the current `CliCommandAppBuilder` implementation that Program.cs now delegates to.

## Root command (`xtraq`)

```
xtraq [options]
```

- The root command no longer accepts a positional project argument. Use `--project-path`/`--project`/`-p` or the explicit subcommand arguments instead.
- Running `xtraq` with no additional arguments routes to `xtraq build --refresh-snapshot`, mirroring the legacy "snapshot + build" workflow.
- Global options:
  - `--verbose`, `-v`: enable verbose console output plus additional logging downstream.
  - `--debug`: switch to the "debug" environment profile and enable extra diagnostics in services.
- `--debug-alias`: promotes `XTRAQ_LOG_LEVEL` to `debug` so alias scope tracing becomes visible.
  - `--no-cache`: force cache misses for schema metadata (sets `CacheControl.ForceReload = true`).
  - `--procedure <schema.name,...>`: restrict execution to a comma-separated allow-list; validation rejects malformed tokens.
  - `--telemetry`: persist telemetry snapshots under `.xtraq/telemetry` after successful runs.
  - `--json-include-null-values`: opt into JSON-null emission; tracked even when explicitly `false` so generators can distinguish overrides.
  - `--entity-framework`: toggle Entity Framework helper generation (`XTRAQ_ENTITY_FRAMEWORK`).
  - `--ci`: suppress Spectre.Console enhancements for plain-text/CI logs.
  - `--project-path`, `--project`, `-p`: supply a directory or `.env` file used to locate `.xtraqconfig`; defaults to the current directory when omitted.

**Implicit behaviour**

- Update checks run asynchronously unless suppressed through `XTRAQ_NO_UPDATE`/`XTRAQ_SKIP_UPDATE`.
- Parsed options are stored in `CommandOptions` so subsequent commands can reuse defaults.
- `DirectoryUtils.ResetBasePath()` runs after each command so per-command path overrides do not leak between invocations.

## Built-in subcommands

### `xtraq snapshot`

- Description: capture database metadata into `.xtraq` snapshots without generating code.
- Arguments: optional `project-path` (alias: `--project-path`/`-p`).
- Options: inherits all global switches; honours `--procedure`, `--no-cache`, `--telemetry`, and `--entity-framework`.
- Notes: schedules asynchronous update checks unless disabled.

### `xtraq build`

- Description: generate code artifacts from the latest snapshot; `--refresh-snapshot` can force a pre-build snapshot.
- Arguments: optional `project-path`.
- Options: inherits global switches plus `--refresh-snapshot` (default false when the subcommand is used explicitly, true when invoked via the root command alias).
- Notes: schedules asynchronous update checks unless disabled.

### `xtraq version`

- Description: print the installed CLI version and the latest published version.
- Options: honours `--verbose` for extra detail.

### `xtraq update`

- Description: update the installed global tool to the newest version.
- Options: honours `--verbose`.

### `xtraq init`

- Description: bootstrap `.env`, `.env.example`, `.xtraqconfig`, and `.gitignore` scaffolding.
- Arguments: optional `project-path` argument or `--project-path` option for the target directory.
- Options:
  - `--force`, `-f`: overwrite an existing `.env`.
  - `--namespace`, `-n`: seed `XTRAQ_NAMESPACE`.
  - `--connection`, `-c`: seed `XTRAQ_GENERATOR_DB`.
  - `--schemas`, `-s`: seed `XTRAQ_BUILD_SCHEMAS` (comma-separated allow-list).
- Side effects: normalises key casing, writes `.xtraqconfig`, and ensures `.gitignore` excludes generated folders.

## Option semantics

- `--procedure` supports `*`/`?` wildcards, trims tokens, deduplicates entries, and surfaces invalid filters at parse time (`CliHostUtilities.TryNormalizeProcedureFilter`).
- `--json-include-null-values` and `--entity-framework` both record `Has*Override` flags so downstream processors can tell whether the option was explicitly set.
- `--telemetry` is honoured on successful snapshot/build runs; version/update/init still capture lightweight telemetry envelopes without touching disk.
- `--project-path` accepts either a directory or an `.env` file; the CLI normalizes the root path before invoking the runtime.

## Current architecture snapshot

1. **Bootstrapping**: `CliEnvironmentBootstrapper` normalizes invocation arguments, wires process-level environment variables, and ensures Spectre.Console defaults are applied before DI spins up.
2. **Host builder**: `CliHostBuilder` centralizes configuration and dependency injection, returning a disposable `CliHostContext` that exposes `IConfiguration`, `IServiceProvider`, and the resolved environment.
3. **Command wiring**: `CliCommandAppBuilder` constructs the `System.CommandLine` tree (options, validators, subcommands) and binds handlers to `IXtraqCommand` implementations or inline logic (`init`).
4. **Execution flow**: `Program.RunCliAsync` is now a thin shim that normalizes args, builds the host, instantiates `CliCommandAppBuilder`, and invokes the resulting `RootCommand`.
5. **Next up**: a focused `CliCommandExecutor` will lift the telemetry + execution logic out of the builder so tests can target it without spinning up the parser.

## Console tooling assessment

`System.CommandLine` handles parsing and binding; Spectre.Console remains the presentation layer for progress, tables, and prompts exposed through `IConsoleService`. The CLI keeps Spectre usage behind abstractions so tests can substitute mock consoles.

## Command interface blueprint

`IXtraqCommand` and `XtraqCommandContext` live under `src/Cli/Commands`. Each verb-specific handler (`SnapshotCommand`, `BuildCommand`, …) resolves services from the ambient `IServiceProvider`, executes its workload, and returns an exit code. Tests can supply fake contexts or interceptors to validate behaviors without spinning up the full CLI host.
