# Xtraq CLI command surface

This document captures the current command- and option-set exposed by the `xtraq` global tool as implemented in `Program.cs` and the helper types under `src/Cli`. It also outlines the planned direction for a modernised command architecture.

## Root command (`xtraq`)

```
xtraq [project-path] [options]
```

- **Arguments**
  - `project-path` (optional): file system path to the project root. When omitted the current working directory is used. If provided, the value can point to a directory or a `.env` file and will be normalised before execution.
- **Options**

  - `--verbose`, `-v`: enables additional console detail, including verbose logging hooks in downstream services.
  - `--debug`: switches to the "debug" environment profile (consumed by runtime services and telemetry).
  - `--debug-alias`: sets `XTRAQ_ALIAS_DEBUG=1`, enabling alias scope tracing.
  - `--no-cache`: disables reads and writes against the local procedure metadata cache (`CacheControl.ForceReload = true`).
  - `--procedure <schemaA.proc1,...>`: restricts execution to the listed procedures. Sets the `XTRAQ_BUILD_PROCEDURES` environment variable for downstream consumers.
  - `--telemetry`: persists run telemetry to `.xtraq/telemetry`. Only honoured after successful build executions.
  - `--json-include-null-values`: opt-in flag for JSON result payload generation. When present the flag is propagated even when `false` to allow explicit overrides.
  - `--ci`: disables Spectre.Console enrichments (charts, markup) to produce plain-text output for CI pipelines.

- **Implicit behaviours**
  - Update checks run asynchronously unless `XTRAQ_NO_UPDATE`/`XTRAQ_SKIP_UPDATE` are set (via `.env` or shell). There is currently no CLI switch to opt-out per invocation.
  - The runtime records the last parsed options in `CommandOptions` so subsequent invocations can reuse defaults.
  - Spectre.Console enrichments (breakdown charts, inline markup) are enabled by default and fall back to plain text when `--ci` is supplied or when the console implementation does not support ANSI sequences.
  - After every command the working directory base path is reset by `DirectoryUtils.ResetBasePath()`.

## Built-in subcommands

### `xtraq version`

- Description: show installed and latest Xtraq versions.
- Options: reuses `--verbose`. All other switches are ignored.

### `xtraq update`

- Description: update the installed Xtraq global tool to the latest version.
- Options: reuses `--verbose`.

### `xtraq init`

- Description: bootstrap a project `.env` and supporting artefacts.
- Options:
  - `--path`, `-p`: target directory (defaults to current directory).
  - `--force`, `-f`: overwrite an existing `.env`.
  - `--namespace`, `-n`: seed `XTRAQ_NAMESPACE` in the generated `.env`.
  - `--connection`, `-c`: seed `XTRAQ_GENERATOR_DB`.
  - `--schemas`, `-s`: comma-separated allow-list for `XTRAQ_BUILD_SCHEMAS`.
- Side effects: writes/updates `.xtraqconfig`, ensures `.env.example` and `.gitignore`, and normalises `.env` keys to uppercase.

## Option semantics

- The single positional argument is always treated as the project path. No action inference is performed; verbs are chosen explicitly or implied via the default workflow.
- `--telemetry` is honoured after successful build executions (including default "snapshot+build" runs triggered without explicit verbs).
- `--procedure` accepts multiple comma-separated tokens; each is trimmed and propagated as-is. Validation remains TODO.
- `--json-include-null-values` stores both the value and a `HasJsonIncludeNullValuesOverride` boolean, allowing downstream components to distinguish between "unset" and "explicit false".
- `--ci` suppresses Spectre.Console widgets and rich formatting so logs remain machine-friendly.

## Target architecture (greenfield CLI)

To design the next iteration of the CLI without legacy constraints, adopt the following baseline:

1. **Command kernel:** define a lightweight `IXtraqCommand` abstraction (e.g. `ValueTask<int> ExecuteAsync(CliInvocation context)`) and register concrete handlers (`SnapshotCommand`, `BuildCommand`, `InitCommand`, etc.) via dependency injection. `Program.cs` only wires the root parser to the command catalog.
2. **Parsing model:** rely on `System.CommandLine` 2.x binding to project parse results directly into immutable option records. Expose explicit verbs (`xtraq init`, `xtraq snapshot`, `xtraq build`) and reserve the single positional argument exclusively for the project path; drop the legacy `rebuild` noun entirely.
3. **Default workflow:**

- Bare `xtraq` (no arguments) resolves to `build --refresh-snapshot`, i.e. snapshot followed by build. This behaviour should be implemented by routing the root command to the `BuildCommand` with a `RefreshSnapshot` flag set.
- When invoked, the root command first probes the target project (current directory or the positional path argument). If `.env`, `.xtraqconfig`, or `.xtraq/` are missing, emit a status block explaining that the project is uninitialised and offer to run `xtraq init <path>`. Include a `--yes` switch for fully automated bootstraps.
- `xtraq <path>` is equivalent to `xtraq <path> build --refresh-snapshot`, while `xtraq <path> snapshot` and `xtraq <path> build` map directly to the respective verbs.
- `snapshot` only refreshes metadata; `build` compiles artefacts; `build --refresh-snapshot` executes both steps; no separate `rebuild` verb exists.

4. **Shared option catalog:** centralise reusable options (`--verbose`, `--debug`, `--no-cache`, `--telemetry`, `--procedure`, `--json-include-null-values`, `--yes`) in a single builder so commands and automation scripts draw from the same definitions. Inject validators to ensure paths exist and procedure filters have valid syntax.
5. **Observability:** emit structured telemetry for each command (verb, options, duration, exit code) and surface feature flags through tracked configuration (`.xtraqconfig`) rather than scattered environment variables to simplify testing.

With this foundation, additional verbs such as `doctor`, `cache clear`, or `plan` can be introduced by adding new `IXtraqCommand` implementations without touching the core parser.

### Console tooling assessment

`System.CommandLine` in .NET 8/10 covers parsing, help, suggestions, and basic terminal output, but it offers limited primitives for richer TUI elements. Because the future CLI requires progress indicators, tables, status panels, and interactive prompts, we should adopt [Spectre.Console](https://spectreconsole.net/) as the presentation layer.

Adoption plan:

- Keep `System.CommandLine` for parsing and binding.
- Extend `IConsoleService` with a Spectre-backed implementation (`SpectreConsoleService`) that exposes high-level helpers for rendering tables, progress bars, confirmation prompts, and rich status output. Provide a simplified shim (ANSI-only) for environments where Spectre is unavailable (e.g. minimal host tooling).
- Use Spectre's `Progress`, `Status`, and `Table` APIs inside the new command handlers to deliver consistent UX across scripted and interactive flows.
- Isolate Spectre-specific code in a dedicated `Xtraq.Cli.Tui` namespace so downstream libraries remain unaffected and tests can swap in a mock renderer.

This approach keeps parsing concerns separate while delivering the TUI feature set expected from the modernised CLI.

## Command interface blueprint

The new command contract lives in `src/Cli/Commands/IXtraqCommand.cs` and consists of:

- `IXtraqCommand`: a single-method interface (`ValueTask<int> ExecuteAsync(...)`) that returns an exit code and accepts a rich `XtraqCommandContext` plus an optional `CancellationToken`.
- `XtraqCommandContext`: immutable invocation data exposing the normalised project path, parsed `ICommandOptions`, the ambient `IServiceProvider`, the console abstraction, and a `RefreshSnapshot` toggle used by the default workflow.

Each verb-specific handler (e.g. `SnapshotCommand`, `BuildCommand`) will implement `IXtraqCommand`, operate solely on the provided context, and resolve further services through `context.Services`. Tests can supply mock implementations of `IXtraqCommand` or substitute the console to validate TUI output without invoking Spectre.Console directly.
