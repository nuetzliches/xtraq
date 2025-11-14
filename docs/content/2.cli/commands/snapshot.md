---
title: snapshot
description: Synchronizes stored procedures and schema metadata from the database.
versionIntroduced: 1.0.0-alpha
experimental: false
authoritative: true
authors: [core]
aiTags: [cli, snapshot, metadata]
---

# snapshot

Synchronizes stored procedures, parameters, and schema metadata into `.xtraq/` using the connection defined in your `.env` file.

## Usage

```bash
xtraq snapshot [options]
```

## Configuration

- `.xtraqconfig` **must** exist in the project root. When it is missing or invalid the CLI prompts to run `xtraq init` and, if approved, bootstraps the file before retrying.
- `XTRAQ_GENERATOR_DB` must be supplied via environment variables or `.env`. The command fails fast (and preserves `.xtraqconfig`) when no connection string is available.
- Optional allow-list filters come from `.xtraqconfig` (`BuildSchemas`) and can be overridden with `XTRAQ_BUILD_SCHEMAS` or `--procedure`.
- Use `XTRAQ_BUILD_PROCEDURES` (set via `--procedure`) to target specific stored procedures when triaging issues.

If the connection string is missing or empty, the command fails fast with guidance to update `.env`.

## Options

| Option                              | Description                                                                               |
| ----------------------------------- | ----------------------------------------------------------------------------------------- |
| `-p, --project-path <project-path>` | Point to a different project root that already contains `.env`.                           |
| `--no-cache`                        | Ignore existing `.xtraq/cache` entries and force a full re-parse.                         |
| `--procedure <schema.proc>`         | Comma-separated filter that maps to `XTRAQ_BUILD_PROCEDURES` for the current run.         |
| `--json-include-null-values`        | Propagate explicit JSON null-inclusion to downstream generators for comparison snapshots. |
| `-v, --verbose`                     | Emit per-procedure progress, timings, and cache reuse hints.                              |
| `--telemetry`                       | Persist a JSON telemetry report for the run under `.xtraq/telemetry`.                     |
| `--ci`                              | Switch to CI-friendly output (plain progress, no ANSI art).                               |

The remaining global switches (such as `--debug`, `--telemetry`, and `--ci`) behave exactly as described in the [CLI overview](../index.md).

When `--no-cache` is specified you will only see `[proc-loaded]` entries (no `[proc-skip]`) and the banner `[cache] Disabled (--no-cache)`. Use this after modifying parsing/JSON heuristics or when validating metadata changes.

## Behavior Contract

| Aspect         | Details                                                                                                                                          |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Invocation     | `xtraq snapshot` with optional project path.                                                                                                     |
| Reads          | `.xtraqconfig`, `.env` (when present), existing `.xtraq/cache` entries, stored procedure metadata from SQL Server.                               |
| Writes         | `.xtraq/snapshots/**/*.json`, `.xtraq/cache/*.json`, optional telemetry files under `.xtraq/telemetry`.                                          |
| Exit codes     | `0` success; non-zero indicates configuration/validation failure (missing config or connection) or database extraction errors.                    |
| Preconditions  | Accessible `.xtraqconfig` and database connection string; optional schema/procedure filters respected from config and overrides.                |
| Side effects   | May bootstrap `.xtraqconfig` via `xtraq init` when permitted; schedules background update check unless disabled; can emit telemetry summaries.   |

## Examples

```bash
# Standard metadata refresh using the current directory
xtraq snapshot

# Force fresh parsing while diagnosing snapshot issues
xtraq snapshot --no-cache --verbose

# Run against the debug sandbox and inspect only the identity schema procedures
xtraq snapshot -p debug --procedure identity.%
```
