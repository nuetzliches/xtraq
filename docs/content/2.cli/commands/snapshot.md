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

- `XTRAQ_GENERATOR_DB` **must** be set in `.env`. Run `xtraq init` to scaffold the key or update it manually.
- `.xtraqconfig` must exist. The CLI treats its absence as "project not initialised" and exits early with guidance to run `xtraq init`.
- Optional allow-list filters come from `.env` (`XTRAQ_BUILD_SCHEMAS=core,identity`).
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

## Behavior Contract (Draft)

```json
{
  "command": "snapshot",
  "reads": [".env"],
  "writes": [".xtraq/snapshots/**/*.json", ".xtraq/cache/*.json"],
  "exitCodes": {
    "0": "Success",
    "1": "ValidationError",
    "2": "ExtractionError"
  }
}
```

## Examples

```bash
# Standard metadata refresh using the current directory
xtraq snapshot

# Force fresh parsing while diagnosing snapshot issues
xtraq snapshot --no-cache --verbose

# Run against the debug sandbox and inspect only the identity schema procedures
xtraq snapshot -p debug --procedure identity.%
```
