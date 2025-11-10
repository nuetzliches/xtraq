---
title: build
description: Executes code generation based on current configuration.
versionIntroduced: 1.0.0-alpha
experimental: false
authoritative: true
aiTags: [cli, build, generation]
---

# build

Generates runtime artefacts (table types, procedure wrappers, DbContext helpers) using the metadata stored under `.xtraq/snapshots/`. JSON helpers ship enabled by default—no preview flags required.

## Usage

```bash
xtraq build [options]
```

## Requirements

- A `.env` file seeded via `xtraq init` that defines `XTRAQ_NAMESPACE`, `XTRAQ_GENERATOR_DB`, and optional output tweaks.
- A fresh snapshot in `.xtraq/snapshots/` created by `xtraq snapshot` (or trigger `xtraq build --refresh-snapshot`).

## Command-Specific Options

| Option               | Description                                                                             |
| -------------------- | --------------------------------------------------------------------------------------- |
| `--refresh-snapshot` | Refreshes the snapshot before generating code (equivalent to running `xtraq snapshot`). |

Combine the command with global flags from the [CLI overview](../index.md) when needed:

- `-p, --project-path` selects another project root.
- `--procedure <schema.proc>` scopes work to selected stored procedures.
- `--telemetry` persists timing data to `.xtraq/telemetry/build-*.json`.
- `--json-include-null-values` ensures JSON generators emit `[JsonIncludeNullValues]` attributes.
- `--ci` forces plain-text output, `-v/--verbose` prints detailed phase messages.

## Behavior Contract (Draft)

```json
{
  "command": "build",
  "reads": [".env", ".xtraq/snapshots/**/*.json"],
  "writes": ["<OutputDir>/**/*.cs"],
  "exitCodes": {
    "0": "Success",
    "1": "ValidationError",
    "2": "GenerationError"
  }
}
```

## Examples

```bash
# Generate artifacts for the current directory
xtraq build

# Target a sandbox project configured under debug/
xtraq build -p debug --verbose

# Refresh metadata and build in one step
xtraq build --refresh-snapshot --telemetry
```
