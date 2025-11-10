---
title: CLI Overview
description: Overview of xtraq command-line interface and global options.
---

# CLI Overview

The xtraq CLI uses a two-file configuration model:

- `.env` (not committed) keeps sensitive values such as `XTRAQ_GENERATOR_DB`.
- `.xtraqconfig` (tracked) mirrors non-sensitive settings so repositories have a stable baseline.

Run `xtraq init` once to scaffold both files, then reuse them across `snapshot`, `build`, and future commands.

## Global Options

| Option                              | Description                                                                                           |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `-p, --project-path <project-path>` | Override the working directory (must contain the target `.env`).                                      |
| `-v, --verbose`                     | Emit detailed logging (pipeline steps, timings, cache hints).                                         |
| `--debug`                           | Use the debug environment wiring for additional diagnostics.                                          |
| `--no-cache`                        | Skip cached metadata (forces a full snapshot refresh).                                                |
| `--procedure <schema.proc>`         | Limit snapshot/build operations to matching stored procedures (comma separated, wildcards supported). |
| `--telemetry`                       | Persist a JSON telemetry report for the executed command under `.xtraq/telemetry`.                    |
| `--json-include-null-values`        | Emit `[JsonIncludeNullValues]` attributes and keep null JSON fields during serialization.             |
| `--ci`                              | Switch console output to CI-friendly mode (plain progress, no ANSI art).                              |

> `--debug-alias` exists for internal debugging and is intentionally undocumented for the first public release.

## Core Commands

| Command                           | Purpose                                                                             |
| --------------------------------- | ----------------------------------------------------------------------------------- |
| [`init`](./commands/init)         | Bootstrap `.env` configuration and namespace metadata.                              |
| [`snapshot`](./commands/snapshot) | Read stored procedures and schema metadata into `.xtraq/` using `.env` credentials. |
| [`build`](./commands/build)       | Generate runtime artefacts (table types, helpers) from the current snapshot.        |
| [`version`](./commands/version)   | Display installed and latest CLI versions, including update hints.                  |
| [`update`](./commands/update)     | Update the xtraq global tool to the latest available package.                       |

## Examples

```bash
xtraq init --connection "Server=.;Database=AppDb;Trusted_Connection=True;"
xtraq build
```
