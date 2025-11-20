---
title: CLI Overview
description: Overview of xtraq command-line interface and global options. The xtraq CLI uses a dual-file configuration model anchored on the tracked `.xtraqconfig` snapshot. Secrets remain outside of source control (typically in `.env` or environment variables). Run `xtraq init` once to scaffold both files, then reuse them across `snapshot`, `build`, and the default `xtraq` entry point.
---

## Global Options

| Option                              | Description                                                                                           |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `-p, --project-path <project-path>` | Override the working directory (must contain the target `.env`).                                      |
| `-v, --verbose`                     | Emit detailed logging (pipeline steps, timings, cache hints).                                         |
| `--debug`                           | Use the debug environment wiring for additional diagnostics.                                          |
| `--no-cache`                        | Skip cached metadata (forces a full snapshot refresh).                                                |
| `--procedure <schema.proc>`         | Limit snapshot/build operations to matching stored procedures (comma separated, wildcards supported). |
| `--telemetry`                       | Persist a detailed database telemetry report for the executed command under `.xtraq/telemetry`.       |
| `--json-include-null-values`        | Emit `[JsonIncludeNullValues]` attributes and keep null JSON fields during serialization.             |
| `--entity-framework`                | Opt in to Entity Framework Core integration helpers (sets `XTRAQ_ENTITY_FRAMEWORK` for the run).      |
| `--ci`                              | Switch console output to CI-friendly mode (plain progress, no ANSI art).                              |

> `--debug-alias` exists for internal debugging and is intentionally undocumented for the first public release.

## Telemetry

Telemetry **strictly opt-in**. Nothing is recorded unless you pass `--telemetry`.

- Append `--telemetry` to `snapshot`, `build`, or the default entry point to capture database query traces, build statistics, and a CLI command summary.
- Reports land in `.xtraq/telemetry` as timestamped JSON files (`cli-command-*.json`, `snapshot-*.json`, `build-*.json`, and optional summaries) that stay on disk until you delete them.
- No environment variables or consent banners are involved anymore—omit the switch and the directory remains untouched.
- Telemetry files never leave your machine unless you share them manually. See [CLI Telemetry Alignment](/meta/cli-telemetry-alignment) for the current rollout plan.

## Core Commands

| Command                              | Purpose                                                                                                       |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| [`xtraq`](/cli/commands/xtraq)       | Default entry point: refresh snapshot and build artefacts in one step (alias for `build --refresh-snapshot`). |
| [`init`](/cli/commands/init)         | Bootstrap `.xtraqconfig` and optional `.env` secrets.                                                         |
| [`snapshot`](/cli/commands/snapshot) | Read stored procedures and schema metadata into `.xtraq/` using configured credentials.                       |
| [`build`](/cli/commands/build)       | Generate runtime artefacts (table types, helpers) from the current snapshot.                                  |
| [`version`](/cli/commands/version)   | Display installed and latest CLI versions, including update hints.                                            |
| [`update`](/cli/commands/update)     | Update the xtraq global tool to the latest available package.                                                 |

## Examples

```bash
xtraq init --connection "Server=.;Database=AppDb;Trusted_Connection=True;"
xtraq build
```
