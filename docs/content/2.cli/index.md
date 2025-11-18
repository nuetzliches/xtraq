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

Xtraq now mirrors the .NET CLI telemetry model:

- Every command writes an anonymised usage event to `.xtraq/telemetry/cli-usage.jsonl` (plus consent + machine markers) unless telemetry is explicitly disabled.
- Set one of `DOTNET_CLI_TELEMETRY_OPTOUT=1`, `XTRAQ_CLI_TELEMETRY_OPTOUT=1`, or `XTRAQ_TELEMETRY_OPTOUT=1`, or run in CI mode, to skip event collection entirely.
- A first-run disclosure banner explains the data flow and is recorded in `.xtraq/telemetry/cli-telemetry-consent.json`. The full rollout plan lives in [CLI Telemetry Alignment](/meta/cli-telemetry-alignment).
- The `--telemetry` switch adds the high-fidelity database report used for troubleshooting query plans in addition to the default anonymised CLI event.

CLI telemetry currently stays local; we will document any remote publishing before enabling it.

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
