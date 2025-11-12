---
title: Quickstart
description: From zero to first generated code in minutes.
version: 1.0.0-alpha.6
---

# Quickstart

Follow these steps to install the xtraq CLI, initialize configuration, and generate your first client code.

## Prerequisites

- .NET 8 SDK (or newer) available on the PATH
- Network access to the SQL Server instance that hosts your stored procedures

## 1. Install or Update the CLI

```bash
dotnet tool update -g xtraq
```

`dotnet tool update` installs the tool if it is missing and upgrades it when a newer version ships.

## 2. Initialize Configuration

```bash
xtraq init --connection "Server=.;Database=AppDb;Trusted_Connection=True;"
```

This command creates (or updates) a tracked `.xtraqconfig` file for non-sensitive settings and a project-scoped `.env` for secrets such as `XTRAQ_GENERATOR_DB`. Re-running `xtraq init` safely updates only the keys you specify.

## 3. Capture a Snapshot

```bash
xtraq snapshot
```

`xtraq snapshot` captures the latest stored procedure metadata and writes it to `.xtraq/snapshots` using the connection string from `.env`. Non-sensitive defaults (namespace, output directory, schema allow-list) travel with the repository through `.xtraqconfig`, so teammates only need to supply their local secrets.

## 4. Generate Code

```bash
xtraq build
```

Generated files land in the `Xtraq/` directory by default. Override the location by setting `XTRAQ_OUTPUT_DIR` in `.xtraqconfig` or by passing `--output` on the command line.

## Next Steps

- Review and commit the contents of the `Xtraq/` directory.
- Review tracked defaults and optional secrets in [Tracked Configuration & Environment Bootstrap](../3.reference/env-bootstrap.md).
- Explore additional commands in the [CLI Reference](../2.cli/index.md).
