---
title: init
description: Initialize a xtraq project with a tracked `.xtraqconfig` snapshot and minimal `.env` secrets.
versionIntroduced: 1.0.0-alpha
experimental: false
authoritative: true
authors: [core]
aiTags: [cli, init, env]
---

# init

Bootstraps a `.env` file (or updates an existing one) with core XTRAQ\_\* keys and writes a tracked `.xtraqconfig` snapshot for non-sensitive defaults.

## Usage

```bash
xtraq init [options]
```

## Options

| Flag                                | Description                     | Maps To               | Notes                                                |
| ----------------------------------- | ------------------------------- | --------------------- | ---------------------------------------------------- |
| `-p, --project-path <project-path>` | Target directory (defaults CWD) | n/a                   | Directory must exist or will be created              |
| `-n, --namespace <name>`            | Root namespace                  | `XTRAQ_NAMESPACE`     | Required (no implicit fallback)                      |
| `-c, --connection <cs>`             | Snapshot connection string      | `XTRAQ_GENERATOR_DB`  | Use least-privilege account                          |
| `-s, --schemas <list>`              | Comma separated allow-list      | `XTRAQ_BUILD_SCHEMAS` | Example: `core,identity`                             |
| `-f, --force`                       | Overwrite existing `.env`       | n/a                   | Recreates from template and refreshes `.xtraqconfig` |
| `-h, --help`                        | Show help                       | n/a                   |                                                      |

## Behavior Contract

```json
{
  "command": "init",
  "idempotent": true,
  "writes": [".env", ".env.example", ".xtraqconfig"],
  "reads": [".env", ".env.example"],
  "exitCodes": { "0": "Success", "2": "WriteFailure" },
  "sideEffects": ["Preserves unknown XTRAQ_* keys and updates provided values"]
}
```

## Examples

```bash
# Minimal non-interactive initialization
xtraq init --connection "Server=.;Database=AppDb;Trusted_Connection=True;TrustServerCertificate=True;"

# Allow-list schemas and force recreate .env
xtraq init -n Acme.Product.Data -c "Server=.;Database=AppDb;Trusted_Connection=True;" -s core,identity --force
```

## Notes

- `.env` contains only the sensitive `XTRAQ_GENERATOR_DB` placeholder/value. All other defaults live in `.xtraqconfig`.
- Unknown XTRAQ\_\* keys in existing `.env` are preserved verbatim.
- Follow up with `xtraq snapshot` to refresh metadata before building or testing.
- Keep `.xtraqconfig` under source control so CI and teammates inherit the same defaults.

## See Also

- [Tracked Configuration & Environment Bootstrap](../../3.reference/env-bootstrap.md)
