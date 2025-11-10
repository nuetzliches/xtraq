---
title: Configuration Reference
description: Configuration precedence and XTRAQ_* environment keys for the current CLI.
---

# Configuration Reference

The modern CLI relies on a dual-file model: a local `.env` for secrets and a tracked `.xtraqconfig` snapshot for non-sensitive defaults. Legacy JSON configuration (`xtraq.json`) is ignored and remains in the repository only for historical reference. This page captures the desired steady-state precedence and supported keys.

## Precedence

1. CLI overrides (e.g. `xtraq snapshot --project-path <project-path>` with explicit options)
2. Process environment variables (`XTRAQ_*`)
3. `.env` file in the target project directory
4. `.xtraqconfig` (non-sensitive fallback written by `xtraq init`)

`.xtraqconfig` only applies when a key is missing from `.env`; think of it as a checked-in baseline that keeps namespaces, output directories, and allow-lists in sync across the team. If a value is not present in any layer the CLI uses built-in defaults. No generator behavior reads configuration values from legacy JSON files.

## Required Keys

| Key                  | Purpose                                                  | Notes                                                                              |
| -------------------- | -------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `XTRAQ_GENERATOR_DB` | Connection string used for metadata snapshots and builds | Required. Must be stored in `.env` or environment not checked into source control. |

## Optional Keys

| Key                                     | Purpose                                                            | Default behavior when unset                      |
| --------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------ |
| `XTRAQ_NAMESPACE`                       | Overrides the inferred root namespace for generated artefacts      | Namespace inferred from project/assembly name    |
| `XTRAQ_OUTPUT_DIR`                      | Customizes the relative output directory for generated artefacts   | `xtraq`                                          |
| `XTRAQ_BUILD_SCHEMAS`                   | Comma-separated allow list of schemas to include during generation | Empty list → all schemas except ignored defaults |
| `XTRAQ_NO_UPDATE` / `XTRAQ_SKIP_UPDATE` | Disables auto-update prompts when set                              | Prompts remain enabled                           |
| `XTRAQ_VERBOSE`                         | Emits additional diagnostics when set to `1`                       | Standard informational logging                   |

> Keep `.env` under source control only when it contains non-sensitive values. Connection strings and credentials belong in secrets management. The matching values in `.xtraqconfig` are safe to commit because secrets never flow into the tracked file.

## File Layout

- `.env` lives at the repository root (or project directory when using `--path`) and stays out of source control when it contains secrets.
- `.xtraqconfig` is generated from `.env` and **must** be checked in so CI and contributors share the same defaults.
- `samples/restapi/.env.example` provides the canonical template for bootstrapping new environments.
- Generator outputs are written beneath `xtraq/` by default; change with `XTRAQ_OUTPUT_DIR` if required.

### `.xtraqconfig` Payload

The tracked JSON exposes only non-sensitive knobs:

```jsonc
{
  "Namespace": "Acme.Product.Data",
  "OutputDir": "Xtraq",
  "TargetFramework": "net8.0",
  "BuildSchemas": ["core", "identity"]
}
```

- `Namespace` maps to `XTRAQ_NAMESPACE`.
- `OutputDir` maps to `XTRAQ_OUTPUT_DIR` (defaults to `Xtraq` when omitted).
- `TargetFramework` currently acts as metadata for tooling and optional templates.
- `BuildSchemas` mirrors `XTRAQ_BUILD_SCHEMAS` as a distinct list.

Secrets such as `XTRAQ_GENERATOR_DB` are never written to `.xtraqconfig`; they stay exclusively in `.env` or external secrets stores.

## Legacy JSON Configuration

- `xtraq.json` is treated as a legacy artefact. The CLI warns when the file is present so teams know to migrate values into `.env`.
- Schema metadata lives exclusively under `.xtraq/snapshots/` (fingerprinted snapshots). The generator never persists metadata inside JSON configuration files.
- Historical documentation for `xtraq.json` has moved to the legacy documentation stream; new features do not extend the legacy file format.

## Validation Checklist

- `.env` exists in every project that runs the current CLI and contains at least `XTRAQ_GENERATOR_DB`.
- CI and local scripts rely on environment variables / `.env` exclusively—no automation reads from `xtraq.json`.
- Legacy configuration files are retained only when needed for archival purposes and are no longer treated as inputs.
