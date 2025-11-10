title: Project Environment Bootstrap & Configuration
description: How the xtraq CLI bootstraps `.env`, keeps tracked configuration in sync, and surfaces optional flags.
layout: docs
version: 5.0
---

# Project Environment Bootstrap & Configuration

The xtraq CLI relies on environment-first configuration. Each project maintains a private `.env` file generated from the authoritative template and a tracked `.xtraqconfig` snapshot that captures non-sensitive defaults. CLI flags or environment variables can override any value at runtime. The `ProjectEnvironmentBootstrapper` orchestrates both files so the generator stays deterministic across machines.

## Precedence Chain

1. CLI arguments (e.g. `xtraq build --output Generated`)
2. Process environment variables (`XTRAQ_NAMESPACE=Acme.App.Data xtraq build`)
3. Project `.env`
4. `.xtraqconfig` (only when `.env` omits a non-sensitive key)
5. Internal defaults

The legacy `xtraq.json` format is no longer consulted for generator settings.

## Bootstrap Workflow

`xtraq init` calls the `ProjectEnvironmentBootstrapper` to handle initial setup and repeatable updates:

1. Detects absence of `.env` and offers to create one from the template.
2. Copies `.env.example` verbatim, preserving comment blocks and section ordering.
3. Applies inferred values (namespace, schemas, connection string) when provided via flags.
4. Writes/refreshes `.xtraqconfig` so the repository tracks the chosen namespace, output folder, target framework, and schema allow-list.
5. Leaves a review notice in the console so you can adjust sensitive entries (connection strings, schema allow-list) before committing.

Re-run `xtraq init` at any time; use `--force` to overwrite existing values while keeping comments intact.

## Authoritative Template (`.env.example`)

The template is the single source of truth for available keys. Comment sections are grouped by feature area (core configuration, optional flags, auto-update control, diagnostics). When a key graduates from preview, update the template and this page together. JSON helpers now ship enabled by default, so no opt-in keys remain for that feature area.

### Stable Keys

| Key                   | Purpose                                        | Notes                                            |
| --------------------- | ---------------------------------------------- | ------------------------------------------------ |
| `XTRAQ_NAMESPACE`     | Root namespace for generated code              | CLI flag `--namespace` overrides                 |
| `XTRAQ_GENERATOR_DB`  | Connection string used during `xtraq snapshot` | Use least-privilege credentials                  |
| `XTRAQ_BUILD_SCHEMAS` | Positive allow-list of schemas to generate     | Empty or absent = include all discovered schemas |
| `XTRAQ_OUTPUT_DIR`    | Optional override for code output folder       | Defaults to `xtraq`                              |
| `XTRAQ_TFM`           | Target framework hint for template selection   | e.g. `net8.0`, `netstandard2.1`                  |

- The matching tracked file `.xtraqconfig` echoes `XTRAQ_NAMESPACE`, `XTRAQ_OUTPUT_DIR`, `XTRAQ_TFM`, and `XTRAQ_BUILD_SCHEMAS` for team-wide consistency. Secrets (`XTRAQ_GENERATOR_DB`, etc.) never leave `.env`.

  | Key               | Purpose                                    | Notes                                                         |
  | ----------------- | ------------------------------------------ | ------------------------------------------------------------- |
  | `XTRAQ_NO_UPDATE` | Disables auto-update checks for the CLI    | Accepts `1`, `true`, `yes`, `on` (case-insensitive)           |
  | `XTRAQ_LOG_LEVEL` | Controls CLI logging verbosity             | `info` by default; set `debug` for detailed pipeline diagnostics |

### Preview / Emerging Keys

These keys remain disabled until their respective features ship. Keep them documented but emphasize the activation criteria.

| Key                              | Activation Criterion                        | Description                                             |
| -------------------------------- | ------------------------------------------- | ------------------------------------------------------- |
| `XTRAQ_STRICT_DIFF`              | Core coverage ≥60% & diff allow-list stable | Fails build on unexpected generator diffs               |
| `XTRAQ_STRICT_GOLDEN`            | Same as strict diff                         | Enforces golden hash manifests                          |
| `XTRAQ_ENABLE_ANALYZER_WARNINGS` | Analyzer package shipping                   | Surfaces generator diagnostics as compile-time warnings |
| `XTRAQ_STRICT_NULLABLE`          | Nullable enforcement finalized              | Promotes nullable analysis warnings to errors           |
| `XTRAQ_GENERATE_API_ENDPOINTS`   | API endpoint templating GA                  | Generates minimal API endpoint stubs                    |

## Security Guidelines

- Never commit secrets. Use integrated authentication or secure secret stores where possible.
- Provision a SQL login with read-only schema access for generator operations.
- Rotate credentials regularly and document the process in team runbooks.
- Keep `.xtraqconfig` under version control to avoid drift between environments; it contains no secrets.

## Determinism & Golden Hash Integration

Golden hash manifests summarize generator output for regression checks. Within the xtraq repository we maintain them under `debug/` for framework development, but consumer projects keep them alongside their configured output directory (default `xtraq/`). Gating remains opt-in until coverage and diff allow-list criteria are met. When you activate `XTRAQ_STRICT_GOLDEN`, ensure checklists capture the decision and update automation accordingly.

## Namespace Override Example

Override the namespace via CLI flag or `.env` and inspect the diff to confirm the new root takes effect. The example below captures how `xtraq build` responds when `XTRAQ_NAMESPACE` changes from `Acme.App` to `Contoso.Billing`.

```diff
--- a/xtraq/xtraqDbContext.cs
+++ b/xtraq/xtraqDbContext.cs
-namespace Acme.App.xtraq;
+namespace Contoso.Billing.xtraq;

-public static class xtraqDbContextServiceCollectionExtensions
+public static class xtraqDbContextServiceCollectionExtensions
```

Tip: run `xtraq build --namespace Contoso.Billing` to test overrides without editing the `.env`. When the diff looks correct, update `XTRAQ_NAMESPACE` in the project `.env` and commit the change alongside regenerated artifacts.

## Example `.env` (Minimal)

```dotenv
XTRAQ_NAMESPACE=Acme.App.Data
XTRAQ_GENERATOR_DB=Server=localhost;Database=AppDb;Trusted_Connection=True;TrustServerCertificate=True;
XTRAQ_BUILD_SCHEMAS=core,identity
XTRAQ_OUTPUT_DIR=xtraq
XTRAQ_TFM=net8.0
```

## Example `.env` (With Preview Flags)

```dotenv
XTRAQ_NAMESPACE=Acme.App.Data
XTRAQ_GENERATOR_DB=Server=localhost;Database=AppDb;Trusted_Connection=True;TrustServerCertificate=True;
XTRAQ_BUILD_SCHEMAS=core,identity
XTRAQ_ENABLE_ANALYZER_WARNINGS=1
```

> Only enable preview flags in feature branches. Capture the rationale and rollback plan in the checklist when activating them, and keep disabled keys commented (`#KEY=0`) until the feature graduates.

## FAQ

**Why retire `xtraq.json`?**

Environment-first configuration offers deterministic precedence, easy diffing, and straightforward automation via CI. The `.env` template keeps context close to the project, while CLI flags simplify one-off overrides.

**Can `.env` live outside the repo?**

Yes. The CLI searches the working directory first, then falls back to parent directories. For containerized or CI scenarios you can supply environment variables directly instead of committing the file.

**How do I refresh `.env` when new keys appear?**

Run `xtraq init --force` after syncing the latest template changes. Review new keys in the diff, decide whether to enable them, and update the checklists accordingly.

---

Future revisions will expand guidance once strict diff, JSON dual mode, and streaming features reach general availability.
