title: Tracked Configuration & Environment Bootstrap
description: Treat `.xtraqconfig` as the canonical project state and keep `.env` reserved for secrets.
layout: docs
version: 6.0
---

# Tracked Configuration & Environment Bootstrap

The xtraq CLI keeps project settings deterministic by committing a `.xtraqconfig` file and limiting the local `.env` to secrets or machine-specific overrides. The tracked JSON snapshot describes everything the team needs to reproduce generator output, while the `.env` file provides developer credentials and optional local toggles. CLI flags and process-level environment variables can always override both.

## Configuration Model Overview

- `.xtraqconfig` – canonical, version-controlled JSON that captures non-sensitive defaults for namespace, output folder, target framework, and schema allow-list. Every clone of the repository reads this file first.
- `.env` – private developer file used for secrets (`XTRAQ_GENERATOR_DB`) and rare local adjustments. It stays out of source control and can remain minimal.
- `ProjectEnvironmentBootstrapper` – ensures both files stay in sync: it scaffolds a minimal `.env`, derives `.xtraqconfig` from the tracked values, and provides diff-safe updates when parameters change.

## Resolution Order

1. CLI arguments (for example `xtraq build --output Generated`)
2. Process environment variables (`XTRAQ_NAMESPACE=Acme.App.Data xtraq build`)
3. `.env` (secrets and local overrides)
4. `.xtraqconfig` (team defaults)
5. Internal defaults

Although `.env` appears earlier in the precedence chain, the recommended workflow is to keep it slim. Any non-secret value you expect others to inherit should live in `.xtraqconfig` so new machines do not rely on copying a local `.env`.

## Editing `.xtraqconfig`

`xtraq init` writes `.xtraqconfig` automatically, but you can edit it directly to capture intentional project-wide changes.

```json
{
  "Namespace": "Contoso.Billing",
  "OutputDir": "Xtraq",
  "TargetFramework": "net8.0",
  "BuildSchemas": [
    "core",
    "identity"
  ]
}
```

| Property          | Description                                                     | Notes                                         |
| ----------------- | --------------------------------------------------------------- | --------------------------------------------- |
| `Namespace`       | Base namespace for generated artifacts                          | Accepts CLI override `--namespace`            |
| `OutputDir`       | Relative folder that receives generated files                   | Defaults to `Xtraq` when omitted              |
| `TargetFramework` | Template hint used during generation (e.g. `net8.0`, `net10.0`) | Default value is `net8.0`; roadmap upgrade pending |
| `BuildSchemas`    | Allow-list of schemas to include                                | Empty array ⇒ include every discovered schema |

Recommendations:

- Commit `.xtraqconfig` with every intentional change so teammates receive the same defaults.
- Prefer arrays for `BuildSchemas` even when only one schema is included; the bootstrapper preserves the order and removes duplicates.
- When experimenting, edit `.xtraqconfig`, run `xtraq build`, review the diff, and include both file changes and generated output in the same commit.

## Minimal `.env` Responsibilities

The bootstrapper copies `.env.example` but only a handful of keys are required for day-to-day work. Keep everything else commented unless you are actively testing a preview feature.

```dotenv
XTRAQ_GENERATOR_DB=Server=localhost;Database=AppDb;Trusted_Connection=True;TrustServerCertificate=True;
```

| Key                  | Purpose                                            | Guidance                                               |
| -------------------- | -------------------------------------------------- | ------------------------------------------------------ |
| `XTRAQ_GENERATOR_DB` | Metadata discovery connection string               | Required secret – never commit, rotate regularly       |
| `XTRAQ_NO_UPDATE`    | Disables CLI auto-update checks                    | Optional local toggle (`1`, `true`, `yes`, `on`)        |
| `XTRAQ_LOG_LEVEL`    | Overrides logging verbosity (`info`, `debug`)      | Useful for temporary diagnostics                        |
| `XTRAQ_CONFIG_PATH`  | Points the CLI at an alternate `.xtraqconfig` root | Rare – primarily for CI scenarios or integration tests |

Preview flags (for example `XTRAQ_STRICT_DIFF`) should remain commented until the corresponding feature reaches GA. Track their rollout plans in project checklists when you do enable them.

## Bootstrap Workflow

Running `xtraq init` or invoking commands that require configuration triggers the bootstrapper:

1. Ensures a `.env` exists; if missing, offers to create a minimal file and copies comments from `.env.example` verbatim.
2. Applies CLI parameters (namespace, schemas, output directory) to the tracked configuration.
3. Writes or updates `.xtraqconfig`, carrying forward existing values to avoid accidental resets.
4. Prints a summary instructing you to review secrets before continuing.

Re-run `xtraq init --force` whenever template keys change upstream or you want to rebuild `.env` from scratch while preserving `.xtraqconfig`.

## Maintenance Checklist

- When adjusting namespaces or output folders, update `.xtraqconfig` first, then regenerate artifacts.
- Keep `.env.example` aligned with the keys that the CLI understands; every documented key should include a sentence in this page.
- If CI needs different settings, prefer environment variables over editing `.xtraqconfig` so local clones remain authoritative.

## Security Guidelines

- Never commit secrets. If a credential slips into source control, rotate it immediately and force-push a scrubbed history when appropriate.
- Use least-privilege SQL accounts for generator operations.
- Document credential rotation in team runbooks and treat `.env` as disposable.

## FAQ

**Why move away from `xtraq.json`?**

The JSON file encouraged bespoke merges and duplicated defaults that proved difficult to audit. `.xtraqconfig` keeps the canonical view in one place, and the CLI handles merging with environment variables automatically.

**Can `.env` live outside the repository?**

Yes. Pass `XTRAQ_CONFIG_PATH` and `XTRAQ_GENERATOR_DB` as process environment variables in CI/CD or containerized environments. The CLI resolves relative hints against the working directory when needed.

**How do I absorb new keys from the template?**

Pull the latest changes, run `xtraq init --force`, review the resulting `.env` diff, and update this page if new keys are promoted to tracked configuration.

---

Future revisions will expand guidance once strict diff, JSON dual mode, and streaming features reach general availability.
