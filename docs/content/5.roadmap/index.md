---
title: Roadmap
description: Release-candidate milestones and near-term tasks.
---

## RC Timeline

- **T-7 days**: Tag RC1, publish CLI artifacts, run docs build, and refresh migration guide.
- **T-3 days**: Address RC feedback, rerun full test suite, verify telemetry/CLI summaries and parameter-binding docs.
- **Release**: Tag stable, publish packages, update README/landing, and archive RC notes.

## Maintenance Updates

- Legacy SchemaManager, SchemaSelectionContext, and SchemaStatusEvaluator were removed. Snapshot/build commands already run exclusively through `XtraqCliRuntime`, so the unused manager layer was deleted to reduce dead code and lower maintenance cost.

