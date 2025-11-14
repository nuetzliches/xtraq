---
title: Branch Protection & PR Validation
description: Guard rails for merging into master
navigation:
  icon: i-heroicons-shield-check
---

This guide captures the recommended guard rails for collaborative development on the `master` branch.

## Protected Branch Rules

Apply the following protection settings to `master` (and mirror them on long-lived release branches when they appear):

- **Require pull request reviews**: At least one approving review, and dismiss approvals when new commits arrive.
- **Require status checks to pass** before merging. Enable the following checks:
  - `build` (dotnet build)
  - `run (net8.0)`
  - `run (net10.0)`
  - `code-style` (format verification)
  - `sql-sample` (SQL container health)
  - `tests` (multi-target tests with coverage gate)
- **Require branches to be up to date** before merging.
- **Restrict who can push** to `master` to the release engineering team.
- **Require signed commits** when possible (optional but recommended).
- **Include administrators** so that the rules apply to maintainers as well.

## Pull Request Checklist

Every PR should:

1. Target `master` (or a designated release branch) and remain rebased on the latest tip.
2. Pass all CI workflows (`code-style.yml`, `sql-container.yml`, `tests.yml`, `publish-global-tool.yml`).
3. Provide checklist updates when roadmap items move state.
4. Include documentation changes whenever workflows, commands, or public APIs evolve.
5. Reference the relevant roadmap entry in the PR description to ease traceability.

## Release Flow

1. Merge feature branches via PR into `master` once all required checks succeed.
2. Create annotated tags `vX.Y.Z` to trigger the global tool packaging workflow.
3. Use the manual dispatch variant of `publish-global-tool.yml` with a NuGet API key when publishing to NuGet.org.
4. Draft release notes summarising merged roadmap items.

Keeping the protection rules active ensures the automated SQL container, multi-target tests, and coverage gate all stay enforced before code reaches the default branch.
