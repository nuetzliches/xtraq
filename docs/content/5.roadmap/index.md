---
title: Roadmap
description: Tracking upcoming work and temporary deferrals.
---

## Test Matrix

- [ ] Re-enable CI execution for .NET 10 once integration test runtime stabilises. _(Temporarily disabled in CI until a stable SDK lands; current runs target net8.0 only.)_
- [ ] Extend the publish pipeline so the global tool ships a net10.0 asset once the hosted runners expose a compatible SDK or we provide it explicitly.

## ResultSet Naming

- [ ] Infer final SELECT base table for CTE-backed procedures to enable renames.
- [x] Add explicit regression tests that dynamic SQL (`EXEC(@sql)`) keeps generic result-set names. _(Covered via `ResultSetNameResolverTests`.)_
- [ ] Add negative tests covering deliberately invalid SQL to verify generic fallback behavior.
- [ ] Finalise and document the streaming naming convention once helper APIs ship.
- [ ] `FOR JSON PATH` root alias extraction so JSON-only result sets can reuse the declared root name.

## Table Types

- [ ] Scope snapshotting and generation to table types referenced by allow-listed procedures (parameter usage or AST-detected consumption).
- [ ] Enable cross-schema table type emission based on dependency analysis so schema allow-lists no longer gate referenced types.
- [ ] Extend validation tests to cover allow-list filtering with `XTRAQ_BUILD_SCHEMAS`.
- [ ] Document binder customization hooks for UDTT execution.
- [ ] Evaluate an analyzer that verifies `ITableType` usage matches expected schema parameters.
