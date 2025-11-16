---
title: Roadmap
description: Tracking upcoming work and temporary deferrals.
---

## Test Matrix

- [ ] Re-enable CI execution for .NET 10 once integration test runtime stabilises. _(Temporarily disabled in CI until a stable SDK lands; current runs target net8.0 only.)_
- [ ] Extend the publish pipeline so the global tool ships a net10.0 asset once the hosted runners expose a compatible SDK or we provide it explicitly.

## ResultSet Naming

Recent changes shipped the CTE-aware resolver, JSON root alias extraction, and the streaming helper surface (`ProcedureExecutor.StreamResultSetAsync` plus generated `StreamResult…Async` wrappers). The checklist below captures what landed during the naming work and helps track any follow-up tasks.

- [x] Infer final SELECT base table for CTE-backed procedures to enable renames. _(Handled via CTE-aware `ResultSetNameResolver` and `TryResolve_WithCteReference_UsesBaseTable` test.)_
- [x] Add explicit regression tests that dynamic SQL (`EXEC(@sql)`) keeps generic result-set names. _(Covered via `ResultSetNameResolverTests`.)_
- [x] Add negative tests covering deliberately invalid SQL to verify generic fallback behavior. _(Result set naming falls back in `TryResolve_WithInvalidSql_ReturnsNull`.)_
- [x] `FOR JSON PATH` root alias extraction so JSON-only result sets can reuse the declared root name. _(Handled via `ResultSetNameResolver.TryResolve` and `TryResolve_WithForJsonRootAlias_UsesRootName` test.)_
- [x] Finalise and document the streaming naming convention once helper APIs ship. _(Helpers emit `StreamResult{Suffix}Async`; documented in `docs/content/3.reference/5.result-set-naming.md`.)_

## Procedure Extensibility

- [ ] Prototype an opt-in fluent builder that wraps generated `IXtraqDbContext` extensions without leaking into the artifact surface.
- [ ] Provide streaming-aware builder overloads so fluent composition can opt into `StreamResult…Async` pipes instead of buffering by default.
- [ ] Document recommended layering (application partials vs. external builders) alongside guidance for DI scoping and XML comments, based on the evaluation in `docs/content/3.reference/1.api-integration.md`.

## Table Types

- [ ] Scope snapshotting and generation to table types referenced by allow-listed procedures (parameter usage or AST-detected consumption).
- [ ] Enable cross-schema table type emission based on dependency analysis so schema allow-lists no longer gate referenced types.
- [ ] Extend validation tests to cover allow-list filtering with `XTRAQ_BUILD_SCHEMAS`.
- [ ] Document binder customization hooks for UDTT execution.
- [ ] Evaluate an analyzer that verifies `ITableType` usage matches expected schema parameters.
