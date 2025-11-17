---
title: Roadmap
description: Tracking upcoming work and temporary deferrals.
---

## Test Matrix

- [x] Re-enable CI execution for .NET 10 once integration test runtime stabilises. _(Matrix now covers net8.0 and net10.0 using the latest preview SDK while we wait for GA.)_
- [x] Extend the publish pipeline so the global tool ships a net10.0 asset once the hosted runners expose a compatible SDK or we provide it explicitly. _(Workflow installs the preview SDK and packs multi-target assets.)_

## ResultSet Naming

Recent changes shipped the CTE-aware resolver, JSON root alias extraction, and the streaming helper surface (`ProcedureExecutor.StreamResultSetAsync` plus generated `StreamResult…Async` wrappers). The checklist below captures what landed during the naming work and helps track any follow-up tasks.

- [x] Infer final SELECT base table for CTE-backed procedures to enable renames. _(Handled via CTE-aware `ResultSetNameResolver` and `TryResolve_WithCteReference_UsesBaseTable` test.)_
- [x] Add explicit regression tests that dynamic SQL (`EXEC(@sql)`) keeps generic result-set names. _(Covered via `ResultSetNameResolverTests`.)_
- [x] Add negative tests covering deliberately invalid SQL to verify generic fallback behavior. _(Result set naming falls back in `TryResolve_WithInvalidSql_ReturnsNull`.)_
- [x] `FOR JSON PATH` root alias extraction so JSON-only result sets can reuse the declared root name. _(Handled via `ResultSetNameResolver.TryResolve` and `TryResolve_WithForJsonRootAlias_UsesRootName` test.)_
- [x] Finalise and document the streaming naming convention once helper APIs ship. _(Helpers emit `StreamResult{Suffix}Async`; documented in `docs/content/3.reference/5.result-set-naming.md`.)_

## Procedure Extensibility

- [x] Prototype an opt-in fluent builder that wraps generated `IXtraqDbContext` extensions without leaking into the artifact surface. _(Emitted as `ProcedureBuilderExtensions` with fluent `ProcedureCallBuilder` support.)_
- [x] Provide streaming-aware builder overloads so fluent composition can opt into `StreamResult…Async` pipes instead of buffering by default. _(Includes `ProcedureStreamBuilder` with aggregation helpers.)_
- [x] Document recommended layering (application partials vs. external builders) alongside guidance for DI scoping and XML comments, based on the evaluation in `docs/content/3.reference/1.api-integration.md`.

## Transaction API

- [ ] Introduce a DI-scoped transaction orchestrator that keeps a shared connection per request and uses savepoint-backed nesting to expose explicit `Begin/Commit/RollbackAsync` helpers. _(Implementation progressing: orchestrator template, savepoint behavior, DI coverage, and pipeline accessor resolved; policy coordination remains outstanding.)_
- [x] Extend generated DbContext options and service registrations so consumers can resolve the orchestrator without breaking existing usage patterns or hand-written partials. _(Options now surface a `TransactionOrchestratorFactory` hook and DI registers `IXtraqTransactionOrchestrator` by default.)_
- [ ] Expose transaction-aware execution policies so global defaults or per-procedure overrides (e.g., ambient participation, isolation level) can be configured declaratively. _(Design follows orchestrator prototype.)_
- [x] Ensure the pipeline execution context resolves the orchestrator via dependency injection, enabling policies to coordinate with logging and telemetry components. _(Procedure pipelines now require an accessor-backed `IXtraqDbContext` and surface the orchestrator via `ProcedureExecutionContext`.)_
- [ ] Align the design with the planned Entity Framework integration by supporting ambient `DbContext` transactions and connection reuse when both stacks operate side by side. _(Joint review with Framework Integrations track.)_
- [ ] Backfill regression tests covering nested savepoint flows, auto-rollback on dispose, and streaming pipeline compatibility.
- [ ] Document orchestrator usage, DI configuration, and migration guidance alongside the SpocR transition playbook.

## Fluent Pipeline Iteration

- [x] Reshape the fluent surface into dedicated configuration and execution types (`ProcedureCallPipeline`, `ProcedureCallExecution`) so interceptors, validation, and telemetry wrap the pipeline without deep delegate nesting. _(Implemented via the new `ProcedurePipelineExtensions` template and updated unit tests.)_
- [x] Introduce composable execution policies (`IProcedureExecutionPolicy`) with `WithPolicy(...)` wiring for retry, timeout, and circuit-breaking concerns. _(Policies now wrap execution through `ProcedureCallPipeline.ApplyPolicies`.)_
- [x] Add optional pipeline labels (for example `WithLabel("post-map")`) that emit structured diagnostics and metric scopes for downstream observability tooling. _(Labels flow through `ProcedureExecutionContext` and policy capture tests.)_
- [x] Retire the initial `ProcedureCallBuilder`/`ProcedureStreamBuilder` shapes and align namespaces/method signatures with the refined design while we are still in alpha. _(Legacy builders removed from `ProcedureBuilders.spt`; docs & tests updated.)_

## Minimal API Integration

- [ ] Ship `RouteHandlerBuilder` extensions (for example `.WithProcedurePipeline(...)`) that bind the fluent pipeline to standard `MapGet`/`MapPost` delegates without replacing the Minimal API entry points.
- [ ] Provide streaming helpers that expose `IAsyncEnumerable` and NDJSON writer adapters while returning `RouteHandlerBuilder`, reusing the existing fluent streaming semantics.
- [ ] Deliver optional scaffolding (template, analyzer, or source generator) that emits the `.WithProcedure...` calls beside generated procedures to standardise adoption.

## Template Conventions

- [ ] Document the rationale for the `.spt` template extension (Xtraq Template) and capture whether we keep the naming or migrate to a more conventional suffix as part of the templating guide.

## Framework Integrations

- [ ] Offer `UseXtraqProcedures` extensions for EF `DbContext` that register the generated `IXtraqDbContext` within the same DI scope, reusing EF's connection pooling and transaction management via `DbContext.Database.GetDbConnection()`.
- [ ] Add an adapter layer (`ProcedureResultEntityAdapter`) that maps procedure rows into tracked EF entities or keyless query types to enable hybrid read patterns without manual mapping.
- [ ] Support EF interceptors so procedure executions can join ambient transactions and leverage EF's logging providers, keeping configuration consistent across direct EF queries and Xtraq procedure calls.

## Telemetry

- [ ] Evaluate whether to migrate CLI telemetry to align with the guidance at https://aka.ms/dotnet-cli-telemetry and document the rollout decision.

## Documentation Alignment

- [ ] Highlight .NET 10.0 as the primary/default target framework across documentation and samples.
- [ ] Remove the remaining .NET 8.0 examples from `docs/content/3.reference/1.api-integration.md` once replacement guidance is ready.

## Table Types

First milestone: slim the table-type surface so `dotnet build` only emits UDTT wrappers actually used by the procedures we keep. That means teaching the metadata layer to map **procedure → table type** dependencies, then trimming both snapshotting and generator phases to that set.

- [x] Scope snapshotting and generation to table types referenced by allow-listed procedures (parameter usage or AST-detected consumption).
  - [x] Extend the metadata snapshot so each `ProcedureDescriptor` carries the names of referenced UDTT parameters (source: `ProcedureParameter.TableTypeName`).
  - [x] When `BuildSchemas` or `XTRAQ_BUILD_SCHEMAS` filters are active, build a `HashSet` of required table types before invoking `TableTypesGenerator`.
  - [x] Update `TableTypesGenerator.Generate` to accept (or compute) that filtered set so we stop writing unused UDTT artifacts.
  - [x] Regression tests: run the sample generator with a one-schema allow list and assert only referenced table types land in `samples/restapi/Xtraq`.
- [x] Enable cross-schema table type emission based on dependency analysis so schema allow-lists no longer gate referenced types.
- [x] Extend validation tests to cover allow-list filtering with `XTRAQ_BUILD_SCHEMAS`.
- [x] Document binder customization hooks for UDTT execution.
- [x] Evaluate an analyzer that verifies `ITableType` usage matches expected schema parameters.

## Next Steps

- Prototype the DI-scoped transaction orchestrator and corresponding execution policy integration to validate coexistence with the planned Entity Framework support and surface API gaps early.
