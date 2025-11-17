````markdown
# Snapshot Builder

This document is the internal handbook for the `xtraq snapshot` pipeline. It is **not** part
of the public documentation site; keep it in `src/SnapshotBuilder/README.md` and link to
it only from engineering artefacts (checklists, PRs, design notes).

The Snapshot Builder orchestrates the three-stage snapshot pipeline:

- Collect -> hydrate database metadata, honour cache fingerprints, build dependency graph.
- Analyze -> enrich procedure metadata (JSON analysis, forwarding, diagnostics).
- Write -> persist deterministic JSON artefacts for the generators and cache state for
  future runs.

Every change must stay in sync with `CHECKLIST.md`/`src/CHECKLIST.md` so the roadmap and
test plans mirror the actual behaviour.

## Operating Principles

- **Checklist first.** Update the Priority 1 sections when cache, telemetry, or snapshot
  semantics move.
- **Deterministic output.** Each stage must produce stable artefacts between runs. Refresh
  golden assets and log deviations as soon as you detect drift.
- **Guardrail driven diagnostics.** Reuse existing switches (`--verbose`,
  `XTRAQ_SNAPSHOT_SUMMARY[_PATH]`) and document any new flags here before wiring them into
  the CLI.
- **ENV-first configuration.** The `.env` flow owns configuration. Warn when legacy files
  resurface and capture remediation guidance.

## Pipeline Overview

| Stage   | Core responsibility                                                 | Artefacts                                         |
| ------- | ------------------------------------------------------------------- | ------------------------------------------------- |
| Collect | Fetch schema metadata, expand dependencies, seed cache fingerprints | Table metadata, procedure signatures, cache       |
| Analyze | Bind inputs/outputs, resolve JSON structures, track diagnostics     | Analyzer output, JSON enrichment, warnings        |
| Write   | Persist snapshots and cache for generator consumption               | `.xtraq/snapshots`, `.xtraq/cache`, run summaries |

Cache entries remain JSON for diffability (`.xtraq/cache/<fingerprint>.json`). Extend
`.ai/README-dot-xtraq.md` plus the roadmap checklist when cache formats change.

## Table Metadata Strategy

During the Collect stage the pipeline loads the full set of tables for every schema listed
in `XTRAQ_BUILD_SCHEMAS`. For each table we execute `TableColumnsListAsync`, which
explains telemetry counts such as `TableQueries.TableColumns = 219` even when a single
procedure is targeted. We need the full table catalogue to:

1. Resolve JSON projections that join across tables.
2. Generate table types for later build steps.
3. Detect schema drift between cached and live metadata.

When cache entries exist the collector reuses them and avoids database round-trips. The
spike only appears in cold snapshot runs (`--no-cache` or first-run scenarios). Optimisation ideas
for batching or dependency-driven selection live in `CHECKLIST.md` under
“Snapshot-/Caching-Strategie”.

## Procedure Dependency Discovery

- **Catalogue enumeration** – `DatabaseProcedureCollector` seeds the run with
  `_dbContext.StoredProcedureListAsync(string.Empty, cancellationToken)`, issuing the
  `StoredProcedureQueries.StoredProcedureListAsync` SQL against `sys.objects`/`sys.schemas`
  to harvest every procedure plus its `modify_date` for later cache comparisons.
- **Graph expansion** – the collector immediately follows with
  `_dbContext.StoredProcedureDependencyListAsync(...)`, which executes the
  `StoredProcedureQueries.StoredProcedureDependencyListAsync` query over
  `sys.sql_expression_dependencies`. A breadth-first search walks the edges so any
  downstream procedure invoked by a seed is included even when filters target a narrow
  schema or name pattern.
- **Cache verification** – for each candidate procedure the collector probes
  `ISnapshotCache.TryGetProcedure`. When a cached fingerprint exists the collector calls
  `_dependencyMetadataProvider.ResolveAsync(cachedDependencies, cancellationToken)` to
  refresh `LastModifiedUtc` values and `HasDependencyChanged` decides whether the run can
  reuse the cached snapshot or must fall back to a full analyse step.
- **Modify-date resolution** – `DatabaseDependencyMetadataProvider` memoises modify-date
  lookups per dependency (schema + name + `ProcedureDependencyKind`) via an internal
  `ConcurrentDictionary`. Depending on the dependency kind it queries:
  `sys.objects` (procedure `P`, function `FN`/`TF`/`IF`, view `V`, table `U`),
  `sys.table_types` joined with `sys.objects` (user-defined table types), or
  `sys.types` with an optional `sys.objects` join (scalar user-defined types). The resolved
  UTC timestamps feed both collector reuse checks and analyzer output.
- **Analyzer integration** – when a procedure is marked for analysis the
  `DatabaseProcedureAnalyzer` rebuilds its dependency list from parameter metadata,
  executed procedure references, and result-set projections (`Collect*Dependencies` helpers).
  The analyzer reuses the same `IDependencyMetadataProvider` to stamp each dependency with
  its current modify date before the snapshot is written. `FileSnapshotCache.RecordAnalysisAsync`
  persists the refreshed dependencies so warm runs avoid redundant database calls.

## Cache & Telemetry Layout

- Snapshots live under `<workingDirectory>/.xtraq/snapshots`.
- Cache fingerprints are stored in `<workingDirectory>/.xtraq/cache`.
- Database telemetry reports are persisted to `<workingDirectory>/.xtraq/telemetry` using
  a timestamped file name (`snapshot-YYYYMMDD-HHmmss.json` or `build-YYYYMMDD-HHmmss.json`) when the
  snapshot or build command executes with `--telemetry`.
- The `--no-cache` flag wipes the in-memory telemetry state between runs and skips reuse
  of cache files for the current execution only. On-disk entries stay intact; delete
  `.xtraq/cache` to force a complete cold run.

Document any new artefacts in this section before merging code.

## Result Set Resolution Flow

`ProcedureModelScriptDomBuilder` walks the ScriptDom AST emitted for each stored procedure
and derives structured result-set metadata without relying on the legacy
`StoredProcedureContentModel`. Understanding its resolution pipeline is essential when
adjusting analyzer fallbacks.

### Column Capture Strategy

- `ProcedureVisitor` captures only the outermost `SELECT` statements that contribute to the
  observable result sets. Statements nested inside `INSERT ... SELECT` or intermediate CTEs
  are skipped via `_selectStatementDepth` and `_selectInsertSourceDepth` guards.
- Each captured `QuerySpecification` builds an alias scope (`BuildAliasScope`) that merges
  named tables, table-valued functions, joins, table variables, APPLY expressions, derived
  tables, and cached CTE definitions. Table metadata is hydrated from `.xtraq/cache` via
  `TableMetadataProvider`/`TableTypeMetadataProvider` and cached in `TableMetadataLookup`
  / `TableTypeMetadataCache`.
- `BuildColumnFromScalar` converts every `SelectScalarExpression` into a
  `ProcedureResultColumn`. Direct column references copy schema/table/column metadata from
  the active alias scope. Computed expressions recurse through the AST so nested `CASE`,
  `COALESCE`, `IIF`, `CAST`, and subqueries can surface usable probes.
- `Select *` projections expand immediately against the alias scope to keep result-set
  ordering deterministic. Each expanded column is seeded with the metadata that backed the
  alias entry.

### Source Binding & JSON Path Mapping

- Alias hints and JSON path bindings keep dotted projection names aligned with the correct
  source alias. `RegisterAliasHint` remembers the alias that produced a column, while
  `RegisterJsonPathBinding` records column maps for JSON-shaped projections. This allows
  `ApplyAliasColumnFallback` and `ApplyJsonPathMetadata` to enrich computed JSON columns
  with the original table metadata when the AST omits an explicit alias qualifier.
- When metadata is sourced through dotted JSON paths the analyzer marks the column as
  nullable—JSON assembly frequently materialises optional properties even when the base
  column is not nullable.
- `ApplyResultSetMetadata` and `ApplyForJsonMetadata` normalise `FOR JSON` projections. All
  JSON result sets default to `nvarchar(max)` and record `ReturnsJson`, `ReturnsJsonArray`,
  and optional `JsonRootProperty` flags. Nested array builders (`JSON_QUERY(CONCAT('[', …
']'))`) are flattened through `TryInferJsonArrayMetadata` so the generator receives an
  actual item descriptor instead of a raw string.

### Type Propagation & Fallback Order

The analyzer prefers metadata gathered from the concrete source before falling back to
heuristics:

1. **Table & CTE metadata** – `ResolveTableColumns` and `ResolveCteColumns` provide full
   field descriptions (type, length, nullability) for direct column references and
   expanded `SELECT *` entries.
2. **Table-valued parameters & variables** – declarations are cached in
   `_tableTypeBindings`, allowing table variables and TVPs to reuse the cached table-type
   definitions.
3. **Scalar variable metadata** – parameter and variable declarations populate
   `_scalarVariableMetadata`. Columns that reduce to a single variable inherit the recorded
   SQL type or user-defined type reference.
4. **Probe propagation** – nested projections and `COALESCE`/`CASE` branches surface probe
   columns. `ApplyProbeMetadata` copies SQL type information, user-defined types, and
   references whenever a single unambiguous probe exists.
5. **Expression merging** – complex expressions merge operand metadata through
   `MergeBinaryOperandMetadata`, which uses `SqlTypePrecedence` to break ties and flips
   `IsNullable` on when any operand permits nulls. Built-in helpers cover `IIF`, `CASE`,
   arithmetic operators, and JSON helper functions.
6. **Built-in function fallbacks** – functions such as `COUNT`, `COUNT_BIG`, `ROW_NUMBER`,
   `SUM`, `CONCAT`, `STRING_AGG`, and `DB_NAME` receive deterministic fallbacks via
   `ApplyBuiltInFunctionFallback`. `SUM` additionally attempts to reuse operand metadata
   before defaulting to `decimal(38,10)`.
7. **Literal fallbacks** – standalone literals set baseline metadata (`nvarchar` for
   strings, `int`/`decimal` for numeric literals, nullable semantics for `NULL`). Null-only
   projections in non-JSON result sets fall back to `int`; JSON result sets prefer
   `nvarchar(max)`.

`ApplyUserTypeResolution` runs at the end of each propagation path to ensure that user
defined types win over bare SQL type guesses whenever cached metadata contains a `TypeRef`.

### Diagnostics & Instrumentation

`[json-type-*]` probes and additional resolution traces gathered via `LogVerbose`.
registration and lookup traces. The loader now promotes values from `.env`, so the flag
also works without exporting environment variables manually. This is the quickest way
to understand why `ApplyAliasColumnFallback` failed to find a unique match.
Priority 1 checklist so downstream owners know which heuristics were adjusted.

## Procedure Cache Artifact

`debug/.xtraq/cache/procedures.json` is the authoritative catalogue produced after each
snapshot run. Every entry mirrors one stored procedure snapshot and captures the metadata we need
for cache invalidation:

- `schema`/`name` – fully qualified procedure identifier used for dependency lookups.
- `lastModifiedUtc` – SQL Server `modify_date`; drift triggers a full rehydrate on the
  next snapshot.
- `snapshotHash`/`snapshotFile` – deterministic checksum plus the relative snapshot path
  under `.xtraq/snapshots/procedures`.
- `lastAnalyzedUtc` – timestamp of the analyzer run that produced the current snapshot.
- `dependencies[]` – flattened dependency graph. `kind` maps to
  `ProcedureDependencyKind` (`1=Procedure`, `2=Function`, `3=View`, `4=Table`,
  `5=UserDefinedTableType`, `6=UserDefinedType`). `lastModifiedUtc` on each dependency is
  captured at snapshot time so the cache can short-circuit warm runs when upstream artefacts
  stay unchanged.
  Warm snapshot runs cache every `modify_date` lookup per dependency instance to avoid duplicate
  probes (`DatabaseDependencyMetadataProvider`).

Cross-check telemetry when assessing cache health: a warm snapshot with unchanged
`lastModifiedUtc` values should show near-zero `Collector.Tables` lookups, whereas a cold
run (or an altered dependency) naturally replays the table scan pattern captured in the
telemetry sample below.

## Telemetry Snapshots (2025-10-31)

### Cold cache

`debug/.xtraq/telemetry/snapshot-20251031-210545.json` represents the latest cold snapshot after
resetting the cache. The collector recorded **262 queries** and **4.116 seconds** of
database time. The hot spots align with the cache artefact described above:

| Slice                        | Queries | Duration (s) |
| ---------------------------- | ------- | ------------ |
| `Collector.Tables`           | 214     | 3.185        |
| `Collector.TableTypes`       | 36      | 0.440        |
| `Collector.Functions`        | 4       | 0.135        |
| `Collector.StoredProcedures` | 3       | 0.190        |

Top operations are `TableQueries.TableColumns` (200 executions, 2.992 s) and
`TableTypeQueries.TableTypeColumns` (35 executions, 0.427 s). The former highlights the
per-schema column scans that feed cache fingerprints during cold snapshot runs; the latter aligns
with the **34 unique user-defined table types** recorded in `procedures.json`.

### Warm cache

`debug/.xtraq/telemetry/snapshot-20251031-221220.json` captures a follow-up snapshot without
purging the cache after introducing dependency-level memoization.
Despite processing the same 557 procedures, the collector now issues only
**185 modify-date probes** totalling **2.484 seconds**:

| Slice                        | Queries | Duration (s) |
| ---------------------------- | ------- | ------------ |
| `DbContext.Query`            | 183     | 2.238        |
| `Collector.Dependencies`     | 1       | 0.091        |
| `Collector.StoredProcedures` | 1       | 0.154        |

The dominant operations (`QueryModifyDateAsync`,
`QueryUserDefinedTypeModifyDateAsync`, and `QueryTableTypeModifyDateAsync`) execute once
per unique dependency recorded in `procedures.json`. By caching the resolved
`modify_date` within `DatabaseDependencyMetadataProvider`, warm snapshot runs avoid the 30× query
amplification previously caused by repeating lookups for shared artefacts (for example
`core.Context` and `internal.CrudResultThrowError`). The cache still suppresses the
`Collector.Tables` hot spot observed during cold snapshot runs while keeping dependency checks
deterministic.

Capture new telemetry snapshots whenever cache semantics or dependency tracking change
and keep this section in sync with the observed behaviour.

## Build Telemetry (2025-10-31)

`debug/.xtraq/telemetry/build-20251031-232039.json` is the first populated build report
after wiring `--telemetry` through the CLI build command. The generator records
phase-level timings and every emitted C# artefact:

| Phase      | Duration (ms) | Artefacts |
| ---------- | ------------- | --------- |
| TableTypes | 141.24        | 35        |
| Procedures | 560.34        | 540       |
| DbContext  | 35.13         | 0         |

The `Files` array lists 575 items with their relative path, size in bytes, category
(top-level folder), and UTC write timestamp. A `Summary` block at the end aggregates the
file totals (count, cumulative size, largest files, per-category breakdown). Use these
metrics to spot abnormal growth in generated output or to compare branch-to-branch
performance. Regenerate the build telemetry whenever template changes affect the number
or size of generated files and update this section with noteworthy shifts.

## Validation Loop

```cmd
:: Cold snapshot to validate full pipeline
dotnet run --project src\Xtraq.csproj -- snapshot -p debug --no-cache

:: Warm snapshot to exercise cache paths
dotnet run --project src\Xtraq.csproj -- snapshot -p debug

:: Targeted procedure refresh for delta validation
dotnet run --project src\Xtraq.csproj -- snapshot -p debug --procedure workflow.WorkflowListAsJson

:: Append --telemetry to any snapshot or build command above if you need a JSON telemetry snapshot.
```
````

After each run compare generator output (`debug/Xtraq`) and snapshot changes, then log
findings in the checklist review section.

## Diagnostics & Recipes

- **Cold verbose snapshot** – highlights every analyzer decision.

  ```cmd
  set XTRAQ_LOG_LEVEL=debug
  dotnet run --project src\Xtraq.csproj -- snapshot -p debug --no-cache --verbose
  ```

- **Cache comparison** – ensures warm runs reuse metadata without drift.

  ```cmd
  dotnet run --project src\Xtraq.csproj -- snapshot -p debug --verbose
  ```

- **Targeted rerun** – isolates a single procedure for regression hunting.

  ```cmd
  set XTRAQ_LOG_LEVEL=debug
  dotnet run --project src\Xtraq.csproj -- snapshot -p debug --no-cache --procedure identity.UserListAsJson --verbose
  ```

Structured tags emitted during verbose runs:

| Tag                       | Purpose                                                         |
| ------------------------- | --------------------------------------------------------------- |
| `[json-type-table]`       | Column bound to cached table metadata.                          |
| `[json-type-upgrade]`     | Analyzer promoted a fallback type.                              |
| `[json-type-summary]`     | Per-procedure JSON enrichment totals.                           |
| `[json-type-run-summary]` | Run-level aggregate covering resolved/upgraded JSON columns.    |
| `[proc-forward-expand]`   | Placeholder result set expanded via procedure forwarding logic. |

Keep representative log excerpts under version control when closing checklist items.

## `FOR JSON` Validation Checklist

1. Inline comments do not affect ScriptDom parsing; confirm `Json.RootProperty`/`Json.IsArray` in the snapshot (`.xtraq/snapshots`).
2. If ScriptDom misses metadata, the textual fallback still marks arrays unless
   `WITHOUT_ARRAY_WRAPPER` is present; inspect verbose logs for `[json-type-summary]`.
3. Nested `JSON_QUERY` projections flag `IsNestedJson=true`; validate both the snapshot and
   generated records in `debug/Xtraq/<Schema>/<Proc>.cs`.

Escalate anomalies with SQL text, verbose trace, and the relevant snapshot file.

## Performance Baseline (2025-10-26)

Recorded against `samples/restapi` with `XTRAQ_LOG_LEVEL=info`. Refresh when pipeline
changes impact throughput.

| Scenario        | Description                                                          | Command                                                                                                         | Total (ms) | Collect (ms) | Analyze (ms) | Write (ms) |
| --------------- | -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- | ---------- | ------------ | ------------ | ---------- |
| Cold Cache      | End-to-end run without cache reuse; forces full analysis and hashing | `dotnet run --project src/Xtraq.csproj -- snapshot -p debug --no-cache`                                         | 7832       | 260          | 7319         | 242        |
| Warm Cache      | Repeated run leveraging cached analysis results                      | `dotnet run --project src/Xtraq.csproj -- snapshot -p debug`                                                    | 9238       | 3465         | 5493         | 227        |
| Procedure Delta | Targeted refresh after cache invalidation                            | `dotnet run --project src/Xtraq.csproj -- snapshot -p debug --procedure workflow.WorkflowListAsJson --no-cache` | 645        | 246          | 185          | 202        |

Warm-cache numbers include the cost of summarising unchanged artefacts; expect variance based
on ambient load and network latency.

## Instrumentation & Telemetry

- Use `--verbose` plus `XTRAQ_LOG_LEVEL=info` (or `debug`) for per-stage timings.
- Enable `XTRAQ_SNAPSHOT_SUMMARY=1` or supply `XTRAQ_SNAPSHOT_SUMMARY_PATH=<file>` to
  persist machine-readable summaries.
- New telemetry fields must be documented here and mirrored in `CHECKLIST.md` before
  merging.
- Database telemetry reports generated by `IDatabaseTelemetryCollector` follow this schema
  (see `.xtraq/telemetry/snapshot-*.json`).
- Build telemetry reports serialised through `BuildTelemetryReport` include `Phases`,
  `Files`, and `Summary` sections (see `.xtraq/telemetry/build-*.json`) to surface
  generator throughput and output footprint.

### Snapshot Summary Payload Example

```jsonc
{
  "timestamp": "2025-10-27T09:14:05.381Z",
  "project": "debug",
  "stages": {
    "collect": { "durationMs": 312, "cacheHits": 128, "cacheMisses": 6 },
    "analyze": { "durationMs": 4412, "jsonResolved": 184, "jsonUpgraded": 27 },
    "write": { "durationMs": 205, "artifacts": 623 }
  },
  "procedures": {
    "identity.UserListAsJson": { "jsonResolved": 12, "jsonUpgraded": 3 }
  }
}
```

Point CI pipelines at `debug/snapshot-summary.json` (or similar) and attach the file to
build artefacts. Update monitoring checklists whenever the payload grows.

## Troubleshooting & Follow-ups

- Re-run the validation loop after changing cache fingerprints, analyzer heuristics, or
  writer output; log determinism regressions immediately.
- Store targeted run artefacts (`debug/.xtraq/snapshots`, `debug/.xtraq/telemetry`) when
  sharing findings.
- Optimisation ideas such as batched table lookups, dependency-aware cache hydration, and
  richer telemetry visualisation live in `CHECKLIST.md`; keep this README aligned as work
  progresses.

```

```
