# SnapshotBuilder Modularization Plan

## Goal

Break down the existing snapshot build pipeline into dedicated modules so AST parsing,
metadata enrichment, and snapshot persistence can run independently. The end state must
support offline snapshots, deterministic unit tests, and faster iteration on individual
stages.

## Current Pain Points

- **Monolithic analyzer**: `DatabaseProcedureAnalyzer` builds models, enriches metadata,
  and writes telemetry in one flow, which complicates isolated testing.
- **Hidden dependencies**: AST visitors read global caches (`TableMetadataLookup`), making
  it difficult to substitute offline fixtures.
- **Tight coupling to ScriptDom**: The analyzer cannot be reused without a database
  connection because enrichment is interleaved with AST traversal.
- **Difficult diagnostics**: The pipeline emits verbose logs, but there is no way to run a
  single stage (e.g., AST capture) without executing the full analyzer.

## Proposed Modules

1. **Collection Stage** _(existing)_

   - Responsibility: pick procedures and load raw definitions.
   - Output: `ProcedureCollectionResult` with descriptors and definitions.

2. **AST Stage** _(new abstraction)_

   - Responsibility: convert SQL definitions into `ProcedureModel` (AST-backed).
   - Implementation: wrap `ProcedureModelScriptDomBuilder`; cache ScriptDom artifacts when
     verbose mode is enabled.
   - Output: `ProcedureModel` plus AST telemetry snapshots for each procedure.

3. **Metadata Enrichment Stage** _(new abstraction)_

   - Responsibility: enrich `ProcedureModel` with database metadata, function JSON info,
     and cached snapshot deltas.
   - Interface: `IProcedureMetadataEnricher.EnrichAsync(ProcedureMetadataEnrichmentRequest)`
     so enrichment can run offline using cached metadata.
   - Output: enriched `ProcedureModel` with resolved types and dependency list.

4. **Artifact Composition Stage**

   - Responsibility: map enriched models to snapshot DTOs (`ProcedureAnalysisResult`).
   - Implementation: lightweight mapper that flattens JSON structures and prepares
     diagnostics (column counts, JSON paths).

5. **Persistence Stage** _(existing writer)_
   - Responsibility: write artifacts to disk, update cache, emit telemetry.

## Step-by-Step Execution

1. **Document Interfaces** _(complete)_

   - Define interfaces for the AST and metadata enrichment stages.
   - Describe expected inputs/outputs and error handling rules.
   - Capture decisions in this plan file.

2. **Introduce Contracts in Code**

   - Add `IProcedureAstBuilder` and `IProcedureMetadataEnricher` interfaces in
     `src/SnapshotBuilder/Analyzers`.
   - Provide default implementations that proxy to existing logic.

3. **Refactor Analyzer to Use Pipeline**

   - Update `DatabaseProcedureAnalyzer` to compose the new interfaces.
   - Keep behaviour intact by delegating to `ProcedureModelScriptDomBuilder` and existing
     enrichment code while improving separation.

4. **Extract Enrichment Responsibilities**

   - Move enrichment helpers (`ApplySnapshotColumnMetadata`, `EnrichColumnRecursiveAsync`,
     etc.) into a dedicated module under `src/SnapshotBuilder/Metadata`.
   - Ensure the orchestrator can execute enrichment without requiring a live database when
     cached metadata is present.

5. **Testing & Offline Harness**
   - Add unit tests for AST builder and metadata enricher using fixture definitions.
   - Provide a CLI flag (e.g., `--stage ast`/`--stage enrich`) to run individual stages for
     debugging.

## Deliverables

- Updated code structure reflecting the new interfaces and modules.
- Tests covering AST and metadata stages separately.
- Documentation updates (`src/SnapshotBuilder/ModularizationPlan.md` and
  `src/SnapshotBuilder/README.md`) that explain the new pipeline.

## Open Questions

- Should AST snapshots be cached on disk for verbose runs to ease debugging?
- How should cached metadata be injected for offline runs (DI container vs. file-based
  provider)?
- Do we need to version interfaces to support future pipeline stages (e.g., preflight
  validation)?
