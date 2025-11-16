authoritative: true # Quelle gilt als maßgeblich
aiTags: [cli, build, generation, pipeline]

# xtraq Documentation Workspace

This directory hosts the Docus- and Nuxt-based documentation site that accompanies the Xtraq CLI and generator. The original concept draft has been replaced with the working details below.

## Documentation Goals

- Keep product and contributor guidance in a single, versionable repository.
- Reduce onboarding time through task-focused guides and reference material.
- Expose authoritative information for automation (CLI help, configuration schema, generator behaviour).
- Stay AI-friendly: docs are structured so tooling can extract behaviour contracts and cross-link code.

## Technology Stack

- **Framework**: [Docus 5.2.1](https://docus.dev/en) on [Nuxt 4.2.1](https://nuxt.com/).
- **Language tooling**: TypeScript-enabled Nuxt workspace, linted via `eslint.config.mjs`.
- **Hosting**: Static export with `npm run generate`; deployed via GitHub Pages workflow.
- **LLM support**: Docus `llms` block configured in `docs/nuxt.config.ts` (see `docs/content/4.meta/2.documentation-stack.md`).
- **Formatting**: Markdown content lives in `docs/content/` using frontmatter metadata and Docus MDC features.

## Current Content Layout

```
docs/
  README.md               # This file
  nuxt.config.ts          # Docus + Nuxt configuration (LLM, theme, navigation)
  content.config.ts       # Content pipeline settings
  content/
    index.md              # Landing page
    1.getting-started/    # Installation and quick start guides
    2.cli/                # Command reference & behaviour contracts
    3.reference/          # Configuration schema, table types, JSON handling
    4.meta/               # Documentation stack, formatting rules
    5.roadmap/            # Roadmap and decision records
    app/ components/      # Custom MDC components
  public/                 # Static assets served verbatim
  scripts/                # Content validation helpers
```

Numbered directories keep navigation stable and make version bumps explicit. Additional versioned trees (e.g., `content/v1/`) can be introduced later; for now the live docs describe the current toolchain.

## Authoring Guidelines

1. **Frontmatter**: Include `title`, `description`, and where relevant `versionIntroduced`, `experimental`, and `aiTags`. See examples under `docs/content/3.reference/`.
2. **Links**: Prefer relative links (`../path/file.md`) so the static build and GitHub view stay in sync.
3. **Code fences**: Specify language (` ```csharp `, ` ```bash `) for proper highlighting and linting.
4. **LLM tags**: When a page feeds automation (CLI contracts, configuration schema) add `aiTags` to support targeted embeddings.
5. **Decision records**: Capture scope decisions under `docs/content/5.roadmap/` (see `udtt-analyzer-evaluation.md`).

## Local Development Workflow

Prerequisites: Node.js 20 LTS (or newer).

```bash
cd docs
npm install
npm run dev
```

Open <http://localhost:3000>. To mirror GitHub Pages paths, set `NUXT_APP_BASE_URL="/xtraq/"` in your shell or `.env`.

### Quality Gates

- `npm run lint` – ESLint, Markdown lint, and Docus validation.
- `npm run generate` – produces the static site (`.output/public`). Run this before publishing.
- `scripts/validate-frontmatter.mjs` – checks required frontmatter fields and reports duplicates.

CI mirrors these steps during pull requests. Keep generated artifacts out of git; only commit source Markdown, configuration, and assets.

## AI & Automation Readiness

- Behaviour contracts for CLI commands live in `docs/content/2.cli/` with accompanying JSON snippets.
- Shared terminology is tracked in `docs/content/4.meta/` (glossary and stack overview).
- Table-type changes and snapshot behaviour are documented under `docs/content/3.reference/4.table-types.md`, including hooks for custom binders.
- Roadmap checklists are canonical in `docs/content/5.roadmap/index.md` with supporting decision notes in adjacent files.

When adding new automation surfaces (e.g., error code matrices), keep them machine-readable by supplying JSON/YAML blocks alongside narrative explanations.

## Future Enhancements

- Introduce versioned content folders once the CLI ships a stable major release.
- Add automated link checking and broken-image detection to the docs pipeline.
- Evaluate interactive playgrounds for generator output previews after the core roadmap items land.

---

Last updated: 2025-11-16
