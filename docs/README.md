authoritative: true # canonical
aiTags: [cli, build, generation, pipeline]

# xtraq Documentation Workspace

Audience: consumers of the CLI. Quickstarts stay lean; technical depth lives in clearly labelled reference pages.

## Goals

- Start fast: install, `xtraq`, register services, one API sample.
- Keep separation: product-facing docs up front, technical/automation details in dedicated deep dives.
- Versioned source only: commit sources, never generated artefacts.

## Structure

```
docs/
  content/
    1.getting-started/  # Installation, Quickstart, What is Xtraq?
    2.cli/              # CLI reference
    3.reference/        # Config, generator workflow, integrations
    4.meta/             # Docs stack, formatting rules
    5.roadmap/          # Roadmap & decisions
  public/               # Static assets
```

## Authoring guidelines

- Write for users, not the source code. Bundle technical detail into reference/deep-dive pages.
- Keep Quickstart examples short (service registration + one endpoint is enough).
- Frontmatter: `title`, `description` required; `aiTags` only for machine-readable contract pages.
- Use Docus routes with leading `/...`. Always set the language on code fences.

## Local development

```bash
cd docs
npm install
npm run dev
```

- `npm run lint` for quality
- `npm run generate` before publishing
- Never commit generated artefacts (`.output/`, `.nuxt/`, `node_modules/`, etc.).

## When you need detail

- Generator details: `docs/content/3.reference/generator-workflow.md`
- Stack & LLM: `docs/content/4.meta/2.documentation-stack.md`
- Frontmatter checks: `scripts/validate-frontmatter.mjs`

Last updated: 2025-12-02
