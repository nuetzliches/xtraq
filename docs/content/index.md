---
title: xtraq
description: Snapshot your database, generate typed clients, and keep .NET applications aligned with your stored procedures—no hand-written plumbing required.
layout: landing
navigation: false
seo:
  title: Stored procedure automation for .NET
  description: Snapshot your database, generate typed clients, and keep .NET applications aligned with your stored procedures—no hand-written plumbing required.
---

::u-page-hero{class="py-12 sm:py-16 lg:py-20"}
#title
Stored procedure automation for .NET

#description
Snapshot your database, generate typed clients, and keep .NET applications aligned with your stored procedures—no hand-written plumbing required.

#links
:::u-button
---
color: primary
size: xl
to: /getting-started/installation
trailing-icon: i-lucide-arrow-right
---

Install the CLI
:::

:::u-button
---
color: neutral
icon: simple-icons-github
size: xl
to: https://github.com/nuetzliches/xtraq
variant: outline
target: \_blank
---

Star on GitHub
:::

#image
:::u-color-mode-image
---
light: /xtraq-logo.svg
dark: /xtraq-logo.svg
alt: xtraq logomark
class: w-40 mx-auto
---

:::
::

::u-container{align="center"}

## Why teams choose xtraq
Focus on SQL-first architecture while shipping .NET clients that stay in sync with every schema change.

:::card-group{align="left"}

  ::card
  ---
  title: Stored procedure first
  icon: i-heroicons-shield-check
  ---
  Respect existing database boundaries, permissions, and auditing while exposing a clean client surface.
  ::

  ::card
  ---
  title: Strongly typed artefacts
  icon: i-heroicons-cube-transparent
  ---
  Generate inputs, outputs, and helpers that match SQL signatures across .NET 8 and .NET 10.
  ::

  ::card
  ---
  title: JSON aware by design
  icon: i-heroicons-code-bracket
  ---
  Capture nested JSON payloads, table-valued parameters, and multi-result sets without manual parsing.
  ::

  ::card
  ---
  title: Fluent pipeline design
  icon: i-heroicons-sparkles
  to: /reference/api-integration
  ---
  Compose policies, transactions, and streaming helpers without losing strongly typed procedures.
  ::

  ::card
  ---
  title: Transaction-aware orchestration
  icon: i-heroicons-arrow-path-rounded-square
  to: /reference/entity-framework-integration
  ---
  Reuse ambient EF connections or spin up savepoint-backed scopes with generated orchestrator wiring.
  ::

  ::card
  ---
  title: Minimal API ready
  icon: i-heroicons-adjustments-horizontal
  to: /reference/api-integration
  ---
  Attach generated route handlers and streaming endpoints without abandoning standard `MapGet`/`MapPost` flows.
  ::

:::

## Get productive in minutes
Follow the guided flow to scaffold your generator project, warm the metadata cache, and ship typed clients.

:::card-group{align="left"}

  ::card
  ---
  title: Understand the workflow
  icon: i-heroicons-question-mark-circle
  to: /getting-started/what-is-xtraq
  ---
  Learn how snapshots, configuration, and generation combine to keep your client code deterministic.
  ::

  ::card
  ---
  title: Run the quickstart
  icon: i-heroicons-rocket-launch
  to: /getting-started/quickstart
  ---
  Scaffold a project, capture metadata, and generate artefacts ready to drop into your solution.
  ::

  ::card
  ---
  title: Explore the CLI
  icon: i-heroicons-command-line
  to: /cli
  ---
  Dive into every command, flag, and workflow for local development and CI/CD automation.
  ::

  ::card
  ---
  title: Browse the reference
  icon: i-heroicons-book-open
  to: /reference
  ---
  Dive into configuration, naming, and binding guides before customising your generator.
  ::

:::

## Stay connected
Join the community, track releases, and deploy the global tool from the platforms you already use.

:::card-group{align="left"}

  ::card
  ---
  title: Star the project
  icon: i-simple-icons-github
  to: https://github.com/nuetzliches/xtraq
  target: _blank
  ---
  Follow development, open issues, and discuss roadmap ideas with the maintainers.
  ::

  ::card
  ---
  title: Install from NuGet
  icon: i-simple-icons-nuget
  to: https://www.nuget.org/packages/xtraq
  target: _blank
  ---
  Pull the latest global tool release directly into your tooling pipeline.
  ::

:::

::
