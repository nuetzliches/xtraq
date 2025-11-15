---
title: xtraq
description: Generate strongly typed .NET clients from SQL Server stored procedures with deterministic snapshots and modern tooling.
layout: landing
navigation: false
seo:
  title: SQL to C# without manual plumbing
  description: Generate strongly typed .NET clients from SQL Server stored procedures with deterministic snapshots and modern tooling.
---

::u-page-hero
#title
SQL to C# without the guesswork.

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
::u-color-mode-image
---
light: /xtraq-logo.svg
dark: /xtraq-logo.svg
alt: xtraq logomark
class: w-40 mx-auto
---

::
::

::u-page-section{align="center" class="py-8 md:py-16"}
#title
Why teams choose xtraq

#description
Focus on SQL-first architecture while shipping .NET clients that stay in sync with every schema change.

:::u-page-grid
::::u-page-card
---
icon: i-heroicons-shield-check
---

#title
Stored procedure first

#description
Respect existing database boundaries, permissions, and auditing while exposing a clean client surface.
::::

::::u-page-card
---
icon: i-heroicons-cube-transparent
---

#title
Strongly typed artefacts

#description
Generate inputs, outputs, and helpers that match SQL signatures across .NET 8 and .NET 10.
::::

::::u-page-card
---
icon: i-heroicons-code-bracket
---

#title
JSON aware by design

#description
Capture nested JSON payloads, table-valued parameters, and multi-result sets without manual parsing.
::::
:::
::

::u-page-section{align="center" class="py-8 md:py-16"}
#title
Get productive in minutes

#description
Follow the guided flow to scaffold your generator project, warm the metadata cache, and ship typed clients.

:::u-page-grid
::::u-page-card{to="/getting-started/what-is-xtraq"}
---
icon: i-heroicons-question-mark-circle
---

#title
Understand the workflow

#description
Learn how snapshots, configuration, and generation combine to keep your client code deterministic.
::::

::::u-page-card{to="/getting-started/quickstart"}
---
icon: i-heroicons-rocket-launch
---

#title
Run the quickstart

#description
Scaffold a project, capture metadata, and generate artefacts ready to drop into your solution.
::::

::::u-page-card{to="/cli"}
---
icon: i-heroicons-command-line
---

#title
Explore the CLI

#description
Dive into every command, flag, and workflow for local development and CI/CD automation.
::::
:::
::

::u-page-section{align="center" class="py-8 md:py-16"}
#title
Stay connected

#description
Join the community, track releases, and deploy the global tool from the platforms you already use.

:::u-page-grid{cols=2}
::::u-page-card{target="\_blank" to="https://github.com/nuetzliches/xtraq"}
---
icon: i-simple-icons-github
---

#title
Star the project

#description
Follow development, open issues, and discuss roadmap ideas with the maintainers.
::::

::::u-page-card{target="\_blank" to="https://www.nuget.org/packages/xtraq"}
---
icon: i-simple-icons-nuget
---

#title
Install from NuGet

#description
Pull the latest global tool release directly into your tooling pipeline.
::::
:::
::
