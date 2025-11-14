---
title: Welcome to xtraq
description: Code generator for SQL Server stored procedures that creates strongly typed C# classes.
layout: landing
---

::hero
---
title: xtraq
description: Code generator for SQL Server stored procedures that creates strongly typed C# classes for inputs, models, and execution.
headline: SQL to C# Code Generation
links:
  - label: Get Started
    to: /getting-started/installation
    size: lg
    color: black
    icon: i-heroicons-rocket-launch
  - label: View on GitHub
    to: https://github.com/nuetzliches/xtraq
    size: lg
    color: white
    variant: outline
    icon: i-simple-icons-github
    target: _blank
---
::

<!-- <UContainer>

::section
---
title: Why teams choose xtraq
lead: xtraq keeps SQL Server stored procedures as the security boundary you already trust while delivering a modern, typed experience for .NET application teams.
---

::card-grid{columns="2"}
  ::card
  ---
  icon: i-heroicons-shield-check-20-solid
  title: Keep stored procedures in charge
  ---
  Snapshots stay aligned with production procedures, so permissions, auditing, and validation remain enforced in the database instead of drifting into ad-hoc queries.
  ::

  ::card
  ---
  icon: i-heroicons-rocket-launch-20-solid
  title: Ship faster with generated code
  ---
  Generate models, inputs, and execution helpers that mirror your schema—eliminating manual ADO.NET plumbing, typo-prone magic strings, and hand-written mapping layers.
  ::
::
::

::section
---
title: Built for your team
---

::columns
  ::column
  :::callout{color="primary" icon="i-heroicons-lock-closed-20-solid"}
  ## Database administrators

  - Publish a ready-to-use access layer in minutes—no hand-written client libraries.
  - Share consistent naming, nullability, and type metadata with every consuming application.
  - Detect signature drift early: snapshot diffs surface breaking changes before they reach production.

  :::
  :::callout{tone="primary" icon="i-heroicons-shield-exclamation-20-solid"}
  🛡️ **Control without compromise** — document the access layer while maintaining full authority over security and change management.
  :::
  ::

  ::column
  :::callout{color="primary" icon="i-heroicons-code-bracket-20-solid"}
  ## Application developers

  - Work with strongly typed models and nullable annotations that match the database exactly.
  - Call async helpers that enforce parameter completeness, ordering, and default values automatically.
  - Automate regeneration in CI so client libraries stay current with every migration.

  :::
  :::callout{tone="primary" icon="i-heroicons-rocket-launch-20-solid"}
  🚀 **Focus on features** — move from SQL snippets to composable services that integrate cleanly with your architecture.
  :::
  ::
::

::section
---
title: How it works
lead: Four CLI commands take you from database connection to distributable client artefacts.
---

::steps
  ::step{title="Install the CLI" icon="i-heroicons-cloud-download-20-solid"}
  `dotnet tool install --global xtraq` installs the global tool once per workstation.
  ::
  ::step{title="Bootstrap configuration" icon="i-heroicons-adjustments-horizontal-20-solid"}
  `xtraq init` writes the connection string, namespaces, and output directories into a project-scoped `.env` file.
  ::
  ::step{title="Capture a snapshot" icon="i-heroicons-document-plus-20-solid"}
  `xtraq snapshot` loads procedure metadata, parameters, and result shapes into the local cache for repeatable builds.
  ::
  ::step{title="Generate code" icon="i-heroicons-cube-transparent-20-solid"}
  `xtraq build` produces strongly typed C# artefacts ready to commit or publish as a NuGet package.
  ::
::

:::callout{tone="primary" icon="i-heroicons-sparkles-20-solid"}
🎉 **You now have a typed gateway** to every stored procedure—share it across teams or integrate directly into CI/CD pipelines.
:::

::section
---
title: Feature highlights
---

::card-grid{columns="2"}
  ::card{title="Multiple output formats" icon="i-heroicons-rectangle-stack-20-solid"}
  Emit contexts, DTOs, and extension helpers aligned with your project layout and layering strategy.
  ::
  ::card{title="Rich JSON support" icon="i-heroicons-code-bracket-20-solid"}
  Map nested JSON payloads, optional fields, and table-valued parameters without losing type safety.
  ::
  ::card{title="Custom types" icon="i-heroicons-variable-20-solid"}
  Generate bindings for table-valued parameters, user-defined types, and reusable result models.
  ::
  ::card{title="CI/CD ready" icon="i-heroicons-cog-6-tooth-20-solid"}
  Run the same commands locally and in pipelines with deterministic results across .NET 8 and .NET 10.
  ::
::

::section
---
title: Ready to get started?
lead: Follow the guides below to install, configure, and integrate xtraq into your workflow.
---

::button-group
  ::button{to="/getting-started/installation" color="black" size="lg"}
  Installation Guide
  ::
  ::button{to="/2.cli/index" variant="outline" color="neutral" size="lg"}
  CLI Reference
  ::
  ::button{to="/1.getting-started/quickstart" variant="outline" color="neutral" size="lg"}
  Quickstart Tutorial
  ::
::

</UContainer> -->