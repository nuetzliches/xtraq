---
title: Getting Started
description: Understand how xtraq structures the generator workflow, metadata cache, and your next steps.
---

## Introduction

xtraq streamlines the journey from SQL Server stored procedures to typed C# artefacts, packaging configuration, snapshotting, and code generation into a predictable toolchain.

::callout{color="info"}
**New caching subsystem**  
The schema cache now self-invalidates whenever `xtraq snapshot` detects a changed stored procedure signature. Fresh metadata is pulled automatically, so your builds stay in sync without manual purge commands.
::

Use this guide to map the high-level lifecycle—installation, configuration, cache warm-up, and generation—before diving into task-specific guides.

## What comes next

- Walk through the tooling prerequisites in [Installation](./2.installation.md).
- Capture your first database snapshot using the [Quickstart](./3.quickstart.md).
- Review [What is xtraq?](./1.what-is-xtraq.md) for a feature-by-feature tour when you need deeper context.
