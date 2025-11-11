# Xtraq

[![NuGet](https://img.shields.io/nuget/v/xtraq?logo=nuget)](https://www.nuget.org/packages/xtraq/)
[![Tests](https://img.shields.io/github/actions/workflow/status/nuetzliches/xtraq/tests.yml?logo=githubactions&label=Tests)](https://github.com/nuetzliches/xtraq/actions/workflows/tests.yml)

👉 Explore the full documentation on GitHub Pages: https://nuetzliches.github.io/xtraq/

Xtraq turns SQL Server stored procedures into strongly typed, production-ready C# libraries.

## What Xtraq does

- **Generates C# access layers** from stored procedures, including inputs, result models, context helpers, and execution pipelines.
- **Captures database intent** by snapshotting procedure signatures, nullability, and types into a repeatable cache.
- **Reflects JSON procedures** by translating JSON outputs into strongly typed records that respect nested payloads and options.
- **Packages reusable artefacts** you can ship as shared libraries, NuGet packages, or internal SDKs.
- **Decouples schema delivery** by generating versioned client SDKs that ship independently from the database deployment cadence.

## Key benefits

- **Stay aligned with the database** – every regeneration reflects the exact SQL contract, preventing drift and runtime surprises.
- **Slash boilerplate** – no manual ADO.NET plumbing, magic strings, or copy-paste DTOs.
- **Build confidence in CI/CD** – deterministic outputs make diff reviews meaningful and keep client libraries in sync with migrations.
- **Empower mixed teams** – DBAs define the contract; application developers consume typed APIs with nullable annotations and async helpers.

## Who it’s for

### Database administrators

- Publish a ready-to-use access layer in minutes without maintaining custom client libraries.
- Share consistent naming, nullability, and metadata with every consuming application.
- Catch signature drift early thanks to snapshot diffs before changes reach production.

### Application developers

- Work with strongly typed, nullable-aware models that mirror the database exactly.
- Call async helpers that enforce parameter completeness, ordering, and defaults automatically.
- Regenerate clients in CI so every service stays aligned with the latest schema.

## Quick start

1. **Install** – `dotnet tool install --global xtraq`
2. **Configure** – `xtraq init` writes non-secret defaults to `.xtraqconfig`, ships an `.env.example` template, and keeps sensitive values in a project-scoped `.env` file.
3. **Snapshot** – `xtraq snapshot` pulls stored procedure metadata and result shapes into the local cache.
4. **Generate** – `xtraq build` emits strongly typed C# artefacts ready to commit, package, or publish.

## Requirements

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) (Preview)

## License

This project is open source and available under the [MIT License](LICENSE).
