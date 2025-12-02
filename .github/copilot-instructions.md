# Xtraq Project – Assistant Guidance

Short guidance for contributions in this repo.

## Project facts

- .NET CLI with multi-targeting `net8.0` and `net10.0` (`src/Xtraq.csproj`).
- Namespace: `Xtraq`.
- Generated artefacts under `Xtraq/` and `.xtraq/` are disposable—do not hand-edit them.

## Development rules

- Global usings live in `src/GlobalUsings.cs`; avoid duplicate imports.
- Treat nullable warnings seriously; prefer `ArgumentNullException.ThrowIfNull` over suppression.
- Stick to modern C# practices; no legacy bridges or hardcoded shortcuts.
- Do not edit generated files; adjust templates/source and regenerate instead.
- Build commands: `dotnet build src/Xtraq.csproj`; run via `dotnet run --project src/Xtraq.csproj --framework net10.0` (or net8.0).

## Documentation rules

- Docs target consumers: short quickstarts, lean examples (service registration + one endpoint is enough).
- Put technical depth in dedicated reference pages (e.g., generator workflow), not in quickstarts.
- Remove unnecessary source-code descriptions; focus on “how do I use this?”
- Update `docs/README.md` and related pages when the docs philosophy changes.

## Security & hygiene

- Never commit secrets (`.env`, connection strings).
- Respect lint/format rules; run `dotnet format` after generation only when agreed.
- Test relevant changes and remove temporary artefacts before committing.

This file is canonical: deviate only if the task/ticket explicitly requires it.
