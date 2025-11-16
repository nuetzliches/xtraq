# Xtraq Project - Copilot Instructions

This is a .NET CLI application project with multi-targeting support for .NET 8 and .NET 10.

## Project Checklist

- [x] Verify that the copilot-instructions.md file in the .github directory is created.
- [x] Clarify Project Requirements (.NET CLI application with .NET 8 and .NET 10 support, namespace: Xtraq)
- [x] Scaffold the Project (Project structure created in src/ directory)
- [x] Customize the Project (Multi-targeting configuration added, Xtraq namespace implemented)
- [x] Install Required Extensions (No specific extensions needed)
- [x] Compile the Project (Successfully builds for both target frameworks)
- [x] Create and Run Task (VS Code tasks created for build and run operations)
- [x] Launch the Project (Application runs successfully on both frameworks)
- [x] Ensure Documentation is Complete (README.md created and copilot-instructions.md cleaned up)
- [x] Complete Namespace and Branding Migration (All legacy references cleaned up, comments translated to English)

## Project Structure

- `src/`: Source code directory with comprehensive SQL Server code generation functionality
  - `Program.cs`: Main CLI application entry point with Xtraq namespace
  - `Xtraq.csproj`: Multi-targeting project file (net8.0;net10.0) with Global Tool configuration
  - `XtraqGenerator.cs`: Core code generation orchestrator
  - Various subdirectories for CLI, Configuration, Data access, Generators, etc.
- `.vscode/tasks.json`: VS Code build and run tasks
- `README.md`: Project documentation
- `docs/`: Documentation site powered by Docus 5 on Nuxt 4 (see `docs/content/4.meta/2.documentation-stack.md` for versioning & maintenance)

## Development Guidelines

- Use the `Xtraq` namespace for all new classes
- **Follow .NET 10 Style Guide**: Adhere to Microsoft's official [.NET 10 coding standards](https://docs.microsoft.com/en-us/dotnet/fundamentals/code-analysis/style-rules/) and modern C# practices
- **Use Top-Level Statements**: Follow the modern .NET programming style using top-level templates as described in [Microsoft's documentation](https://learn.microsoft.com/de-de/dotnet/core/tutorials/top-level-templates)
- **Global Usings**: The project uses global using directives defined in `src/GlobalUsings.cs` for common .NET namespaces. **NEVER add redundant using statements** for:
  - `System`, `System.Collections.Generic`, `System.IO`, `System.Linq`
  - `System.Threading`, `System.Threading.Tasks`, `System.Text`, `System.Text.Json`
  - `System.Text.RegularExpressions`, `System.Collections.Concurrent`
  - `System.Diagnostics`, `System.Globalization`, `System.Reflection`, `System.Security.Cryptography`
  - `Microsoft.Extensions.DependencyInjection`
  - **Always remove these imports if found in individual .cs files**
  - Use `tools/Clean-RedundantUsings.ps1` to automatically clean redundant usings
- **Code Quality**: All comments should be in English, avoid legacy terminology like "vNext"
- **Nullable Guidelines**: Treat nullable reference warnings as errors-in-waiting—prefer explicit null checks, non-null constructors, and `ArgumentNullException.ThrowIfNull` helpers over blanket suppression. When adding new APIs, choose precise nullability annotations and update call sites accordingly. Only keep `NoWarn` entries while actively fixing the underlying issues.
- Add XML documentation comments for every method; supply clean English summaries when missing
- **AST-Only Analysis**: No heuristics or hardcoded function names (e.g., "RecordAsJson") in program code. All SQL function analysis must be purely AST-based using Microsoft.SqlServer.TransactSql.ScriptDom
- **SQL Surface**: Preserve the SQL Server feature set and map it to equivalent C# outputs; focus on stored procedures, not an EF clone or ad-hoc query endpoints.
- **Nullable Context**: Enabled throughout the project, but warnings temporarily suppressed
- Test changes against both .NET 8 and .NET 10 target frameworks
- Use VS Code tasks for building and running the application
- When updating the documentation, follow the Docus maintenance guide in `docs/content/4.meta/2.documentation-stack.md` (includes LLM integration details)
- Treat generated artifacts as read-only. Do not edit files under `samples/restapi/Xtraq`, `debug/Xtraq`, or any other `Xtraq` artifact folders—rerun the generator instead.
- After generation, run `dotnet format` as documented in `docs/content/4.meta/3.formatting-generated-artifacts.md` to keep emitted code aligned with the repository style rules.
- Follow .NET coding conventions and best practices

## Build and Run Instructions

- Build: `dotnet build src/Xtraq.csproj`
- Run with .NET 8: `dotnet run --project src/Xtraq.csproj --framework net8.0`
- Run with .NET 10: `dotnet run --project src/Xtraq.csproj --framework net10.0`

## Current Status

The project has completed its initial namespace and branding migration phase:

- ✅ Legacy "XtraqVNext" references removed
- ✅ German comments translated to English
- ✅ Template directory structure modernized
- ✅ CLI outputs use consistent Xtraq branding
- ✅ All source files use proper Xtraq namespace
- ✅ Code cleanup: Removed obsolete GoldenHash functionality and placeholder implementations

## Next Priority Areas

1. **Cache Invalidation** - Complete SQL object cache invalidation implementation
2. **CLI Output Optimization** - Reduce verbosity and improve user experience
3. **Framework Auto-Selection** - Automatically detect target framework instead of requiring explicit parameter
4. **Nullable Warning Resolution** - Remove NoWarn suppressions progressively and fix nullable reference type warnings
