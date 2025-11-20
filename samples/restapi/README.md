# Xtraq Sample REST API

This folder contains a lightweight ASP.NET Core minimal API that exercises the curated `samples/sql` database objects. It is intended for local development scenarios and integration-test experiments rather than production workloads.

## Prerequisites

- .NET 10 SDK — the sample defaults to net10.0 but still multi-targets net8.0 for compatibility
- Running SQL Server instance seeded with the scripts under `samples/sql` (the Docker setup in `samples/mssql` exposes a ready-made container on port 1433)

## Useful commands

```cmd
:: Restore packages
 dotnet restore

:: Run the API (listens on http://localhost:5143 by default)
 dotnet run --project RestApiSample.csproj

:: Call the health endpoint
 curl http://localhost:5143/

:: Fetch the user catalogue
 curl http://localhost:5143/api/users

:: List orders for a user
 curl http://localhost:5143/api/users/1/orders

:: Create an order (sample payload)
 curl -X POST http://localhost:5143/api/users/1/orders ^
      -H "Content-Type: application/json" ^
      -d "{\"orderNumber\":\"WEB-1001\",\"totalAmount\":49.99,\"currency\":\"USD\"}"
```

## Nullability contract

- The generated client library under `samples/restapi/Xtraq` ships with `#nullable enable` in every file and the sample project treats nullable flow warnings (`CS86xx`, `CS8762`, `CS8763`) as errors to surface risky assumptions early.
- Keep `<Nullable>enable</Nullable>` (or `warnings` / `annotations`) turned on in any consumer project; otherwise the generated `string?` / `DateTime?` annotations degrade to comments instead of compile-time guarantees.
- Mirror the `WarningsAsErrors` list from `RestApiSample.csproj` if you copy these artifacts into your own solution. This prevents silent regressions when stored procedures evolve and shape changes introduce new nullable paths.
- Prefer interpreting `null`-annotated members exactly as declared. For example, `string? Email` indicates the column may be missing in SQL and should be checked before usage, whereas non-nullable scalars (`int`, `decimal`) are guaranteed by the generator because the source metadata marks them as `NOT NULL`.
- When exposing the generated models on HTTP or other boundaries, consider adding explicit validation to fail-fast on unexpected `null` payloads instead of relying solely on the annotations.
