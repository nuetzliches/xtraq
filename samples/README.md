# Xtraq SQL Samples

This directory collects curated T-SQL assets that the Xtraq tooling and its automated tests can rely on. All sample objects live in the `sample` schema inside the `XtraqSample` database. The goal is to offer compact, purpose-built definitions that still exercise the same features we encounter in production metadata (multi-set procedures, JSON projections, table-valued parameters, custom scalar types, and so on).

## Folder layout

- `sql/` – raw T-SQL scripts organised by object type; can be executed with `sqlcmd`, Azure Data Studio, or the container setup under `samples/mssql`
- `mssql/` – containerised SQL Server sample environment driven by docker compose and the curated scripts in `sql/`
- `restapi/` – minimal ASP.NET Core API that exposes selected sample data for end-to-end experiments

Each script is idempotent so it can be re-run during local development or automated testing without manual cleanup.
