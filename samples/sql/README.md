# SQL Object Catalogue

The scripts in this folder materialise the `sample` schema inside the `XtraqSample` database. They are grouped by object type to simplify selective execution while keeping re-runs idempotent. Execute them in the following order:

1. `schema/` – create the database and schema container
2. `types/` – register custom scalar and table types used throughout the samples
3. `tables/` – create relational tables with constraints, computed columns, and indexes
4. `data/` – seed deterministic reference data
5. `functions/` – reusable scalar and table-valued functions referenced by procedures
6. `procedures/` – define stored procedures that exercise the generator surface area (multiple result sets, JSON output, TVPs, scalar types)

All scripts use `GO` batch separators and guard clauses (`IF DB_ID`, `IF OBJECT_ID`, `IF TYPE_ID`) so you can re-run them without dropping the database first. The resulting dataset is intentionally small but covers:

- multi-result-set and JSON-returning procedures
- table-valued parameter upserts and scalar type usage
- computed columns, filtered indexes, check constraints, and `ROWVERSION`
- nested JSON projections suitable for the enhanced JSON analyzer

You can execute the entire catalogue with a plain `sqlcmd` session:

```
sqlcmd -S localhost -d master -i schema/00-create-database.sql
sqlcmd -S localhost -d XtraqSample -i schema/01-create-schema.sql
sqlcmd -S localhost -d XtraqSample -i types/01-scalar-types.sql
sqlcmd -S localhost -d XtraqSample -i types/02-table-types.sql
sqlcmd -S localhost -d XtraqSample -i tables/Users.sql
...
```

(Replace the server and authentication parameters as needed.)
