# Xtraq Sample SQL Server Container

This containerized sample environment:

- Creates (or recreates) the XtraqSample database
- Ensures the sample schema exists
- Executes every T-SQL script in ../sql in a stable, deterministic order (lexical filename order) so local runs and automated tests share the same baseline

Use it to:

- Experiment with Xtraq code generation against a predictable SQL Server schema
- Add new sample objects (place a numbered or clearly ordered .sql file in ../sql and rebuild)
- Run repeatable integration tests without polluting a developer’s global SQL instance

Prerequisites: Docker Desktop (allocate at least 2.5 GB memory for the SQL Server container).

Note: Data persists in the named volume between runs; use docker compose down -v for a clean slate.

## Quick start

```
cd samples/mssql
copy .env.example .env   # Windows; provide a secure password afterwards
docker compose up --build -d
```

The SQL Server instance listens on `localhost,1433`. Connect using the password you configured in `.env`.

To stop and remove the container (but keep the volume):

```
docker compose down
```

Append `-v` to the command if you need a clean slate.

## How it works

- `docker-compose.yml` builds the image, mounts `../sql` read-only, and starts SQL Server with a deterministic bootstrap
- `scripts/entrypoint.sh` starts SQL Server, waits for readiness, and executes the ordered list of scripts from `../sql`
- update the script list in `scripts/entrypoint.sh` whenever you add new sample objects
