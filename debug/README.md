# Xtraq

## Debug Directory

The `debug/` directory is a transient workspace used during local development and troubleshooting. It is intentionally excluded from version control (except for explicitly whitelisted helper files) so that generated artifacts, scratch outputs, and temporary logs do not pollute commits.

### Usage Steps

1. Copy the environment template:
   ```
   cp .env.example .env
   ```
2. Adjust `.env` for your local test database. By default the Docker SQL Server container in `samples/mssql` is assumed.
3. You may point to any SQL Server instance (local, container, remote) by editing the connection-related entries in `.env`.
4. Run the CLI; ephemeral outputs, experimental generated code, and diagnostic dumps will appear under `debug/`.

### Guidelines

- Do not commit secrets; keep credentials only in your local `.env`.
- Add a placeholder (e.g. `.gitkeep`) only if you need to preserve required structure.
- Clean stale artifacts before validating generation output to avoid confusion.
- Treat everything in `debug/` as disposable; nothing there is part of the public API surface.

### Recommended Pattern

- Maintain a stable `.env` for the main container.
- Create ad‑hoc variant files (e.g. `.env.local`) but do not commit them.
- When switching databases, update `.env` rather than editing code.

### Notes

- If additional temporary folders are needed, prefer subdirectories inside `debug/`.
- Avoid mixing permanent configuration files with transient run outputs.
