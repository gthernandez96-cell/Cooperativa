# CoopAhorro — AI Agent Instructions

## Project Overview
CoopAhorro is a Flask-based web system for managing credit unions (cooperativas), handling member savings, loans, and financial operations. Built with Python 3.9+, Flask, and SQLite (with PostgreSQL migration support). All UI and data in Spanish.

## Quick Start
- **Run locally**: `python app.py` → http://localhost:8001 (o https://localhost:8001 si `USE_HTTPS=True` en `.env`)
- **Demo credentials**: username: admin, password: admin123
- **Tests**: `pytest tests/`
- See [README.md](README.md) for full setup instructions.

## Key Architecture
- **Database**: SQLite by default; use `utils/db.py` helpers (`get_db()`, `db_execute()`, `db_fetchone()`) for backend-agnostic queries. Manual `conn.commit()` required after writes.
- **Auth**: Role-based with decorators (`@login_required()`, `@permission_required()`). Roles: Administrador, Operador, Asociado.
- **Structure**: Monolithic `app.py` (blueprint refactoring pending), templates in `templates/`, static files in `static/`.
- **Financial logic**: Custom calculations for loans (amortization), savings interest, collections. All transactions audited.

## Conventions
- **Dates**: Always ISO format (YYYY-MM-DD) using `date.today().isoformat()`.
- **Forms**: Include `{{ csrf_token() }}` in templates; handle POST with redirect-after-POST.
- **Errors**: Use `flash()` for user messages; catch exceptions, don't let them bubble.
- **Files**: Uploads to `static/uploads/`; validate with `utils/images.py`.
- **Locale**: Spanish labels/messages; no internationalization.

## Common Pitfalls
- Don't hardcode SQLite-specific SQL; use helpers for PostgreSQL compatibility.
- Always close DB connections in `finally` blocks.
- Validate inputs before queries to prevent SQL injection.
- Use `os.path.join()` for paths; avoid hardcoded `/`.
- Demo data only seeds if DB empty; don't modify constants.

## Development Notes
- **No ORM**: Raw SQL throughout; no SQLAlchemy.
- **Migrations**: Handled in `init_db()`; no separate migration tool.
- **Tests**: Use `pytest` with fixtures; temp DB per test.
- **Deployment**: Scripts in `scripts/` for PythonAnywhere.
- **HTTPS & Proxy**: Configured with `ProxyFix` for reverse proxies (e.g. PythonAnywhere). Local SSL supported via `ssl_context='adhoc'` if `USE_HTTPS=True` in `.env`.

## Links
- [README.md](README.md) — Setup and deployment
- [schema.sql](schema.sql) — Database schema reference
- [/memories/repo/postgres-migration-notes.md](/memories/repo/postgres-migration-notes.md) — Migration details</content>
<parameter name="filePath">/Users/gustavohernandez/Documents/cooperativa/AGENTS.md