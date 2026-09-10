# Repository Guidelines

## Project Structure

- `src/complaint_dedup/` contains the FastAPI application, Excel parser, deterministic normalization rules, full-corpus sync service, time-window comparison service, exporter, licensing, and database wiring.
- `src/complaint_dedup/full_corpus.py` owns synchronization, audit snapshots, window partitioning, event grouping, and `cannot_links` isolation.
- `src/complaint_dedup/full_corpus_web.py` and `templates/full_*.html` provide the current single-file upload and comparison UI.
- `alembic/versions/20260908_0001_full_corpus_baseline.py` is the active empty-database baseline. The repository intentionally starts from a new schema because old databases are not migrated.
- `tests/` mirrors parser, normalization, sync, comparison, export, licensing, and web behavior. `runtime/` is local scratch space and must not contain committed data.

## Build, Test, and Run

Run from the repository root:

- `uv sync` installs locked dependencies.
- `uv run pytest -q` runs the full regression suite.
- `uv run alembic upgrade head` is retained for migration tests; normal Docker startup initializes and validates the active PostgreSQL schema automatically.
- `uv run uvicorn complaint_dedup.main:app --host 127.0.0.1 --port 8765` starts the current web service.
- `docker compose -f deploy/compose.intranet.yaml up -d` runs the PostgreSQL-backed internal deployment; the public port is `28765`. API startup creates missing current tables and rejects legacy or incomplete schemas; worker waits for API health.

The current workflow uploads one full Excel file to update the complete work-order store, then creates an independent comparison task using either `completed_at` or `received_at`. Without a manual reference window, the reference side is the complement of the target window; records missing the selected date enter the target side and are counted separately.

## Style and Naming

Use Python 3.12, four-space indentation, type hints on public APIs, `snake_case` functions and variables, and `PascalCase` classes. Keep persistence, domain logic, web routes, and templates in their existing modules. Use UTF-8 Simplified Chinese for UI text. Prefer structured SQLAlchemy expressions and pure helper functions over string-built SQL.

## Testing

Use `pytest` with explicit `pytest.mark.asyncio` for async tests. Add a failing regression test before changing behavior. New sync/comparison behavior belongs in `tests/test_full_corpus_window.py`; web contracts belong in `tests/test_full_corpus_web.py`. Schema changes require a migration test and a fresh SQLite upgrade check.

## Commits and Pull Requests

Use concise Conventional Commit messages such as `feat: add full corpus window comparison` or `fix: reject overlapping comparison windows`. Pull requests should describe data-model changes, deployment impact, authorization/PyArmor implications, and verification commands. Never commit `.env` files, credentials, customer spreadsheets, exports, or runtime databases.

## Security and Deployment

Keep PostgreSQL credentials and external LLM settings in server-side `config/.env`. Keep licensing checks, Shanghai-time expiry handling, clock rollback protection, and PyArmor build arguments intact. Do not reintroduce account isolation, daily-batch queues, or vector database dependencies into the current runtime.
