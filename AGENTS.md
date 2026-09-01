# Repository Guidelines

## Project Structure & Module Organization



- `src/complaint_dedup/`: FastAPI routes, corpus processing, repositories, LLM integration, configuration, and worker entry points.
- `tests/`: Pytest suites mirroring the modules they cover, such as `test_corpus_pipeline.py`.
- `alembic/`: database migrations; apply new schema changes through ordered version files under `alembic/versions/`.
- `templates/` and `static/`: Jinja2 HTML templates and CSS/vendor assets.
- `scripts/`: operational and validation helpers.
- `docs/`: runtime notes, design specifications, and implementation plans.
- `deploy/` and `Dockerfile`: container deployment configuration.

Do not copy production spreadsheets or exports into the application tree.

## Build, Test, and Development Commands

Run from the repository root:

- `uv sync`: install dependencies and development tools.
- `Copy-Item .env.example .env`: create local environment configuration.
- `uv run alembic upgrade head`: initialize or migrate the local database.
- `uv run uvicorn complaint_dedup.main:app --host 127.0.0.1 --port 8765`: run the web API locally.
- `uv run python -m complaint_dedup.worker_main`: run the background worker, normally alongside PostgreSQL.
- `uv run pytest -q`: execute the automated test suite.
- `uv build`: build the distributable package.

## Coding Style & Naming Conventions

Write Python 3.12 with type hints where they clarify public interfaces and async APIs. Follow existing four-space indentation and module organization. Use lowercase snake_case for functions and variables, PascalCase for classes and models, uppercase snake_case for settings constants, and descriptive `test_<behavior>.py` filenames. Keep route, pipeline, persistence, schema, and UI concerns in their existing modules rather than introducing broad helper files. There is currently no configured formatter or linter; preserve surrounding formatting and keep changes focused.

## Testing Guidelines

Add or update Pytest coverage for parser behavior, state transitions, persistence queries, migration integrity, export output, and UI contracts affected by a change. Prefer focused regression tests that reproduce the defect first. Run `uv run pytest -q` before submitting changes; migration-sensitive work must include Alembic coverage.

## Commit & Pull Request Guidelines

History uses Conventional Commits, for example `feat: strengthen incremental event matching`, `fix: harden SiliconFlow structured responses`, `chore: ...`, and `docs: ...`. Keep commits scoped and imperative.

Pull requests should describe the motivation, behavioral change, data/migration impact, and verification performed. Link related issues or design documents. Include screenshots for visible UI changes and call out any configuration, deployment, or backward-compatibility requirements.

## Configuration & Security

Keep credentials and deployment-specific values in `.env` or server-side Compose configuration; never commit real keys, customer data, uploaded files, exports, or runtime databases.
