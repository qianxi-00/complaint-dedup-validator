# Repository Guidelines

## Project Overview

投诉事件归一化与判重系统 — an incremental complaint work-order normalization and deduplication workspace. Built around a frozen, generation-scoped history corpus plus standard street/anchor/issue dictionaries and exact event keys. There is **no** transitive similarity merging, no Milvus/embedding/rerank, no KMeans/DBSCAN — matching is deterministic (dictionary keys, bounded fuzzy thresholds, LLM candidate selection, occurrence identifiers, strong signals).

Core event key: `generation_id + street_id + anchor_id + issue_id + occurrence_key + event_key_version`.

Three import modes drive everything:
- `bootstrap_history` — upload history file B, extract candidate dictionary, approve → frozen event generation becomes active (atomically replaces the old one).
- `bootstrap_compare` — freeze B first, then stage same-day file A as the first daily increment (pauses at `awaiting_daily`).
- `daily_increment` — only allowed when an active generation exists; batch `min(received_at)` must be strictly later than the active history's `max_committed_received_at` or the upload is rejected.

Uploads are never parsed in the request; the API saves files and creates batches, a background worker claims them via leases, and the UI polls progress over HTMX.

## Project Structure & Module Organization

- `src/complaint_dedup/` — application package:
  - Entry/wiring: `main.py` (FastAPI app factory, `python -m complaint_dedup.main`), `worker_main.py` (standalone worker, `python -m complaint_dedup.worker_main`), `config.py` (pydantic-settings `Settings`, frozen, `get_settings()` cached), `async_database.py` + `corpus_database.py` (SQLite/aiosqlite vs PostgreSQL/asyncpg URL switch).
  - Ingestion: `file_inspection.py` (format sniffing, Chinese header alias mapping, row limits), `corpus_io.py` (`load_records_auto`), `corpus_parser.py` (pure-regex `rules-v2` parser for Chinese complaint addresses/titles/phones/occurrence ids), `corpus_models.py` (`InputRecord`).
  - Normalization: `corpus_normalizer.py` (fuzzy alias match ≥0.82, location signatures, >200-name groups skip pairwise fuzz), `corpus_dictionary.py` (hard-coded audited Jiangmen anchor merges), `corpus_prompts.py` + `llm_models.py` + `llm_client.py` (OpenAI-compatible structured-output client with retries; LLM may only pick from offered candidate ids, confidence-gated).
  - Orchestration: `corpus_pipeline.py` (`CorpusProcessor`: `stage_records`, `approve_bootstrap`, `commit_increment`; issue keyword taxonomy; enterprise `event-key-v3` family handling; occurrence keys `order:`/`complaint:`/`fact:`).
  - Persistence: `corpus_schema.py` (22 SQLAlchemy tables on shared metadata), `corpus_repository.py` (~4.4k lines: bulk upserts, event assignment passes, lease/heartbeat queue, dictionary review/split/merge, event queries, rename/exclude, exports, generation metrics).
  - Serving: `corpus_web.py` (routes + embedded worker in SQLite mode), `ui_labels.py` (Chinese display labels), `corpus_worker.py` (`CorpusBatchWorker` claim/heartbeat/process loop), `corpus_exporter.py` (XlsxWriter export).
  - Ops: `history_rebuild.py` (rebuild active generation into a candidate; manual activation via CLI).
- `tests/`: Pytest suites mirroring the modules they cover (156 tests).
- `alembic/`: migrations; single head `20260817_0016`; `env.py` resolves the DB URL from app settings (unless `preserve_sqlalchemy_url` is set); `legacy_schema.py` exists only for pre-corpus migrations 0001–0005.
- `templates/` and `static/`: Jinja2 templates extending `base.html` and vendored HTMX; mobile card-table CSS keyed on `data-label` attributes.
- `scripts/`: `rebuild_history.py` (CLI wrapper), `run_real_corpus_validation.py`/`.ps1` (end-to-end real-data validation against a running server).
- `docs/投诉判重系统运行与算法说明.md`: authoritative runtime + algorithm reference. `docs/superpowers/`: design specs and implementation plans.
- `deploy/`, `Dockerfile`: container deployment (compose services `migrate` → `api`/`worker`, host port 28765).
- `output/` and `runtime/`: gitignored scratch artifacts, uploads, exports, and local databases.

Do not copy production spreadsheets, exports, or databases into the application tree; `.gitignore` blocks spreadsheets and DBs (only `tests/fixtures/*` is exempt).

## Architecture & Data Flow

1. Web (`corpus_web.py`) accepts uploads → saves to `runtime/corpus_uploads/` → creates a `daily_batches` row with status `uploaded`.
2. Worker (`corpus_worker.py`, embedded when SQLite, separate process when PostgreSQL) claims batches (`FOR UPDATE SKIP LOCKED` on PostgreSQL, lease + heartbeat columns) and dispatches by status: `uploaded|parsing|normalizing` → `stage_records`; `approval_requested` → `approve_bootstrap`; `commit_requested`/`awaiting_daily` → `commit_increment`.
3. `CorpusProcessor.stage_records`: parse (rules-v2) → issue name + occurrence key → dictionary resolution ladder (exact alias → scoped fuzzy 0.9 anchor / 0.88 issue → LLM candidate selection gated by `NORMALIZATION_LLM_MIN_CONFIDENCE` → new approved items for daily → conservative singleton) → bulk persist via repository.
4. Approve/commit run four assignment passes: exact event keys (`event-key-v2` ordinary, `event-key-v3` enterprise organizations) → `previous_work_order` linked records → strong signals (unique scoped title/phone, explicit occurrence ids collapse duplicates) → safe singletons (`manual-singleton-{record_id}` synthetic keys).
5. Manual review: event rename (revision + snapshot + audit) and member exclusion (creates permanent `cannot_links`, respected by rebuilds).
6. Export: two worksheets `重复项` / `孤立工单`, alternating event colors `#EAF2FB`/`#FFF8E7`, frozen header, autofilter, formula-injection guard.

Key version constants in `corpus_pipeline.py`: `PARSER_VERSION="rules-v2"`, `EVENT_KEY_VERSION="event-key-v2"`, `ENTERPRISE_EVENT_KEY_VERSION="event-key-v3"`.

## Build, Test, and Development Commands

Run from the repository root (Windows/PowerShell):

- `uv sync`: install dependencies and development tools.
- `Copy-Item .env.example .env`: create local environment configuration.
- `uv run alembic upgrade head`: initialize or migrate the local database.
- `uv run uvicorn complaint_dedup.main:app --host 127.0.0.1 --port 8765` (or `uv run python -m complaint_dedup.main`, or `.\start.ps1`): run the web API locally; SQLite mode auto-starts an embedded worker.
- `uv run python -m complaint_dedup.worker_main`: run the background worker, required alongside PostgreSQL.
- `uv run python scripts/rebuild_history.py --activate-generation <id>` (or `--rollback-generation`): rebuild/activate history generations.
- `uv run python scripts/run_real_corpus_validation.py --mode daily_increment --daily <file>`: end-to-end validation against a running server (downloads the export to `runtime/corpus_results/`).
- `uv run pytest -q`: execute the automated test suite.
- `uv build`: build the distributable package.

## Coding Style & Naming Conventions

Python 3.12 (`.python-version` pins 3.12; `requires-python >=3.12,<3.13`) with type hints on public interfaces and async APIs. Four-space indentation, `from __future__ import annotations` headers, dataclasses/frozen models for value objects. Lowercase snake_case for functions/variables, PascalCase for classes, uppercase snake_case for settings constants. Keep route, pipeline, persistence, schema, and UI concerns in their existing modules rather than introducing broad helper files. There is no configured formatter or linter; preserve surrounding formatting and keep changes focused. Do not add comments unless necessary; UI text is Simplified Chinese.

## Testing Guidelines

Tests run in strict-mode pytest-asyncio: async tests are explicitly marked `@pytest.mark.asyncio` and fixtures use `@pytest_asyncio.fixture`; each test gets its own temporary SQLite (aiosqlite) file DB under `tmp_path` via the `corpus` fixture (`tests/conftest.py`). Web tests use sync `TestClient`; LLM tests use `httpx.MockTransport`; migration tests run real `alembic upgrade head`.

Add or update coverage for parser behavior, dictionary state transitions, event-key semantics, assignment passes, persistence queries, migration integrity (`test_migrations.py` pins final constraint shapes), export output, and UI contracts (including mobile `data-label` CSS assertions) affected by a change. Prefer focused regression tests that reproduce the defect first. Run `uv run pytest -q` before submitting changes; schema changes must include an Alembic version file (inspect-guarded, `batch_alter_table` for SQLite compatibility) and migration test updates.

## Commit & Pull Request Guidelines

History uses Conventional Commits, for example `feat: implement corpus generation and event review workflow`, `feat: strengthen incremental event matching`, `fix: harden SiliconFlow structured responses`, `docs: ...`, `chore: ...`. Keep commits scoped and imperative.

Pull requests should describe the motivation, behavioral change, data/migration impact, and verification performed. Link related issues or design documents under `docs/superpowers/`. Include screenshots for visible UI changes and call out any configuration, deployment, or backward-compatibility requirements.

## Configuration & Security

Configuration lives in `.env` (see `.env.example` for the full surface): `APP_HOST/APP_PORT/APP_TIMEZONE`; `DATABASE_MODE` (code default `sqlite`, deployment default `postgresql`), `DATABASE_PATH`, `DB_*` (pool size must be ≥ `DAILY_BATCH_CONCURRENCY`, heartbeat < lease); capacity/worker knobs (`MAX_TOTAL_ROWS`, `JOB_LEASE_SECONDS`, `JOB_HEARTBEAT_SECONDS`); dictionary/LLM normalization knobs (`NORMALIZATION_LLM_*`); HTTP/LLM client settings (`LLM_BASE_URL`, `LLM_API_KEY`, `LLM_MODEL`, `LLM_JUDGEMENT_MODEL`, `LLM_ENABLE_THINKING`, ...). `config.validate_runtime_capacity` enforces cross-field invariants at startup.

Keep credentials and deployment-specific values in `.env` or server-side `config/.env` (Compose mounts code read-only, `runtime/` writable); never commit real keys, customer data, uploaded files, exports, or runtime databases.
