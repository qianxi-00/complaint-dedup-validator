# Repository Guidelines

## Project Overview

投诉全量工单比对系统 (`complaint-dedup-validator`) maintains a continuously updated full work-order corpus and runs duplicate detection between a target time window and a reference side (manual window or complement of the corpus). Every comparison is frozen as an independent task snapshot, so later syncs never change historical results. Results are filterable, reviewable, manually adjustable, and exportable.

Stack: Python 3.12, FastAPI, SQLAlchemy 2 async, Alembic, Jinja2 server-rendered templates, loguru, xlsxwriter; SQLite for local runs, PostgreSQL for intranet Docker. Event grouping is deterministic hard merge + gray-zone candidate recall + external LLM event-card adjudication + conservative rule fallback. Do not reintroduce accounts, daily-batch queues, generation rollover, HTMX partials, vector stores, or `cannot_links`.

## Project Structure

- `src/complaint_dedup/` — application package.
  - `main.py` — entry point and `build_app` factory.
  - `full_corpus.py` — `FullCorpusService` core: sync upsert / missing marking / version snapshots, target-reference window partitioning (complement, manual, empty-date handling), event persistence, filtered/paginated event queries, export rows.
  - `full_corpus_web.py` — FastAPI routes, upload staging under `runtime/full_uploads`, Jinja filters (`localtime`, `endday`), license middleware, app lifespan with embedded worker for SQLite.
  - `full_corpus_worker.py` — background job loop (`claim_job` / `complete_job` / `fail_job`); embedded in the API for SQLite, standalone process (`python -m complaint_dedup.full_corpus_worker`) for PostgreSQL.
  - `dedup_engine.py` — `DedupEngine`: hard merge (canonical work-order ID including `HBDn` suffix, complaint fingerprint, occurrence IDs), multi-path candidate recall, event-card LLM partition/validation, request/time-budget conservative fallback.
  - `dedup_features.py` — reusable feature normalization (`FEATURE_VERSION = "feature-v2"`): HBD suffix stripping, complaint fingerprint, occurrence identifiers, PII sanitizing for model payloads.
  - `corpus_parser.py` — complaint text parsing: region/street/road/building/unit/room/anchor, organization subject, previous work-order IDs, occurrence IDs, issue segments.
  - `full_corpus_exporter.py` — xlsxwriter export: `重复项` / `孤立工单` sheets, event coloring, frozen header, autofilter, formula-injection guard.
  - `corpus_schema.py` — 10 business tables with Chinese comments. `async_database.py` creates/validates the schema and applies PostgreSQL comments; `corpus_database.py` builds SQLite/PostgreSQL URLs.
  - `llm_client.py` / `llm_models.py` — OpenAI-compatible async client with retries/backoff and Pydantic response models (`EventCardBatchResponse` etc.).
  - `licensing.py` / `logging_setup.py` — Shanghai-time expiry checks, clock-rollback guard via `license_state`, loguru console + rotating file sinks.
- `templates/full_*.html` — four server-rendered templates: `full_base.html`, `full_index.html` (workbench), `full_comparisons.html` (event library), `full_event_detail.html`. No partials; `static/vendor/htmx.min.js` is unused.
- `alembic/versions/` — four migrations, single head `20260910_0004`. Baseline `20260908_0001_full_corpus_baseline.py` targets empty databases; legacy schemas are never migrated in place.
- `tests/` — 14 pytest modules (~100 test functions) plus `export_ui.test.cjs` (Node built-in runner). No `conftest.py`; async fixtures live per file.
- `scripts/` — `reset_database.py` (destructive reset), `build_intranet.ps1` / `build_intranet_bundle.sh` (offline delivery), `obfuscate.sh` (PyArmor in builder).
- `runtime/` — local scratch (uploads, exports, logs, SQLite databases); never commit its contents.

## Build, Test, and Run

Run from the repository root:

- `uv sync` — install locked dependencies.
- `uv run pytest -q` — full regression suite.
- `node --test tests/export_ui.test.cjs` — export button JS tests.
- `uv run alembic upgrade head` — retained for migration tests and empty databases; normal Docker startup creates and validates the current schema automatically.
- `uv run uvicorn complaint_dedup.main:app --host 127.0.0.1 --port 8765` — local web service (SQLite embedded worker).
- `uv run python -m complaint_dedup.full_corpus_worker` — standalone worker.
- `docker compose --project-directory . -f deploy/compose.intranet.yaml up -d` — PostgreSQL intranet stack; API maps `28765:8765`, worker waits for API health.

## Domain Workflow

1. `POST /sync` stores one full `.xlsx` / `.xls` / `.csv` upload; the worker parses it (`corpus_io.load_records_auto`) and `sync_records` upserts by `record_key` (`wo:<工单编号>` or `fp:<sha256>`), marks absent orders `missing_in_latest_upload`, and writes `work_order_versions` audit snapshots. Unchanged rows update only `last_sync_id`, no version snapshot.
2. `POST /comparisons` creates a comparison task. `time_field` is `received_at` (default) or `completed_at`; empty dates default to the latest local date in the corpus. Without a manual reference window the reference side is the complement; records with an empty selected date go to the target side and are counted in `missing_time_count`. Manual windows must cover both ends and must not overlap the target window; records outside both manual windows are excluded.
3. `DedupEngine.cluster` runs over target + reference rows: hard merges first, gray-zone recall next, LLM event-card adjudication for ambiguous components, and a conservative `legacy event_key` fallback on any model error, invalid output, or request/time budget exhaustion.
4. Results are frozen in `comparison_runs`, `comparison_record_members`, `comparison_events`, `comparison_event_members`, and `comparison_decisions` (decision audit). The event library supports region/street/department/completed-date filters, keyword search scoped to the current result or `search_all`, sorting, pagination, rename, and member move.
5. Export has two scopes: full (all events) and filtered (event set from current filters, then all members of those events). Both write the same workbook format.

## Style and Naming

Use Python 3.12, four-space indentation, type hints on public APIs, `snake_case` functions and variables, and `PascalCase` classes. Keep persistence, domain logic, web routes, worker, and templates in their existing modules. Use UTF-8 Simplified Chinese for UI text. Prefer structured SQLAlchemy expressions and pure helper functions over string-built SQL.

## Testing

Use `pytest` with explicit `@pytest.mark.asyncio` for async tests and per-file `pytest_asyncio.fixture` databases under `tmp_path`. Add a failing regression test before changing behavior.

- Dedup rules, candidate recall, LLM partition/validation/fallback: `tests/test_dedup_engine.py` (fake LLM clients).
- Feature normalization and PII masking: `tests/test_dedup_features.py`.
- Parser behavior: `tests/test_corpus_parser.py`; file inspection: `tests/test_file_inspection.py`.
- Sync / window / snapshot / export behavior: `tests/test_full_corpus_window.py`.
- Web route contracts: `tests/test_full_corpus_web.py`.
- Schema and migrations: `tests/test_migrations.py` and `tests/test_database_initialization.py`. Schema changes require a migration test, a fresh SQLite upgrade check, and a single Alembic head.

## Dedup Evaluation

- Rebuild the frozen set from a local database: `uv run python scripts/build_eval_set.py --db runtime/<db>.db --source-xlsx "<原始全量文件>.xlsx"`.
- Review the uncertain queue and write accepted labels back: `uv run python scripts/review_eval_queue.py --apply` (rule + model review; supports `--no-llm`, `--from-results`, `--refine`).
- Deterministic (no model) metrics: `uv run python scripts/eval_dedup.py --dataset tests/fixtures/dedup_eval --no-llm`; add `--compare <previous.json>` for before/after tables.
- Baseline report: `docs/判重评估报告-基线-v3.0.md`. Any dedup behavior change must rerun the evaluation and add a matching regression test; thresholds are enforced by `tests/test_dedup_eval.py`.
- Never fabricate code versions, model parameters, vLLM details, or case IDs. Unverifiable facts must be recorded as `不可核验`; case IDs must come from the frozen dataset or the database.

## Commits and Pull Requests

Use concise Conventional Commit messages such as `feat: add full corpus window comparison` or `fix: reject overlapping comparison windows`. Pull requests should describe data-model changes, deployment impact, authorization/PyArmor implications, and verification commands. Never commit `.env` files, credentials, customer spreadsheets, exports, or runtime databases.

## Security and Deployment

Keep PostgreSQL credentials and external LLM settings in server-side `config/.env`. Keep licensing checks, Shanghai-time expiry handling, clock-rollback protection, and PyArmor build arguments (`EXPIRE_DATE`, `PYARMOR_REQUIRE_FULL`) intact; `licensing_conf.py` is generated at build time and must never be committed. API startup creates missing current tables, applies PostgreSQL Chinese comments, and rejects legacy or incomplete schemas. The intranet database is managed by Compose; do not edit it manually.
