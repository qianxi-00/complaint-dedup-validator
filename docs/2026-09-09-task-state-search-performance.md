# Task State, Search, and Event Library Performance Implementation Plan

> **For agentic workers:** Execute this plan task-by-task with test-first changes and verification after each task.

**Goal:** Remove progress displays, reject duplicate active submissions, add scoped/full keyword search, and reduce event-library query latency without changing comparison semantics.

**Architecture:** Keep the existing FastAPI + async SQLAlchemy + Jinja2 architecture. Enforce the single-active-job rule inside `FullCorpusService.create_job`, expose Chinese status labels at the presentation boundary, and extend `EventFilters` so list, pagination, and filtered export share the same search predicate. Replace Python-side event filtering/sorting with database queries only where the existing schema supports it, preserving current event/member semantics.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy Async, SQLite/PostgreSQL, Jinja2, Pytest, Playwright.

---

### Task 1: Regression tests for job admission and status presentation

**Files:**
- Modify: `tests/test_full_corpus_web.py`
- Modify: `templates/full_index.html`

- [ ] Add tests proving a second `/sync` or `/comparisons` submission receives a clear 409 response while a `queued` or `running` job exists.
- [ ] Add tests proving the home page and job JSON expose Chinese status text and no progress column/value.
- [ ] Run the focused tests and confirm they fail before production changes.

### Task 2: Single active job and Chinese status mapping

**Files:**
- Modify: `src/complaint_dedup/full_corpus.py`
- Modify: `src/complaint_dedup/full_corpus_web.py`
- Modify: `templates/full_index.html`

- [ ] Add an atomic service check for `queued`/`running` jobs before insertion; raise a dedicated conflict error containing the active job kind and ID.
- [ ] Map internal states to `待处理`、`执行中`、`已完成`、`失败` in API/template data.
- [ ] Remove progress markup and progress interpolation from all home-page task views and banners.
- [ ] Return HTTP 409 with a Chinese message for rejected submissions.

### Task 3: Search filters and shared query parameters

**Files:**
- Modify: `src/complaint_dedup/full_corpus.py`
- Modify: `src/complaint_dedup/full_corpus_web.py`
- Modify: `templates/full_comparisons.html`
- Modify: `tests/test_full_corpus_web.py`

- [ ] Add `keyword` and `search_all` to `EventFilters`.
- [ ] Search event name, issue name, work-order ID, title, citizen appeal, region, street,所属部门, and处理部门.
- [ ] Apply existing filters when `search_all` is false; omit them when true while retaining the selected comparison.
- [ ] Preserve pagination and pass search parameters into both filtered-export and page URLs.
- [ ] Add focused tests for scoped search, full search, and no-match behavior.

### Task 4: Event-library performance

**Files:**
- Modify: `src/complaint_dedup/full_corpus.py`
- Modify: `src/complaint_dedup/corpus_schema.py` only if an existing query lacks an index
- Modify: `tests/test_full_corpus_web.py`

- [ ] Avoid loading unrelated job/event data during the initial page render.
- [ ] Ensure event pagination is applied before materializing page results where safe; retain exact counts and export semantics.
- [ ] Cache immutable comparison event/member snapshots per comparison and invalidate on event mutation.
- [ ] Add a regression check that the event page remains paginated and search does not expand the result set unexpectedly.

### Task 5: Verification and browser acceptance

- [ ] Run focused tests, then `uv run pytest -q`.
- [ ] Start or use the local service and verify HTTP 409 duplicate submission behavior.
- [ ] Use Playwright to verify no progress text, Chinese status labels, search scope/full-search behavior, pagination, and no layout overlap.
- [ ] Check `git diff`, `git status`, and report any remaining warnings or performance limitations.
