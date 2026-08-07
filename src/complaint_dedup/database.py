import sqlite3
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'queued',
    stage TEXT NOT NULL DEFAULT 'queued',
    match_preset TEXT NOT NULL DEFAULT 'balanced',
    time_window_days INTEGER NOT NULL DEFAULT 0,
    source_a_name TEXT,
    source_b_name TEXT,
    total_records INTEGER NOT NULL DEFAULT 0,
    extracted_records INTEGER NOT NULL DEFAULT 0,
    candidate_count INTEGER NOT NULL DEFAULT 0,
    judged_count INTEGER NOT NULL DEFAULT 0,
    extraction_failure_count INTEGER NOT NULL DEFAULT 0,
    judgement_failure_count INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    pause_requested INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    source TEXT NOT NULL CHECK (source IN ('A', 'B')),
    source_row INTEGER NOT NULL,
    work_order_id TEXT,
    received_at TEXT,
    title TEXT,
    category TEXT,
    appeal_text TEXT,
    raw_json TEXT NOT NULL,
    extraction_status TEXT NOT NULL DEFAULT 'pending',
    extraction_json TEXT,
    extraction_error TEXT,
    UNIQUE (job_id, source, source_row)
);

CREATE TABLE IF NOT EXISTS llm_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    batch_type TEXT NOT NULL CHECK (batch_type IN ('extraction', 'judgement')),
    batch_index INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    request_json TEXT,
    response_json TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (job_id, batch_type, batch_index)
);

CREATE TABLE IF NOT EXISTS candidate_pairs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    record_a_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
    record_b_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
    candidate_key TEXT,
    rule_status TEXT NOT NULL DEFAULT 'pending',
    judgement_status TEXT NOT NULL DEFAULT 'pending',
    llm_decision TEXT,
    confidence REAL,
    evidence_json TEXT,
    hard_conflicts_json TEXT,
    event_name_suggestion TEXT,
    judgement_error TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (job_id, record_a_id, record_b_id),
    CHECK (record_a_id <> record_b_id)
);

CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_pair_id INTEGER NOT NULL UNIQUE
        REFERENCES candidate_pairs(id) ON DELETE CASCADE,
    decision TEXT NOT NULL CHECK (decision IN ('duplicate', 'not_duplicate')),
    note TEXT,
    reviewed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS event_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS event_members (
    event_group_id INTEGER NOT NULL REFERENCES event_groups(id) ON DELETE CASCADE,
    record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
    PRIMARY KEY (event_group_id, record_id)
);

CREATE INDEX IF NOT EXISTS idx_records_job_source
    ON records(job_id, source);
CREATE INDEX IF NOT EXISTS idx_batches_job_status
    ON llm_batches(job_id, status);
CREATE INDEX IF NOT EXISTS idx_pairs_job_status
    ON candidate_pairs(job_id, judgement_status);
"""


def connect_database(path: str | Path) -> sqlite3.Connection:
    database_path = Path(path)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    return connection


def initialize_database(path: str | Path) -> None:
    database_path = Path(path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with connect_database(database_path) as connection:
        connection.executescript(SCHEMA)
