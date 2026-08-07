import sqlite3
from pathlib import Path

from complaint_dedup.database import connect_database, initialize_database


EXPECTED_TABLES = {
    "jobs",
    "records",
    "llm_batches",
    "candidate_pairs",
    "reviews",
    "event_groups",
    "event_members",
}


def test_initialize_database_creates_schema_and_pragmas(tmp_path: Path) -> None:
    database_path = tmp_path / "nested" / "app.db"

    initialize_database(database_path)

    with connect_database(database_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
            if not row[0].startswith("sqlite_")
        }
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]

    assert tables == EXPECTED_TABLES
    assert journal_mode.lower() == "wal"
    assert foreign_keys == 1


def test_foreign_keys_are_enforced(tmp_path: Path) -> None:
    database_path = tmp_path / "app.db"
    initialize_database(database_path)

    with connect_database(database_path) as connection:
        try:
            connection.execute(
                "INSERT INTO records (job_id, source, source_row, raw_json) "
                "VALUES ('missing', 'A', 1, '{}')"
            )
        except sqlite3.IntegrityError:
            pass
        else:
            raise AssertionError("records.job_id must enforce its foreign key")


def test_initialize_database_is_idempotent(tmp_path: Path) -> None:
    database_path = tmp_path / "app.db"

    initialize_database(database_path)
    initialize_database(database_path)
