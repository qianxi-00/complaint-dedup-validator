from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect


def test_alembic_upgrades_empty_database_to_corpus_head(tmp_path: Path) -> None:
    database_path = tmp_path / "migration.db"
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database_path.as_posix()}")
    config.attributes["preserve_sqlalchemy_url"] = True

    command.upgrade(config, "head")

    inspector = inspect(create_engine(f"sqlite:///{database_path.as_posix()}"))
    assert {
        "corpus_records",
        "dictionary_versions",
        "canonical_anchors",
        "canonical_issues",
        "events",
        "corpus_event_members",
    }.issubset(
        inspector.get_table_names()
    )
    assert "event_key_version" in {
        column["name"] for column in inspector.get_columns("events")
    }
