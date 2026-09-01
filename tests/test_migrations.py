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
    assert "occurrence_key" in {
        column["name"] for column in inspector.get_columns("events")
    }
    assert "occurrence_key" in {
        column["name"] for column in inspector.get_columns("corpus_records")
    }
    assert "anchor_key_hash" in {
        column["name"] for column in inspector.get_columns("canonical_anchors")
    }
    anchor_constraints = {
        constraint["name"]: tuple(constraint["column_names"])
        for constraint in inspector.get_unique_constraints("canonical_anchors")
    }
    assert anchor_constraints["uq_anchor_hash_version"] == (
        "street_id",
        "anchor_key_hash",
        "dictionary_version_id",
    )
    assert "uq_anchor_scope_version" not in anchor_constraints
    assert "alias_key_hash" in {
        column["name"] for column in inspector.get_columns("anchor_aliases")
    }
    alias_constraints = {
        constraint["name"]: tuple(constraint["column_names"])
        for constraint in inspector.get_unique_constraints("anchor_aliases")
    }
    assert alias_constraints["uq_anchor_alias_hash"] == (
        "anchor_id",
        "alias_key_hash",
    )
    assert "uq_anchor_alias" not in alias_constraints
    event_constraints = {
        constraint["name"]: tuple(constraint["column_names"])
        for constraint in inspector.get_unique_constraints("events")
    }
    assert event_constraints["uq_corpus_event_generation_key"] == (
        "generation_id",
        "street_id",
        "anchor_id",
        "issue_id",
        "occurrence_key",
        "event_key_version",
    )
    assert "uq_event_key_version" not in event_constraints
