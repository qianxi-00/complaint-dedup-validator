from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect
from sqlalchemy import text


def test_alembic_upgrades_empty_database_to_full_corpus_head(tmp_path: Path) -> None:
    database_path = tmp_path / "migration.db"
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database_path.as_posix()}")
    config.attributes["preserve_sqlalchemy_url"] = True
    command.upgrade(config, "head")

    inspector = inspect(create_engine(f"sqlite:///{database_path.as_posix()}"))
    expected = {
        "license_state",
        "work_orders",
        "sync_runs",
        "work_order_versions",
        "comparison_runs",
        "comparison_record_members",
        "comparison_events",
        "comparison_event_members",
        "work_order_cannot_links",
    }
    assert set(inspector.get_table_names()) == expected | {"alembic_version"}
    assert "missing_time_count" in {
        column["name"] for column in inspector.get_columns("comparison_runs")
    }


def test_full_corpus_schema_has_chinese_table_and_column_comments(tmp_path: Path) -> None:
    database_path = tmp_path / "comments.db"
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database_path.as_posix()}")
    config.attributes["preserve_sqlalchemy_url"] = True
    command.upgrade(config, "head")

    from complaint_dedup.async_database import metadata
    from complaint_dedup import corpus_schema  # noqa: F401

    for table in metadata.tables.values():
        assert table.comment, table.name
        assert all(column.comment for column in table.columns), table.name


def test_upgrade_removes_known_legacy_tables(tmp_path: Path) -> None:
    database_path = tmp_path / "cleanup.db"
    engine = create_engine(f"sqlite:///{database_path.as_posix()}")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE jobs (id TEXT PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE corpus_records (id INTEGER PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY)"))

    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database_path.as_posix()}")
    config.attributes["preserve_sqlalchemy_url"] = True
    command.upgrade(config, "head")

    inspector = inspect(engine)
    assert "jobs" not in inspector.get_table_names()
    assert "corpus_records" not in inspector.get_table_names()
    assert "users" not in inspector.get_table_names()


def test_alembic_has_single_new_baseline_head() -> None:
    config = Config("alembic.ini")
    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == ["20260908_0001"]


def test_alembic_can_downgrade_and_reupgrade(tmp_path: Path) -> None:
    database_path = tmp_path / "roundtrip.db"
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database_path.as_posix()}")
    config.attributes["preserve_sqlalchemy_url"] = True

    command.upgrade(config, "head")
    command.downgrade(config, "base")
    command.upgrade(config, "head")

    inspector = inspect(create_engine(f"sqlite:///{database_path.as_posix()}"))
    assert "work_orders" in inspector.get_table_names()
