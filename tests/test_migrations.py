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
        "processing_jobs",
        "work_order_versions",
        "comparison_runs",
        "comparison_record_members",
        "comparison_events",
        "comparison_event_members",
        "comparison_decisions",
    }
    assert set(inspector.get_table_names()) == expected | {"alembic_version"}
    assert "missing_time_count" in {
        column["name"] for column in inspector.get_columns("comparison_runs")
    }
    assert "business_columns" in {
        column["name"] for column in inspector.get_columns("sync_runs")
    }
    assert {
        "canonical_work_order_id",
        "complaint_fingerprint",
        "feature_json",
        "feature_version",
    } <= {column["name"] for column in inspector.get_columns("work_orders")}
    assert {
        "algorithm_version",
        "feature_version",
        "prompt_version",
        "model_id",
        "llm_coverage",
        "fallback_count",
        "decision_count",
    } <= {column["name"] for column in inspector.get_columns("comparison_runs")}
    assert {
        "card_ids",
        "assigned_card_ids",
        "final_event_key",
    } <= {
        column["name"]
        for column in inspector.get_columns("comparison_decisions")
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


def test_upgrade_adds_business_columns_to_existing_sync_runs(tmp_path: Path) -> None:
    database_path = tmp_path / "existing-sync-runs.db"
    engine = create_engine(f"sqlite:///{database_path.as_posix()}")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE sync_runs (id VARCHAR(64) PRIMARY KEY)"))

    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{database_path.as_posix()}")
    config.attributes["preserve_sqlalchemy_url"] = True
    command.upgrade(config, "head")

    assert "business_columns" in {
        column["name"] for column in inspect(engine).get_columns("sync_runs")
    }


def test_upgrade_backfills_dedup_features_for_existing_work_orders(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "backfill.db"
    config = Config("alembic.ini")
    config.set_main_option(
        "sqlalchemy.url", f"sqlite+aiosqlite:///{database_path.as_posix()}"
    )
    config.attributes["preserve_sqlalchemy_url"] = True
    command.upgrade(config, "20260909_0002")

    engine = create_engine(f"sqlite:///{database_path.as_posix()}")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO work_orders (
                    record_key, work_order_id, title_raw, appeal_text, category,
                    location, raw_json, missing_in_latest_upload,
                    created_at, updated_at
                ) VALUES (
                    'wo:BASE001HBD1', 'BASE001HBD1', '东宁路路灯不亮',
                    '地址：江海区礼乐街道东宁路1号。事项：路灯连续多日不亮。',
                    '路灯故障', '江海区礼乐街道东宁路1号', '{}', 0,
                    '2026-09-01 00:00:00', '2026-09-01 00:00:00'
                )
                """
            )
        )

    command.upgrade(config, "head")
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT canonical_work_order_id, complaint_fingerprint,
                       feature_json, feature_version
                FROM work_orders
                WHERE record_key = 'wo:BASE001HBD1'
                """
            )
        ).mappings().one()

    assert row["canonical_work_order_id"] == "BASE001"
    assert row["complaint_fingerprint"]
    assert row["feature_json"]
    assert row["feature_version"] == "feature-v2"


def test_alembic_has_single_new_baseline_head() -> None:
    config = Config("alembic.ini")
    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == ["20260910_0004"]


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
