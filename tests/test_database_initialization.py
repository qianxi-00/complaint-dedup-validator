from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

from complaint_dedup.async_database import AsyncDatabase, DatabaseInitializationError


@pytest.mark.asyncio
async def test_initialize_rejects_current_table_with_missing_columns(tmp_path: Path) -> None:
    database_path = tmp_path / "incomplete.db"
    engine = create_engine(f"sqlite:///{database_path.as_posix()}")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE work_orders (record_key VARCHAR(128) PRIMARY KEY)"))
    engine.dispose()

    database = AsyncDatabase(f"sqlite+aiosqlite:///{database_path.as_posix()}")
    try:
        with pytest.raises(DatabaseInitializationError, match="work_orders.*缺少字段"):
            await database.initialize()
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_initialize_rejects_known_legacy_tables(tmp_path: Path) -> None:
    database_path = tmp_path / "legacy.db"
    engine = create_engine(f"sqlite:///{database_path.as_posix()}")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY)"))
    engine.dispose()

    database = AsyncDatabase(f"sqlite+aiosqlite:///{database_path.as_posix()}")
    try:
        with pytest.raises(DatabaseInitializationError, match="users"):
            await database.initialize()
    finally:
        await database.close()


def test_intranet_compose_uses_internal_database_defaults_and_no_migrate_service() -> None:
    compose = Path("deploy/compose.intranet.yaml").read_text(encoding="utf-8")
    env_template = Path("deploy/env.intranet.example").read_text(encoding="utf-8")

    assert "  migrate:" not in compose
    assert "POSTGRES_PASSWORD" in compose
    assert "DB_PASSWORD" in compose
    assert "depends_on:\n      api:\n        condition: service_healthy" in compose
    assert "- runtime_data:/app/runtime" in compose
    assert "./runtime:/app/runtime" not in compose
    assert "POSTGRES_PASSWORD=" not in env_template
    assert "DB_PASSWORD=" not in env_template


def test_intranet_tester_runs_obfuscated_package_with_deployment_inputs() -> None:
    dockerfile = Path("Dockerfile.intranet").read_text(encoding="utf-8")
    assert "COPY deploy/ /build/deploy/" in dockerfile
    assert "PYTHONPATH=/build/obf python -m pytest" in dockerfile
