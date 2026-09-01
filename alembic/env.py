import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import async_engine_from_config

from complaint_dedup.async_database import metadata
from complaint_dedup import corpus_schema  # noqa: F401
from complaint_dedup.corpus_database import corpus_database_url
from complaint_dedup.config import get_settings


config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)
if not config.attributes.get("preserve_sqlalchemy_url"):
    config.set_main_option(
        "sqlalchemy.url", corpus_database_url(get_settings()).replace("%", "%%")
    )
target_metadata = metadata


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_sync_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    engine = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        pool_pre_ping=True,
    )
    async with engine.connect() as connection:
        await connection.run_sync(run_sync_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_async_migrations())
