from sqlalchemy import MetaData, event, inspect, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


metadata = MetaData()


class DatabaseInitializationError(RuntimeError):
    """当前数据库不是可直接运行的应用 schema。"""


_LEGACY_TABLES = frozenset(
    {
        "users",
        "user_sessions",
        "corpus_records",
        "corpus_generations",
        "daily_batches",
        "dictionary_versions",
        "canonical_issues",
        "canonical_anchors",
        "canonical_streets",
        "events",
        "event_members",
        "candidate_events",
        "candidate_event_members",
        "cannot_links",
        "work_order_cannot_links",
        "jobs",
        "job_batches",
        "records",
        "reviews",
    }
)


class AsyncDatabase:
    def __init__(
        self,
        database_url: str,
        *,
        pool_size: int = 10,
        max_overflow: int = 10,
    ) -> None:
        options = {"pool_pre_ping": True}
        if not database_url.startswith("sqlite"):
            options.update(pool_size=pool_size, max_overflow=max_overflow)
        self.engine: AsyncEngine = create_async_engine(database_url, **options)
        if database_url.startswith("sqlite"):
            @event.listens_for(self.engine.sync_engine, "connect")
            def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA busy_timeout=30000")
                cursor.close()

    async def initialize(self) -> None:
        # Importing the schema here keeps direct database users consistent with the web app.
        from complaint_dedup import corpus_schema  # noqa: F401

        async with self.engine.begin() as connection:
            await connection.run_sync(metadata.create_all)
            await connection.run_sync(_validate_schema)
            if connection.dialect.name == "postgresql":
                await connection.run_sync(_apply_postgresql_comments)

    async def close(self) -> None:
        await self.engine.dispose()


def _validate_schema(connection) -> None:
    inspector = inspect(connection)
    table_names = set(inspector.get_table_names())
    legacy = sorted(table_names & _LEGACY_TABLES)
    if legacy:
        raise DatabaseInitializationError(
            "数据库包含旧版无用表，无法自动兼容，请使用新的空数据库卷：" + ", ".join(legacy)
        )

    missing_tables = sorted(set(metadata.tables) - table_names)
    if missing_tables:
        raise DatabaseInitializationError(
            "数据库缺少当前版本数据表：" + ", ".join(missing_tables)
        )

    missing_columns: list[str] = []
    for table in metadata.tables.values():
        columns = {column["name"] for column in inspector.get_columns(table.name)}
        missing = sorted(set(table.columns.keys()) - columns)
        if missing:
            missing_columns.append(f"{table.name}（缺少字段：{', '.join(missing)}）")
    if missing_columns:
        raise DatabaseInitializationError(
            "数据库表结构不完整，无法自动补字段：" + "; ".join(missing_columns)
        )


def _apply_postgresql_comments(connection) -> None:
    for table in metadata.tables.values():
        if table.comment:
            connection.execute(
                text(
                    f'COMMENT ON TABLE "{table.name}" IS '
                    f"{_sql_literal(table.comment)}"
                )
            )
        for column in table.columns:
            if column.comment:
                connection.execute(
                    text(
                        f'COMMENT ON COLUMN "{table.name}"."{column.name}" IS '
                        f"{_sql_literal(column.comment)}"
                    )
                )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
