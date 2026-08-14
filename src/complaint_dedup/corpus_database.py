from sqlalchemy.engine import URL

from complaint_dedup.config import Settings


def corpus_database_url(settings: Settings) -> str:
    if settings.database_mode == "sqlite":
        return f"sqlite+aiosqlite:///{settings.database_path}"
    return URL.create(
        drivername="postgresql+asyncpg",
        username=settings.db_user,
        password=settings.db_password,
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
    ).render_as_string(hide_password=False)
