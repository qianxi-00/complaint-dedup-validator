from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


metadata = MetaData()


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

    async def initialize(self) -> None:
        async with self.engine.begin() as connection:
            await connection.run_sync(metadata.create_all)

    async def close(self) -> None:
        await self.engine.dispose()
