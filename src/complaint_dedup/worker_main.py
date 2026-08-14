import asyncio
from contextlib import suppress

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.config import get_settings
from complaint_dedup.corpus_database import corpus_database_url
from complaint_dedup.corpus_pipeline import CorpusProcessor
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.corpus_worker import CorpusBatchWorker


async def run_worker() -> None:
    settings = get_settings()
    database = AsyncDatabase(
        corpus_database_url(settings),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )
    await database.initialize()
    repository = CorpusRepository(database)
    runner = CorpusBatchWorker(
        repository,
        CorpusProcessor(
            repository,
            dictionary_review_required=settings.dictionary_review_required,
        ),
        settings,
    )
    await runner.start()
    try:
        await asyncio.Event().wait()
    finally:
        with suppress(Exception):
            await runner.stop()
        await database.close()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
