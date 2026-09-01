import asyncio
from contextlib import suppress

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.config import get_settings
from complaint_dedup.corpus_database import corpus_database_url
from complaint_dedup.corpus_pipeline import CorpusProcessor
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.corpus_worker import CorpusBatchWorker
from complaint_dedup.llm_client import build_llm_client


async def run_worker() -> None:
    settings = get_settings()
    database = AsyncDatabase(
        corpus_database_url(settings),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )
    await database.initialize()
    repository = CorpusRepository(database)
    normalization_llm = (
        build_llm_client(
            settings,
            model=settings.llm_judgement_model or settings.llm_model,
            concurrency=settings.normalization_llm_concurrency,
        )
        if settings.normalization_llm_enabled and settings.llm_model
        else None
    )
    runner = CorpusBatchWorker(
        repository,
        CorpusProcessor(
            repository,
            dictionary_review_required=settings.dictionary_review_required,
            normalization_llm_client=normalization_llm,
            normalization_llm_enabled=settings.normalization_llm_enabled,
            normalization_llm_min_confidence=settings.normalization_llm_min_confidence,
            normalization_llm_batch_size=settings.normalization_llm_batch_size,
        ),
        settings,
    )
    await runner.start()
    try:
        await asyncio.Event().wait()
    finally:
        with suppress(Exception):
            await runner.stop()
        if normalization_llm is not None:
            with suppress(Exception):
                await normalization_llm.aclose()
        await database.close()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
