from pathlib import Path

import pytest_asyncio

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_pipeline import CorpusProcessor
from complaint_dedup.corpus_repository import CorpusRepository


@pytest_asyncio.fixture
async def corpus(tmp_path: Path):
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'corpus.db'}")
    await database.initialize()
    repository = CorpusRepository(database)
    yield repository, CorpusProcessor(repository)
    await database.close()
