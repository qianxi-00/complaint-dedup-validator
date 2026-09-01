from io import BytesIO
from pathlib import Path

import pytest
from openpyxl import Workbook

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.config import Settings
from complaint_dedup.corpus_pipeline import CorpusProcessor
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.corpus_worker import CorpusBatchWorker


def _workbook(path: Path) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(
        [
            "工单编号",
            "受理时间",
            "诉求标题",
            "事发地点",
            "事项分类四级",
            "市民诉求",
        ]
    )
    sheet.append(
        [
            "WO-1",
            "2026-08-12 08:00:00",
            "德昌电机门口积水",
            "江海区礼乐街道德昌电机门口",
            "道路积水",
            "地址：江海区礼乐街道德昌电机门口。\n事项：道路积水。",
        ]
    )
    workbook.save(path)


@pytest.mark.asyncio
async def test_batch_lease_prevents_two_workers_from_claiming_same_batch(tmp_path: Path):
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'lease.db'}")
    await database.initialize()
    repository = CorpusRepository(database)
    batch_id = await repository.create_batch(
        "历史",
        "bootstrap_history",
        input_files={"history": {"path": "history.xlsx"}},
    )

    first = await repository.claim_batches("worker-1", limit=1, lease_seconds=60)
    second = await repository.claim_batches("worker-2", limit=1, lease_seconds=60)

    assert [row["id"] for row in first] == [batch_id]
    assert second == []
    assert await repository.heartbeat_batch(
        batch_id, "worker-1", lease_seconds=60
    )
    await repository.release_batch_lease(batch_id, "worker-1")
    await database.close()


@pytest.mark.asyncio
async def test_worker_stages_and_commits_history_without_user_dictionary_step(tmp_path: Path):
    path = tmp_path / "history.xlsx"
    _workbook(path)
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'worker.db'}")
    await database.initialize()
    repository = CorpusRepository(database)
    processor = CorpusProcessor(repository)
    settings = Settings(
        database_mode="sqlite",
        database_path=tmp_path / "worker.db",
        daily_batch_concurrency=1,
        _env_file=None,
    )
    batch_id = await repository.create_batch(
        "历史",
        "bootstrap_history",
        input_files={
            "history": {
                "path": str(path),
                "file_name": path.name,
                "file_hash": "a" * 64,
            }
        },
    )
    worker = CorpusBatchWorker(repository, processor, settings)

    assert await worker.run_once() == 1
    committed = await repository.get_batch(batch_id)
    assert committed["status"] == "committed"
    assert committed["total_records"] == 1
    assert committed["dictionary_version_id"] is not None
    assert len(await repository.list_events()) == 1
    await database.close()
