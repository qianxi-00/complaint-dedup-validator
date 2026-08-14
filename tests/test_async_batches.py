import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from complaint_dedup.async_database import AsyncDatabase


@pytest.mark.asyncio
async def test_succeeded_batch_is_skipped_on_resume(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    await database.enqueue_job("job-1", "批次任务", mode="single", total_records=1)

    assert await database.start_batch("job-1", "embedding", 0, [1]) is True
    assert (await database.get_job("job-1"))["inflight_batches"] == 1
    await database.finish_batch("job-1", "embedding", 0)
    assert (await database.get_job("job-1"))["inflight_batches"] == 0
    assert await database.start_batch("job-1", "embedding", 0, [1]) is False

    batches = await database.list_batches("job-1")
    assert batches[0]["status"] == "succeeded"
    assert batches[0]["attempts"] == 1
    await database.close()


@pytest.mark.asyncio
async def test_failed_batch_can_be_retried_and_tracks_attempts(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    await database.enqueue_job("job-1", "批次任务", mode="single", total_records=1)

    await database.start_batch("job-1", "judgement", 0, [5])
    await database.fail_batch("job-1", "judgement", 0, "模型超时")
    assert await database.start_batch("job-1", "judgement", 0, [5]) is True

    batch = (await database.list_batches("job-1"))[0]
    assert batch["status"] == "running"
    assert batch["attempts"] == 2
    assert batch["error_message"] is None
    assert (await database.get_job("job-1"))["retry_count"] == 1
    await database.close()


@pytest.mark.asyncio
async def test_running_batch_is_not_claimed_twice(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    await database.enqueue_job("job-1", "批次任务", mode="single", total_records=1)

    assert await database.start_batch("job-1", "embedding", 0, [1]) is True
    assert await database.start_batch("job-1", "embedding", 0, [1]) is False

    batch = (await database.list_batches("job-1"))[0]
    assert batch["attempts"] == 1
    await database.close()


@pytest.mark.asyncio
async def test_concurrent_batch_claim_has_single_winner(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    await database.enqueue_job("job-1", "批次任务", mode="single", total_records=1)

    results = await asyncio.gather(
        database.start_batch("job-1", "embedding", 0, [1]),
        database.start_batch("job-1", "embedding", 0, [1]),
    )

    assert sorted(results) == [False, True]
    assert (await database.list_batches("job-1"))[0]["attempts"] == 1
    await database.close()


@pytest.mark.asyncio
async def test_recover_expired_job_marks_running_batches_failed(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    await database.enqueue_job("job-1", "批次任务", mode="single", total_records=1)
    claimed_at = datetime.now(UTC)
    await database.claim_jobs("worker-a", limit=1, lease_seconds=1, now=claimed_at)
    await database.start_batch("job-1", "embedding", 0, [1])

    await database.recover_expired_jobs(now=claimed_at + timedelta(seconds=2))

    batch = (await database.list_batches("job-1"))[0]
    assert batch["status"] == "failed"
    assert "租约过期" in batch["error_message"]
    assert (await database.get_job("job-1"))["inflight_batches"] == 0
    await database.close()
