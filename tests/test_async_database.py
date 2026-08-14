from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from complaint_dedup.async_database import AsyncDatabase


@pytest.mark.asyncio
async def test_database_initializes_and_enqueues_single_job(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()

    await database.enqueue_job("job-1", "单文件任务", mode="single", total_records=3)
    job = await database.get_job("job-1")

    assert job["mode"] == "single"
    assert job["status"] == "queued"
    assert job["total_records"] == 3
    assert job["match_preset"] == "balanced"
    assert job["time_window_days"] == 0
    await database.close()


@pytest.mark.asyncio
async def test_job_persists_explicit_match_parameters(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()

    await database.enqueue_job(
        "job-1",
        "严格任务",
        mode="single",
        total_records=3,
        match_preset="strict",
        time_window_days=14,
    )

    job = await database.get_job("job-1")
    assert job["match_preset"] == "strict"
    assert job["time_window_days"] == 14
    await database.close()


@pytest.mark.asyncio
async def test_worker_claims_distinct_jobs_with_leases(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    await database.enqueue_job("job-1", "任务一", mode="single", total_records=1)
    await database.enqueue_job("job-2", "任务二", mode="cross", total_records=2)

    first = await database.claim_jobs("worker-a", limit=1, lease_seconds=300)
    second = await database.claim_jobs("worker-b", limit=1, lease_seconds=300)

    assert [job["id"] for job in first] == ["job-1"]
    assert [job["id"] for job in second] == ["job-2"]
    assert first[0]["lease_owner"] == "worker-a"
    await database.close()


@pytest.mark.asyncio
async def test_expired_job_is_requeued_but_paused_job_is_not(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    await database.enqueue_job("running", "运行任务", mode="single", total_records=1)
    await database.enqueue_job("paused", "暂停任务", mode="single", total_records=1)
    claimed_at = datetime.now(UTC)
    await database.claim_jobs("worker-a", limit=2, lease_seconds=1, now=claimed_at)
    await database.request_pause("paused", True)

    recovered = await database.recover_expired_jobs(now=claimed_at + timedelta(seconds=2))

    assert recovered == 1
    assert (await database.get_job("running"))["status"] == "queued"
    assert (await database.get_job("paused"))["status"] == "paused"
    await database.close()

@pytest.mark.asyncio
async def test_running_job_cannot_be_resumed_and_reclaimed(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'resume.db'}")
    await database.initialize()
    await database.enqueue_job("job-running", "运行中任务", mode="single", total_records=1)
    claimed = await database.claim_jobs("worker-a", limit=1, lease_seconds=300)
    assert len(claimed) == 1

    with pytest.raises(ValueError, match="运行中的任务不能重复继续"):
        await database.resume_job("job-running")

    assert await database.claim_jobs("worker-b", limit=1, lease_seconds=300) == []
    await database.close()
