import asyncio
from pathlib import Path

import pytest

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.async_worker import AsyncJobRunner


class TrackingProcessor:
    def __init__(self, database: AsyncDatabase) -> None:
        self.database = database
        self.active = 0
        self.max_active = 0

    async def process(self, job_id: str) -> None:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.02)
        await self.database.set_job_state(job_id, status="review_ready", stage="review")
        self.active -= 1


class RecoveringDatabase:
    def __init__(self) -> None:
        self.recover_calls = 0
        self.claim_calls = 0

    async def recover_expired_jobs(self) -> int:
        self.recover_calls += 1
        return 0

    async def claim_jobs(self, worker_id: str, *, limit: int, lease_seconds: int):
        self.claim_calls += 1
        return []


class BlockingProcessor:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def process(self, job_id: str) -> None:
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class LostLeaseDatabase:
    def __init__(self) -> None:
        self.heartbeats = 0
        self.failed_updates = 0
        self.releases = 0

    async def heartbeat_job(self, job_id: str, worker_id: str, *, lease_seconds: int) -> bool:
        self.heartbeats += 1
        return False

    async def set_job_state(self, job_id: str, *, status: str, stage: str) -> None:
        self.failed_updates += 1

    async def release_lease(self, job_id: str, worker_id: str) -> None:
        self.releases += 1


@pytest.mark.asyncio
async def test_runner_processes_multiple_jobs_up_to_configured_concurrency(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    for index in range(4):
        await database.enqueue_job(
            f"job-{index}", f"任务{index}", mode="single", total_records=1
        )
    processor = TrackingProcessor(database)
    runner = AsyncJobRunner(
        database=database,
        processor=processor,
        concurrency=2,
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval=0.001,
    )

    await runner.run_until_idle()

    assert processor.max_active == 2
    statuses = [
        (await database.get_job(f"job-{index}"))["status"] for index in range(4)
    ]
    assert statuses == ["review_ready"] * 4
    await database.close()


@pytest.mark.asyncio
async def test_running_loop_periodically_recovers_expired_jobs() -> None:
    database = RecoveringDatabase()
    runner = AsyncJobRunner(
        database=database,  # type: ignore[arg-type]
        processor=object(),
        concurrency=1,
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval=0.005,
    )

    await runner.start()
    await asyncio.sleep(0.025)
    await runner.stop()

    assert database.recover_calls >= 2
    assert database.claim_calls >= 2


@pytest.mark.asyncio
async def test_lost_lease_cancels_processor_without_writing_failed_state() -> None:
    database = LostLeaseDatabase()
    processor = BlockingProcessor()
    runner = AsyncJobRunner(
        database=database,  # type: ignore[arg-type]
        processor=processor,
        concurrency=1,
        lease_seconds=1,
        heartbeat_seconds=0.001,  # type: ignore[arg-type]
        poll_interval=0.001,
    )

    task = asyncio.create_task(runner._process_job("job-lost"))
    await processor.started.wait()
    await asyncio.wait_for(processor.cancelled.wait(), timeout=0.1)
    await task

    assert database.heartbeats == 1
    assert database.failed_updates == 0
    assert database.releases == 1
