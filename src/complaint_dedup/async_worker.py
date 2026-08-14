import asyncio
import uuid
from contextlib import suppress
from typing import Any

from complaint_dedup.async_database import AsyncDatabase


class AsyncJobRunner:
    def __init__(
        self,
        *,
        database: AsyncDatabase,
        processor: Any,
        concurrency: int,
        lease_seconds: int,
        heartbeat_seconds: int,
        poll_interval: float = 1.0,
    ) -> None:
        self.database = database
        self.processor = processor
        self.concurrency = concurrency
        self.lease_seconds = lease_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.poll_interval = poll_interval
        self.worker_id = uuid.uuid4().hex
        self._loop_task: asyncio.Task[None] | None = None
        self._active: set[asyncio.Task[None]] = set()
        self._stopping = False

    async def start(self) -> None:
        await self.database.recover_expired_jobs()
        self._loop_task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopping = True
        if self._loop_task:
            self._loop_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._loop_task
        if self._active:
            await asyncio.gather(*self._active, return_exceptions=True)

    async def run_until_idle(self) -> None:
        await self.database.recover_expired_jobs()
        while True:
            await self._fill_slots()
            if not self._active:
                return
            done, _ = await asyncio.wait(self._active, return_when=asyncio.FIRST_COMPLETED)
            self._active.difference_update(done)
            for task in done:
                task.result()

    async def _loop(self) -> None:
        while not self._stopping:
            await self.database.recover_expired_jobs()
            await self._fill_slots()
            if not self._active:
                await asyncio.sleep(self.poll_interval)
                continue
            done, _ = await asyncio.wait(
                self._active,
                timeout=self.poll_interval,
                return_when=asyncio.FIRST_COMPLETED,
            )
            self._active.difference_update(done)
            for task in done:
                with suppress(Exception):
                    task.result()

    async def _fill_slots(self) -> None:
        available = self.concurrency - len(self._active)
        if available <= 0:
            return
        claimed = await self.database.claim_jobs(
            self.worker_id,
            limit=available,
            lease_seconds=self.lease_seconds,
        )
        for job in claimed:
            self._active.add(asyncio.create_task(self._process_job(job["id"])))

    async def _process_job(self, job_id: str) -> None:
        processing = asyncio.create_task(self.processor.process(job_id))
        heartbeat = asyncio.create_task(self._heartbeat(job_id))
        try:
            done, _ = await asyncio.wait(
                {processing, heartbeat},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat in done and heartbeat.result() is False:
                processing.cancel()
                with suppress(asyncio.CancelledError):
                    await processing
                return
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
            await processing
        except Exception:
            await self.database.set_job_state(job_id, status="failed", stage="failed")
            raise
        finally:
            if not processing.done():
                processing.cancel()
                with suppress(asyncio.CancelledError):
                    await processing
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
            await self.database.release_lease(job_id, self.worker_id)

    async def _heartbeat(self, job_id: str) -> bool:
        while True:
            await asyncio.sleep(self.heartbeat_seconds)
            updated = await self.database.heartbeat_job(
                job_id,
                self.worker_id,
                lease_seconds=self.lease_seconds,
            )
            if not updated:
                return False
