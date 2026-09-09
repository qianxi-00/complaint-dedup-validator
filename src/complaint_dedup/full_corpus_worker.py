from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup import licensing
from complaint_dedup.config import Settings
from complaint_dedup.corpus_database import corpus_database_url
from complaint_dedup.corpus_io import load_records_auto
from complaint_dedup.full_corpus import FullCorpusService
from complaint_dedup.logging_setup import setup_logging


class FullCorpusWorker:
    def __init__(self, service: FullCorpusService, settings: Settings) -> None:
        self.service = service
        self.settings = settings
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self.run(), name="full-corpus-worker")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def run(self) -> None:
        while not self._stop.is_set():
            job = await self.service.claim_job()
            if job is None:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=0.25)
                except asyncio.TimeoutError:
                    pass
                continue
            try:
                result = await self._run_job(job)
                await self.service.complete_job(job["id"], result)
            except Exception as exc:
                await self.service.fail_job(job["id"], str(exc))

    async def _run_job(self, job: dict) -> dict:
        return await asyncio.to_thread(self._run_job_in_thread, job)

    def _run_job_in_thread(self, job: dict) -> dict:
        return asyncio.run(self._run_job_with_isolated_database(job))

    async def _run_job_with_isolated_database(self, job: dict) -> dict:
        licensing.ensure_not_expired()
        database = AsyncDatabase(
            corpus_database_url(self.settings),
            pool_size=self.settings.db_pool_size,
            max_overflow=self.settings.db_max_overflow,
        )
        try:
            service = FullCorpusService(database)
            payload = job.get("payload") or {}
            if job["kind"] == "sync":
                path = Path(str(payload["path"]))
                records = await asyncio.to_thread(load_records_auto, path)
                if len(records) > self.settings.max_total_rows:
                    raise ValueError(
                        f"文件共 {len(records)} 行，超过上限 {self.settings.max_total_rows} 行"
                    )
                result = await service.sync_records(
                    records,
                    file_name=str(payload["file_name"]),
                    file_hash=str(payload["file_hash"]),
                )
                return {
                    "sync_id": result.sync_id,
                    "inserted": result.inserted,
                    "updated": result.updated,
                    "missing": result.missing,
                }
            if job["kind"] == "comparison":
                result = await service.compare(
                    time_field=str(payload.get("time_field") or "completed_at"),
                    target_from=date.fromisoformat(str(payload["target_from"])),
                    target_to=date.fromisoformat(str(payload["target_to"])),
                    reference_from=(
                        date.fromisoformat(str(payload["reference_from"]))
                        if payload.get("reference_from")
                        else None
                    ),
                    reference_to=(
                        date.fromisoformat(str(payload["reference_to"]))
                        if payload.get("reference_to")
                        else None
                    ),
                )
                return {
                    "comparison_id": result.comparison_id,
                    "target_count": result.target_count,
                    "reference_count": result.reference_count,
                    "event_count": result.event_count,
                    "singleton_count": result.singleton_count,
                    "missing_time_count": result.missing_time_count,
                }
            raise ValueError("后台任务类型无效")
        finally:
            await database.close()


async def run_worker(settings: Settings | None = None) -> None:
    settings = settings or Settings()
    database = AsyncDatabase(
        corpus_database_url(settings),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )
    await database.initialize()
    setup_logging(
        log_dir=settings.log_dir,
        process_name="worker",
        level=settings.log_level,
        retention_days=settings.log_retention_days,
    )
    await licensing.ensure_license_ok(database)
    worker = FullCorpusWorker(FullCorpusService(database), settings)
    try:
        await worker.run()
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(run_worker())
