import asyncio
from contextlib import suppress
from pathlib import Path

from complaint_dedup.database import connect_database
from complaint_dedup.pipeline import JobProcessor


class JobRunner:
    def __init__(self, database_path: str | Path, processor: JobProcessor) -> None:
        self.database_path = Path(database_path)
        self.processor = processor
        self._task: asyncio.Task | None = None
        self._stopping = False

    async def start(self) -> None:
        self._recover_interrupted()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopping = True
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _loop(self) -> None:
        while not self._stopping:
            job_id = self._next_job()
            if not job_id:
                await asyncio.sleep(1)
                continue
            try:
                await self.processor.process(job_id)
            except Exception as exc:
                with connect_database(self.database_path) as connection:
                    connection.execute(
                        "UPDATE jobs SET status = 'failed', stage = 'failed', error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (str(exc), job_id),
                    )

    def _next_job(self) -> str | None:
        with connect_database(self.database_path) as connection:
            row = connection.execute(
                "SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at LIMIT 1"
            ).fetchone()
        return row["id"] if row else None

    def _recover_interrupted(self) -> None:
        with connect_database(self.database_path) as connection:
            connection.execute("UPDATE llm_batches SET status = 'pending' WHERE status = 'running'")
            connection.execute(
                """
                UPDATE jobs
                SET status = CASE WHEN pause_requested = 1 THEN 'paused' ELSE 'queued' END,
                    stage = CASE WHEN pause_requested = 1 THEN stage ELSE 'queued' END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = 'running'
                """
            )
