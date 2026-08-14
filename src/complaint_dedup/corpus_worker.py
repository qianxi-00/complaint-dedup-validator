from __future__ import annotations

import asyncio
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any

from complaint_dedup.config import Settings
from complaint_dedup.corpus_io import load_records_auto
from complaint_dedup.corpus_pipeline import CorpusProcessor
from complaint_dedup.corpus_repository import CorpusRepository


class CorpusBatchWorker:
    def __init__(
        self,
        repository: CorpusRepository,
        processor: CorpusProcessor,
        settings: Settings,
    ) -> None:
        self.repository = repository
        self.processor = processor
        self.settings = settings
        self.worker_id = uuid.uuid4().hex
        self._loop_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        if self._loop_task is None:
            self._stopping.clear()
            self._loop_task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopping.set()
        if self._loop_task is not None:
            self._loop_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._loop_task
            self._loop_task = None

    async def run_once(self) -> int:
        batches = await self.repository.claim_batches(
            self.worker_id,
            limit=self.settings.daily_batch_concurrency,
            lease_seconds=self.settings.job_lease_seconds,
        )
        if not batches:
            return 0
        async with asyncio.TaskGroup() as group:
            for batch in batches:
                group.create_task(self._process_claimed(batch))
        return len(batches)

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            processed = await self.run_once()
            if processed == 0:
                await asyncio.sleep(0.5)

    async def _process_claimed(self, batch: dict[str, Any]) -> None:
        batch_id = str(batch["id"])
        heartbeat = asyncio.create_task(self._heartbeat(batch_id))
        try:
            status = str(batch["status"])
            if status in {"uploaded", "parsing", "normalizing"}:
                await self._stage_uploaded(batch)
            elif status == "approval_requested":
                await self._approve_bootstrap(batch)
            elif status == "commit_requested":
                await self.processor.commit_increment(batch_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.repository.mark_batch_failed(batch_id, str(exc))
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
            await self.repository.release_batch_lease(batch_id, self.worker_id)

    async def _stage_uploaded(self, batch: dict[str, Any]) -> None:
        batch_type = str(batch["batch_type"])
        input_files = dict(batch.get("input_files") or {})
        source_key = (
            "history"
            if batch_type in {"bootstrap_history", "bootstrap_compare"}
            else "daily"
        )
        payload = dict(input_files.get(source_key) or {})
        path = Path(str(payload.get("path") or ""))
        if not path.is_file():
            raise ValueError("上传文件不存在，无法继续处理")
        source = "B" if source_key == "history" else "A"
        records = await asyncio.to_thread(load_records_auto, path, source=source)
        if len(records) > self.settings.max_total_rows:
            raise ValueError(
                f"文件共 {len(records)} 行，超过上限 {self.settings.max_total_rows} 行"
            )
        await self.processor.stage_records(
            name=str(batch["name"]),
            batch_type=batch_type,
            file_name=str(payload.get("file_name") or path.name),
            file_hash=str(payload.get("file_hash") or ""),
            records=records,
            batch_id=str(batch["id"]),
        )

    async def _approve_bootstrap(self, batch: dict[str, Any]) -> None:
        version_id = batch.get("dictionary_version_id")
        if version_id is None:
            raise ValueError("该批次没有待发布词典")
        await self.processor.approve_bootstrap(
            str(batch["id"]), int(version_id), approved_by="本机管理员"
        )
        if batch["batch_type"] != "bootstrap_compare":
            return
        input_files = dict(batch.get("input_files") or {})
        daily = dict(input_files.get("daily") or {})
        if not daily:
            return
        await self.repository.create_batch(
            f"{batch['name']}-当天新增",
            "daily_increment",
            input_files={"daily": daily},
        )

    async def _heartbeat(self, batch_id: str) -> None:
        while True:
            await asyncio.sleep(self.settings.job_heartbeat_seconds)
            renewed = await self.repository.heartbeat_batch(
                batch_id,
                self.worker_id,
                lease_seconds=self.settings.job_lease_seconds,
            )
            if not renewed:
                return
