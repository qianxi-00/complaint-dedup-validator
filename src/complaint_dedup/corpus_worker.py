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
            elif status == "awaiting_daily":
                await self._stage_compare_daily(batch)
            elif status == "approval_requested":
                await self._approve_bootstrap(batch)
            elif status == "commit_requested":
                await self.processor.commit_increment(batch_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.repository.mark_batch_failed(batch_id, str(exc))
            generation = await self.repository.generation_for_batch(batch_id)
            if generation is not None and generation.get("status") == "building":
                with suppress(Exception):
                    await self.repository.fail_generation(
                        int(generation["id"]), str(exc)
                    )
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
        if batch_type in {"bootstrap_history", "bootstrap_compare"}:
            await self._approve_bootstrap(batch)
        else:
            await self.processor.commit_increment(str(batch["id"]))

    async def _approve_bootstrap(self, batch: dict[str, Any]) -> None:
        current = await self.repository.get_batch(str(batch["id"]))
        version_id = current.get("dictionary_version_id")
        if version_id is None:
            raise ValueError("该批次没有待发布词典")
        await self.processor.approve_bootstrap(
            str(batch["id"]),
            int(version_id),
            approved_by="本机管理员",
            defer_final_commit=batch["batch_type"] == "bootstrap_compare",
        )
        if current["batch_type"] != "bootstrap_compare":
            return
        await self._stage_compare_daily(await self.repository.get_batch(str(batch["id"])))

    async def _stage_compare_daily(self, batch: dict[str, Any]) -> None:
        if batch["batch_type"] != "bootstrap_compare":
            raise ValueError("只有首次联合比对批次需要处理当天文件")
        input_files = dict(batch.get("input_files") or {})
        daily = dict(input_files.get("daily") or {})
        if not daily:
            return
        path = Path(str(daily.get("path") or ""))
        if not path.is_file():
            raise ValueError("首次联合比对的当天文件不存在")
        records = await asyncio.to_thread(load_records_auto, path, source="A")
        if len(records) > self.settings.max_total_rows:
            raise ValueError(
                f"当天文件共 {len(records)} 行，超过上限 {self.settings.max_total_rows} 行"
            )
        active_generation = await self.repository.active_generation()
        if active_generation is None or active_generation.get("dictionary_version_id") is None:
            raise ValueError("历史库冷启动未成功，无法继续首次联合比对")
        await self.processor.stage_records(
            name=str(batch["name"]),
            batch_type="bootstrap_compare",
            file_name=str(daily.get("file_name") or path.name),
            file_hash=str(daily.get("file_hash") or ""),
            records=records,
            batch_id=str(batch["id"]),
            source_type_override="daily",
            dictionary_version_id_override=int(active_generation["dictionary_version_id"]),
        )
        await self.processor.commit_increment(str(batch["id"]))

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
