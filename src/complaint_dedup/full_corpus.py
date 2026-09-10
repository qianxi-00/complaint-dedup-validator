from __future__ import annotations

import asyncio
import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import delete, func, insert, select, update

from complaint_dedup.category_rules import canonical_category, problem_family_for
from complaint_dedup.corpus_models import InputRecord
from complaint_dedup.corpus_parser import (
    extract_organization_subject,
    extract_title_anchor,
    normalize_organization_name,
    parse_complaint,
)
from complaint_dedup.corpus_schema import (
    comparison_decisions,
    comparison_event_members,
    comparison_events,
    comparison_record_members,
    comparison_runs,
    processing_jobs,
    sync_runs,
    work_order_versions,
    work_orders,
)
from complaint_dedup.dedup_engine import (
    DedupEngine,
    DedupEngineOptions,
    JsonChatClient,
)
from complaint_dedup.dedup_features import (
    FEATURE_VERSION,
    FeatureBuildInput,
    build_record_features,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
MAX_COMPARISON_EVENT_CACHE = 4
MAX_FILTERED_EVENT_CACHE = 64
ENTERPRISE_FAMILIES = {
    "food_safety": ("食品安全", "食品卫生", "餐饮卫生"),
    "wage": ("欠薪", "拖欠工资", "工资拖欠", "农民工工资"),
    "product_quality": ("产品质量", "商品质量", "产品质量问题"),
}
FAMILY_LABELS = {
    "food_safety": "食品安全",
    "wage": "欠薪",
    "product_quality": "产品质量",
}


class WindowOverlapError(ValueError):
    pass


class ActiveJobConflictError(RuntimeError):
    def __init__(self, job: dict[str, Any]) -> None:
        self.job = job
        super().__init__(
            f"当前有正在执行的任务：{_job_kind_label(job.get('kind'))}（任务 ID：{job.get('id')}），请等待任务完成后再提交。"
        )


JOB_STATUS_LABELS = {
    "queued": "待处理",
    "running": "执行中",
    "completed": "已完成",
    "failed": "失败",
}


def _job_kind_label(kind: str | None) -> str:
    return "全量同步" if kind == "sync" else "时间窗口比对"


def job_status_label(status: str | None) -> str:
    return JOB_STATUS_LABELS.get(str(status or ""), "未知状态")


@dataclass(frozen=True)
class EventFilters:
    region: str = ""
    street: str = ""
    processing_department: str = ""
    completed_from: date | None = None
    completed_to: date | None = None
    missing_completed: bool = False
    has_target_records: bool = False
    hide_singletons: bool = False
    keyword: str = ""
    search_all: bool = False


@dataclass(frozen=True)
class SyncResult:
    sync_id: str
    inserted: int
    updated: int
    missing: int


@dataclass(frozen=True)
class ComparisonResult:
    comparison_id: str
    target_count: int
    reference_count: int
    event_count: int
    singleton_count: int
    missing_time_count: int
    algorithm_version: str
    llm_coverage: float
    fallback_count: int
    decision_count: int
    request_count: int = 0
    llm_error_count: int = 0
    span_guard_count: int = 0


class FullCorpusService:
    def __init__(
        self,
        database,
        *,
        llm_client: JsonChatClient | None = None,
        settings: Any | None = None,
    ) -> None:
        self.database = database
        self.llm_client = llm_client
        self.settings = settings
        self._operation_lock = asyncio.Lock()
        self._comparison_event_cache: dict[str, list[dict[str, Any]]] = {}
        self._filtered_event_cache: dict[tuple[str, EventFilters], list[dict[str, Any]]] = {}

    async def sync_records(
        self,
        records: list[InputRecord],
        *,
        file_name: str,
        file_hash: str | None = None,
    ) -> SyncResult:
        async with self._operation_lock:
            return await self._sync_records(
                records,
                file_name=file_name,
                file_hash=file_hash,
            )

    async def _sync_records(
        self,
        records: list[InputRecord],
        *,
        file_name: str,
        file_hash: str | None = None,
    ) -> SyncResult:
        if not records:
            raise ValueError("全量文件没有可同步的工单")
        payloads = [_normalize_record(record) for record in records]
        deduped = _dedupe_payloads(payloads)
        file_hash = file_hash or hashlib.sha256(
            json.dumps(
                deduped,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            ).encode()
        ).hexdigest()
        sync_id = uuid.uuid4().hex
        now = datetime.now(UTC)
        inserted_count = updated_count = missing_count = 0
        business_columns = _business_columns_from_payloads(deduped.values())

        async with self.database.engine.begin() as connection:
            await connection.execute(
                insert(sync_runs).values(
                    id=sync_id,
                    file_name=file_name,
                    file_hash=file_hash,
                    status="running",
                    total_rows=len(records),
                    business_columns=business_columns,
                    created_at=now,
                )
            )
        try:
            async with self.database.engine.begin() as connection:
                existing_rows = (await connection.execute(select(work_orders))).mappings().all()
                existing = {str(row["record_key"]): dict(row) for row in existing_rows}
                for payload in deduped.values():
                    key = payload["record_key"]
                    before = existing.get(key)
                    values = {name: value for name, value in payload.items() if name != "record_key"}
                    values.update(last_sync_id=sync_id, missing_in_latest_upload=False, updated_at=now)
                    if before is None:
                        values["created_at"] = now
                        await connection.execute(insert(work_orders).values(record_key=key, **values))
                        action = "inserted"
                        inserted_count += 1
                    else:
                        changed = _changed_fields(before, values)
                        if changed:
                            await connection.execute(
                                update(work_orders).where(work_orders.c.record_key == key).values(**values)
                            )
                            action = "updated"
                            updated_count += 1
                        else:
                            await connection.execute(
                                update(work_orders)
                                .where(work_orders.c.record_key == key)
                                .values(last_sync_id=sync_id, missing_in_latest_upload=False)
                            )
                            continue
                    await connection.execute(
                        insert(work_order_versions).values(
                            sync_id=sync_id,
                            record_key=key,
                            action=action,
                            before_json=_snapshot(before) if before else None,
                            after_json=_snapshot(values),
                            created_at=now,
                        )
                    )

                uploaded_keys = set(deduped)
                for key, before in existing.items():
                    if key in uploaded_keys or before.get("missing_in_latest_upload"):
                        continue
                    await connection.execute(
                        update(work_orders)
                        .where(work_orders.c.record_key == key)
                        .values(missing_in_latest_upload=True, last_sync_id=sync_id, updated_at=now)
                    )
                    await connection.execute(
                        insert(work_order_versions).values(
                            sync_id=sync_id,
                            record_key=key,
                            action="missing",
                            before_json=_snapshot(before),
                            after_json={"missing_in_latest_upload": True},
                            created_at=now,
                        )
                    )
                    missing_count += 1
                await connection.execute(
                    update(sync_runs)
                    .where(sync_runs.c.id == sync_id)
                    .values(
                        status="completed",
                        inserted_rows=inserted_count,
                        updated_rows=updated_count,
                        missing_rows=missing_count,
                        completed_at=now,
                    )
                )
        except Exception as exc:
            async with self.database.engine.begin() as connection:
                await connection.execute(
                    update(sync_runs)
                    .where(sync_runs.c.id == sync_id)
                    .values(status="failed", error_message=str(exc), completed_at=datetime.now(UTC))
                )
            raise
        return SyncResult(sync_id, inserted_count, updated_count, missing_count)

    async def list_current_orders(self) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            result = await connection.execute(select(work_orders).order_by(work_orders.c.record_key))
            return [dict(row) for row in result.mappings().all()]

    async def count_current_orders(self) -> int:
        async with self.database.engine.connect() as connection:
            value = await connection.scalar(select(func.count()).select_from(work_orders))
        return int(value or 0)

    async def latest_local_date(self, time_field: str) -> date | None:
        if time_field not in {"completed_at", "received_at"}:
            raise ValueError("time_field 必须是 completed_at 或 received_at")
        async with self.database.engine.connect() as connection:
            values = (
                await connection.execute(
                    select(getattr(work_orders.c, time_field)).where(
                        work_orders.c.missing_in_latest_upload.is_(False),
                        getattr(work_orders.c, time_field).is_not(None),
                    )
                )
            ).scalars().all()
        return max((_local_date(value) for value in values), default=None)

    async def version_count(self, sync_id: str) -> int:
        async with self.database.engine.connect() as connection:
            result = await connection.execute(
                select(work_order_versions.c.id).where(work_order_versions.c.sync_id == sync_id)
            )
            return len(result.all())

    async def list_sync_runs(
        self, *, limit: int | None = None, offset: int = 0
    ) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            statement = select(sync_runs).order_by(sync_runs.c.created_at.desc()).offset(offset)
            if limit is not None:
                statement = statement.limit(limit)
            result = await connection.execute(statement)
            return [dict(row) for row in result.mappings().all()]

    async def create_job(self, kind: str, payload: dict[str, Any]) -> str:
        if kind not in {"sync", "comparison"}:
            raise ValueError("后台任务类型无效")
        job_id = uuid.uuid4().hex
        async with self._operation_lock:
            async with self.database.engine.begin() as connection:
                active = (
                    await connection.execute(
                        select(processing_jobs)
                        .where(processing_jobs.c.status.in_(["queued", "running"]))
                        .order_by(processing_jobs.c.created_at, processing_jobs.c.id)
                        .limit(1)
                    )
                ).mappings().first()
                if active:
                    raise ActiveJobConflictError(dict(active))
                await connection.execute(
                    insert(processing_jobs).values(
                        id=job_id,
                        kind=kind,
                        status="queued",
                        payload=payload,
                        progress=0,
                        created_at=datetime.now(UTC),
                    )
                )
        return job_id

    async def get_job(self, job_id: str) -> dict[str, Any] | None:
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(
                    select(processing_jobs).where(processing_jobs.c.id == job_id)
                )
            ).mappings().first()
        return dict(row) if row else None

    async def list_jobs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(processing_jobs)
                    .order_by(processing_jobs.c.created_at.desc())
                    .limit(limit)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def claim_job(self) -> dict[str, Any] | None:
        now = datetime.now(UTC)
        async with self.database.engine.begin() as connection:
            row = (
                await connection.execute(
                    select(processing_jobs)
                    .where(processing_jobs.c.status == "queued")
                    .order_by(processing_jobs.c.created_at, processing_jobs.c.id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
            ).mappings().first()
            if row is None:
                return None
            await connection.execute(
                update(processing_jobs)
                .where(
                    processing_jobs.c.id == row["id"],
                    processing_jobs.c.status == "queued",
                )
                .values(status="running", progress=1, started_at=now)
            )
        return {**dict(row), "status": "running", "progress": 1, "started_at": now}

    async def complete_job(self, job_id: str, result: dict[str, Any]) -> None:
        async with self.database.engine.begin() as connection:
            await connection.execute(
                update(processing_jobs)
                .where(processing_jobs.c.id == job_id)
                .values(
                    status="completed",
                    progress=100,
                    result_json=result,
                    completed_at=datetime.now(UTC),
                )
            )

    async def fail_job(self, job_id: str, message: str) -> None:
        async with self.database.engine.begin() as connection:
            await connection.execute(
                update(processing_jobs)
                .where(processing_jobs.c.id == job_id)
                .values(
                    status="failed",
                    progress=100,
                    error_message=message,
                    completed_at=datetime.now(UTC),
                )
            )

    async def list_comparisons(
        self, *, limit: int | None = None, offset: int = 0
    ) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            statement = select(comparison_runs).order_by(comparison_runs.c.created_at.desc()).offset(offset)
            if limit is not None:
                statement = statement.limit(limit)
            result = await connection.execute(statement)
            return [dict(row) for row in result.mappings().all()]

    async def count_comparisons(self) -> int:
        async with self.database.engine.connect() as connection:
            value = await connection.scalar(select(func.count()).select_from(comparison_runs))
        return int(value or 0)

    async def get_comparison(self, comparison_id: str) -> dict[str, Any] | None:
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(select(comparison_runs).where(comparison_runs.c.id == comparison_id))
            ).mappings().first()
            return dict(row) if row else None

    async def list_comparison_events(self, comparison_id: str) -> list[dict[str, Any]]:
        cached = self._comparison_event_cache.get(comparison_id)
        if cached is not None:
            return cached
        async with self.database.engine.connect() as connection:
            events = (
                await connection.execute(
                    select(comparison_events)
                    .where(comparison_events.c.comparison_id == comparison_id)
                    .order_by(comparison_events.c.id)
                )
            ).mappings().all()
            members = (
                await connection.execute(
                    select(comparison_event_members).where(
                        comparison_event_members.c.event_id.in_([row["id"] for row in events] or [-1])
                    )
                )
            ).mappings().all()
            snapshots = (
                await connection.execute(
                    select(comparison_record_members).where(
                        comparison_record_members.c.comparison_id == comparison_id
                    )
                )
            ).mappings().all()
        snapshot_by_key = {
            str(row["record_key"]): dict(row["snapshot_json"] or {})
            for row in snapshots
        }
        by_event: dict[int, list[dict[str, Any]]] = {}
        for member in members:
            value = dict(member)
            value["snapshot"] = snapshot_by_key.get(str(member["record_key"]), {})
            by_event.setdefault(int(member["event_id"]), []).append(value)
        result = [
            {**dict(event), "members": by_event.get(int(event["id"]), [])}
            for event in events
        ]
        self._comparison_event_cache[comparison_id] = result
        if len(self._comparison_event_cache) > MAX_COMPARISON_EVENT_CACHE:
            self._comparison_event_cache.pop(
                next(iter(self._comparison_event_cache))
            )
        return result

    async def list_event_member_snapshots(self, event_id: int) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            links = (
                await connection.execute(select(comparison_event_members).where(comparison_event_members.c.event_id == event_id))
            ).mappings().all()
            result = []
            for link in links:
                row = (
                    await connection.execute(
                        select(comparison_record_members).where(
                            comparison_record_members.c.comparison_id == select(comparison_events.c.comparison_id).where(comparison_events.c.id == event_id).scalar_subquery(),
                            comparison_record_members.c.record_key == link["record_key"],
                        )
                    )
                ).mappings().first()
                if row:
                    result.append({**dict(row), "event_id": event_id})
            return result

    async def compare(
        self,
        *,
        time_field: str = "received_at",
        target_from: date,
        target_to: date,
        reference_from: date | None = None,
        reference_to: date | None = None,
    ) -> ComparisonResult:
        async with self._operation_lock:
            return await self._compare(
                time_field=time_field,
                target_from=target_from,
                target_to=target_to,
                reference_from=reference_from,
                reference_to=reference_to,
            )

    async def _compare(
        self,
        *,
        time_field: str = "received_at",
        target_from: date,
        target_to: date,
        reference_from: date | None = None,
        reference_to: date | None = None,
    ) -> ComparisonResult:
        if time_field not in {"completed_at", "received_at"}:
            raise ValueError("time_field 必须是 completed_at 或 received_at")
        if target_from > target_to:
            raise ValueError("待比对开始日期不能晚于结束日期")
        manual_reference = reference_from is not None or reference_to is not None
        if manual_reference and (reference_from is None or reference_to is None):
            raise ValueError("被比对时间段必须同时提供开始和结束日期")
        if manual_reference and reference_from > reference_to:
            raise ValueError("被比对开始日期不能晚于结束日期")
        if manual_reference and reference_from <= target_to and target_from <= reference_to:
            raise WindowOverlapError("待比对和被比对时间段不能重叠")

        async with self.database.engine.connect() as connection:
            current_sync = (
                await connection.execute(
                    select(sync_runs.c.id)
                    .where(sync_runs.c.status == "completed")
                    .order_by(sync_runs.c.completed_at.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            if current_sync is None:
                raise ValueError("尚未同步全量工单")
            rows = (
                await connection.execute(
                    select(work_orders).where(work_orders.c.missing_in_latest_upload.is_(False))
                )
            ).mappings().all()

        target_rows: list[dict[str, Any]] = []
        reference_rows: list[dict[str, Any]] = []
        missing_time_count = 0
        for row in rows:
            local_date = _local_date(row[time_field])
            if local_date is None:
                target_rows.append(dict(row))
                missing_time_count += 1
            elif target_from <= local_date <= target_to:
                target_rows.append(dict(row))
            elif manual_reference and reference_from <= local_date <= reference_to:
                reference_rows.append(dict(row))
            elif not manual_reference:
                reference_rows.append(dict(row))

        comparison_id = uuid.uuid4().hex
        now = datetime.now(UTC)
        target_start = _local_start(target_from)
        target_end = _local_start(target_to + timedelta(days=1))
        reference_start = _local_start(reference_from) if reference_from else None
        reference_end = _local_start(reference_to + timedelta(days=1)) if reference_to else None
        dedup = await DedupEngine(
            llm_client=self.llm_client,
            options=DedupEngineOptions.from_settings(self.settings),
        ).cluster([*target_rows, *reference_rows])
        groups = [
            (
                _final_event_key(member_rows, dedup.algorithm_version),
                member_rows,
            )
            for member_rows in dedup.groups
        ]
        record_event_keys = {
            str(row["record_key"]): event_key
            for event_key, member_rows in groups
            for row in member_rows
        }
        expected_keys = {
            str(row["record_key"]) for row in (*target_rows, *reference_rows)
        }
        grouped_record_keys = [
            str(row["record_key"])
            for _, member_rows in groups
            for row in member_rows
        ]
        if (
            len(grouped_record_keys) != len(set(grouped_record_keys))
            or set(grouped_record_keys) != expected_keys
        ):
            raise RuntimeError("判重结果存在工单丢失或重复成员，任务已中止")
        singleton_count = sum(
            1 for _, member_rows in groups if len(member_rows) == 1
        )
        async with self.database.engine.begin() as connection:
            await connection.execute(
                insert(comparison_runs).values(
                    id=comparison_id,
                    sync_id=current_sync,
                    time_field=time_field,
                    target_from=target_start,
                    target_to=target_end,
                    reference_from=reference_start,
                    reference_to=reference_end,
                    reference_mode="manual" if manual_reference else "complement",
                    status="completed",
                    target_count=len(target_rows),
                    reference_count=len(reference_rows),
                    event_count=len(groups),
                    singleton_count=singleton_count,
                    missing_time_count=missing_time_count,
                    algorithm_version=dedup.algorithm_version,
                    feature_version=dedup.feature_version,
                    prompt_version=dedup.prompt_version,
                    model_id=dedup.model_id or None,
                    llm_coverage=dedup.llm_coverage,
                    fallback_count=dedup.fallback_count,
                    decision_count=dedup.decision_count,
                    created_at=now,
                )
            )
            for side, side_rows in (("target", target_rows), ("reference", reference_rows)):
                for row in side_rows:
                    await connection.execute(
                        insert(comparison_record_members).values(
                            comparison_id=comparison_id,
                            record_key=row["record_key"],
                            side=side,
                            snapshot_json=_snapshot(row),
                            event_key=record_event_keys[str(row["record_key"])],
                        )
                    )
            target_keys = {row["record_key"] for row in target_rows}
            for event_key, members in groups:
                name = _event_name(members[0])
                event_id = (
                    await connection.execute(
                        insert(comparison_events)
                        .values(
                            comparison_id=comparison_id,
                            event_key=event_key,
                            event_name=name,
                            status="active",
                            created_at=now,
                        )
                        .returning(comparison_events.c.id)
                    )
                ).scalar_one()
                for row in members:
                    side = "target" if row["record_key"] in target_keys else "reference"
                    await connection.execute(
                        insert(comparison_event_members).values(
                            event_id=event_id,
                            record_key=row["record_key"],
                            side=side,
                        )
                    )
            for audit in dedup.audits:
                await connection.execute(
                    insert(comparison_decisions).values(
                        comparison_id=comparison_id,
                        card_id=str(audit["card_id"])[:128],
                        card_ids=audit.get("card_ids") or [],
                        assigned_card_ids=audit.get("assigned_card_ids") or [],
                        decision=str(audit["decision"]),
                        confidence=float(audit.get("confidence") or 0),
                        evidence_json=audit.get("evidence_json") or [],
                        conflict_json=audit.get("conflict_json") or [],
                        rule_version=str(audit["rule_version"]),
                        prompt_version=audit.get("prompt_version"),
                        model_id=audit.get("model_id"),
                        final_event_key=_audit_final_event_key(
                            audit, record_event_keys
                        ),
                        fallback_reason=audit.get("fallback_reason"),
                        elapsed_ms=audit.get("elapsed_ms"),
                        record_keys=audit.get("record_keys") or [],
                        created_at=now,
                    )
                )
        return ComparisonResult(
            comparison_id,
            len(target_rows),
            len(reference_rows),
            len(groups),
            singleton_count,
            missing_time_count,
            dedup.algorithm_version,
            dedup.llm_coverage,
            dedup.fallback_count,
            dedup.decision_count,
            dedup.request_count,
            dedup.llm_error_count,
            dedup.span_guard_count,
        )

    async def list_comparison_members(self, comparison_id: str) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            result = await connection.execute(
                select(comparison_record_members).where(comparison_record_members.c.comparison_id == comparison_id)
            )
            members = []
            for row in result.mappings().all():
                value = dict(row)
                value["work_order_id"] = (value.get("snapshot_json") or {}).get("work_order_id")
                members.append(value)
            return members

    async def list_event_summaries(
        self,
        comparison_id: str,
        *,
        filters: EventFilters | None = None,
        sort: str = "updated_desc",
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(await self._filtered_event_summaries(comparison_id, filters or EventFilters()))
        if sort == "member_count_desc":
            rows.sort(key=lambda row: (-row["member_count"], row["id"]))
        elif sort == "member_count_asc":
            rows.sort(key=lambda row: (row["member_count"], row["id"]))
        else:
            rows.sort(key=lambda row: row["id"], reverse=True)
        total = len(rows)
        return rows[offset : offset + max(limit, 0)], total

    async def count_singleton_events(
        self, comparison_id: str, *, filters: EventFilters | None = None
    ) -> int:
        rows = await self._filtered_event_summaries(comparison_id, filters or EventFilters())
        return sum(1 for row in rows if row["member_count"] == 1)

    async def event_filter_options(
        self, comparison_id: str, *, region: str = "", street: str = ""
    ) -> dict[str, list[str]]:
        rows = await self._filtered_event_summaries(
            comparison_id,
            EventFilters(region=region, street=street),
        )
        regions = sorted({str(row["region"]) for row in rows if row.get("region")})
        streets = sorted({str(row["street_name"]) for row in rows if row.get("street_name")})
        departments = sorted(
            {
                str(department)
                for row in rows
                for department in row.get("processing_departments", [])
                if department
            }
        )
        return {
            "regions": regions,
            "streets": streets,
            "processing_departments": departments,
        }

    async def get_event(self, event_id: int) -> dict[str, Any] | None:
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(
                    select(comparison_events).where(comparison_events.c.id == event_id)
                )
            ).mappings().first()
        return dict(row) if row else None

    async def list_event_records(
        self, event_id: int, *, limit: int = 20, offset: int = 0
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]], int]:
        event = await self.get_event(event_id)
        if event is None:
            return None, [], 0
        async with self.database.engine.connect() as connection:
            count = await connection.scalar(
                select(func.count())
                .select_from(comparison_event_members)
                .where(comparison_event_members.c.event_id == event_id)
            )
            rows = (
                await connection.execute(
                    select(
                        comparison_event_members.c.record_key,
                        comparison_event_members.c.side,
                        comparison_record_members.c.snapshot_json,
                    )
                    .select_from(
                        comparison_event_members.join(
                            comparison_record_members,
                            (comparison_record_members.c.comparison_id == event["comparison_id"])
                            & (comparison_record_members.c.record_key == comparison_event_members.c.record_key),
                        )
                    )
                    .where(comparison_event_members.c.event_id == event_id)
                    .order_by(comparison_event_members.c.record_key)
                    .offset(offset)
                    .limit(limit)
                )
            ).mappings().all()
        records = []
        for row in rows:
            value = dict(row)
            value["event_id"] = event_id
            value["snapshot"] = value.pop("snapshot_json") or {}
            records.append(value)
        return event, records, int(count or 0)

    async def update_event_name(self, event_id: int, name: str) -> None:
        normalized = name.strip()
        if not normalized:
            raise ValueError("事件名称不能为空")
        event = await self.get_event(event_id)
        if event is None:
            raise KeyError(event_id)
        async with self.database.engine.begin() as connection:
            await connection.execute(
                update(comparison_events)
                .where(comparison_events.c.id == event_id)
                .values(event_name=normalized)
            )
        self._invalidate_comparison_cache(str(event["comparison_id"]))

    async def exclude_event_member(
        self, event_id: int, record_key: str, *, target_name: str = ""
    ) -> int:
        event = await self.get_event(event_id)
        if event is None:
            raise KeyError(event_id)
        async with self.database.engine.begin() as connection:
            link = (
                await connection.execute(
                    select(comparison_event_members).where(
                        comparison_event_members.c.event_id == event_id,
                        comparison_event_members.c.record_key == record_key,
                    )
                )
            ).mappings().first()
            if link is None:
                raise KeyError(record_key)
            await connection.execute(
                delete(comparison_event_members).where(
                    comparison_event_members.c.event_id == event_id,
                    comparison_event_members.c.record_key == record_key,
                )
            )
            target_event_id = None
            normalized_target = target_name.strip()
            if normalized_target:
                target_event_id = await connection.scalar(
                    select(comparison_events.c.id).where(
                        comparison_events.c.comparison_id == event["comparison_id"],
                        comparison_events.c.event_name == normalized_target,
                        comparison_events.c.status == "active",
                    ).limit(1)
                )
            if target_event_id is None:
                result = await connection.execute(
                    insert(comparison_events).values(
                        comparison_id=event["comparison_id"],
                        event_key=f"manual|{uuid.uuid4().hex}",
                        event_name=normalized_target or "单例事件",
                        status="active",
                        created_at=datetime.now(UTC),
                    ).returning(comparison_events.c.id)
                )
                target_event_id = result.scalar_one()
            await connection.execute(
                insert(comparison_event_members).values(
                    event_id=target_event_id,
                    record_key=record_key,
                    side=link["side"],
                )
            )
            remaining = await connection.scalar(
                select(func.count())
                .select_from(comparison_event_members)
                .where(comparison_event_members.c.event_id == event_id)
            )
            if not remaining:
                await connection.execute(
                    update(comparison_events)
                    .where(comparison_events.c.id == event_id)
                    .values(status="archived")
                )
        self._invalidate_comparison_cache(str(event["comparison_id"]))
        return int(target_event_id)

    async def export_rows(
        self, comparison_id: str, *, filters: EventFilters | None = None
    ) -> list[dict[str, Any]]:
        events = await self._filtered_event_summaries(comparison_id, filters or EventFilters())
        selected_ids = {int(row["id"]) for row in events}
        if not selected_ids:
            return []
        all_events = await self.list_comparison_events(comparison_id)
        rows: list[dict[str, Any]] = []
        for event in all_events:
            if int(event["id"]) not in selected_ids:
                continue
            for member in event["members"]:
                value = dict(member["snapshot"] or {})
                value.update(
                    event_id=event["id"],
                    event_name=event["event_name"],
                    side=member["side"],
                    record_key=member["record_key"],
                )
                rows.append(value)
        return rows

    async def sync_business_columns(self, comparison_id: str) -> list[str]:
        comparison = await self.get_comparison(comparison_id)
        if comparison is None:
            raise KeyError(comparison_id)
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(
                    select(sync_runs.c.business_columns).where(
                        sync_runs.c.id == comparison["sync_id"]
                    )
                )
            ).scalar_one_or_none()
        return [str(value) for value in (row or [])]

    async def _filtered_event_summaries(
        self, comparison_id: str, filters: EventFilters
    ) -> list[dict[str, Any]]:
        cache_key = (comparison_id, filters)
        cached = self._filtered_event_cache.get(cache_key)
        if cached is not None:
            return cached
        events = await self.list_comparison_events(comparison_id)
        result: list[dict[str, Any]] = []
        for event in events:
            members = event["members"]
            if not members:
                continue
            snapshots = [member["snapshot"] or {} for member in members]
            first = snapshots[0]
            completed_dates = [
                _local_date(_parse_datetime(row.get("completed_at")))
                for row in snapshots
            ]
            departments = sorted(
                {
                    str(row.get("processing_department"))
                    for row in snapshots
                    if row.get("processing_department")
                }
            )
            summary = {
                "id": int(event["id"]),
                "comparison_id": comparison_id,
                "event_name": event["event_name"],
                "region": first.get("region"),
                "street_name": first.get("street") or "未知街道",
                "issue_name": FAMILY_LABELS.get(
                    first.get("issue_family"), first.get("category") or "未分类"
                ),
                "member_count": len(members),
                "target_count": sum(
                    1 for member in members if member["side"] == "target"
                ),
                "first_received_at": min(
                    (row.get("received_at") for row in snapshots if row.get("received_at")),
                    default=None,
                ),
                "last_received_at": max(
                    (row.get("received_at") for row in snapshots if row.get("received_at")),
                    default=None,
                ),
                "processing_departments": departments,
            }
            keyword = filters.keyword.strip().casefold()
            if keyword:
                searchable_values = [
                    summary["event_name"],
                    summary["issue_name"],
                    summary["region"],
                    summary["street_name"],
                    *departments,
                    *[
                        snapshot.get(field)
                        for snapshot in snapshots
                        for field in (
                            "work_order_id",
                            "title_raw",
                            "appeal_text",
                            "department",
                            "processing_department",
                        )
                    ],
                ]
                if not any(keyword in str(value or "").casefold() for value in searchable_values):
                    continue
            if not filters.search_all:
                if filters.region.strip() and summary["region"] != filters.region.strip():
                    continue
                if filters.street.strip() and summary["street_name"] != filters.street.strip():
                    continue
                if (
                    filters.processing_department.strip()
                    and filters.processing_department.strip() not in departments
                ):
                    continue
                if filters.has_target_records and summary["target_count"] == 0:
                    continue
                if filters.hide_singletons and summary["member_count"] <= 1:
                    continue
                if filters.missing_completed and any(
                    value is not None for value in completed_dates
                ):
                    continue
                filtered_completed = [value for value in completed_dates if value is not None]
                if filters.completed_from or filters.completed_to:
                    if not any(
                        (filters.completed_from is None or value >= filters.completed_from)
                        and (filters.completed_to is None or value <= filters.completed_to)
                        for value in filtered_completed
                    ):
                        continue
            result.append(summary)
        self._filtered_event_cache[cache_key] = result
        if len(self._filtered_event_cache) > MAX_FILTERED_EVENT_CACHE:
            self._filtered_event_cache.pop(
                next(iter(self._filtered_event_cache))
            )
        return result

    def _invalidate_comparison_cache(self, comparison_id: str) -> None:
        self._comparison_event_cache.pop(comparison_id, None)
        for key in [key for key in self._filtered_event_cache if key[0] == comparison_id]:
            self._filtered_event_cache.pop(key, None)

def _normalize_record(record: InputRecord) -> dict[str, Any]:
    location = record.location or _raw(record.raw_fields, "事发地点", "地址")
    appeal = record.appeal_text
    parsed = parse_complaint(title=record.title, appeal=appeal, location=location)
    category = (
        record.category
        or record.category_level_4
        or (parsed.issue_segments[0].label if parsed.issue_segments else None)
        or _raw(record.raw_fields, "事项分类", "最终事项分类")
    )
    org = normalize_organization_name(
        extract_organization_subject(record.title, appeal, location)
    )
    family = _issue_family(record.title, category, appeal)
    street = parsed.street or "未知街道"
    # 锚点缺失时从标题二次抽取，减少“未知地点”导致的强制单例
    anchor = parsed.anchor_raw or extract_title_anchor(record.title) or "未知地点"
    canonical = canonical_category(category)
    problem = problem_family_for(canonical)
    work_order_id = _clean(record.work_order_id)
    stable_source = work_order_id or json.dumps(
        {
            "title": record.title,
            "appeal": appeal,
            "location": location,
            "category": category,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    record_key = f"wo:{work_order_id}" if work_order_id else "fp:" + hashlib.sha256(stable_source.encode()).hexdigest()
    event_key = _event_key(
        parsed.region,
        street,
        anchor,
        canonical,
        problem,
        org,
        family,
        record_key,
    )
    received_at = _parse_datetime(record.received_at)
    completed_at = _parse_datetime(
        record.completed_at or _raw(record.raw_fields, "办结时间", "办结日期", "结案时间")
    )
    department = _raw(record.raw_fields, "所属部门", "部门")
    processing_department = (
        record.processing_department
        or _raw(record.raw_fields, "处理部门", "承办部门")
        or department
    )
    features = build_record_features(
        FeatureBuildInput(
            work_order_id=work_order_id,
            title=record.title,
            appeal_text=appeal,
            location=location,
            category=category,
            parsed=parsed,
            source_row=record.source_row,
        )
    )
    raw = dict(record.raw_fields)
    raw.setdefault("受理时间", record.received_at)
    raw.setdefault("办结时间", record.completed_at)
    raw.setdefault("事发地点", location)
    raw.setdefault("处理部门", processing_department)
    return {
        "record_key": record_key,
        "work_order_id": work_order_id,
        "canonical_work_order_id": features.canonical_work_order_id,
        "complaint_fingerprint": features.complaint_fingerprint,
        "received_at": received_at,
        "completed_at": completed_at,
        "title_raw": record.title,
        "title_normalized": parsed.normalized_title,
        "appeal_text": appeal,
        "category": category,
        "processing_department": processing_department,
        "department": department,
        "location": location,
        "region": parsed.region,
        "street": street,
        "anchor": anchor,
        "organization_subject": org,
        "issue_family": family,
        "event_key": event_key,
        "feature_json": features.to_json(),
        "feature_version": FEATURE_VERSION,
        "raw_json": raw,
        "source_row": record.source_row,
    }


def _business_columns_from_payloads(payloads) -> list[str]:
    columns: list[str] = []
    seen: set[str] = set()
    for payload in payloads:
        for key in (payload.get("raw_json") or {}):
            normalized = str(key)
            if normalized and normalized not in seen:
                seen.add(normalized)
                columns.append(normalized)
    return columns


def _dedupe_payloads(payloads: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        record_key = str(payload["record_key"])
        if record_key not in deduped:
            deduped[record_key] = payload
        else:
            deduped[record_key] = _merge_payloads(deduped[record_key], payload)
    return deduped


def _merge_payloads(
    left: dict[str, Any], right: dict[str, Any]
) -> dict[str, Any]:
    merged = dict(left)
    for field in (
        "work_order_id",
        "title_raw",
        "appeal_text",
        "category",
        "processing_department",
        "department",
        "location",
    ):
        merged[field] = _prefer_longer(left.get(field), right.get(field))
    merged["received_at"] = _prefer_later_datetime(
        left.get("received_at"), right.get("received_at")
    )
    merged["completed_at"] = _prefer_later_datetime(
        left.get("completed_at"), right.get("completed_at")
    )
    raw_json = _merge_raw_json(
        left.get("raw_json") or {},
        right.get("raw_json") or {},
    )
    merged["raw_json"] = raw_json
    source_rows = {
        int(value)
        for payload in (left, right)
        for value in (
            (payload.get("feature_json") or {}).get("source_rows")
            or [payload.get("source_row")]
        )
        if value not in (None, "")
    }
    merged["source_row"] = min(source_rows) if source_rows else None
    record = InputRecord(
        source_row=int(merged.get("source_row") or 0),
        work_order_id=merged.get("work_order_id"),
        title=merged.get("title_raw"),
        category=merged.get("category"),
        appeal_text=merged.get("appeal_text"),
        received_at=merged.get("received_at"),
        completed_at=merged.get("completed_at"),
        location=merged.get("location"),
        processing_department=merged.get("processing_department"),
        raw_fields=raw_json,
    )
    refreshed = _normalize_record(record)
    refreshed["record_key"] = str(left["record_key"])
    refreshed["feature_json"]["source_rows"] = sorted(source_rows)
    return refreshed


def _prefer_longer(left: Any, right: Any) -> Any:
    if left in (None, ""):
        return right
    if right in (None, ""):
        return left
    return max((left, right), key=lambda value: len(str(value)))


def _prefer_later_datetime(left: Any, right: Any) -> Any:
    if left in (None, ""):
        return right
    if right in (None, ""):
        return left
    left_dt = _parse_datetime(left)
    right_dt = _parse_datetime(right)
    if left_dt is None:
        return right
    if right_dt is None:
        return left
    return max(left_dt, right_dt)


def _merge_raw_json(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    merged = dict(left)
    for key, value in right.items():
        current = merged.get(key)
        if current in (None, ""):
            merged[key] = value
            continue
        if value in (None, ""):
            continue
        if "时间" in str(key) or "日期" in str(key):
            merged[key] = _prefer_later_value(current, value)
        elif isinstance(current, dict) and isinstance(value, dict):
            merged[key] = _merge_raw_json(current, value)
        else:
            merged[key] = _prefer_longer(current, value)
    return merged


def _prefer_later_value(left: Any, right: Any) -> Any:
    left_dt = _parse_datetime(left)
    right_dt = _parse_datetime(right)
    if left_dt is None:
        return right
    if right_dt is None:
        return left
    return left if left_dt >= right_dt else right


def _final_event_key(rows: list[dict[str, Any]], algorithm_version: str) -> str:
    record_keys = sorted(str(row["record_key"]) for row in rows)
    if len(record_keys) == 1:
        return "singleton|" + _key_part(record_keys[0])
    digest = hashlib.sha256(
        ("\x1f".join(record_keys) + "\x1e" + algorithm_version).encode("utf-8")
    ).hexdigest()
    return f"cluster|{algorithm_version}|{digest}"


def _audit_final_event_key(
    audit: dict[str, Any],
    record_event_keys: dict[str, str],
) -> str | None:
    event_keys = {
        record_event_keys[str(record_key)]
        for record_key in audit.get("record_keys") or []
        if str(record_key) in record_event_keys
    }
    return next(iter(event_keys)) if len(event_keys) == 1 else None


def _event_key(
    region: str | None,
    street: str,
    anchor: str,
    canonical_category_value: str | None,
    problem_family: str | None,
    org: str | None,
    family: str | None,
    record_key: str,
) -> str:
    """确定性基础事件键（v4）。

    企业问题族（食品安全/欠薪/产品质量）仍按“主体 + 族”聚合；
    普通键升级为“地点 + 问题族”，落实“同地点同问题族可合、跨族拆分”。
    锚点未知或无分类时保持单例，由内容/正文指纹与模型灰区兜底。
    """
    if family and org:
        return "enterprise|" + "|".join(_key_part(v) for v in (region, street, org, family))
    if family:
        return "singleton|" + _key_part(record_key)
    group = problem_family or canonical_category_value
    if anchor == "未知地点" or not group:
        return "singleton|" + _key_part(record_key)
    return "ordinary|" + "|".join(_key_part(v) for v in (region, street, anchor, group))


def _event_name(row: dict[str, Any]) -> str:
    family = row.get("issue_family")
    issue = FAMILY_LABELS.get(str(family), row.get("category") or "未分类")
    subject = row.get("organization_subject") or row.get("anchor")
    if str(row.get("event_key") or "").startswith("singleton|") and row.get("title_raw"):
        subject = row.get("title_raw")
    return "｜".join(
        str(value or "未知")
        for value in (row.get("region"), row.get("street"), subject, issue)
    )


def _issue_family(*values: str | None) -> str | None:
    text = "".join(str(value or "") for value in values)
    for family, keywords in ENTERPRISE_FAMILIES.items():
        if any(keyword in text for keyword in keywords):
            return family
    return None


def _raw(raw: dict[str, Any], *names: str) -> str | None:
    for name in names:
        value = raw.get(name)
        if value not in (None, ""):
            return str(value)
    return None


def _clean(value: str | None) -> str | None:
    value = str(value or "").strip()
    return value or None


def _key_part(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).casefold() or "-"


def _parse_datetime(value: str | datetime | None) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        current = value
    else:
        text = str(value).strip().replace("/", "-")
        try:
            current = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y.%m.%d"):
                try:
                    current = datetime.strptime(text, pattern)
                    break
                except ValueError:
                    continue
            else:
                return None
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    return current.astimezone(UTC)


def _local_date(value: datetime | None) -> date | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(SHANGHAI).date()


def _local_start(value: date | None) -> datetime | None:
    return datetime.combine(value, time.min, tzinfo=SHANGHAI).astimezone(UTC) if value else None


def _snapshot(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {key: _json_value(item) for key, item in value.items()}


def _changed_fields(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    ignored = {"created_at", "updated_at", "last_sync_id"}
    return {
        key: value
        for key, value in after.items()
        if key not in ignored and _comparable(before.get(key)) != _comparable(value)
    }


def _comparable(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    if isinstance(value, dict):
        return {str(key): _comparable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_comparable(item) for item in value]
    return value


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value
