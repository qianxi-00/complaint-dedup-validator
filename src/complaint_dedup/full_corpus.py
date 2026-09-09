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

from sqlalchemy import insert, select, update

from complaint_dedup.corpus_models import InputRecord
from complaint_dedup.corpus_parser import (
    extract_organization_subject,
    normalize_organization_name,
    parse_complaint,
)
from complaint_dedup.corpus_schema import (
    comparison_event_members,
    comparison_events,
    comparison_record_members,
    comparison_runs,
    sync_runs,
    work_order_cannot_links,
    work_order_versions,
    work_orders,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
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


class FullCorpusService:
    def __init__(self, database) -> None:
        self.database = database
        self._operation_lock = asyncio.Lock()

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
        deduped: dict[str, dict[str, Any]] = {}
        for payload in payloads:
            deduped[payload["record_key"]] = payload
        file_hash = file_hash or hashlib.sha256(
            json.dumps(payloads, ensure_ascii=False, sort_keys=True, default=str).encode()
        ).hexdigest()
        sync_id = uuid.uuid4().hex
        now = datetime.now(UTC)
        inserted_count = updated_count = missing_count = 0

        async with self.database.engine.begin() as connection:
            await connection.execute(
                insert(sync_runs).values(
                    id=sync_id,
                    file_name=file_name,
                    file_hash=file_hash,
                    status="running",
                    total_rows=len(records),
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

    async def version_count(self, sync_id: str) -> int:
        async with self.database.engine.connect() as connection:
            result = await connection.execute(
                select(work_order_versions.c.id).where(work_order_versions.c.sync_id == sync_id)
            )
            return len(result.all())

    async def list_sync_runs(self) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            result = await connection.execute(select(sync_runs).order_by(sync_runs.c.created_at.desc()))
            return [dict(row) for row in result.mappings().all()]

    async def list_comparisons(self) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            result = await connection.execute(select(comparison_runs).order_by(comparison_runs.c.created_at.desc()))
            return [dict(row) for row in result.mappings().all()]

    async def get_comparison(self, comparison_id: str) -> dict[str, Any] | None:
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(select(comparison_runs).where(comparison_runs.c.id == comparison_id))
            ).mappings().first()
            return dict(row) if row else None

    async def list_comparison_events(self, comparison_id: str) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            events = (
                await connection.execute(select(comparison_events).where(comparison_events.c.comparison_id == comparison_id).order_by(comparison_events.c.id))
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
        return [{**dict(event), "members": by_event.get(int(event["id"]), [])} for event in events]

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
        time_field: str = "completed_at",
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
        time_field: str = "completed_at",
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
        groups = _split_conflicting_groups(
            {"target": target_rows, "reference": reference_rows},
            await self._cannot_pairs({row["record_key"] for row in (*target_rows, *reference_rows)}),
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
                    singleton_count=sum(1 for rows_ in groups.values() if len(rows_) == 1),
                    missing_time_count=missing_time_count,
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
                            event_key=row["event_key"],
                        )
                    )
            target_keys = {row["record_key"] for row in target_rows}
            for event_key, members in groups.items():
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
        return ComparisonResult(comparison_id, len(target_rows), len(reference_rows), len(groups), sum(1 for rows_ in groups.values() if len(rows_) == 1), missing_time_count)

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

    async def add_cannot_link(
        self,
        comparison_id: str,
        left_record_key: str,
        right_record_key: str,
        *,
        reason: str,
    ) -> None:
        if left_record_key == right_record_key:
            raise ValueError("不能与自身建立禁止关系")
        left, right = sorted((left_record_key, right_record_key))
        async with self.database.engine.begin() as connection:
            existing = (
                await connection.execute(
                    select(comparison_record_members.c.record_key).where(
                        comparison_record_members.c.comparison_id == comparison_id,
                        comparison_record_members.c.record_key.in_([left, right]),
                    )
                )
            ).scalars().all()
            if len(existing) != 2:
                raise ValueError("禁止关系两端工单必须属于当前比对任务")
            exists = await connection.execute(
                select(work_order_cannot_links.c.id).where(
                    work_order_cannot_links.c.left_record_key == left,
                    work_order_cannot_links.c.right_record_key == right,
                )
            )
            if exists.scalar_one_or_none() is None:
                await connection.execute(
                    insert(work_order_cannot_links).values(
                        left_record_key=left,
                        right_record_key=right,
                        reason=reason,
                        created_at=datetime.now(UTC),
                    )
                )

    async def _cannot_pairs(self, record_keys: set[str]) -> set[tuple[str, str]]:
        if not record_keys:
            return set()
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(select(work_order_cannot_links))
            ).mappings().all()
        return {
            (str(row["left_record_key"]), str(row["right_record_key"]))
            for row in rows
            if row["left_record_key"] in record_keys and row["right_record_key"] in record_keys
        }

    async def list_cannot_links(self) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            result = await connection.execute(
                select(work_order_cannot_links).order_by(
                    work_order_cannot_links.c.created_at.desc()
                )
            )
            return [dict(row) for row in result.mappings().all()]


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
    anchor = parsed.anchor_raw or "未知地点"
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
        category,
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
    raw = dict(record.raw_fields)
    raw.setdefault("受理时间", record.received_at)
    raw.setdefault("办结时间", record.completed_at)
    raw.setdefault("事发地点", location)
    raw.setdefault("处理部门", processing_department)
    return {
        "record_key": record_key,
        "work_order_id": work_order_id,
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
        "raw_json": raw,
        "source_row": record.source_row,
    }


def _event_key(
    region: str | None,
    street: str,
    anchor: str,
    category: str | None,
    org: str | None,
    family: str | None,
    record_key: str,
) -> str:
    if family and org:
        return "enterprise|" + "|".join(_key_part(v) for v in (region, street, org, family))
    if family:
        return "singleton|" + _key_part(record_key)
    if anchor == "未知地点" or not category:
        return "singleton|" + _key_part(record_key)
    return "ordinary|" + "|".join(_key_part(v) for v in (region, street, anchor, category or "未分类"))


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


def _split_conflicting_groups(sides: dict[str, list[dict[str, Any]]], pairs: set[tuple[str, str]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in (*sides["target"], *sides["reference"]):
        grouped.setdefault(str(row["event_key"]), []).append(row)
    result: dict[str, list[dict[str, Any]]] = {}
    for key, rows in grouped.items():
        conflict_ids = {
            record_key
            for left, right in pairs
            for record_key in (left, right)
            if left in {row["record_key"] for row in rows} and right in {row["record_key"] for row in rows}
        }
        remaining = [row for row in rows if row["record_key"] not in conflict_ids]
        if remaining:
            result[key] = remaining
        for record in [row for row in rows if row["record_key"] in conflict_ids]:
            result[f"{key}|singleton|{record['record_key']}"] = [record]
    return result


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
