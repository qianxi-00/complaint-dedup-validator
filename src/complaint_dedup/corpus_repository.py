from __future__ import annotations

import hashlib
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Any

from sqlalchemy import and_, bindparam, delete, func, insert, or_, select, update

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_normalizer import anchor_location_signature
from complaint_dedup.corpus_schema import (
    anchor_aliases,
    batch_records,
    canonical_anchors,
    canonical_issues,
    canonical_streets,
    corpus_event_members,
    corpus_generations,
    corpus_records,
    corpus_review_actions,
    corpus_sources,
    cannot_links,
    daily_batches,
    dictionary_extraction_runs,
    dictionary_review_actions,
    dictionary_versions,
    events,
    event_snapshots,
    issue_aliases,
    issue_mentions,
    normalization_decisions,
    record_links,
    street_aliases,
)


_SQL_IN_CHUNK = 8000
@dataclass(frozen=True)
class EventFilters:
    region: str = ""
    street: str = ""
    event_name: str = ""
    processing_department: str = ""
    completed_from: date | None = None
    completed_to: date | None = None
    missing_completed: bool = False
    has_daily_records: bool = False
    hide_singletons: bool = False


def _chunks(values: Any, size: int = _SQL_IN_CHUNK):
    items = list(values)
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _event_has_members():
    return select(corpus_event_members.c.event_id).where(
        corpus_event_members.c.event_id == events.c.id
    ).exists()


def _dictionary_dimension(dimension: str):
    dimensions = {
        "street": (canonical_streets, street_aliases, street_aliases.c.street_id),
        "anchor": (canonical_anchors, anchor_aliases, anchor_aliases.c.anchor_id),
        "issue": (canonical_issues, issue_aliases, issue_aliases.c.issue_id),
    }
    try:
        return dimensions[dimension]
    except KeyError as exc:
        raise ValueError("词典维度无效") from exc


def _dictionary_record_column(dimension: str):
    columns = {
        "street": (corpus_records.c.street_id, corpus_records.c.street_raw),
        "anchor": (corpus_records.c.anchor_id, corpus_records.c.anchor_raw),
        "issue": (corpus_records.c.issue_id, corpus_records.c.final_category),
    }
    try:
        return columns[dimension]
    except KeyError as exc:
        raise ValueError("词典维度无效") from exc


def _anchor_key_hash(
    canonical_name: str, anchor_type: str, location_signature: str
) -> str:
    value = "\x1f".join((canonical_name, anchor_type, location_signature))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _alias_key_hash(alias: str) -> str:
    return hashlib.sha256(alias.encode("utf-8")).hexdigest()


class CorpusRepository:
    def __init__(self, database: AsyncDatabase) -> None:
        self.database = database

    async def create_generation(
        self, batch_id: str, dictionary_version_id: int | None
    ) -> int:
        generation_key = uuid.uuid4().hex
        async with self.database.engine.begin() as connection:
            result = await connection.execute(
                insert(corpus_generations).values(
                    generation_key=generation_key,
                    status="building",
                    source_batch_id=batch_id,
                    dictionary_version_id=dictionary_version_id,
                    created_at=datetime.now(UTC),
                )
            )
        return int(result.inserted_primary_key[0])

    async def get_generation(self, generation_id: int) -> dict[str, Any]:
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(
                    select(corpus_generations).where(corpus_generations.c.id == generation_id)
                )
            ).mappings().first()
        if row is None:
            raise KeyError(generation_id)
        return dict(row)

    async def list_generations(self) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(corpus_generations).order_by(corpus_generations.c.id)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def active_generation(self) -> dict[str, Any] | None:
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(
                    select(corpus_generations)
                    .where(corpus_generations.c.status == "active")
                    .order_by(corpus_generations.c.activated_at.desc(), corpus_generations.c.id.desc())
                    .limit(1)
                )
            ).mappings().first()
        return dict(row) if row is not None else None

    async def activate_generation(self, generation_id: int) -> None:
        now = datetime.now(UTC)
        async with self.database.engine.begin() as connection:
            exists = await connection.scalar(
                select(corpus_generations.c.id).where(corpus_generations.c.id == generation_id)
            )
            if exists is None:
                raise KeyError(generation_id)
            await connection.execute(
                update(corpus_generations)
                .where(corpus_generations.c.status == "active")
                .values(status="archived")
            )
            await connection.execute(
                update(corpus_generations)
                .where(corpus_generations.c.id == generation_id)
                .values(status="active", activated_at=now, error_message=None)
            )

    async def fail_generation(self, generation_id: int, message: str) -> None:
        async with self.database.engine.begin() as connection:
            result = await connection.execute(
                update(corpus_generations)
                .where(corpus_generations.c.id == generation_id)
                .values(status="failed", error_message=message)
            )
        if not result.rowcount:
            raise KeyError(generation_id)

    async def generation_for_batch(self, batch_id: str) -> dict[str, Any] | None:
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(
                    select(corpus_generations)
                    .where(corpus_generations.c.source_batch_id == batch_id)
                    .order_by(corpus_generations.c.id.desc())
                    .limit(1)
                )
            ).mappings().first()
        return dict(row) if row is not None else None

    async def create_source(
        self,
        *,
        file_name: str,
        file_hash: str,
        source_type: str,
        business_columns: list[str],
        column_mapping: dict[str, str] | None = None,
        row_count: int = 0,
        generation_id: int | None = None,
    ) -> int:
        async with self.database.engine.begin() as connection:
            source_scope = [corpus_sources.c.file_hash == file_hash]
            if generation_id is None:
                source_scope.append(corpus_sources.c.generation_id.is_(None))
            else:
                source_scope.append(corpus_sources.c.generation_id == generation_id)
            existing = await connection.scalar(select(corpus_sources.c.id).where(*source_scope))
            if existing is not None:
                return int(existing)
            result = await connection.execute(
                insert(corpus_sources).values(
                    file_name=file_name,
                    file_hash=file_hash,
                    source_type=source_type,
                    column_mapping=column_mapping or {},
                    business_columns=business_columns,
                    row_count=row_count,
                    generation_id=generation_id,
                    created_at=datetime.now(UTC),
                )
            )
            return int(result.inserted_primary_key[0])

    async def create_batch(
        self,
        name: str,
        batch_type: str,
        *,
        total_records: int = 0,
        dictionary_version_id: int | None = None,
        input_files: dict[str, Any] | None = None,
    ) -> str:
        if batch_type not in {"bootstrap_history", "bootstrap_compare", "daily_increment"}:
            raise ValueError("批次类型无效")
        batch_id = uuid.uuid4().hex
        now = datetime.now(UTC)
        async with self.database.engine.begin() as connection:
            await connection.execute(
                insert(daily_batches).values(
                    id=batch_id,
                    name=name,
                    batch_type=batch_type,
                    status="uploaded",
                    stage="uploaded",
                    input_files=input_files or {},
                    dictionary_version_id=dictionary_version_id,
                    total_records=total_records,
                    created_at=now,
                    updated_at=now,
                )
            )
        return batch_id

    async def update_batch_setup(
        self,
        batch_id: str,
        *,
        total_records: int,
        dictionary_version_id: int,
        generation_id: int | None = None,
    ) -> None:
        async with self.database.engine.begin() as connection:
            result = await connection.execute(
                update(daily_batches)
                .where(daily_batches.c.id == batch_id)
                .values(
                    total_records=total_records,
                    dictionary_version_id=dictionary_version_id,
                    generation_id=generation_id,
                    updated_at=datetime.now(UTC),
                )
            )
        if not result.rowcount:
            raise KeyError(batch_id)

    async def update_batch_record_count(self, batch_id: str, total_records: int) -> None:
        async with self.database.engine.begin() as connection:
            result = await connection.execute(
                update(daily_batches)
                .where(daily_batches.c.id == batch_id)
                .values(total_records=total_records, updated_at=datetime.now(UTC))
            )
        if not result.rowcount:
            raise KeyError(batch_id)

    async def claim_batches(
        self, worker_id: str, *, limit: int, lease_seconds: int
    ) -> list[dict[str, Any]]:
        now = datetime.now(UTC)
        claimable_statuses = (
            "uploaded",
            "parsing",
            "normalizing",
            "approval_requested",
            "commit_requested",
            "awaiting_daily",
        )
        async with self.database.engine.begin() as connection:
            query = (
                select(daily_batches)
                .where(
                    daily_batches.c.status.in_(claimable_statuses),
                    or_(
                        daily_batches.c.lease_owner.is_(None),
                        daily_batches.c.lease_expires_at < now,
                    ),
                )
                .order_by(daily_batches.c.created_at, daily_batches.c.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            rows = (await connection.execute(query)).mappings().all()
            expires_at = now + timedelta(seconds=lease_seconds)
            for row in rows:
                await connection.execute(
                    update(daily_batches)
                    .where(daily_batches.c.id == row["id"])
                    .values(
                        lease_owner=worker_id,
                        lease_expires_at=expires_at,
                        heartbeat_at=now,
                        updated_at=now,
                    )
                )
        return [
            {
                **dict(row),
                "lease_owner": worker_id,
                "lease_expires_at": expires_at,
                "heartbeat_at": now,
            }
            for row in rows
        ]

    async def heartbeat_batch(
        self, batch_id: str, worker_id: str, *, lease_seconds: int
    ) -> bool:
        now = datetime.now(UTC)
        async with self.database.engine.begin() as connection:
            result = await connection.execute(
                update(daily_batches)
                .where(
                    daily_batches.c.id == batch_id,
                    daily_batches.c.lease_owner == worker_id,
                )
                .values(
                    heartbeat_at=now,
                    lease_expires_at=now + timedelta(seconds=lease_seconds),
                    updated_at=now,
                )
            )
        return bool(result.rowcount)

    async def release_batch_lease(self, batch_id: str, worker_id: str) -> None:
        async with self.database.engine.begin() as connection:
            await connection.execute(
                update(daily_batches)
                .where(
                    daily_batches.c.id == batch_id,
                    daily_batches.c.lease_owner == worker_id,
                )
                .values(lease_owner=None, lease_expires_at=None)
            )

    async def request_batch_action(self, batch_id: str, action: str) -> None:
        statuses = {"approve": "approval_requested", "commit": "commit_requested"}
        if action not in statuses:
            raise ValueError("无效的批次操作")
        batch = await self.get_batch(batch_id)
        if batch["status"] != "reviewing":
            raise ValueError("批次尚未进入审核阶段")
        if action == "approve" and batch["batch_type"] not in {
            "bootstrap_history",
            "bootstrap_compare",
        }:
            raise ValueError("该批次不能发布历史词典")
        if action == "commit" and batch["batch_type"] != "daily_increment":
            raise ValueError("该批次不能提交增量")
        await self.set_batch_stage(batch_id, statuses[action], status=statuses[action])

    async def mark_batch_failed(self, batch_id: str, message: str) -> None:
        now = datetime.now(UTC)
        async with self.database.engine.begin() as connection:
            await connection.execute(
                update(daily_batches)
                .where(daily_batches.c.id == batch_id)
                .values(
                    status="failed",
                    stage="failed",
                    error_message=message,
                    lease_owner=None,
                    lease_expires_at=None,
                    updated_at=now,
                )
            )

    async def get_batch(self, batch_id: str) -> dict[str, Any]:
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(
                    select(daily_batches).where(daily_batches.c.id == batch_id)
                )
            ).mappings().first()
        if row is None:
            raise KeyError(batch_id)
        return dict(row)

    async def list_batches(
        self, *, limit: int | None = None, offset: int = 0
    ) -> list[dict[str, Any]]:
        query = select(daily_batches).order_by(daily_batches.c.updated_at.desc())
        if limit is not None:
            query = query.limit(limit).offset(offset)
        async with self.database.engine.connect() as connection:
            rows = (await connection.execute(query)).mappings().all()
        return [dict(row) for row in rows]

    async def count_batches(self) -> int:
        async with self.database.engine.connect() as connection:
            value = await connection.scalar(select(func.count(daily_batches.c.id)))
        return int(value or 0)

    async def set_batch_stage(
        self, batch_id: str, stage: str, *, status: str | None = None
    ) -> None:
        values: dict[str, Any] = {"stage": stage, "updated_at": datetime.now(UTC)}
        if status is not None:
            values["status"] = status
        async with self.database.engine.begin() as connection:
            result = await connection.execute(
                update(daily_batches).where(daily_batches.c.id == batch_id).values(**values)
            )
        if not result.rowcount:
            raise KeyError(batch_id)

    async def upsert_record(self, value: dict[str, Any]) -> int:
        key = (
            str(value["source_file_hash"]),
            int(value["source_row"]),
            str(value["row_hash"]),
        )
        async with self.database.engine.begin() as connection:
            existing = await connection.scalar(
                select(corpus_records.c.id).where(
                    corpus_records.c.source_file_hash == key[0],
                    corpus_records.c.source_row == key[1],
                    corpus_records.c.row_hash == key[2],
                )
            )
            if existing is not None:
                return int(existing)
            payload = dict(value)
            payload.setdefault("raw_json", {})
            payload.setdefault("occurrence_key", "")
            payload.setdefault("occurrence_identifiers", [])
            payload.setdefault("parser_version", "rules-v1")
            payload.setdefault("phone_is_valid", False)
            payload.setdefault("anchor_resolution_status", "unknown")
            payload.setdefault("issue_resolution_status", "unknown")
            payload.setdefault("committed", False)
            payload.setdefault("created_at", datetime.now(UTC))
            result = await connection.execute(insert(corpus_records).values(**payload))
            record_id = int(result.inserted_primary_key[0])
            batch_id = payload.get("source_batch_id")
            if batch_id:
                await connection.execute(
                    insert(batch_records).values(
                        batch_id=batch_id, record_id=record_id, status="uploaded"
                    )
                )
            return record_id

    async def get_record(self, record_id: int) -> dict[str, Any]:
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(
                    select(corpus_records).where(corpus_records.c.id == record_id)
                )
            ).mappings().first()
        if row is None:
            raise KeyError(record_id)
        return dict(row)

    async def update_record_normalization(
        self,
        record_id: int,
        *,
        street_id: int,
        anchor_id: int,
        issue_id: int,
        anchor_status: str,
        issue_status: str,
    ) -> None:
        async with self.database.engine.begin() as connection:
            result = await connection.execute(
                update(corpus_records)
                .where(corpus_records.c.id == record_id)
                .values(
                    street_id=street_id,
                    anchor_id=anchor_id,
                    issue_id=issue_id,
                    anchor_resolution_status=anchor_status,
                    issue_resolution_status=issue_status,
                )
            )
        if not result.rowcount:
            raise KeyError(record_id)

    async def create_dictionary_version(
        self,
        version: str,
        *,
        status: str = "candidate",
        source_record_count: int = 0,
    ) -> int:
        async with self.database.engine.begin() as connection:
            existing = await connection.scalar(
                select(dictionary_versions.c.id).where(dictionary_versions.c.version == version)
            )
            if existing is not None:
                return int(existing)
            result = await connection.execute(
                insert(dictionary_versions).values(
                    version=version,
                    status=status,
                    source_record_count=source_record_count,
                    approved_at=datetime.now(UTC) if status == "approved" else None,
                    created_at=datetime.now(UTC),
                )
            )
            return int(result.inserted_primary_key[0])

    async def active_dictionary_version(self) -> dict[str, Any] | None:
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(
                    select(dictionary_versions)
                    .where(dictionary_versions.c.status == "approved")
                    .order_by(dictionary_versions.c.approved_at.desc(), dictionary_versions.c.id.desc())
                    .limit(1)
                )
            ).mappings().first()
        return dict(row) if row is not None else None

    async def get_dictionary_version(self, version_id: int) -> dict[str, Any]:
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(
                    select(dictionary_versions).where(dictionary_versions.c.id == version_id)
                )
            ).mappings().first()
        if row is None:
            raise KeyError(version_id)
        return dict(row)

    async def start_dictionary_extraction_run(
        self,
        *,
        source_id: int,
        dictionary_version_id: int,
        source_file_hash: str,
        parser_version: str,
        model_name: str | None = None,
    ) -> int:
        async with self.database.engine.begin() as connection:
            existing = await connection.scalar(
                select(dictionary_extraction_runs.c.id).where(
                    dictionary_extraction_runs.c.source_id == source_id,
                    dictionary_extraction_runs.c.dictionary_version_id
                    == dictionary_version_id,
                )
            )
            if existing is not None:
                return int(existing)
            result = await connection.execute(
                insert(dictionary_extraction_runs).values(
                    source_id=source_id,
                    dictionary_version_id=dictionary_version_id,
                    source_file_hash=source_file_hash,
                    parser_version=parser_version,
                    model_name=model_name,
                    status="running",
                    candidate_counts={},
                    created_at=datetime.now(UTC),
                )
            )
            return int(result.inserted_primary_key[0])

    async def complete_dictionary_extraction_run(
        self, run_id: int, *, candidate_counts: dict[str, int]
    ) -> None:
        async with self.database.engine.begin() as connection:
            result = await connection.execute(
                update(dictionary_extraction_runs)
                .where(dictionary_extraction_runs.c.id == run_id)
                .values(
                    status="completed",
                    candidate_counts=candidate_counts,
                    completed_at=datetime.now(UTC),
                )
            )
        if not result.rowcount:
            raise KeyError(run_id)

    async def list_dictionary_extraction_runs(
        self, version_id: int
    ) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(dictionary_extraction_runs)
                    .where(
                        dictionary_extraction_runs.c.dictionary_version_id
                        == version_id
                    )
                    .order_by(dictionary_extraction_runs.c.id)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def approve_dictionary_version(
        self, version_id: int, *, approved_by: str
    ) -> None:
        now = datetime.now(UTC)
        async with self.database.engine.begin() as connection:
            target = await connection.scalar(
                select(dictionary_versions.c.id)
                .where(dictionary_versions.c.id == version_id)
                .with_for_update()
            )
            if target is None:
                raise KeyError(version_id)
            await connection.execute(
                update(dictionary_versions)
                .where(
                    dictionary_versions.c.status == "approved",
                    dictionary_versions.c.id != version_id,
                )
                .values(status="deprecated")
            )
            await connection.execute(
                update(dictionary_versions)
                .where(dictionary_versions.c.id == version_id)
                .values(status="approved", approved_by=approved_by, approved_at=now)
            )
            for table in (canonical_streets, canonical_anchors, canonical_issues):
                await connection.execute(
                    update(table)
                    .where(table.c.dictionary_version_id == version_id)
                    .values(review_status="approved")
                )
            for table in (street_aliases, anchor_aliases, issue_aliases):
                foreign_key = {
                    street_aliases: street_aliases.c.street_id,
                    anchor_aliases: anchor_aliases.c.anchor_id,
                    issue_aliases: issue_aliases.c.issue_id,
                }[table]
                canonical = {
                    street_aliases: canonical_streets,
                    anchor_aliases: canonical_anchors,
                    issue_aliases: canonical_issues,
                }[table]
                item_ids = select(canonical.c.id).where(
                    canonical.c.dictionary_version_id == version_id
                )
                await connection.execute(
                    update(table)
                    .where(foreign_key.in_(item_ids))
                    .values(review_status="approved")
                )

    async def publish_dictionary_version(
        self, version_id: int, *, approved_by: str
    ) -> None:
        now = datetime.now(UTC)
        async with self.database.engine.begin() as connection:
            target = await connection.scalar(
                select(dictionary_versions.c.id)
                .where(dictionary_versions.c.id == version_id)
                .with_for_update()
            )
            if target is None:
                raise KeyError(version_id)
            await connection.execute(
                update(dictionary_versions)
                .where(
                    dictionary_versions.c.status == "approved",
                    dictionary_versions.c.id != version_id,
                )
                .values(status="deprecated")
            )
            await connection.execute(
                update(dictionary_versions)
                .where(dictionary_versions.c.id == version_id)
                .values(status="approved", approved_by=approved_by, approved_at=now)
            )

    async def bulk_approve_dictionary_items(
        self,
        version_id: int,
        *,
        min_evidence: int,
        reviewed_by: str,
    ) -> int:
        if min_evidence < 1:
            raise ValueError("最小证据数必须大于 0")
        approved = 0
        async with self.database.engine.begin() as connection:
            version_exists = await connection.scalar(
                select(dictionary_versions.c.id).where(
                    dictionary_versions.c.id == version_id
                )
            )
            if version_exists is None:
                raise KeyError(version_id)
            dimensions = (
                (canonical_streets, street_aliases, street_aliases.c.street_id),
                (canonical_anchors, anchor_aliases, anchor_aliases.c.anchor_id),
                (canonical_issues, issue_aliases, issue_aliases.c.issue_id),
            )
            for canonical, aliases, foreign_key in dimensions:
                evidence_ids = (
                    select(foreign_key)
                    .group_by(foreign_key)
                    .having(func.sum(aliases.c.evidence_count) >= min_evidence)
                )
                conditions = [
                    canonical.c.dictionary_version_id == version_id,
                    canonical.c.review_status == "candidate",
                    canonical.c.id.in_(evidence_ids),
                ]
                if canonical is canonical_anchors:
                    ambiguous_names = (
                        select(canonical_anchors.c.canonical_name)
                        .where(
                            canonical_anchors.c.dictionary_version_id == version_id
                        )
                        .group_by(canonical_anchors.c.canonical_name)
                        .having(func.count(func.distinct(canonical_anchors.c.street_id)) > 1)
                    )
                    conditions.append(
                        canonical_anchors.c.canonical_name.not_in(ambiguous_names)
                    )
                item_ids = select(canonical.c.id).where(*conditions)
                ids = list((await connection.execute(item_ids)).scalars())
                if not ids:
                    continue
                result = await connection.execute(
                    update(canonical)
                    .where(canonical.c.id.in_(ids))
                    .values(review_status="approved")
                )
                approved += int(result.rowcount or 0)
                await connection.execute(
                    update(aliases)
                    .where(foreign_key.in_(ids))
                    .values(review_status="approved")
                )
        return approved

    async def list_dictionary_items(
        self,
        version_id: int,
        *,
        dimension: str,
        status: str = "",
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        canonical, aliases, foreign_key = _dictionary_dimension(dimension)
        evidence = func.coalesce(func.sum(aliases.c.evidence_count), 0)
        alias_count = func.count(aliases.c.id)
        conditions = [canonical.c.dictionary_version_id == version_id]
        if status:
            conditions.append(canonical.c.review_status == status)
        grouped = (
            select(
                *canonical.c,
                evidence.label("evidence_count"),
                alias_count.label("alias_count"),
            )
            .select_from(canonical.outerjoin(aliases, foreign_key == canonical.c.id))
            .where(*conditions)
            .group_by(*canonical.c)
        )
        async with self.database.engine.connect() as connection:
            total = int(
                await connection.scalar(
                    select(func.count()).select_from(grouped.subquery())
                )
                or 0
            )
            rows = (
                await connection.execute(
                    grouped.order_by(evidence.desc(), canonical.c.id)
                    .limit(limit)
                    .offset(offset)
                )
            ).mappings().all()
        return [dict(row) for row in rows], total

    async def dictionary_review_summary(self, version_id: int) -> dict[str, Any]:
        summary: dict[str, Any] = {}
        async with self.database.engine.connect() as connection:
            for dimension in ("street", "anchor", "issue"):
                canonical, _, _ = _dictionary_dimension(dimension)
                rows = (
                    await connection.execute(
                        select(canonical.c.review_status, func.count(canonical.c.id))
                        .where(canonical.c.dictionary_version_id == version_id)
                        .group_by(canonical.c.review_status)
                    )
                ).all()
                summary[dimension] = {
                    str(row[0]): int(row[1]) for row in rows
                }
        return summary

    async def review_dictionary_item(
        self,
        version_id: int,
        *,
        dimension: str,
        item_id: int,
        action: str,
        reviewed_by: str,
        name: str | None = None,
    ) -> None:
        canonical, aliases, foreign_key = _dictionary_dimension(dimension)
        statuses = {
            "approve": "approved",
            "reject": "rejected",
            "uncertain": "uncertain",
        }
        if action not in {*statuses, "rename"}:
            raise ValueError("无效的词典审核操作")
        async with self.database.engine.begin() as connection:
            row = (
                await connection.execute(
                    select(canonical)
                    .where(
                        canonical.c.id == item_id,
                        canonical.c.dictionary_version_id == version_id,
                    )
                    .with_for_update()
                )
            ).mappings().first()
            if row is None:
                raise KeyError(item_id)
            before = dict(row)
            values: dict[str, Any]
            if action == "rename":
                normalized = str(name or "").strip()
                if not normalized:
                    raise ValueError("标准名称不能为空")
                values = {"canonical_name": normalized}
            else:
                values = {"review_status": statuses[action]}
            await connection.execute(
                update(canonical).where(canonical.c.id == item_id).values(**values)
            )
            if action in statuses:
                await connection.execute(
                    update(aliases)
                    .where(foreign_key == item_id)
                    .values(review_status=statuses[action])
                )
            after = {**before, **values}
            await connection.execute(
                insert(dictionary_review_actions).values(
                    dictionary_version_id=version_id,
                    dimension=dimension,
                    item_id=item_id,
                    action=action,
                    before_json=before,
                    after_json=after,
                    reviewed_by=reviewed_by,
                    created_at=datetime.now(UTC),
                )
            )

    async def add_dictionary_alias(
        self,
        version_id: int,
        *,
        dimension: str,
        item_id: int,
        alias: str,
        evidence_count: int = 1,
    ) -> int:
        canonical, aliases, foreign_key = _dictionary_dimension(dimension)
        normalized = alias.strip()
        if not normalized:
            raise ValueError("别名不能为空")
        async with self.database.engine.begin() as connection:
            item = await connection.scalar(
                select(canonical.c.id).where(
                    canonical.c.id == item_id,
                    canonical.c.dictionary_version_id == version_id,
                )
            )
            if item is None:
                raise KeyError(item_id)
            existing = await connection.scalar(
                select(aliases.c.id).where(
                    foreign_key == item_id,
                    aliases.c.alias == normalized,
                )
            )
            if existing is not None:
                return int(existing)
            payload = {
                foreign_key.name: item_id,
                "alias": normalized,
                "evidence_count": evidence_count,
                "review_status": "candidate",
            }
            if aliases is anchor_aliases:
                payload["alias_key_hash"] = _alias_key_hash(normalized)
            result = await connection.execute(
                insert(aliases).values(**payload)
            )
            return int(result.inserted_primary_key[0])

    async def get_dictionary_item(
        self, version_id: int, *, dimension: str, item_id: int
    ) -> dict[str, Any]:
        canonical, aliases, foreign_key = _dictionary_dimension(dimension)
        _, raw_column = _dictionary_record_column(dimension)
        async with self.database.engine.connect() as connection:
            item = (
                await connection.execute(
                    select(canonical).where(
                        canonical.c.id == item_id,
                        canonical.c.dictionary_version_id == version_id,
                    )
                )
            ).mappings().first()
            if item is None:
                raise KeyError(item_id)
            alias_rows = (
                await connection.execute(
                    select(aliases)
                    .where(foreign_key == item_id)
                    .order_by(aliases.c.evidence_count.desc(), aliases.c.id)
                )
            ).mappings().all()
            samples = (
                await connection.execute(
                    select(
                        corpus_records.c.id,
                        corpus_records.c.work_order_id,
                        corpus_records.c.title_raw,
                        corpus_records.c.address_line,
                        corpus_records.c.appeal_text,
                        raw_column.label("matched_value"),
                    )
                    .where(_dictionary_record_column(dimension)[0] == item_id)
                    .order_by(corpus_records.c.id)
                    .limit(10)
                )
            ).mappings().all()
        return {
            "item": dict(item),
            "aliases": [dict(row) for row in alias_rows],
            "samples": [dict(row) for row in samples],
        }

    async def split_dictionary_item(
        self,
        version_id: int,
        *,
        dimension: str,
        item_id: int,
        new_name: str,
        alias_ids: list[int],
        reviewed_by: str,
    ) -> int:
        canonical, aliases, foreign_key = _dictionary_dimension(dimension)
        record_fk, raw_column = _dictionary_record_column(dimension)
        normalized = new_name.strip()
        if not normalized or not alias_ids:
            raise ValueError("拆分必须填写新标准名并选择别名")
        async with self.database.engine.begin() as connection:
            source = (
                await connection.execute(
                    select(canonical)
                    .where(
                        canonical.c.id == item_id,
                        canonical.c.dictionary_version_id == version_id,
                    )
                    .with_for_update()
                )
            ).mappings().first()
            if source is None:
                raise KeyError(item_id)
            selected = (
                await connection.execute(
                    select(aliases).where(
                        aliases.c.id.in_(alias_ids),
                        foreign_key == item_id,
                    )
                )
            ).mappings().all()
            if len(selected) != len(set(alias_ids)):
                raise ValueError("存在不属于当前标准项的别名")
            payload = dict(source)
            payload.pop("id", None)
            payload["canonical_name"] = normalized
            if canonical is canonical_anchors:
                payload["anchor_key_hash"] = _anchor_key_hash(
                    normalized,
                    str(payload["anchor_type"]),
                    str(payload["location_signature"]),
                )
            payload["review_status"] = "candidate"
            result = await connection.execute(insert(canonical).values(**payload))
            new_id = int(result.inserted_primary_key[0])
            await connection.execute(
                update(aliases)
                .where(aliases.c.id.in_(alias_ids))
                .values(**{foreign_key.name: new_id})
            )
            alias_values = [str(row["alias"]) for row in selected]
            await connection.execute(
                update(corpus_records)
                .where(record_fk == item_id, raw_column.in_(alias_values))
                .values(**{record_fk.name: new_id})
            )
            await connection.execute(
                insert(dictionary_review_actions).values(
                    dictionary_version_id=version_id,
                    dimension=dimension,
                    item_id=item_id,
                    action="split",
                    before_json={"item_id": item_id, "alias_ids": alias_ids},
                    after_json={"new_item_id": new_id, "new_name": normalized},
                    reviewed_by=reviewed_by,
                    created_at=datetime.now(UTC),
                )
            )
        return new_id

    async def merge_dictionary_item(
        self,
        version_id: int,
        *,
        dimension: str,
        item_id: int,
        target_id: int,
        reviewed_by: str,
    ) -> None:
        if item_id == target_id:
            raise ValueError("不能合并到自身")
        canonical, aliases, foreign_key = _dictionary_dimension(dimension)
        record_fk, _ = _dictionary_record_column(dimension)
        async with self.database.engine.begin() as connection:
            rows = (
                await connection.execute(
                    select(canonical)
                    .where(
                        canonical.c.id.in_([item_id, target_id]),
                        canonical.c.dictionary_version_id == version_id,
                    )
                    .with_for_update()
                )
            ).mappings().all()
            if len(rows) != 2:
                raise KeyError(item_id if not any(row["id"] == item_id for row in rows) else target_id)
            source_aliases = (
                await connection.execute(
                    select(aliases).where(foreign_key == item_id)
                )
            ).mappings().all()
            moved_aliases: list[int] = []
            for alias in source_aliases:
                duplicate = await connection.scalar(
                    select(aliases.c.id).where(
                        foreign_key == target_id,
                        aliases.c.alias == alias["alias"],
                    )
                )
                if duplicate is None:
                    await connection.execute(
                        update(aliases)
                        .where(aliases.c.id == alias["id"])
                        .values(**{foreign_key.name: target_id})
                    )
                    moved_aliases.append(int(alias["id"]))
                else:
                    await connection.execute(
                        delete(aliases).where(aliases.c.id == alias["id"])
                    )
            await connection.execute(
                update(corpus_records)
                .where(record_fk == item_id)
                .values(**{record_fk.name: target_id})
            )
            await connection.execute(
                update(canonical)
                .where(canonical.c.id == item_id)
                .values(review_status="merged")
            )
            await connection.execute(
                insert(dictionary_review_actions).values(
                    dictionary_version_id=version_id,
                    dimension=dimension,
                    item_id=item_id,
                    action="merge",
                    before_json={"source_id": item_id},
                    after_json={
                        "target_id": target_id,
                        "moved_alias_ids": moved_aliases,
                    },
                    reviewed_by=reviewed_by,
                    created_at=datetime.now(UTC),
                )
            )

    async def list_dictionary_review_actions(
        self, version_id: int
    ) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(dictionary_review_actions)
                    .where(
                        dictionary_review_actions.c.dictionary_version_id == version_id
                    )
                    .order_by(dictionary_review_actions.c.id)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def create_street(
        self,
        region: str | None,
        canonical_name: str,
        dictionary_version_id: int,
        *,
        review_status: str = "approved",
    ) -> int:
        async with self.database.engine.begin() as connection:
            existing = await connection.scalar(
                select(canonical_streets.c.id).where(
                    canonical_streets.c.region == region,
                    canonical_streets.c.canonical_name == canonical_name,
                    canonical_streets.c.dictionary_version_id == dictionary_version_id,
                )
            )
            if existing is not None:
                return int(existing)
            result = await connection.execute(
                insert(canonical_streets).values(
                    region=region,
                    canonical_name=canonical_name,
                    review_status=review_status,
                    dictionary_version_id=dictionary_version_id,
                )
            )
            street_id = int(result.inserted_primary_key[0])
            await connection.execute(
                insert(street_aliases).values(
                    street_id=street_id,
                    alias=canonical_name,
                    evidence_count=1,
                    review_status=review_status,
                )
            )
            return street_id

    async def create_anchor(
        self,
        street_id: int | None,
        canonical_name: str,
        anchor_type: str,
        dictionary_version_id: int,
        review_status: str = "approved",
        **parts: Any,
    ) -> int:
        location_signature = str(
            parts.get("location_signature")
            or anchor_location_signature(
                parts.get("road"),
                parts.get("house_no"),
                parts.get("building"),
                parts.get("direction"),
                parts.get("shop_no"),
                parts.get("floor"),
            )
        )
        anchor_key_hash = _anchor_key_hash(
            canonical_name, anchor_type, location_signature
        )
        async with self.database.engine.begin() as connection:
            existing = await connection.scalar(
                select(canonical_anchors.c.id).where(
                    canonical_anchors.c.street_id == street_id,
                    canonical_anchors.c.anchor_key_hash == anchor_key_hash,
                    canonical_anchors.c.dictionary_version_id == dictionary_version_id,
                )
            )
            if existing is not None:
                return int(existing)
            allowed = {
                key: parts[key]
                for key in ("road", "house_no", "building", "shop_no", "floor", "direction")
                if parts.get(key) is not None
            }
            result = await connection.execute(
                insert(canonical_anchors).values(
                    street_id=street_id,
                    canonical_name=canonical_name,
                    anchor_type=anchor_type,
                    location_signature=location_signature,
                    anchor_key_hash=anchor_key_hash,
                    review_status=review_status,
                    dictionary_version_id=dictionary_version_id,
                    **allowed,
                )
            )
            anchor_id = int(result.inserted_primary_key[0])
            await connection.execute(
                insert(anchor_aliases).values(
                    anchor_id=anchor_id,
                    alias=canonical_name,
                    alias_key_hash=_alias_key_hash(canonical_name),
                    evidence_count=1,
                    review_status=review_status,
                )
            )
            return anchor_id

    async def create_issue(
        self,
        canonical_name: str,
        dictionary_version_id: int,
        *,
        review_status: str = "approved",
        **categories: Any,
    ) -> int:
        async with self.database.engine.begin() as connection:
            existing = await connection.scalar(
                select(canonical_issues.c.id).where(
                    canonical_issues.c.canonical_name == canonical_name,
                    canonical_issues.c.dictionary_version_id == dictionary_version_id,
                )
            )
            if existing is not None:
                return int(existing)
            allowed = {
                key: categories[key]
                for key in (
                    "category_level_1",
                    "category_level_2",
                    "category_level_3",
                    "category_level_4",
                )
                if categories.get(key) is not None
            }
            result = await connection.execute(
                insert(canonical_issues).values(
                    canonical_name=canonical_name,
                    review_status=review_status,
                    dictionary_version_id=dictionary_version_id,
                    **allowed,
                )
            )
            issue_id = int(result.inserted_primary_key[0])
            await connection.execute(
                insert(issue_aliases).values(
                    issue_id=issue_id,
                    alias=canonical_name,
                    evidence_count=1,
                    review_status=review_status,
                )
            )
            return issue_id

    async def ensure_dictionary_items(
        self,
        *,
        dictionary_version_id: int,
        streets: list[tuple[str | None, str]],
        anchors: list[dict[str, Any]],
        issues: list[dict[str, Any]],
        review_status: str,
        update_existing_evidence: bool = True,
    ) -> dict[str, dict[Any, int]]:
        async with self.database.engine.begin() as connection:
            street_rows = (
                await connection.execute(
                    select(canonical_streets).where(
                        canonical_streets.c.dictionary_version_id == dictionary_version_id
                    )
                )
            ).mappings().all()
            street_map = {
                (row["region"], row["canonical_name"]): int(row["id"])
                for row in street_rows
            }
            street_counts = Counter(streets)
            missing_streets: list[dict[str, Any]] = []
            existing_street_updates: list[dict[str, Any]] = []
            for (region, name), evidence_count in street_counts.items():
                key = (region, name)
                if key in street_map:
                    existing_street_updates.append(
                        {
                            "match_street_id": street_map[key],
                            "match_alias": name,
                            "new_evidence_count": evidence_count,
                        }
                    )
                    continue
                missing_streets.append(
                    {
                        "region": region,
                        "canonical_name": name,
                        "review_status": review_status,
                        "dictionary_version_id": dictionary_version_id,
                    }
                )
            if update_existing_evidence and existing_street_updates:
                await connection.execute(
                    update(street_aliases)
                    .where(
                        street_aliases.c.street_id == bindparam("match_street_id"),
                        street_aliases.c.alias == bindparam("match_alias"),
                    )
                    .values(evidence_count=bindparam("new_evidence_count")),
                    existing_street_updates,
                )
            if missing_streets:
                inserted_ids = list(
                    (
                        await connection.execute(
                            insert(canonical_streets).returning(canonical_streets.c.id),
                            missing_streets,
                        )
                    ).scalars()
                )
                await connection.execute(
                    insert(street_aliases),
                    [
                        {
                            "street_id": int(street_id),
                            "alias": value["canonical_name"],
                            "evidence_count": street_counts[
                                (value["region"], value["canonical_name"])
                            ],
                            "review_status": review_status,
                        }
                        for value, street_id in zip(missing_streets, inserted_ids, strict=True)
                    ],
                )
                street_map.update(
                    (
                        (value["region"], value["canonical_name"]),
                        int(street_id),
                    )
                    for value, street_id in zip(missing_streets, inserted_ids, strict=True)
                )

            anchor_rows = (
                await connection.execute(
                    select(canonical_anchors).where(
                        canonical_anchors.c.dictionary_version_id == dictionary_version_id
                    )
                )
            ).mappings().all()
            anchor_map = {
                (
                    int(row["street_id"]),
                    row["canonical_name"],
                    row["anchor_type"],
                    row["location_signature"],
                ): int(row["id"])
                for row in anchor_rows
                if row["street_id"] is not None
            }
            normalized_anchors = []
            for item in anchors:
                value = dict(item)
                value["location_signature"] = (
                    item.get("location_signature")
                    or anchor_location_signature(
                        item.get("road"),
                        item.get("house_no"),
                        item.get("building"),
                        item.get("direction"),
                        item.get("shop_no"),
                        item.get("floor"),
                    )
                )
                value.setdefault("alias", item["canonical_name"])
                normalized_anchors.append(value)
            canonical_examples = {
                (
                    item["street_key"],
                    item["canonical_name"],
                    item["anchor_type"],
                    item["location_signature"],
                ): item
                for item in normalized_anchors
            }
            missing_anchors: dict[tuple[int, str, str, str], dict[str, Any]] = {}
            for raw_key, item in canonical_examples.items():
                street_id = street_map.get(item["street_key"])
                if street_id is None:
                    continue
                location_signature = raw_key[3]
                key = (
                    street_id,
                    item["canonical_name"],
                    item["anchor_type"],
                    location_signature,
                )
                if key not in anchor_map:
                    missing_anchors[key] = {
                        "street_id": street_id,
                        "canonical_name": item["canonical_name"],
                        "anchor_type": item["anchor_type"],
                        "location_signature": location_signature,
                        "anchor_key_hash": _anchor_key_hash(
                            item["canonical_name"],
                            item["anchor_type"],
                            location_signature,
                        ),
                        "road": item.get("road"),
                        "house_no": item.get("house_no"),
                        "building": item.get("building"),
                        "direction": item.get("direction"),
                        "review_status": review_status,
                        "dictionary_version_id": dictionary_version_id,
                    }
            if missing_anchors:
                inserted_ids = list(
                    (
                        await connection.execute(
                            insert(canonical_anchors).returning(canonical_anchors.c.id),
                            list(missing_anchors.values()),
                        )
                    ).scalars()
                )
                anchor_map.update(
                    (key, int(anchor_id))
                    for key, anchor_id in zip(
                        missing_anchors, inserted_ids, strict=True
                    )
                )

            alias_counts = Counter()
            for item in normalized_anchors:
                base = (
                    item["street_key"],
                    item["canonical_name"],
                    item["anchor_type"],
                    item["location_signature"],
                )
                alias_counts[(*base, item["alias"])] += 1
                if item["alias"] != item["canonical_name"]:
                    alias_counts[(*base, item["canonical_name"])] += 1
            existing_anchor_aliases = {
                (int(row.anchor_id), str(row.alias)): int(row.id)
                for row in (
                    await connection.execute(
                        select(
                            anchor_aliases.c.id,
                            anchor_aliases.c.anchor_id,
                            anchor_aliases.c.alias,
                        )
                        .join(
                            canonical_anchors,
                            canonical_anchors.c.id == anchor_aliases.c.anchor_id,
                        )
                        .where(
                            canonical_anchors.c.dictionary_version_id
                            == dictionary_version_id
                        )
                    )
                )
            }
            new_anchor_aliases: list[dict[str, Any]] = []
            existing_anchor_updates: list[dict[str, Any]] = []
            anchor_alias_map: dict[tuple[int, str, str, str], int] = {}
            for raw_key, evidence_count in alias_counts.items():
                street_id = street_map.get(raw_key[0])
                if street_id is None:
                    continue
                canonical_key = (street_id, raw_key[1], raw_key[2], raw_key[3])
                anchor_id = anchor_map[canonical_key]
                alias = str(raw_key[4])
                existing_alias = existing_anchor_aliases.get((anchor_id, alias))
                if existing_alias is None:
                    new_anchor_aliases.append(
                        {
                            "anchor_id": anchor_id,
                            "alias": alias,
                            "alias_key_hash": _alias_key_hash(alias),
                            "evidence_count": evidence_count,
                            "review_status": review_status,
                        }
                    )
                else:
                    existing_anchor_updates.append(
                        {
                            "match_alias_id": existing_alias,
                            "new_evidence_count": evidence_count,
                        }
                    )
                anchor_alias_map[(street_id, alias, raw_key[2], raw_key[3])] = anchor_id
            if update_existing_evidence and existing_anchor_updates:
                await connection.execute(
                    update(anchor_aliases)
                    .where(anchor_aliases.c.id == bindparam("match_alias_id"))
                    .values(evidence_count=bindparam("new_evidence_count")),
                    existing_anchor_updates,
                )
            if new_anchor_aliases:
                await connection.execute(insert(anchor_aliases), new_anchor_aliases)

            issue_rows = (
                await connection.execute(
                    select(canonical_issues).where(
                        canonical_issues.c.dictionary_version_id == dictionary_version_id
                    )
                )
            ).mappings().all()
            issue_map = {row["canonical_name"]: int(row["id"]) for row in issue_rows}
            issue_counts = Counter(entry["canonical_name"] for entry in issues)
            issue_examples = {entry["canonical_name"]: entry for entry in issues}
            missing_issues: list[dict[str, Any]] = []
            existing_issue_updates: list[dict[str, Any]] = []
            for name, evidence_count in issue_counts.items():
                item = issue_examples[name]
                if name in issue_map:
                    existing_issue_updates.append(
                        {
                            "match_issue_id": issue_map[name],
                            "match_alias": name,
                            "new_evidence_count": evidence_count,
                        }
                    )
                    continue
                missing_issues.append(
                    {
                        "canonical_name": name,
                        "category_level_1": item.get("category_level_1"),
                        "category_level_2": item.get("category_level_2"),
                        "category_level_3": item.get("category_level_3"),
                        "category_level_4": item.get("category_level_4"),
                        "review_status": review_status,
                        "dictionary_version_id": dictionary_version_id,
                    }
                )
            if update_existing_evidence and existing_issue_updates:
                await connection.execute(
                    update(issue_aliases)
                    .where(
                        issue_aliases.c.issue_id == bindparam("match_issue_id"),
                        issue_aliases.c.alias == bindparam("match_alias"),
                    )
                    .values(evidence_count=bindparam("new_evidence_count")),
                    existing_issue_updates,
                )
            if missing_issues:
                inserted_ids = list(
                    (
                        await connection.execute(
                            insert(canonical_issues).returning(canonical_issues.c.id),
                            missing_issues,
                        )
                    ).scalars()
                )
                await connection.execute(
                    insert(issue_aliases),
                    [
                        {
                            "issue_id": int(issue_id),
                            "alias": value["canonical_name"],
                            "evidence_count": issue_counts[value["canonical_name"]],
                            "review_status": review_status,
                        }
                        for value, issue_id in zip(missing_issues, inserted_ids, strict=True)
                    ],
                )
                issue_map.update(
                    (value["canonical_name"], int(issue_id))
                    for value, issue_id in zip(missing_issues, inserted_ids, strict=True)
                )
        return {"streets": street_map, "anchors": anchor_alias_map, "issues": issue_map}

    async def dictionary_maps(self, dictionary_version_id: int) -> dict[str, dict[Any, int]]:
        async with self.database.engine.connect() as connection:
            streets = (
                await connection.execute(
                    select(
                        canonical_streets.c.region,
                        street_aliases.c.alias,
                        canonical_streets.c.id,
                    )
                    .join(street_aliases, street_aliases.c.street_id == canonical_streets.c.id)
                    .where(
                        canonical_streets.c.dictionary_version_id == dictionary_version_id,
                        canonical_streets.c.review_status == "approved",
                        street_aliases.c.review_status == "approved",
                    )
                )
            ).all()
            anchors = (
                await connection.execute(
                    select(
                        canonical_anchors.c.street_id,
                        anchor_aliases.c.alias,
                        canonical_anchors.c.anchor_type,
                        canonical_anchors.c.location_signature,
                        canonical_anchors.c.id,
                    )
                    .join(anchor_aliases, anchor_aliases.c.anchor_id == canonical_anchors.c.id)
                    .where(
                        canonical_anchors.c.dictionary_version_id == dictionary_version_id,
                        canonical_anchors.c.review_status == "approved",
                        anchor_aliases.c.review_status == "approved",
                    )
                )
            ).all()
            issues = (
                await connection.execute(
                    select(issue_aliases.c.alias, canonical_issues.c.id)
                    .join(canonical_issues, canonical_issues.c.id == issue_aliases.c.issue_id)
                    .where(
                        canonical_issues.c.dictionary_version_id == dictionary_version_id,
                        canonical_issues.c.review_status == "approved",
                        issue_aliases.c.review_status == "approved",
                    )
                )
            ).all()
        return {
            "streets": {(row.region, row.alias): int(row.id) for row in streets},
            "anchors": {
                (
                    int(row.street_id),
                    row.alias,
                    row.anchor_type,
                    row.location_signature,
                ): int(row.id)
                for row in anchors
                if row.street_id is not None
            },
            "issues": {row.alias: int(row.id) for row in issues},
        }

    async def upsert_records(self, values: list[dict[str, Any]]) -> list[int]:
        if not values:
            return []
        async with self.database.engine.begin() as connection:
            existing_map: dict[tuple[int | None, str, int, str], int] = {}
            for generation_id, file_hash in {
                (value.get("generation_id"), str(value["source_file_hash"]))
                for value in values
            }:
                scope = [corpus_records.c.source_file_hash == file_hash]
                if generation_id is None:
                    scope.append(corpus_records.c.generation_id.is_(None))
                else:
                    scope.append(corpus_records.c.generation_id == generation_id)
                rows = (
                    await connection.execute(
                        select(
                            corpus_records.c.id,
                            corpus_records.c.source_row,
                            corpus_records.c.row_hash,
                        ).where(*scope)
                    )
                ).all()
                existing_map.update(
                    {
                        (generation_id, file_hash, int(row.source_row), str(row.row_hash)): int(row.id)
                        for row in rows
                    }
                )

            missing: dict[tuple[int | None, str, int, str], dict[str, Any]] = {}
            for value in values:
                key = (
                    value.get("generation_id"),
                    str(value["source_file_hash"]),
                    int(value["source_row"]),
                    str(value["row_hash"]),
                )
                if key in existing_map or key in missing:
                    continue
                payload = dict(value)
                payload.setdefault("raw_json", {})
                payload.setdefault("occurrence_key", "")
                payload.setdefault("occurrence_identifiers", [])
                payload.setdefault("parser_version", "rules-v1")
                payload.setdefault("phone_is_valid", False)
                payload.setdefault("anchor_resolution_status", "unknown")
                payload.setdefault("issue_resolution_status", "unknown")
                payload.setdefault("committed", False)
                payload.setdefault("created_at", datetime.now(UTC))
                missing[key] = payload
            if missing:
                await connection.execute(insert(corpus_records), list(missing.values()))
                for generation_id, file_hash in {
                    (key[0], key[1]) for key in missing
                }:
                    scope = [corpus_records.c.source_file_hash == file_hash]
                    if generation_id is None:
                        scope.append(corpus_records.c.generation_id.is_(None))
                    else:
                        scope.append(corpus_records.c.generation_id == generation_id)
                    inserted_rows = (
                        await connection.execute(
                            select(
                                corpus_records.c.id,
                                corpus_records.c.source_row,
                                corpus_records.c.row_hash,
                            ).where(*scope)
                        )
                    ).all()
                    existing_map.update(
                        (
                            (generation_id, file_hash, int(row.source_row), str(row.row_hash)),
                            int(row.id),
                        )
                        for row in inserted_rows
                    )

            result_ids = [
                existing_map[
                    (
                        value.get("generation_id"),
                        str(value["source_file_hash"]),
                        int(value["source_row"]),
                        str(value["row_hash"]),
                    )
                ]
                for value in values
            ]
            batch_links = {
                (str(value["source_batch_id"]), record_id)
                for value, record_id in zip(values, result_ids, strict=True)
                if value.get("source_batch_id")
            }
            if batch_links:
                batch_ids = {batch_id for batch_id, _ in batch_links}
                record_ids = {record_id for _, record_id in batch_links}
                existing_links = {
                    (str(row.batch_id), int(row.record_id))
                    for record_chunk in _chunks(record_ids)
                    for row in (
                        await connection.execute(
                            select(
                                batch_records.c.batch_id,
                                batch_records.c.record_id,
                            ).where(
                                batch_records.c.batch_id.in_(batch_ids),
                                batch_records.c.record_id.in_(record_chunk),
                            )
                        )
                    )
                }
                new_links = [
                    {
                        "batch_id": batch_id,
                        "record_id": record_id,
                        "status": "uploaded",
                    }
                    for batch_id, record_id in batch_links - existing_links
                ]
                if new_links:
                    await connection.execute(insert(batch_records), new_links)
        return result_ids

    async def apply_normalization_decisions(
        self,
        values: list[dict[str, Any]],
        *,
        dictionary_version_id: int,
    ) -> None:
        if not values:
            return
        now = datetime.now(UTC)
        street_updates = [
            {
                "_record_id": int(value["record_id"]),
                "_street_id": int(value["street_id"]),
            }
            for value in values
            if value.get("street_id") is not None
        ]
        anchor_updates = [
            {
                "_record_id": int(value["record_id"]),
                "_anchor_id": int(value["anchor_id"]),
                "_status": str(value.get("anchor_status") or "llm"),
            }
            for value in values
            if value.get("anchor_id") is not None
        ]
        issue_updates = [
            {
                "_record_id": int(value["record_id"]),
                "_issue_id": int(value["issue_id"]),
                "_status": str(value.get("issue_status") or "llm"),
                "_final_category": value.get("issue_name"),
            }
            for value in values
            if value.get("issue_id") is not None
        ]
        decision_rows: list[dict[str, Any]] = []
        for value in values:
            record_id = int(value["record_id"])
            if value.get("anchor_attempted"):
                decision_rows.append(
                    {
                        "record_id": record_id,
                        "dimension": "anchor",
                        "raw_value": value.get("anchor_raw"),
                        "canonical_id": value.get("anchor_id"),
                        "method": value.get("anchor_method") or "llm",
                        "score": value.get("anchor_confidence"),
                        "evidence": {"reason": value.get("reason", "")},
                        "dictionary_version_id": dictionary_version_id,
                        "created_at": now,
                    }
                )
            if value.get("issue_attempted"):
                decision_rows.append(
                    {
                        "record_id": record_id,
                        "dimension": "issue",
                        "raw_value": value.get("issue_raw"),
                        "canonical_id": value.get("issue_id"),
                        "method": value.get("issue_method") or "llm",
                        "score": value.get("issue_confidence"),
                        "evidence": {"reason": value.get("reason", "")},
                        "dictionary_version_id": dictionary_version_id,
                        "created_at": now,
                    }
                )
        async with self.database.engine.begin() as connection:
            if street_updates:
                await connection.execute(
                    update(corpus_records)
                    .where(corpus_records.c.id == bindparam("_record_id"))
                    .values(street_id=bindparam("_street_id")),
                    street_updates,
                )
            if anchor_updates:
                await connection.execute(
                    update(corpus_records)
                    .where(corpus_records.c.id == bindparam("_record_id"))
                    .values(
                        anchor_id=bindparam("_anchor_id"),
                        anchor_resolution_status=bindparam("_status"),
                    ),
                    anchor_updates,
                )
            if issue_updates:
                await connection.execute(
                    update(corpus_records)
                    .where(corpus_records.c.id == bindparam("_record_id"))
                    .values(
                        issue_id=bindparam("_issue_id"),
                        issue_resolution_status=bindparam("_status"),
                        final_category=bindparam("_final_category"),
                    ),
                    issue_updates,
                )
            if decision_rows:
                await connection.execute(insert(normalization_decisions), decision_rows)

    async def add_issue_mentions_bulk(
        self, values: list[dict[str, Any]]
    ) -> None:
        if not values:
            return
        async with self.database.engine.begin() as connection:
            record_ids = {int(value["record_id"]) for value in values}
            existing = {
                (int(row.record_id), int(row.segment_no))
                for record_chunk in _chunks(record_ids)
                for row in (
                    await connection.execute(
                        select(
                            issue_mentions.c.record_id,
                            issue_mentions.c.segment_no,
                        ).where(issue_mentions.c.record_id.in_(record_chunk))
                    )
                )
            }
            new_values = []
            seen = set(existing)
            for value in values:
                key = (int(value["record_id"]), int(value["segment_no"]))
                if key in seen:
                    continue
                seen.add(key)
                new_values.append(value)
            if new_values:
                await connection.execute(insert(issue_mentions), new_values)

    async def add_previous_work_order_links(
        self, source_record_id: int, work_order_ids: list[str]
    ) -> None:
        await self.add_previous_work_order_links_bulk(
            [
                {
                    "source_record_id": source_record_id,
                    "work_order_ids": work_order_ids,
                }
            ]
        )

    async def add_previous_work_order_links_bulk(
        self, values: list[dict[str, Any]]
    ) -> None:
        normalized = [
            {
                "source_record_id": int(value["source_record_id"]),
                "work_order_ids": [
                    str(work_order_id).strip()
                    for work_order_id in value.get("work_order_ids", [])
                    if str(work_order_id).strip()
                ],
            }
            for value in values
            if value.get("work_order_ids")
        ]
        if not normalized:
            return
        source_ids = {int(value["source_record_id"]) for value in normalized}
        work_order_ids = {
            work_order_id
            for value in normalized
            for work_order_id in value["work_order_ids"]
        }
        async with self.database.engine.begin() as connection:
            source_generation_rows = (
                await connection.execute(
                    select(corpus_records.c.id, corpus_records.c.generation_id).where(
                        corpus_records.c.id.in_(source_ids)
                    )
                )
            ).all()
            source_generations = {
                int(row.id): row.generation_id for row in source_generation_rows
            }
            target_rows = []
            for work_order_chunk in _chunks(work_order_ids):
                target_rows.extend(
                    (
                        await connection.execute(
                            select(
                                corpus_records.c.work_order_id,
                                corpus_records.c.id,
                                corpus_records.c.generation_id,
                            )
                            .where(corpus_records.c.work_order_id.in_(work_order_chunk))
                            .order_by(
                                corpus_records.c.work_order_id,
                                corpus_records.c.committed.desc(),
                                corpus_records.c.id,
                            )
                        )
                    ).all()
                )
            targets_by_scope: dict[tuple[Any, str], list[int]] = {}
            for work_order_id, target_id, generation_id in target_rows:
                targets_by_scope.setdefault(
                    (generation_id, str(work_order_id)), []
                ).append(int(target_id))
            existing = {
                (
                    int(row.source_record_id),
                    int(row.target_record_id) if row.target_record_id is not None else None,
                    str(row.raw_value),
                )
                for source_chunk in _chunks(source_ids)
                for row in (
                    await connection.execute(
                        select(
                            record_links.c.source_record_id,
                            record_links.c.target_record_id,
                            record_links.c.raw_value,
                        ).where(
                            record_links.c.source_record_id.in_(source_chunk),
                            record_links.c.link_type == "previous_work_order",
                            record_links.c.revoked.is_(False),
                        )
                    )
                )
                if str(row.raw_value) in work_order_ids
            }
            new_links: list[dict[str, Any]] = []
            seen = set(existing)
            now = datetime.now(UTC)
            for value in normalized:
                source_record_id = int(value["source_record_id"])
                generation_id = source_generations.get(source_record_id)
                for work_order_id in value["work_order_ids"]:
                    target_ids: list[int | None] = targets_by_scope.get(
                        (generation_id, work_order_id), [None]
                    )
                    for target_id in target_ids:
                        key = (source_record_id, target_id, work_order_id)
                        if key in seen:
                            continue
                        seen.add(key)
                        new_links.append(
                            {
                                "source_record_id": source_record_id,
                                "target_record_id": target_id,
                                "link_type": "previous_work_order",
                                "raw_value": work_order_id,
                                "score": 1.0 if target_id is not None else 0.0,
                                "evidence": {"context_validated": True},
                                "revoked": False,
                                "created_at": now,
                            }
                        )
            if new_links:
                await connection.execute(insert(record_links), new_links)

    async def assign_linked_records(
        self, batch_id: str, *, data_source: str | None = None
    ) -> set[int]:
        source_records = corpus_records.alias("source_records")
        target_records = corpus_records.alias("target_records")
        target_members = source_records.join(
            record_links,
            record_links.c.source_record_id == source_records.c.id,
        ).join(
            target_records,
            target_records.c.id == record_links.c.target_record_id,
        ).join(
            corpus_event_members,
            corpus_event_members.c.record_id == target_records.c.id,
        ).join(
            batch_records,
            batch_records.c.record_id == source_records.c.id,
        )
        async with self.database.engine.begin() as connection:
            conditions = [
                batch_records.c.batch_id == batch_id,
                target_records.c.generation_id == source_records.c.generation_id,
                record_links.c.revoked.is_(False),
                record_links.c.link_type == "previous_work_order",
            ]
            if data_source is not None:
                conditions.append(source_records.c.data_source == data_source)
            rows = (
                await connection.execute(
                    select(
                        source_records.c.id.label("source_record_id"),
                        corpus_event_members.c.event_id,
                    )
                    .select_from(target_members)
                    .where(*conditions)
                    .distinct()
                )
            ).all()
            candidates: dict[int, set[int]] = {}
            for source_record_id, event_id in rows:
                candidates.setdefault(int(source_record_id), set()).add(int(event_id))
            assignments: dict[int, tuple[int, str]] = {}
            for record_id, event_ids in candidates.items():
                if len(event_ids) != 1:
                    continue
                event_id = next(iter(event_ids))
                assignments[record_id] = (event_id, "previous_work_order")
            await self._replace_event_members(connection, assignments)
        return set(assignments)

    async def assign_strong_signal_records(
        self, batch_id: str, *, data_source: str | None = None
    ) -> set[int]:
        """Assign unresolved records when a scoped title or phone match is unique."""
        batch_join = corpus_records.join(
            batch_records, batch_records.c.record_id == corpus_records.c.id
        )
        async with self.database.engine.begin() as connection:
            source_conditions = [batch_records.c.batch_id == batch_id]
            if data_source is not None:
                source_conditions.append(corpus_records.c.data_source == data_source)
            source_rows = (
                await connection.execute(
                    select(
                        corpus_records.c.id,
                        corpus_records.c.generation_id,
                        corpus_records.c.street_id,
                        corpus_records.c.issue_id,
                        corpus_records.c.road,
                        corpus_records.c.house_no,
                        corpus_records.c.building,
                        corpus_records.c.direction,
                        corpus_records.c.title_normalized,
                        corpus_records.c.phone_exact,
                        corpus_records.c.phone_is_valid,
                        corpus_records.c.occurrence_key,
                    )
                    .select_from(batch_join)
                    .where(*source_conditions)
                )
            ).mappings().all()
            assigned_ids: set[int] = set()
            source_record_ids = [int(row["id"]) for row in source_rows]
            for record_chunk in _chunks(source_record_ids):
                assigned_ids.update(
                    int(value)
                    for value in (
                        await connection.execute(
                            select(corpus_event_members.c.record_id).where(
                                corpus_event_members.c.record_id.in_(record_chunk)
                            )
                        )
                    ).scalars()
                )
            target_rows = (
                await connection.execute(
                    select(
                        corpus_records.c.id,
                        corpus_records.c.generation_id,
                        corpus_records.c.street_id,
                        corpus_records.c.issue_id,
                        corpus_records.c.road,
                        corpus_records.c.house_no,
                        corpus_records.c.building,
                        corpus_records.c.direction,
                        corpus_records.c.title_normalized,
                        corpus_records.c.phone_exact,
                        corpus_records.c.occurrence_key,
                        corpus_event_members.c.event_id,
                    ).select_from(
                        corpus_records.join(
                            corpus_event_members,
                            corpus_event_members.c.record_id == corpus_records.c.id,
                        )
                    )
                )
            ).mappings().all()
            title_index: dict[tuple[Any, ...], set[int]] = {}
            phone_index: dict[tuple[Any, ...], set[int]] = {}
            occurrence_index: dict[tuple[Any, ...], set[int]] = {}
            occurrence_records: dict[tuple[Any, ...], set[int]] = {}
            for target in target_rows:
                scope = (
                    target.get("generation_id"),
                    target.get("street_id"),
                    target.get("issue_id"),
                    target.get("road") or "",
                    target.get("house_no") or "",
                    target.get("building") or "",
                    target.get("direction") or "",
                )
                event_id = int(target["event_id"])
                if target.get("occurrence_key"):
                    occurrence_scope = (
                        target.get("generation_id"),
                        str(target["occurrence_key"]),
                    )
                    occurrence_index.setdefault(occurrence_scope, set()).add(event_id)
                    occurrence_records.setdefault(occurrence_scope, set()).add(
                        int(target["id"])
                    )
                if target.get("title_normalized"):
                    title_index.setdefault(
                        (*scope, str(target["title_normalized"])), set()
                    ).add(event_id)
                if target.get("phone_exact"):
                    phone_index.setdefault(
                        (*scope, str(target["phone_exact"])), set()
                    ).add(event_id)

            assignments: dict[int, tuple[int, str]] = {}
            merged_event_ids: set[int] = set()
            explicit_occurrence_scopes = {
                (row.get("generation_id"), str(row["occurrence_key"]))
                for row in source_rows
                if str(row.get("occurrence_key") or "").startswith(
                    ("order:", "complaint:")
                )
            }
            for occurrence_scope in explicit_occurrence_scopes:
                event_ids = occurrence_index.get(occurrence_scope, set())
                if not event_ids:
                    continue
                canonical_event_id = min(event_ids)
                merged_event_ids.update(event_ids - {canonical_event_id})
                for record_id in occurrence_records.get(occurrence_scope, set()):
                    assignments[record_id] = (canonical_event_id, "occurrence_id")

            for row in source_rows:
                record_id = int(row["id"])
                if record_id in assignments:
                    continue
                if record_id in assigned_ids and not row.get("occurrence_key"):
                    continue
                if row.get("occurrence_key"):
                    event_ids = occurrence_index.get(
                        (row.get("generation_id"), str(row["occurrence_key"])),
                        set(),
                    )
                    if len(event_ids) == 1:
                        assignments[record_id] = (next(iter(event_ids)), "occurrence_id")
                        continue
                if record_id in assigned_ids:
                    continue
                if not row.get("street_id") or not row.get("issue_id"):
                    continue
                if not row.get("house_no") and not row.get("building"):
                    continue
                scope = (
                    row.get("generation_id"),
                    row.get("street_id"),
                    row.get("issue_id"),
                    row.get("road") or "",
                    row.get("house_no") or "",
                    row.get("building") or "",
                    row.get("direction") or "",
                )
                event_ids: set[int] = set()
                if row.get("title_normalized"):
                    event_ids.update(
                        title_index.get((*scope, str(row["title_normalized"])), set())
                    )
                if row.get("phone_is_valid") and row.get("phone_exact"):
                    event_ids.update(
                        phone_index.get((*scope, str(row["phone_exact"])), set())
                    )
                if len(event_ids) == 1:
                    assignments[record_id] = (next(iter(event_ids)), "strong_signal")
            await self._replace_event_members(connection, assignments)
            if merged_event_ids:
                await connection.execute(
                    update(events)
                    .where(events.c.id.in_(merged_event_ids))
                    .values(status="merged", updated_at=datetime.now(UTC))
                )
            return set(assignments).intersection(source_record_ids)

    async def _replace_event_members(
        self,
        connection,
        assignments: dict[int, tuple[int, str]],
    ) -> None:
        if not assignments:
            return
        record_ids = list(assignments)
        assigned_at = datetime.now(UTC)
        for record_chunk in _chunks(record_ids):
            await connection.execute(
                delete(corpus_event_members).where(
                    corpus_event_members.c.record_id.in_(record_chunk)
                )
            )
        await connection.execute(
            insert(corpus_event_members),
            [
                {
                    "event_id": event_id,
                    "record_id": record_id,
                    "assignment_source": source,
                    "assigned_at": assigned_at,
                }
                for record_id, (event_id, source) in assignments.items()
            ],
        )
        event_ids = {event_id for event_id, _ in assignments.values()}
        date_rows = []
        for event_chunk in _chunks(event_ids):
            date_rows.extend(
                (
                    await connection.execute(
                        select(
                            corpus_event_members.c.event_id,
                            func.min(corpus_records.c.received_at),
                            func.max(corpus_records.c.received_at),
                        )
                        .select_from(
                            corpus_event_members.join(
                                corpus_records,
                                corpus_records.c.id == corpus_event_members.c.record_id,
                            )
                        )
                        .where(corpus_event_members.c.event_id.in_(event_chunk))
                        .group_by(corpus_event_members.c.event_id)
                    )
                ).all()
            )
        if date_rows:
            await connection.execute(
                update(events)
                .where(events.c.id == bindparam("_event_id"))
                .values(
                    first_received_at=bindparam("_first_received_at"),
                    last_received_at=bindparam("_last_received_at"),
                    updated_at=bindparam("_updated_at"),
                ),
                [
                    {
                        "_event_id": int(event_id),
                        "_first_received_at": first_received_at,
                        "_last_received_at": last_received_at,
                        "_updated_at": assigned_at,
                    }
                    for event_id, first_received_at, last_received_at in date_rows
                ],
            )

    async def get_or_create_event(
        self,
        *,
        street_id: int,
        anchor_id: int,
        issue_id: int,
        event_key_version: str,
        event_name: str,
        occurrence_key: str = "",
        is_frozen: bool = False,
        generation_id: int | None = None,
    ) -> int:
        conditions = (
            events.c.street_id == street_id,
            events.c.anchor_id == anchor_id,
            events.c.issue_id == issue_id,
            events.c.occurrence_key == occurrence_key,
            events.c.event_key_version == event_key_version,
        )
        if generation_id is not None:
            conditions = (*conditions, events.c.generation_id == generation_id)
        async with self.database.engine.begin() as connection:
            existing = await connection.scalar(select(events.c.id).where(*conditions))
            if existing is not None:
                return int(existing)
            now = datetime.now(UTC)
            result = await connection.execute(
                insert(events).values(
                    street_id=street_id,
                    anchor_id=anchor_id,
                    issue_id=issue_id,
                    occurrence_key=occurrence_key,
                    generation_id=generation_id,
                    event_key_version=event_key_version,
                    event_revision=1,
                    event_name=event_name,
                    name_source="program",
                    status="active",
                    is_frozen=is_frozen,
                    created_at=now,
                    updated_at=now,
                )
            )
            return int(result.inserted_primary_key[0])

    async def bulk_assign_exact_events(
        self,
        rows: list[dict[str, Any]],
        *,
        event_key_version: str,
        frozen: bool,
        generation_id: int | None = None,
    ) -> None:
        if not rows:
            return
        selected_rows = {
            int(row["record_id"]): row
            for row in rows
        }
        async with self.database.engine.begin() as connection:
            existing_rows = (
                await connection.execute(
                    select(events).where(
                        events.c.event_key_version == event_key_version,
                        *(
                            [events.c.generation_id == generation_id]
                            if generation_id is not None
                            else []
                        ),
                    )
                )
            ).mappings().all()
            event_map = {
                self._exact_event_lookup_key(row): int(row["id"])
                for row in existing_rows
            }
            current_dates = {
                int(row["id"]): (row["first_received_at"], row["last_received_at"])
                for row in existing_rows
            }
            missing_events: dict[tuple[Any, ...], dict[str, Any]] = {}
            now = datetime.now(UTC)
            for row in selected_rows.values():
                key = self._exact_event_lookup_key(row)
                if key in event_map or key in missing_events:
                    continue
                missing_events[key] = {
                    "street_id": int(row["street_id"]),
                    "anchor_id": int(row["anchor_id"]),
                    "issue_id": int(row["issue_id"]),
                    "occurrence_key": str(row.get("occurrence_key") or ""),
                    "generation_id": generation_id,
                    "event_key_version": event_key_version,
                    "event_revision": 1,
                    "event_name": row["event_name"],
                    "name_source": "program",
                    "status": "active",
                    "is_frozen": frozen,
                    "created_at": now,
                    "updated_at": now,
                }
            if missing_events:
                inserted_ids = list(
                    (
                        await connection.execute(
                            insert(events).returning(events.c.id),
                            list(missing_events.values()),
                        )
                    ).scalars()
                )
                for key, event_id in zip(missing_events, inserted_ids, strict=True):
                    event_map[key] = int(event_id)
                    current_dates[int(event_id)] = (None, None)

            event_dates: dict[int, list[datetime]] = {}
            member_rows: list[dict[str, Any]] = []
            assigned_at = datetime.now(UTC)
            for row in selected_rows.values():
                key = self._exact_event_lookup_key(row)
                event_id = event_map[key]
                member_rows.append(
                    {
                        "event_id": event_id,
                        "record_id": int(row["record_id"]),
                        "assignment_source": "exact_key",
                        "assigned_at": assigned_at,
                    }
                )
                if row.get("received_at") is not None:
                    event_dates.setdefault(event_id, []).append(row["received_at"])
            record_ids = list(selected_rows)
            for record_chunk in _chunks(record_ids):
                await connection.execute(
                    delete(corpus_event_members).where(
                        corpus_event_members.c.record_id.in_(record_chunk)
                    )
                )
            await connection.execute(insert(corpus_event_members), member_rows)

            date_updates = []
            updated_at = datetime.now(UTC)
            for event_id, dates in event_dates.items():
                current_first, current_last = current_dates[event_id]
                first_values = [value for value in (current_first, min(dates)) if value is not None]
                last_values = [value for value in (current_last, max(dates)) if value is not None]
                date_updates.append(
                    {
                        "_event_id": event_id,
                        "_first_received_at": min(first_values) if first_values else None,
                        "_last_received_at": max(last_values) if last_values else None,
                        "_updated_at": updated_at,
                    }
                )
            if date_updates:
                await connection.execute(
                    update(events)
                    .where(events.c.id == bindparam("_event_id"))
                    .values(
                        first_received_at=bindparam("_first_received_at"),
                        last_received_at=bindparam("_last_received_at"),
                        updated_at=bindparam("_updated_at"),
                    ),
                    date_updates,
                )

    async def bulk_assign_safe_singletons(
        self,
        rows: list[dict[str, Any]],
        *,
        dictionary_version_id: int,
        generation_id: int | None = None,
    ) -> None:
        """Batch-create one independent event for every unresolved record.

        Safe singletons deliberately use a unique event key per record.  This
        keeps unresolved records auditable without allowing two records with
        the same fallback values to merge accidentally.
        """
        selected_rows = {
            int(row["record_id"]): row
            for row in rows
            if row.get("record_id") is not None
        }
        if not selected_rows:
            return

        async with self.database.engine.begin() as connection:
            # Resolve or create streets in one round trip.
            street_rows = (
                await connection.execute(
                    select(canonical_streets).where(
                        canonical_streets.c.dictionary_version_id
                        == dictionary_version_id
                    )
                )
            ).mappings().all()
            street_map = {
                (row["region"], row["canonical_name"]): int(row["id"])
                for row in street_rows
            }
            street_specs: dict[tuple[str | None, str], dict[str, Any]] = {}
            for row in selected_rows.values():
                if row.get("street_id"):
                    continue
                region = row.get("region") or "未知地区"
                street_name = row.get("street_name") or row.get("street_raw") or "未知街道"
                key = (region, str(street_name))
                street_specs.setdefault(
                    key,
                    {
                        "region": region,
                        "canonical_name": str(street_name),
                        "review_status": "candidate",
                        "dictionary_version_id": dictionary_version_id,
                    },
                )
            missing_streets = [
                value for key, value in street_specs.items() if key not in street_map
            ]
            if missing_streets:
                inserted_ids = list(
                    (
                        await connection.execute(
                            insert(canonical_streets).returning(canonical_streets.c.id),
                            missing_streets,
                        )
                    ).scalars()
                )
                for value, street_id in zip(missing_streets, inserted_ids, strict=True):
                    street_map[(value["region"], value["canonical_name"])] = int(street_id)
                await connection.execute(
                    insert(street_aliases),
                    [
                        {
                            "street_id": int(street_id),
                            "alias": value["canonical_name"],
                            "evidence_count": 1,
                            "review_status": "candidate",
                        }
                        for value, street_id in zip(missing_streets, inserted_ids, strict=True)
                    ],
                )

            # Resolve or create anchors.  The location signature is part of
            # the key so same-named landmarks at different positions remain
            # separate singletons.
            anchor_rows = (
                await connection.execute(
                    select(canonical_anchors).where(
                        canonical_anchors.c.dictionary_version_id
                        == dictionary_version_id
                    )
                )
            ).mappings().all()
            anchor_map = {
                (
                    int(row["street_id"]),
                    row["canonical_name"],
                    row["anchor_type"],
                    row["location_signature"],
                ): int(row["id"])
                for row in anchor_rows
                if row["street_id"] is not None
            }
            anchor_specs: dict[tuple[int, str, str, str], dict[str, Any]] = {}
            resolved_street_ids: dict[int, int] = {}
            for record_id, row in selected_rows.items():
                street_id = int(row["street_id"]) if row.get("street_id") else street_map[
                    (row.get("region") or "未知地区", row.get("street_name") or row.get("street_raw") or "未知街道")
                ]
                resolved_street_ids[record_id] = street_id
                if row.get("anchor_id"):
                    continue
                anchor_name = (
                    row.get("anchor_name")
                    or row.get("anchor_raw")
                    or row.get("work_order_id")
                    or row.get("title_normalized")
                    or f"记录{record_id}"
                )
                anchor_type = row.get("anchor_type") or "unknown"
                location_signature = anchor_location_signature(
                    row.get("road"),
                    row.get("house_no"),
                    row.get("building"),
                    row.get("direction"),
                    row.get("shop_no"),
                    row.get("floor"),
                )
                key = (street_id, str(anchor_name), str(anchor_type), location_signature)
                anchor_specs.setdefault(
                    key,
                    {
                        "street_id": street_id,
                        "canonical_name": str(anchor_name),
                        "anchor_type": str(anchor_type),
                        "location_signature": location_signature,
                        "anchor_key_hash": _anchor_key_hash(
                            str(anchor_name), str(anchor_type), location_signature
                        ),
                        "road": row.get("road"),
                        "house_no": row.get("house_no"),
                        "building": row.get("building"),
                        "shop_no": row.get("shop_no"),
                        "floor": row.get("floor"),
                        "direction": row.get("direction"),
                        "review_status": "candidate",
                        "dictionary_version_id": dictionary_version_id,
                    },
                )
            missing_anchors = [
                value for key, value in anchor_specs.items() if key not in anchor_map
            ]
            if missing_anchors:
                inserted_ids = list(
                    (
                        await connection.execute(
                            insert(canonical_anchors).returning(canonical_anchors.c.id),
                            missing_anchors,
                        )
                    ).scalars()
                )
                for value, anchor_id in zip(missing_anchors, inserted_ids, strict=True):
                    key = (
                        int(value["street_id"]),
                        value["canonical_name"],
                        value["anchor_type"],
                        value["location_signature"],
                    )
                    anchor_map[key] = int(anchor_id)
                await connection.execute(
                    insert(anchor_aliases),
                    [
                        {
                            "anchor_id": int(anchor_id),
                            "alias": value["canonical_name"],
                            "alias_key_hash": _alias_key_hash(value["canonical_name"]),
                            "evidence_count": 1,
                            "review_status": "candidate",
                        }
                        for value, anchor_id in zip(missing_anchors, inserted_ids, strict=True)
                    ],
                )

            # Resolve or create issues in one round trip.
            issue_rows = (
                await connection.execute(
                    select(canonical_issues).where(
                        canonical_issues.c.dictionary_version_id
                        == dictionary_version_id
                    )
                )
            ).mappings().all()
            issue_map = {
                str(row["canonical_name"]): int(row["id"])
                for row in issue_rows
            }
            issue_specs: dict[str, dict[str, Any]] = {}
            for row in selected_rows.values():
                if row.get("issue_id"):
                    continue
                issue_name = str(row.get("issue_name") or row.get("final_category") or "未识别事项")
                issue_specs.setdefault(
                    issue_name,
                    {
                        "canonical_name": issue_name,
                        "review_status": "candidate",
                        "dictionary_version_id": dictionary_version_id,
                        "category_level_1": row.get("category_level_1"),
                        "category_level_2": row.get("category_level_2"),
                        "category_level_3": row.get("category_level_3"),
                        "category_level_4": row.get("category_level_4"),
                    },
                )
            missing_issues = [
                value for key, value in issue_specs.items() if key not in issue_map
            ]
            if missing_issues:
                inserted_ids = list(
                    (
                        await connection.execute(
                            insert(canonical_issues).returning(canonical_issues.c.id),
                            missing_issues,
                        )
                    ).scalars()
                )
                for value, issue_id in zip(missing_issues, inserted_ids, strict=True):
                    issue_map[value["canonical_name"]] = int(issue_id)
                await connection.execute(
                    insert(issue_aliases),
                    [
                        {
                            "issue_id": int(issue_id),
                            "alias": value["canonical_name"],
                            "evidence_count": 1,
                            "review_status": "candidate",
                        }
                        for value, issue_id in zip(missing_issues, inserted_ids, strict=True)
                    ],
                )

            # Update all unresolved records in one executemany statement.
            record_updates: list[dict[str, Any]] = []
            event_specs: list[dict[str, Any]] = []
            for record_id, row in selected_rows.items():
                street_id = resolved_street_ids[record_id]
                if row.get("anchor_id"):
                    anchor_id = int(row["anchor_id"])
                else:
                    anchor_name = (
                        row.get("anchor_name")
                        or row.get("anchor_raw")
                        or row.get("work_order_id")
                        or row.get("title_normalized")
                        or f"记录{record_id}"
                    )
                    anchor_type = row.get("anchor_type") or "unknown"
                    location_signature = anchor_location_signature(
                        row.get("road"),
                        row.get("house_no"),
                        row.get("building"),
                        row.get("direction"),
                        row.get("shop_no"),
                        row.get("floor"),
                    )
                    anchor_id = anchor_map[
                        (street_id, str(anchor_name), str(anchor_type), location_signature)
                    ]
                issue_id = (
                    int(row["issue_id"])
                    if row.get("issue_id")
                    else issue_map[str(row.get("issue_name") or row.get("final_category") or "未识别事项")]
                )
                record_updates.append(
                    {
                        "_record_id": record_id,
                        "_street_id": street_id,
                        "_anchor_id": anchor_id,
                        "_issue_id": issue_id,
                        "_anchor_status": "manual_singleton",
                        "_issue_status": "manual_singleton",
                    }
                )
                received_at = row.get("received_at")
                event_specs.append(
                    {
                        "street_id": street_id,
                        "anchor_id": anchor_id,
                        "issue_id": issue_id,
                        "generation_id": generation_id,
                        "event_key_version": f"manual-singleton-{record_id}",
                        "event_revision": 1,
                        "event_name": row.get("event_name") or f"未归类工单-{record_id}",
                        "name_source": "program",
                        "status": "active",
                        "is_frozen": True,
                        "first_received_at": received_at,
                        "last_received_at": received_at,
                        "created_at": datetime.now(UTC),
                        "updated_at": datetime.now(UTC),
                    }
                )
            await connection.execute(
                update(corpus_records)
                .where(corpus_records.c.id == bindparam("_record_id"))
                .values(
                    street_id=bindparam("_street_id"),
                    anchor_id=bindparam("_anchor_id"),
                    issue_id=bindparam("_issue_id"),
                    anchor_resolution_status=bindparam("_anchor_status"),
                    issue_resolution_status=bindparam("_issue_status"),
                ),
                record_updates,
            )

            existing_events = []
            for key_chunk in _chunks(
                [value["event_key_version"] for value in event_specs]
            ):
                existing_events.extend(
                    (
                        await connection.execute(
                            select(events.c.id, events.c.event_key_version).where(
                                events.c.generation_id == generation_id,
                                events.c.event_key_version.in_(key_chunk),
                            )
                        )
                    ).all()
                )
            event_map = {str(key): int(event_id) for event_id, key in existing_events}
            missing_events = [
                value for value in event_specs if value["event_key_version"] not in event_map
            ]
            if missing_events:
                inserted_ids = list(
                    (
                        await connection.execute(
                            insert(events).returning(events.c.id),
                            missing_events,
                        )
                    ).scalars()
                )
                for value, event_id in zip(missing_events, inserted_ids, strict=True):
                    event_map[value["event_key_version"]] = int(event_id)

            assigned_at = datetime.now(UTC)
            for record_chunk in _chunks(selected_rows):
                await connection.execute(
                    delete(corpus_event_members).where(
                        corpus_event_members.c.record_id.in_(record_chunk)
                    )
                )
            await connection.execute(
                insert(corpus_event_members),
                [
                    {
                        "event_id": event_map[value["event_key_version"]],
                        "record_id": record_id,
                        "assignment_source": "manual_singleton",
                        "assigned_at": assigned_at,
                    }
                    for record_id, value in zip(selected_rows, event_specs, strict=True)
                ],
            )

    async def assign_record(self, event_id: int, record_id: int, *, source: str) -> None:
        async with self.database.engine.begin() as connection:
            await connection.execute(
                delete(corpus_event_members).where(
                    corpus_event_members.c.record_id == record_id
                )
            )
            await connection.execute(
                insert(corpus_event_members).values(
                    event_id=event_id,
                    record_id=record_id,
                    assignment_source=source,
                    assigned_at=datetime.now(UTC),
                )
            )
            await self._refresh_event_dates(connection, event_id)

    async def event_member_ids(self, event_id: int) -> list[int]:
        async with self.database.engine.connect() as connection:
            rows = await connection.execute(
                select(corpus_event_members.c.record_id)
                .where(corpus_event_members.c.event_id == event_id)
                .order_by(corpus_event_members.c.record_id)
            )
        return [int(value) for value in rows.scalars()]

    async def list_events(self) -> list[dict[str, Any]]:
        active = await self.active_generation()
        if active is None:
            return []
        async with self.database.engine.connect() as connection:
            query = select(events).where(
                events.c.status == "active",
                events.c.generation_id == int(active["id"]),
                _event_has_members(),
            )
            rows = (await connection.execute(query.order_by(events.c.id))).mappings().all()
        return [dict(row) for row in rows]

    async def count_events(self) -> int:
        active = await self.active_generation()
        if active is None:
            return 0
        async with self.database.engine.connect() as connection:
            value = await connection.scalar(
                select(func.count())
                .select_from(events)
                .where(
                    events.c.status == "active",
                    events.c.generation_id == int(active["id"]),
                    _event_has_members(),
                )
            )
        return int(value or 0)

    async def search_event_options(
        self,
        query: str = "",
        *,
        exclude_event_id: int | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        active = await self.active_generation()
        if active is None:
            return []
        conditions = [
            events.c.status == "active",
            events.c.generation_id == int(active["id"]),
            _event_has_members(),
        ]
        if query.strip():
            conditions.append(events.c.event_name.ilike(f"%{query.strip()}%"))
        if exclude_event_id is not None:
            conditions.append(events.c.id != exclude_event_id)
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(events.c.id, events.c.event_name)
                    .where(*conditions)
                    .order_by(events.c.updated_at.desc(), events.c.id)
                    .limit(limit)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    def _event_filter_member_conditions(
        self,
        filters: EventFilters,
        generation_id: int,
        *,
        alias_prefix: str,
    ):
        member_alias = corpus_event_members.alias(f"{alias_prefix}_members")
        record_alias = corpus_records.alias(f"{alias_prefix}_records")
        scoped = select(member_alias.c.event_id).select_from(
            member_alias.join(
                record_alias,
                record_alias.c.id == member_alias.c.record_id,
            )
        ).where(
            member_alias.c.event_id == events.c.id,
            record_alias.c.generation_id == generation_id,
            record_alias.c.committed.is_(True),
        )
        conditions = []
        if filters.processing_department.strip():
            conditions.append(
                scoped.where(
                    record_alias.c.processing_department
                    == filters.processing_department.strip()
                ).exists()
            )
        if filters.completed_from is not None or filters.completed_to is not None:
            timezone = ZoneInfo("Asia/Shanghai")
            date_conditions = [record_alias.c.completed_at.is_not(None)]
            if filters.completed_from is not None:
                date_conditions.append(
                    record_alias.c.completed_at
                    >= datetime.combine(filters.completed_from, time.min, timezone)
                )
            if filters.completed_to is not None:
                end_date = filters.completed_to + timedelta(days=1)
                date_conditions.append(
                    record_alias.c.completed_at
                    < datetime.combine(end_date, time.min, timezone)
                )
            conditions.append(scoped.where(*date_conditions).exists())
        if filters.missing_completed:
            conditions.append(
                ~scoped.where(record_alias.c.completed_at.is_not(None)).exists()
            )
        return conditions
    async def list_event_summaries(
        self,
        *,
        region: str = "",
        street: str = "",
        event_name: str = "",
        sort: str = "updated_desc",
        has_daily_records: bool = False,
        hide_singletons: bool = False,
        filters: EventFilters | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        active = await self.active_generation()
        if active is None:
            return [], 0
        joins = (
            events.join(canonical_streets, canonical_streets.c.id == events.c.street_id)
            .join(canonical_anchors, canonical_anchors.c.id == events.c.anchor_id)
            .join(canonical_issues, canonical_issues.c.id == events.c.issue_id)
            .join(corpus_event_members, corpus_event_members.c.event_id == events.c.id)
        )
        conditions = [
            events.c.status == "active",
            events.c.generation_id == int(active["id"]),
        ]
        if filters is not None:
            region = filters.region
            street = filters.street
            event_name = filters.event_name
            has_daily_records = filters.has_daily_records
            hide_singletons = filters.hide_singletons
            conditions.extend(
                self._event_filter_member_conditions(
                    filters, int(active["id"]), alias_prefix="event_filters"
                )
            )
        if region.strip():
            conditions.append(canonical_streets.c.region == region.strip())
        if street.strip():
            conditions.append(canonical_streets.c.canonical_name == street.strip())
        if event_name.strip():
            conditions.append(events.c.event_name.ilike(f"%{event_name.strip()}%"))
        if has_daily_records:
            latest_daily_batch_id = await self._latest_daily_batch_id(
                int(active["id"])
            )
            if latest_daily_batch_id is None:
                return [], 0
            daily_members = corpus_event_members.alias("daily_event_members")
            daily_records = corpus_records.alias("daily_event_records")
            conditions.append(
                select(daily_members.c.event_id)
                .select_from(
                    daily_members.join(
                        daily_records,
                        daily_records.c.id == daily_members.c.record_id,
                    )
                )
                .where(
                    daily_members.c.event_id == events.c.id,
                    daily_records.c.generation_id == int(active["id"]),
                    daily_records.c.data_source == "daily",
                    daily_records.c.source_batch_id == latest_daily_batch_id,
                )
                .exists()
            )
        member_count = func.count(corpus_event_members.c.record_id)
        grouped = (
            select(
                events,
                canonical_streets.c.region.label("region"),
                canonical_streets.c.canonical_name.label("street_name"),
                canonical_anchors.c.canonical_name.label("anchor_name"),
                canonical_issues.c.canonical_name.label("issue_name"),
                member_count.label("member_count"),
            )
            .select_from(joins)
            .where(*conditions)
            .group_by(
                events.c.id,
                canonical_streets.c.region,
                canonical_streets.c.canonical_name,
                canonical_anchors.c.canonical_name,
                canonical_issues.c.canonical_name,
            )
        )
        if hide_singletons:
            grouped = grouped.having(member_count > 1)
        order_by = {
            "member_count_desc": (
                member_count.desc(),
                events.c.updated_at.desc(),
                events.c.id,
            ),
            "member_count_asc": (
                member_count.asc(),
                events.c.updated_at.desc(),
                events.c.id,
            ),
        }.get(
            sort,
            (events.c.updated_at.desc(), events.c.id),
        )
        async with self.database.engine.connect() as connection:
            total = int(
                await connection.scalar(
                    select(func.count()).select_from(grouped.subquery())
                )
                or 0
            )
            rows = (
                await connection.execute(
                    grouped.order_by(*order_by)
                    .limit(limit)
                    .offset(offset)
                )
            ).mappings().all()
        return [dict(row) for row in rows], total

    async def _latest_daily_batch_id(self, generation_id: int) -> str | None:
        async with self.database.engine.connect() as connection:
            value = await connection.scalar(
                select(daily_batches.c.id)
                .where(
                    daily_batches.c.generation_id == generation_id,
                    daily_batches.c.batch_type.in_(
                        ("daily_increment", "bootstrap_compare")
                    ),
                    daily_batches.c.status == "committed",
                )
                .order_by(
                    daily_batches.c.committed_at.desc(),
                    daily_batches.c.updated_at.desc(),
                    daily_batches.c.id.desc(),
                )
                .limit(1)
            )
        return str(value) if value is not None else None

    async def count_singleton_events(
        self,
        *,
        region: str = "",
        street: str = "",
        event_name: str = "",
        has_daily_records: bool = False,
        filters: EventFilters | None = None,
    ) -> int:
        active = await self.active_generation()
        if active is None:
            return 0
        joins = (
            events.join(canonical_streets, canonical_streets.c.id == events.c.street_id)
            .join(canonical_anchors, canonical_anchors.c.id == events.c.anchor_id)
            .join(canonical_issues, canonical_issues.c.id == events.c.issue_id)
            .join(corpus_event_members, corpus_event_members.c.event_id == events.c.id)
        )
        conditions = [
            events.c.status == "active",
            events.c.generation_id == int(active["id"]),
        ]
        if filters is not None:
            region = filters.region
            street = filters.street
            event_name = filters.event_name
            has_daily_records = filters.has_daily_records
            hide_singletons = filters.hide_singletons
            conditions.extend(
                self._event_filter_member_conditions(
                    filters, int(active["id"]), alias_prefix="event_filters"
                )
            )
        if region.strip():
            conditions.append(canonical_streets.c.region == region.strip())
        if street.strip():
            conditions.append(canonical_streets.c.canonical_name == street.strip())
        if event_name.strip():
            conditions.append(events.c.event_name.ilike(f"%{event_name.strip()}%"))
        if has_daily_records:
            latest_daily_batch_id = await self._latest_daily_batch_id(
                int(active["id"])
            )
            if latest_daily_batch_id is None:
                return 0
            daily_members = corpus_event_members.alias("singleton_daily_members")
            daily_records = corpus_records.alias("singleton_daily_records")
            conditions.append(
                select(daily_members.c.event_id)
                .select_from(
                    daily_members.join(
                        daily_records,
                        daily_records.c.id == daily_members.c.record_id,
                    )
                )
                .where(
                    daily_members.c.event_id == events.c.id,
                    daily_records.c.generation_id == int(active["id"]),
                    daily_records.c.data_source == "daily",
                    daily_records.c.source_batch_id == latest_daily_batch_id,
                )
                .exists()
            )
        grouped = (
            select(events.c.id)
            .select_from(joins)
            .where(*conditions)
            .group_by(events.c.id)
            .having(func.count(corpus_event_members.c.record_id) == 1)
        )
        async with self.database.engine.connect() as connection:
            value = await connection.scalar(
                select(func.count()).select_from(grouped.subquery())
            )
        return int(value or 0)

    async def event_filter_options(
        self, *, region: str = "", street: str = ""
    ) -> dict[str, list[str]]:
        active = await self.active_generation()
        if active is None:
            return {"regions": [], "streets": [], "processing_departments": []}
        event_join = (
            events.join(canonical_streets, canonical_streets.c.id == events.c.street_id)
            .join(corpus_event_members, corpus_event_members.c.event_id == events.c.id)
        )
        region_conditions = [
            events.c.generation_id == int(active["id"]),
            events.c.status == "active",
        ]
        street_conditions = list(region_conditions)
        if region.strip():
            street_conditions.append(canonical_streets.c.region == region.strip())
        department_members = corpus_event_members.alias("filter_option_members")
        department_records = corpus_records.alias("filter_option_records")
        department_join = (
            department_members.join(
                department_records,
                department_records.c.id == department_members.c.record_id,
            ).join(
                events,
                events.c.id == department_members.c.event_id,
            ).join(
                canonical_streets,
                canonical_streets.c.id == events.c.street_id,
            )
        )
        department_conditions = [
            department_records.c.generation_id == int(active["id"]),
            department_records.c.committed.is_(True),
            department_records.c.processing_department.is_not(None),
            department_records.c.processing_department != "",
        ]
        if region.strip():
            department_conditions.append(canonical_streets.c.region == region.strip())
        if street.strip():
            department_conditions.append(
                canonical_streets.c.canonical_name == street.strip()
            )
        async with self.database.engine.connect() as connection:
            regions = (
                await connection.execute(
                    select(canonical_streets.c.region)
                    .select_from(event_join)
                    .where(
                        *region_conditions,
                        canonical_streets.c.region.is_not(None),
                    )
                    .distinct()
                    .order_by(canonical_streets.c.region)
                )
            ).scalars()
            streets = (
                await connection.execute(
                    select(canonical_streets.c.canonical_name)
                    .select_from(event_join)
                    .where(*street_conditions)
                    .distinct()
                    .order_by(canonical_streets.c.canonical_name)
                )
            ).scalars()
            departments = (
                await connection.execute(
                    select(department_records.c.processing_department)
                    .select_from(department_join)
                    .where(*department_conditions)
                    .distinct()
                    .order_by(department_records.c.processing_department)
                )
            ).scalars()
        return {
            "regions": [str(value) for value in regions if value],
            "streets": [str(value) for value in streets if value],
            "processing_departments": [str(value) for value in departments if value],
        }

    async def get_event(self, event_id: int) -> dict[str, Any]:
        active = await self.active_generation()
        if active is None:
            raise KeyError(event_id)
        async with self.database.engine.connect() as connection:
            conditions = [
                events.c.id == event_id,
                events.c.status == "active",
                events.c.generation_id == int(active["id"]),
                _event_has_members(),
            ]
            row = (await connection.execute(select(events).where(*conditions))).mappings().first()
        if row is None:
            raise KeyError(event_id)
        return dict(row)

    async def event_records(
        self,
        event_id: int,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        active = await self.active_generation()
        if active is None:
            return []
        async with self.database.engine.connect() as connection:
            conditions = [
                corpus_event_members.c.event_id == event_id,
                corpus_records.c.generation_id == int(active["id"]),
            ]
            statement = (
                select(corpus_records)
                .join(
                    corpus_event_members,
                    corpus_event_members.c.record_id == corpus_records.c.id,
                )
                .where(*conditions)
                .order_by(corpus_records.c.received_at, corpus_records.c.id)
                .offset(offset)
            )
            if limit is not None:
                statement = statement.limit(limit)
            rows = (await connection.execute(statement)).mappings().all()
        return [dict(row) for row in rows]

    async def count_event_records(self, event_id: int) -> int:
        active = await self.active_generation()
        if active is None:
            return 0
        async with self.database.engine.connect() as connection:
            value = await connection.scalar(
                select(func.count())
                .select_from(
                    corpus_records.join(
                        corpus_event_members,
                        corpus_event_members.c.record_id == corpus_records.c.id,
                    )
                )
                .where(
                    corpus_event_members.c.event_id == event_id,
                    corpus_records.c.generation_id == int(active["id"]),
                )
            )
        return int(value or 0)

    async def export_business_columns(self) -> list[str]:
        active = await self.active_generation()
        if active is None:
            return []
        async with self.database.engine.connect() as connection:
            source_conditions = [
                corpus_sources.c.source_type == "history",
                corpus_sources.c.generation_id == int(active["id"]),
            ]
            value = await connection.scalar(
                select(corpus_sources.c.business_columns)
                .where(*source_conditions)
                .order_by(corpus_sources.c.id)
                .limit(1)
            )
            if value is None:
                fallback_conditions = [
                    corpus_sources.c.generation_id == int(active["id"])
                ]
                value = await connection.scalar(
                    select(corpus_sources.c.business_columns)
                    .where(*fallback_conditions)
                    .order_by(corpus_sources.c.id)
                    .limit(1)
                )
        return [str(item) for item in (value or []) if str(item).strip() != "Unnamed: 37"]

    async def _matching_event_ids(
        self,
        filters: EventFilters,
        generation_id: int,
    ) -> list[int]:
        conditions = [
            events.c.status == "active",
            events.c.generation_id == generation_id,
            *self._event_filter_member_conditions(
                filters, generation_id, alias_prefix="matching_events"
            ),
        ]
        statement = select(events.c.id).select_from(
            events.join(
                canonical_streets,
                canonical_streets.c.id == events.c.street_id,
            ).join(
                corpus_event_members,
                corpus_event_members.c.event_id == events.c.id,
            )
        )
        hide_singletons = filters.hide_singletons
        if filters.region.strip():
            conditions.append(canonical_streets.c.region == filters.region.strip())
        if filters.street.strip():
            conditions.append(
                canonical_streets.c.canonical_name == filters.street.strip()
            )
        if filters.event_name.strip():
            conditions.append(events.c.event_name.ilike(f"%{filters.event_name.strip()}%"))
        if filters.has_daily_records:
            latest_daily_batch_id = await self._latest_daily_batch_id(generation_id)
            if latest_daily_batch_id is None:
                return []
            daily_members = corpus_event_members.alias("matching_daily_members")
            daily_records = corpus_records.alias("matching_daily_records")
            conditions.append(
                select(daily_members.c.event_id)
                .select_from(
                    daily_members.join(
                        daily_records,
                        daily_records.c.id == daily_members.c.record_id,
                    )
                )
                .where(
                    daily_members.c.event_id == events.c.id,
                    daily_records.c.generation_id == generation_id,
                    daily_records.c.data_source == "daily",
                    daily_records.c.source_batch_id == latest_daily_batch_id,
                )
                .exists()
            )
        if filters.hide_singletons:
            statement = statement.group_by(events.c.id).having(
                func.count(corpus_event_members.c.record_id) > 1
            )
        async with self.database.engine.connect() as connection:
            return [
                int(value)
                for value in (
                    await connection.execute(statement.where(*conditions))
                ).scalars()
            ]
    async def export_rows(
        self, *, filters: EventFilters | None = None
    ) -> list[dict[str, Any]]:
        active = await self.active_generation()
        if active is None:
            return []
        matching_event_ids: list[int] | None = None
        if filters is not None:
            matching_event_ids = await self._matching_event_ids(
                filters, int(active["id"])
            )
        member_count = (
            select(func.count(corpus_event_members.c.record_id))
            .where(corpus_event_members.c.event_id == events.c.id)
            .correlate(events)
            .scalar_subquery()
        )
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(
                        corpus_records.c.id,
                        corpus_records.c.work_order_id,
                        corpus_records.c.received_at,
                        corpus_records.c.data_source,
                        corpus_records.c.raw_json,
                        events.c.id.label("event_id"),
                        events.c.event_name,
                        member_count.label("member_count"),
                    )
                    .select_from(
                        corpus_records.join(
                            corpus_event_members,
                            corpus_event_members.c.record_id == corpus_records.c.id,
                        ).join(events, events.c.id == corpus_event_members.c.event_id)
                    )
                    .where(
                        corpus_records.c.committed.is_(True),
                        events.c.status == "active",
                        corpus_records.c.generation_id == int(active["id"]),
                        *(
                            [corpus_event_members.c.event_id.in_(matching_event_ids)]
                            if matching_event_ids is not None
                            else []
                        ),
                    )
                    .order_by(
                        events.c.event_name,
                        events.c.id,
                        corpus_records.c.received_at,
                        corpus_records.c.id,
                    )
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def mark_generation_records_committed(self, generation_id: int) -> None:
        async with self.database.engine.begin() as connection:
            await connection.execute(
                update(corpus_records)
                .where(corpus_records.c.generation_id == generation_id)
                .values(committed=True)
            )

    async def generation_record_id_map(
        self, source_generation_id: int, target_generation_id: int
    ) -> dict[int, int]:
        async with self.database.engine.connect() as connection:
            source_ids = (
                await connection.execute(
                    select(corpus_records.c.id)
                    .where(corpus_records.c.generation_id == source_generation_id)
                    .order_by(corpus_records.c.id)
                )
            ).scalars().all()
            target_ids = (
                await connection.execute(
                    select(corpus_records.c.id)
                    .where(corpus_records.c.generation_id == target_generation_id)
                    .order_by(corpus_records.c.id)
                )
            ).scalars().all()
        if len(source_ids) != len(target_ids):
            raise ValueError("重建前后工单数量不一致")
        return {
            int(source_id): int(target_id)
            for source_id, target_id in zip(source_ids, target_ids, strict=True)
        }

    async def split_conflicting_events(
        self,
        generation_id: int,
        record_id_map: dict[int, int],
    ) -> int:
        """Isolate rebuilt records that inherit a manual cannot-link conflict."""
        async with self.database.engine.begin() as connection:
            links = (
                await connection.execute(
                    select(
                        cannot_links.c.left_record_id,
                        cannot_links.c.right_record_id,
                    )
                )
            ).all()
            mapped_links = [
                (record_id_map[int(left)], record_id_map[int(right)])
                for left, right in links
                if int(left) in record_id_map and int(right) in record_id_map
            ]
            members = (
                await connection.execute(
                    select(
                        corpus_event_members.c.event_id,
                        corpus_event_members.c.record_id,
                    ).where(corpus_records.c.generation_id == generation_id)
                    .select_from(
                        corpus_event_members.join(
                            corpus_records,
                            corpus_records.c.id == corpus_event_members.c.record_id,
                        )
                    )
                )
            ).all()
            by_event: dict[int, set[int]] = {}
            for event_id, record_id in members:
                by_event.setdefault(int(event_id), set()).add(int(record_id))
            split_count = 0
            now = datetime.now(UTC)
            for event_id, member_ids in by_event.items():
                conflicts: set[int] = set()
                for left, right in mapped_links:
                    if left in member_ids and right in member_ids:
                        conflicts.update((left, right))
                if not conflicts:
                    continue
                source_rows = (
                    await connection.execute(
                        select(corpus_records).where(
                            corpus_records.c.id.in_(conflicts),
                            corpus_records.c.generation_id == generation_id,
                        )
                    )
                ).mappings().all()
                await connection.execute(
                    delete(corpus_event_members).where(
                        corpus_event_members.c.record_id.in_(conflicts)
                    )
                )
                for row in source_rows:
                    result = await connection.execute(
                        insert(events).values(
                            street_id=row["street_id"],
                            anchor_id=row["anchor_id"],
                            issue_id=row["issue_id"],
                            occurrence_key=f"rebuild-singleton-{row['id']}",
                            generation_id=generation_id,
                            event_key_version=f"manual-rebuild-{row['id']}",
                            event_revision=1,
                            event_name=row.get("title_normalized") or row["work_order_id"],
                            name_source="manual",
                            status="active",
                            is_frozen=True,
                            created_at=now,
                            updated_at=now,
                        )
                    )
                    await connection.execute(
                        insert(corpus_event_members).values(
                            event_id=int(result.inserted_primary_key[0]),
                            record_id=int(row["id"]),
                            assignment_source="manual",
                            assigned_at=now,
                        )
                    )
                    split_count += 1
                remaining_count = await connection.scalar(
                    select(func.count())
                    .select_from(corpus_event_members)
                    .where(corpus_event_members.c.event_id == event_id)
                )
                await connection.execute(
                    update(events)
                    .where(events.c.id == event_id)
                    .values(
                        status="archived" if not remaining_count else "active",
                        updated_at=now,
                    )
                )
        return split_count

    async def generation_metrics(self, generation_id: int) -> dict[str, Any]:
        async with self.database.engine.connect() as connection:
            record_count = await connection.scalar(
                select(func.count())
                .select_from(corpus_records)
                .where(
                    corpus_records.c.generation_id == generation_id,
                    corpus_records.c.committed.is_(True),
                )
            ) or 0
            size_rows = (
                await connection.execute(
                    select(events.c.id, func.count(corpus_event_members.c.record_id).label("size"))
                    .select_from(
                        events.join(
                            corpus_event_members,
                            corpus_event_members.c.event_id == events.c.id,
                        )
                    )
                    .where(
                        events.c.generation_id == generation_id,
                        events.c.status == "active",
                    )
                    .group_by(events.c.id)
                )
            ).all()
            enterprise_groups = (
                await connection.scalar(
                    select(func.count())
                    .select_from(
                        select(events.c.id, func.count(corpus_event_members.c.record_id).label("size"))
                        .select_from(
                            events.join(
                                corpus_event_members,
                                corpus_event_members.c.event_id == events.c.id,
                            )
                        )
                        .where(
                            events.c.generation_id == generation_id,
                            events.c.status == "active",
                            events.c.event_key_version == "event-key-v3",
                        )
                        .group_by(events.c.id)
                        .having(func.count(corpus_event_members.c.record_id) > 1)
                        .subquery()
                    )
                )
                or 0
            )
        sizes = [int(row.size) for row in size_rows]
        return {
            "record_count": int(record_count),
            "event_count": len(sizes),
            "singleton_count": sum(size == 1 for size in sizes),
            "max_event_size": max(sizes, default=0),
            "enterprise_merge_groups": int(enterprise_groups),
        }
    async def max_committed_received_at(self) -> datetime | None:
        active = await self.active_generation()
        if active is None:
            return None
        async with self.database.engine.connect() as connection:
            conditions = [
                corpus_records.c.committed.is_(True),
                corpus_records.c.generation_id == int(active["id"]),
            ]
            return await connection.scalar(select(func.max(corpus_records.c.received_at)).where(*conditions))

    async def rename_event(
        self, event_id: int, name: str, *, reviewed_by: str
    ) -> None:
        normalized = name.strip()
        if not normalized:
            raise ValueError("事件名称不能为空")
        async with self.database.engine.begin() as connection:
            row = (
                await connection.execute(select(events).where(events.c.id == event_id))
            ).mappings().first()
            if row is None:
                raise KeyError(event_id)
            revision = int(row["event_revision"]) + 1
            await connection.execute(
                update(events)
                .where(events.c.id == event_id)
                .values(
                    event_name=normalized,
                    name_source="manual",
                    event_revision=revision,
                    updated_at=datetime.now(UTC),
                )
            )
            await self._snapshot_event(
                connection, event_id, revision, normalized, reason="rename"
            )
            await connection.execute(
                insert(corpus_review_actions).values(
                    event_id=event_id,
                    action="rename",
                    details={"old_name": row["event_name"], "new_name": normalized},
                    reviewed_by=reviewed_by,
                    created_at=datetime.now(UTC),
                )
            )

    async def exclude_record(
        self,
        event_id: int,
        record_id: int,
        *,
        target_name: str | None,
        reviewed_by: str,
    ) -> int:
        async with self.database.engine.begin() as connection:
            source_event = (
                await connection.execute(select(events).where(events.c.id == event_id))
            ).mappings().first()
            if source_event is None:
                raise KeyError(event_id)
            generation_id = source_event["generation_id"]
            membership = await connection.scalar(
                select(corpus_event_members.c.record_id).where(
                    corpus_event_members.c.event_id == event_id,
                    corpus_event_members.c.record_id == record_id,
                )
            )
            if membership is None:
                raise KeyError(record_id)
            target = None
            normalized_name = str(target_name or "").strip()
            if normalized_name:
                target = await connection.scalar(
                    select(events.c.id)
                    .where(
                        events.c.event_name == normalized_name,
                        events.c.generation_id == generation_id,
                        events.c.status == "active",
                    )
                    .limit(1)
                )
            if target is None:
                record = (
                    await connection.execute(
                        select(corpus_records.c.work_order_id).where(
                            corpus_records.c.id == record_id
                        )
                    )
                ).first()
                name = normalized_name or f"{source_event['event_name']}｜单例-{record.work_order_id or record_id}"
                now = datetime.now(UTC)
                result = await connection.execute(
                    insert(events).values(
                        street_id=source_event["street_id"],
                        anchor_id=source_event["anchor_id"],
                        issue_id=source_event["issue_id"],
                        generation_id=generation_id,
                        event_key_version=f"manual-{uuid.uuid4().hex}",
                        event_revision=1,
                        event_name=name,
                        name_source="manual",
                        status="active",
                        is_frozen=True,
                        created_at=now,
                        updated_at=now,
                    )
                )
                target = int(result.inserted_primary_key[0])
            target_id = int(target)

            remaining = list(
                (
                    await connection.execute(
                        select(corpus_event_members.c.record_id).where(
                            corpus_event_members.c.event_id == event_id,
                            corpus_event_members.c.record_id != record_id,
                        )
                    )
                ).scalars()
            )
            await connection.execute(
                delete(corpus_event_members).where(
                    corpus_event_members.c.record_id == record_id
                )
            )
            await connection.execute(
                insert(corpus_event_members).values(
                    event_id=target_id,
                    record_id=record_id,
                    assignment_source="manual",
                    assigned_at=datetime.now(UTC),
                )
            )
            remaining_ids = {int(value) for value in remaining}
            existing_cannot_links: set[tuple[int, int]] = set()
            if remaining_ids:
                existing_cannot_links = {
                    (int(left), int(right))
                    for left, right in (
                        await connection.execute(
                            select(
                                cannot_links.c.left_record_id,
                                cannot_links.c.right_record_id,
                            ).where(
                                or_(
                                    and_(
                                        cannot_links.c.left_record_id == record_id,
                                        cannot_links.c.right_record_id.in_(remaining_ids),
                                    ),
                                    and_(
                                        cannot_links.c.right_record_id == record_id,
                                        cannot_links.c.left_record_id.in_(remaining_ids),
                                    ),
                                )
                            )
                        )
                    ).all()
                }
            now = datetime.now(UTC)
            new_cannot_links = []
            for other_id in remaining_ids:
                left, right = sorted((record_id, other_id))
                if (left, right) in existing_cannot_links:
                    continue
                new_cannot_links.append(
                    {
                        "left_record_id": left,
                        "right_record_id": right,
                        "reason": "人工从事件中剔除",
                        "source": "manual",
                        "created_at": now,
                    }
                )
            if new_cannot_links:
                await connection.execute(insert(cannot_links), new_cannot_links)
            revision = int(source_event["event_revision"]) + 1
            await connection.execute(
                update(events)
                .where(events.c.id == event_id)
                .values(
                    event_revision=revision,
                    status="active" if remaining else "archived",
                    updated_at=datetime.now(UTC),
                )
            )
            await self._refresh_event_dates(connection, event_id)
            await self._refresh_event_dates(connection, target_id)
            await self._snapshot_event(
                connection,
                event_id,
                revision,
                str(source_event["event_name"]),
                reason="exclude_member",
            )
            await connection.execute(
                insert(corpus_review_actions).values(
                    event_id=event_id,
                    record_id=record_id,
                    action="exclude",
                    details={"target_event_id": target_id, "target_name": normalized_name},
                    reviewed_by=reviewed_by,
                    created_at=datetime.now(UTC),
                )
            )
        return target_id

    async def list_event_snapshots(self, event_id: int) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(event_snapshots)
                    .where(event_snapshots.c.event_id == event_id)
                    .order_by(event_snapshots.c.event_revision)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def list_review_actions(self) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(corpus_review_actions).order_by(corpus_review_actions.c.id)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def records_for_batch(
        self,
        batch_id: str,
        *,
        data_source: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        conditions = [batch_records.c.batch_id == batch_id]
        if data_source is not None:
            conditions.append(corpus_records.c.data_source == data_source)
        async with self.database.engine.connect() as connection:
            statement = (
                select(corpus_records)
                .join(batch_records, batch_records.c.record_id == corpus_records.c.id)
                .where(*conditions)
                .order_by(corpus_records.c.id)
                .offset(offset)
            )
            if limit is not None:
                statement = statement.limit(limit)
            rows = (await connection.execute(statement)).mappings().all()
        return [dict(row) for row in rows]

    async def records_for_exact_assignment(
        self,
        batch_id: str,
        *,
        data_source: str | None = None,
        approved_only: bool = False,
    ) -> list[dict[str, Any]]:
        joins = corpus_records.join(
            batch_records, batch_records.c.record_id == corpus_records.c.id
        )
        conditions = [
            batch_records.c.batch_id == batch_id,
            corpus_records.c.street_id.is_not(None),
            corpus_records.c.anchor_id.is_not(None),
            corpus_records.c.issue_id.is_not(None),
            corpus_records.c.anchor_resolution_status.in_(
                ("exact", "fuzzy", "llm", "new_standard")
            ),
            corpus_records.c.issue_resolution_status.in_(
                ("exact", "fuzzy", "llm", "new_standard")
            ),
        ]
        if approved_only:
            joins = (
                joins.join(
                    canonical_streets,
                    canonical_streets.c.id == corpus_records.c.street_id,
                )
                .join(
                    canonical_anchors,
                    canonical_anchors.c.id == corpus_records.c.anchor_id,
                )
                .join(
                    canonical_issues,
                    canonical_issues.c.id == corpus_records.c.issue_id,
                )
            )
            conditions.extend(
                (
                    canonical_streets.c.review_status == "approved",
                    canonical_anchors.c.review_status == "approved",
                    canonical_issues.c.review_status == "approved",
                )
            )
        if data_source is not None:
            conditions.append(corpus_records.c.data_source == data_source)
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(
                        corpus_records.c.id,
                        corpus_records.c.region,
                        corpus_records.c.street_raw,
                        corpus_records.c.road,
                        corpus_records.c.house_no,
                        corpus_records.c.building,
                        corpus_records.c.shop_no,
                        corpus_records.c.floor,
                        corpus_records.c.anchor_raw,
                        corpus_records.c.anchor_type,
                        corpus_records.c.final_category,
                        corpus_records.c.received_at,
                        corpus_records.c.street_id,
                        corpus_records.c.anchor_id,
                        corpus_records.c.issue_id,
                        corpus_records.c.occurrence_key,
                    )
                    .select_from(joins)
                    .where(*conditions)
                    .order_by(corpus_records.c.id)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def unassigned_records_for_batch(
        self, batch_id: str, *, data_source: str | None = None
    ) -> list[dict[str, Any]]:
        joins = (
            corpus_records.join(
                batch_records, batch_records.c.record_id == corpus_records.c.id
            ).outerjoin(
                corpus_event_members,
                corpus_event_members.c.record_id == corpus_records.c.id,
            )
        )
        conditions = [
            batch_records.c.batch_id == batch_id,
            corpus_event_members.c.record_id.is_(None),
        ]
        if data_source is not None:
            conditions.append(corpus_records.c.data_source == data_source)
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(
                        corpus_records.c.id,
                        corpus_records.c.generation_id,
                        corpus_records.c.source_batch_id,
                        corpus_records.c.work_order_id,
                        corpus_records.c.received_at,
                        corpus_records.c.title_normalized,
                        corpus_records.c.category_level_1,
                        corpus_records.c.category_level_2,
                        corpus_records.c.category_level_3,
                        corpus_records.c.category_level_4,
                        corpus_records.c.final_category,
                        corpus_records.c.region,
                        corpus_records.c.street_id,
                        corpus_records.c.street_raw,
                        corpus_records.c.road,
                        corpus_records.c.house_no,
                        corpus_records.c.building,
                        corpus_records.c.shop_no,
                        corpus_records.c.floor,
                        corpus_records.c.direction,
                        corpus_records.c.anchor_raw,
                        corpus_records.c.anchor_type,
                        corpus_records.c.anchor_id,
                        corpus_records.c.issue_id,
                        corpus_records.c.occurrence_key,
                    )
                    .select_from(joins)
                    .where(*conditions)
                    .order_by(corpus_records.c.id)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    @staticmethod
    def _exact_event_lookup_key(row: Any) -> tuple[Any, ...]:
        occurrence_key = str(row.get("occurrence_key") or "")
        if occurrence_key.startswith(("order:", "complaint:")):
            return ("identifier", occurrence_key)
        if occurrence_key:
            return (
                "fact",
                int(row["street_id"]),
                int(row["anchor_id"]),
                occurrence_key,
            )
        return (
            "event",
            int(row["street_id"]),
            int(row["anchor_id"]),
            int(row["issue_id"]),
            "",
        )

    async def count_records_for_batch(self, batch_id: str) -> int:
        async with self.database.engine.connect() as connection:
            value = await connection.scalar(
                select(func.count())
                .select_from(batch_records)
                .where(batch_records.c.batch_id == batch_id)
            )
        return int(value or 0)

    async def approved_record_ids_for_batch(
        self, batch_id: str, *, data_source: str | None = None
    ) -> set[int]:
        joins = (
            corpus_records.join(
                batch_records, batch_records.c.record_id == corpus_records.c.id
            )
            .join(canonical_streets, canonical_streets.c.id == corpus_records.c.street_id)
            .join(canonical_anchors, canonical_anchors.c.id == corpus_records.c.anchor_id)
            .join(canonical_issues, canonical_issues.c.id == corpus_records.c.issue_id)
        )
        conditions = [
            batch_records.c.batch_id == batch_id,
            canonical_streets.c.review_status == "approved",
            canonical_anchors.c.review_status == "approved",
            canonical_issues.c.review_status == "approved",
        ]
        if data_source is not None:
            conditions.append(corpus_records.c.data_source == data_source)
        async with self.database.engine.connect() as connection:
            values = (
                await connection.execute(
                    select(corpus_records.c.id)
                    .select_from(joins)
                    .where(*conditions)
                )
            ).scalars()
        return {int(value) for value in values}

    async def assigned_record_ids(self, record_ids: list[int]) -> set[int]:
        if not record_ids:
            return set()
        async with self.database.engine.connect() as connection:
            assigned: set[int] = set()
            for record_chunk in _chunks(record_ids):
                values = (
                    await connection.execute(
                        select(corpus_event_members.c.record_id).where(
                            corpus_event_members.c.record_id.in_(record_chunk)
                        )
                    )
                ).scalars()
                assigned.update(int(value) for value in values)
        return assigned

    async def resolve_street(
        self, region: str | None, alias: str, dictionary_version_id: int
    ) -> int | None:
        async with self.database.engine.connect() as connection:
            value = await connection.scalar(
                select(canonical_streets.c.id)
                .join(street_aliases, street_aliases.c.street_id == canonical_streets.c.id)
                .where(
                    canonical_streets.c.region == region,
                    street_aliases.c.alias == alias,
                    canonical_streets.c.dictionary_version_id == dictionary_version_id,
                    canonical_streets.c.review_status == "approved",
                    street_aliases.c.review_status == "approved",
                )
            )
        return int(value) if value is not None else None

    async def resolve_anchor(
        self,
        street_id: int | None,
        alias: str,
        anchor_type: str,
        dictionary_version_id: int,
    ) -> int | None:
        async with self.database.engine.connect() as connection:
            value = await connection.scalar(
                select(canonical_anchors.c.id)
                .join(anchor_aliases, anchor_aliases.c.anchor_id == canonical_anchors.c.id)
                .where(
                    canonical_anchors.c.street_id == street_id,
                    anchor_aliases.c.alias == alias,
                    canonical_anchors.c.anchor_type == anchor_type,
                    canonical_anchors.c.dictionary_version_id == dictionary_version_id,
                    canonical_anchors.c.review_status == "approved",
                    anchor_aliases.c.review_status == "approved",
                )
            )
        return int(value) if value is not None else None

    async def resolve_issue(self, alias: str, dictionary_version_id: int) -> int | None:
        async with self.database.engine.connect() as connection:
            value = await connection.scalar(
                select(canonical_issues.c.id)
                .join(issue_aliases, issue_aliases.c.issue_id == canonical_issues.c.id)
                .where(
                    issue_aliases.c.alias == alias,
                    canonical_issues.c.dictionary_version_id == dictionary_version_id,
                    canonical_issues.c.review_status == "approved",
                    issue_aliases.c.review_status == "approved",
                )
            )
        return int(value) if value is not None else None

    async def add_issue_mentions(
        self, record_id: int, segments: list[dict[str, Any]]
    ) -> None:
        if not segments:
            return
        async with self.database.engine.begin() as connection:
            for segment in segments:
                await connection.execute(
                    insert(issue_mentions).values(record_id=record_id, **segment)
                )

    async def commit_batch(
        self, batch_id: str, *, committed_at: datetime | None = None
    ) -> None:
        timestamp = committed_at or datetime.now(UTC)
        async with self.database.engine.begin() as connection:
            batch = (
                await connection.execute(
                    select(daily_batches).where(daily_batches.c.id == batch_id)
                )
            ).mappings().first()
            if batch is None:
                raise KeyError(batch_id)

            batch_scope = corpus_records.join(
                batch_records, batch_records.c.record_id == corpus_records.c.id
            )
            has_daily = bool(
                await connection.scalar(
                    select(func.count())
                    .select_from(batch_scope)
                    .where(
                        batch_records.c.batch_id == batch_id,
                        corpus_records.c.data_source == "daily",
                    )
                )
            )
            scope_conditions = [batch_records.c.batch_id == batch_id]
            if has_daily:
                scope_conditions.append(corpus_records.c.data_source == "daily")
            scoped_record_ids = (
                select(corpus_records.c.id)
                .select_from(batch_scope)
                .where(*scope_conditions)
                .subquery("scoped_record_ids")
            )
            scoped_members = (
                select(
                    corpus_event_members.c.event_id,
                    corpus_event_members.c.record_id,
                )
                .join(
                    scoped_record_ids,
                    scoped_record_ids.c.id == corpus_event_members.c.record_id,
                )
                .subquery("scoped_members")
            )
            existing_event_ids = (
                select(corpus_event_members.c.event_id)
                .join(
                    corpus_records,
                    corpus_records.c.id == corpus_event_members.c.record_id,
                )
                .where(
                    corpus_event_members.c.event_id.in_(
                        select(scoped_members.c.event_id)
                    ),
                    corpus_event_members.c.record_id.not_in(
                        select(scoped_record_ids.c.id)
                    ),
                    corpus_records.c.committed.is_(True),
                )
                .distinct()
            )
            matched_records = int(
                await connection.scalar(
                    select(func.count())
                    .select_from(scoped_members)
                    .where(scoped_members.c.event_id.in_(existing_event_ids))
                )
                or 0
            )
            new_events = int(
                await connection.scalar(
                    select(func.count(func.distinct(scoped_members.c.event_id)))
                    .select_from(scoped_members)
                    .where(scoped_members.c.event_id.not_in(existing_event_ids))
                )
                or 0
            )
            scoped_count = int(
                await connection.scalar(
                    select(func.count()).select_from(scoped_record_ids)
                )
                or 0
            )
            assigned_count = int(
                await connection.scalar(
                    select(func.count()).select_from(scoped_members)
                )
                or 0
            )

            generation_id = batch.get("generation_id")
            if generation_id is not None:
                await connection.execute(
                    update(events)
                    .where(
                        events.c.generation_id == int(generation_id),
                        events.c.status == "active",
                        ~_event_has_members(),
                    )
                    .values(status="archived", updated_at=timestamp)
                )
            record_ids = select(batch_records.c.record_id).where(
                batch_records.c.batch_id == batch_id
            )
            await connection.execute(
                update(corpus_records)
                .where(corpus_records.c.id.in_(record_ids))
                .values(committed=True)
            )
            await connection.execute(
                update(batch_records)
                .where(batch_records.c.batch_id == batch_id)
                .values(status="committed")
            )
            result = await connection.execute(
                update(daily_batches)
                .where(daily_batches.c.id == batch_id)
                .values(
                    status="committed",
                    stage="committed",
                    matched_records=matched_records,
                    new_events=new_events,
                    review_records=max(scoped_count - assigned_count, 0),
                    committed_at=timestamp,
                    updated_at=timestamp,
                )
            )

    async def _refresh_event_dates(self, connection: Any, event_id: int) -> None:
        member_records = corpus_event_members.join(
            corpus_records, corpus_records.c.id == corpus_event_members.c.record_id
        )
        values = (
            await connection.execute(
                select(
                    func.min(corpus_records.c.received_at),
                    func.max(corpus_records.c.received_at),
                )
                .select_from(member_records)
                .where(corpus_event_members.c.event_id == event_id)
            )
        ).one()
        await connection.execute(
            update(events)
            .where(events.c.id == event_id)
            .values(first_received_at=values[0], last_received_at=values[1])
        )

    async def _snapshot_event(
        self,
        connection: Any,
        event_id: int,
        revision: int,
        event_name: str,
        *,
        reason: str,
    ) -> None:
        members = list(
            (
                await connection.execute(
                    select(corpus_event_members.c.record_id)
                    .where(corpus_event_members.c.event_id == event_id)
                    .order_by(corpus_event_members.c.record_id)
                )
            ).scalars()
        )
        await connection.execute(
            insert(event_snapshots).values(
                event_id=event_id,
                event_revision=revision,
                event_name=event_name,
                member_ids=[int(value) for value in members],
                reason=reason,
                created_at=datetime.now(UTC),
            )
        )
