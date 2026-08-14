from __future__ import annotations

import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, insert, or_, select, update

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_normalizer import anchor_location_signature
from complaint_dedup.corpus_schema import (
    anchor_aliases,
    batch_records,
    canonical_anchors,
    canonical_issues,
    canonical_streets,
    corpus_event_members,
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
    record_links,
    street_aliases,
)


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


class CorpusRepository:
    def __init__(self, database: AsyncDatabase) -> None:
        self.database = database

    async def create_source(
        self,
        *,
        file_name: str,
        file_hash: str,
        source_type: str,
        business_columns: list[str],
        column_mapping: dict[str, str] | None = None,
        row_count: int = 0,
    ) -> int:
        async with self.database.engine.begin() as connection:
            existing = await connection.scalar(
                select(corpus_sources.c.id).where(corpus_sources.c.file_hash == file_hash)
            )
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
        if batch_type not in {
            "bootstrap_history",
            "bootstrap_compare",
            "daily_increment",
            "correction",
        }:
            raise ValueError("无效的批次类型")
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
    ) -> None:
        async with self.database.engine.begin() as connection:
            result = await connection.execute(
                update(daily_batches)
                .where(daily_batches.c.id == batch_id)
                .values(
                    total_records=total_records,
                    dictionary_version_id=dictionary_version_id,
                    updated_at=datetime.now(UTC),
                )
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
        if action == "commit" and batch["batch_type"] not in {
            "daily_increment",
            "correction",
        }:
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

    async def list_batches(self) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(daily_batches).order_by(daily_batches.c.updated_at.desc())
                )
            ).mappings().all()
        return [dict(row) for row in rows]

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
            result = await connection.execute(
                insert(aliases).values(
                    **{
                        foreign_key.name: item_id,
                        "alias": normalized,
                        "evidence_count": evidence_count,
                        "review_status": "candidate",
                    }
                )
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
            )
        )
        async with self.database.engine.begin() as connection:
            existing = await connection.scalar(
                select(canonical_anchors.c.id).where(
                    canonical_anchors.c.street_id == street_id,
                    canonical_anchors.c.canonical_name == canonical_name,
                    canonical_anchors.c.anchor_type == anchor_type,
                    canonical_anchors.c.location_signature == location_signature,
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
            for (region, name), evidence_count in street_counts.items():
                key = (region, name)
                if key in street_map:
                    await connection.execute(
                        update(street_aliases)
                        .where(
                            street_aliases.c.street_id == street_map[key],
                            street_aliases.c.alias == name,
                        )
                        .values(evidence_count=evidence_count)
                    )
                    continue
                result = await connection.execute(
                    insert(canonical_streets).values(
                        region=region,
                        canonical_name=name,
                        review_status=review_status,
                        dictionary_version_id=dictionary_version_id,
                    )
                )
                street_id = int(result.inserted_primary_key[0])
                street_map[key] = street_id
                await connection.execute(
                    insert(street_aliases).values(
                        street_id=street_id,
                        alias=name,
                        evidence_count=evidence_count,
                        review_status=review_status,
                    )
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
                    result = await connection.execute(
                        insert(canonical_anchors).values(
                            street_id=street_id,
                            canonical_name=item["canonical_name"],
                            anchor_type=item["anchor_type"],
                            location_signature=location_signature,
                            road=item.get("road"),
                            house_no=item.get("house_no"),
                            building=item.get("building"),
                            direction=item.get("direction"),
                            review_status=review_status,
                            dictionary_version_id=dictionary_version_id,
                        )
                    )
                    anchor_map[key] = int(result.inserted_primary_key[0])

            alias_counts = Counter(
                (
                    item["street_key"],
                    item["canonical_name"],
                    item["anchor_type"],
                    item["location_signature"],
                    item["alias"],
                )
                for item in normalized_anchors
            )
            anchor_alias_map: dict[tuple[int, str, str, str], int] = {}
            for raw_key, evidence_count in alias_counts.items():
                street_id = street_map.get(raw_key[0])
                if street_id is None:
                    continue
                canonical_key = (street_id, raw_key[1], raw_key[2], raw_key[3])
                anchor_id = anchor_map[canonical_key]
                alias = str(raw_key[4])
                existing_alias = await connection.scalar(
                    select(anchor_aliases.c.id).where(
                        anchor_aliases.c.anchor_id == anchor_id,
                        anchor_aliases.c.alias == alias,
                    )
                )
                if existing_alias is None:
                    await connection.execute(
                        insert(anchor_aliases).values(
                            anchor_id=anchor_id,
                            alias=alias,
                            evidence_count=evidence_count,
                            review_status=review_status,
                        )
                    )
                else:
                    await connection.execute(
                        update(anchor_aliases)
                        .where(anchor_aliases.c.id == existing_alias)
                        .values(evidence_count=evidence_count)
                    )
                anchor_alias_map[(street_id, alias, raw_key[2], raw_key[3])] = anchor_id

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
            for name, evidence_count in issue_counts.items():
                item = issue_examples[name]
                if name in issue_map:
                    await connection.execute(
                        update(issue_aliases)
                        .where(
                            issue_aliases.c.issue_id == issue_map[name],
                            issue_aliases.c.alias == name,
                        )
                        .values(evidence_count=evidence_count)
                    )
                    continue
                result = await connection.execute(
                    insert(canonical_issues).values(
                        canonical_name=name,
                        category_level_1=item.get("category_level_1"),
                        category_level_2=item.get("category_level_2"),
                        category_level_3=item.get("category_level_3"),
                        category_level_4=item.get("category_level_4"),
                        review_status=review_status,
                        dictionary_version_id=dictionary_version_id,
                    )
                )
                issue_id = int(result.inserted_primary_key[0])
                issue_map[name] = issue_id
                await connection.execute(
                    insert(issue_aliases).values(
                        issue_id=issue_id,
                        alias=name,
                        evidence_count=evidence_count,
                        review_status=review_status,
                    )
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
        result_ids: list[int] = []
        async with self.database.engine.begin() as connection:
            for value in values:
                existing = await connection.scalar(
                    select(corpus_records.c.id).where(
                        corpus_records.c.source_file_hash == value["source_file_hash"],
                        corpus_records.c.source_row == value["source_row"],
                        corpus_records.c.row_hash == value["row_hash"],
                    )
                )
                if existing is None:
                    payload = dict(value)
                    payload.setdefault("raw_json", {})
                    payload.setdefault("parser_version", "rules-v1")
                    payload.setdefault("phone_is_valid", False)
                    payload.setdefault("anchor_resolution_status", "unknown")
                    payload.setdefault("issue_resolution_status", "unknown")
                    payload.setdefault("committed", False)
                    payload.setdefault("created_at", datetime.now(UTC))
                    cursor = await connection.execute(insert(corpus_records).values(**payload))
                    record_id = int(cursor.inserted_primary_key[0])
                else:
                    record_id = int(existing)
                result_ids.append(record_id)
                batch_id = value.get("source_batch_id")
                if batch_id:
                    linked = await connection.scalar(
                        select(batch_records.c.record_id).where(
                            batch_records.c.batch_id == batch_id,
                            batch_records.c.record_id == record_id,
                        )
                    )
                    if linked is None:
                        await connection.execute(
                            insert(batch_records).values(
                                batch_id=batch_id,
                                record_id=record_id,
                                status="uploaded",
                            )
                        )
        return result_ids

    async def add_issue_mentions_bulk(
        self, values: list[dict[str, Any]]
    ) -> None:
        if not values:
            return
        async with self.database.engine.begin() as connection:
            for value in values:
                existing = await connection.scalar(
                    select(issue_mentions.c.id).where(
                        issue_mentions.c.record_id == value["record_id"],
                        issue_mentions.c.segment_no == value["segment_no"],
                    )
                )
                if existing is None:
                    await connection.execute(insert(issue_mentions).values(**value))

    async def add_previous_work_order_links(
        self, source_record_id: int, work_order_ids: list[str]
    ) -> None:
        if not work_order_ids:
            return
        async with self.database.engine.begin() as connection:
            for work_order_id in work_order_ids:
                target_ids = list(
                    (
                        await connection.execute(
                            select(corpus_records.c.id)
                            .where(corpus_records.c.work_order_id == work_order_id)
                            .order_by(corpus_records.c.committed.desc(), corpus_records.c.id)
                        )
                    ).scalars()
                )
                if not target_ids:
                    target_ids = [None]
                for target_id in target_ids:
                    existing = await connection.scalar(
                        select(record_links.c.id).where(
                            record_links.c.source_record_id == source_record_id,
                            record_links.c.target_record_id == target_id,
                            record_links.c.raw_value == work_order_id,
                            record_links.c.revoked.is_(False),
                        )
                    )
                    if existing is None:
                        await connection.execute(
                            insert(record_links).values(
                                source_record_id=source_record_id,
                                target_record_id=target_id,
                                link_type="previous_work_order",
                                raw_value=work_order_id,
                                score=1.0 if target_id is not None else 0.0,
                                evidence={"context_validated": True},
                                revoked=False,
                                created_at=datetime.now(UTC),
                            )
                        )

    async def assign_linked_records(self, batch_id: str) -> set[int]:
        target_members = record_links.join(
            corpus_event_members,
            corpus_event_members.c.record_id == record_links.c.target_record_id,
        ).join(
            batch_records,
            batch_records.c.record_id == record_links.c.source_record_id,
        )
        async with self.database.engine.begin() as connection:
            rows = (
                await connection.execute(
                    select(
                        record_links.c.source_record_id,
                        corpus_event_members.c.event_id,
                    )
                    .select_from(target_members)
                    .where(
                        batch_records.c.batch_id == batch_id,
                        record_links.c.revoked.is_(False),
                        record_links.c.link_type == "previous_work_order",
                    )
                    .distinct()
                )
            ).all()
            candidates: dict[int, set[int]] = {}
            for source_record_id, event_id in rows:
                candidates.setdefault(int(source_record_id), set()).add(int(event_id))
            assigned: set[int] = set()
            for record_id, event_ids in candidates.items():
                if len(event_ids) != 1:
                    continue
                event_id = next(iter(event_ids))
                await connection.execute(
                    delete(corpus_event_members).where(
                        corpus_event_members.c.record_id == record_id
                    )
                )
                await connection.execute(
                    insert(corpus_event_members).values(
                        event_id=event_id,
                        record_id=record_id,
                        assignment_source="previous_work_order",
                        assigned_at=datetime.now(UTC),
                    )
                )
                await self._refresh_event_dates(connection, event_id)
                assigned.add(record_id)
        return assigned

    async def get_or_create_event(
        self,
        *,
        street_id: int,
        anchor_id: int,
        issue_id: int,
        event_key_version: str,
        event_name: str,
        is_frozen: bool = False,
    ) -> int:
        conditions = (
            events.c.street_id == street_id,
            events.c.anchor_id == anchor_id,
            events.c.issue_id == issue_id,
            events.c.event_key_version == event_key_version,
        )
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
    ) -> None:
        if not rows:
            return
        async with self.database.engine.begin() as connection:
            existing_rows = (
                await connection.execute(
                    select(events).where(events.c.event_key_version == event_key_version)
                )
            ).mappings().all()
            event_map = {
                (int(row["street_id"]), int(row["anchor_id"]), int(row["issue_id"])): int(row["id"])
                for row in existing_rows
            }
            event_dates: dict[int, list[datetime]] = {}
            for row in rows:
                key = (
                    int(row["street_id"]),
                    int(row["anchor_id"]),
                    int(row["issue_id"]),
                )
                event_id = event_map.get(key)
                if event_id is None:
                    now = datetime.now(UTC)
                    cursor = await connection.execute(
                        insert(events).values(
                            street_id=key[0],
                            anchor_id=key[1],
                            issue_id=key[2],
                            event_key_version=event_key_version,
                            event_revision=1,
                            event_name=row["event_name"],
                            name_source="program",
                            status="active",
                            is_frozen=frozen,
                            created_at=now,
                            updated_at=now,
                        )
                    )
                    event_id = int(cursor.inserted_primary_key[0])
                    event_map[key] = event_id
                await connection.execute(
                    delete(corpus_event_members).where(
                        corpus_event_members.c.record_id == int(row["record_id"])
                    )
                )
                await connection.execute(
                    insert(corpus_event_members).values(
                        event_id=event_id,
                        record_id=int(row["record_id"]),
                        assignment_source="exact_key",
                        assigned_at=datetime.now(UTC),
                    )
                )
                if row.get("received_at") is not None:
                    event_dates.setdefault(event_id, []).append(row["received_at"])
            for event_id, dates in event_dates.items():
                current = (
                    await connection.execute(
                        select(events.c.first_received_at, events.c.last_received_at).where(
                            events.c.id == event_id
                        )
                    )
                ).one()
                first_values = [value for value in (current[0], min(dates)) if value is not None]
                last_values = [value for value in (current[1], max(dates)) if value is not None]
                await connection.execute(
                    update(events)
                    .where(events.c.id == event_id)
                    .values(
                        first_received_at=min(first_values) if first_values else None,
                        last_received_at=max(last_values) if last_values else None,
                        updated_at=datetime.now(UTC),
                    )
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
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(select(events).order_by(events.c.id))
            ).mappings().all()
        return [dict(row) for row in rows]

    async def list_event_summaries(
        self,
        *,
        region: str = "",
        street: str = "",
        issue: str = "",
        event_name: str = "",
        limit: int = 10,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        joins = (
            events.join(canonical_streets, canonical_streets.c.id == events.c.street_id)
            .join(canonical_anchors, canonical_anchors.c.id == events.c.anchor_id)
            .join(canonical_issues, canonical_issues.c.id == events.c.issue_id)
            .outerjoin(corpus_event_members, corpus_event_members.c.event_id == events.c.id)
        )
        conditions = []
        if region.strip():
            conditions.append(canonical_streets.c.region == region.strip())
        if street.strip():
            conditions.append(canonical_streets.c.canonical_name == street.strip())
        if issue.strip():
            conditions.append(canonical_issues.c.canonical_name == issue.strip())
        if event_name.strip():
            conditions.append(events.c.event_name.ilike(f"%{event_name.strip()}%"))
        grouped = (
            select(
                events,
                canonical_streets.c.region.label("region"),
                canonical_streets.c.canonical_name.label("street_name"),
                canonical_anchors.c.canonical_name.label("anchor_name"),
                canonical_issues.c.canonical_name.label("issue_name"),
                func.count(corpus_event_members.c.record_id).label("member_count"),
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
        async with self.database.engine.connect() as connection:
            total = int(
                await connection.scalar(
                    select(func.count()).select_from(grouped.subquery())
                )
                or 0
            )
            rows = (
                await connection.execute(
                    grouped.order_by(events.c.updated_at.desc(), events.c.id)
                    .limit(limit)
                    .offset(offset)
                )
            ).mappings().all()
        return [dict(row) for row in rows], total

    async def event_filter_options(
        self, *, region: str = "", street: str = ""
    ) -> dict[str, list[str]]:
        event_join = (
            events.join(canonical_streets, canonical_streets.c.id == events.c.street_id)
            .join(canonical_issues, canonical_issues.c.id == events.c.issue_id)
        )
        street_conditions = []
        if region.strip():
            street_conditions.append(canonical_streets.c.region == region.strip())
        issue_conditions = list(street_conditions)
        if street.strip():
            issue_conditions.append(
                canonical_streets.c.canonical_name == street.strip()
            )
        async with self.database.engine.connect() as connection:
            regions = (
                await connection.execute(
                    select(canonical_streets.c.region)
                    .select_from(event_join)
                    .where(canonical_streets.c.region.is_not(None))
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
            issues = (
                await connection.execute(
                    select(canonical_issues.c.canonical_name)
                    .select_from(event_join)
                    .where(*issue_conditions)
                    .distinct()
                    .order_by(canonical_issues.c.canonical_name)
                )
            ).scalars()
        return {
            "regions": [str(value) for value in regions if value],
            "streets": [str(value) for value in streets if value],
            "issues": [str(value) for value in issues if value],
        }

    async def get_event(self, event_id: int) -> dict[str, Any]:
        async with self.database.engine.connect() as connection:
            row = (
                await connection.execute(select(events).where(events.c.id == event_id))
            ).mappings().first()
        if row is None:
            raise KeyError(event_id)
        return dict(row)

    async def event_records(self, event_id: int) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(corpus_records)
                    .join(
                        corpus_event_members,
                        corpus_event_members.c.record_id == corpus_records.c.id,
                    )
                    .where(corpus_event_members.c.event_id == event_id)
                    .order_by(corpus_records.c.received_at, corpus_records.c.id)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def export_business_columns(self) -> list[str]:
        async with self.database.engine.connect() as connection:
            value = await connection.scalar(
                select(corpus_sources.c.business_columns)
                .where(corpus_sources.c.source_type == "history")
                .order_by(corpus_sources.c.id)
                .limit(1)
            )
            if value is None:
                value = await connection.scalar(
                    select(corpus_sources.c.business_columns)
                    .order_by(corpus_sources.c.id)
                    .limit(1)
                )
        return [str(item) for item in (value or []) if str(item) != "Unnamed: 37"]

    async def export_rows(self) -> list[dict[str, Any]]:
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
                        corpus_records.c.received_at,
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
                    .where(corpus_records.c.committed.is_(True))
                    .order_by(events.c.event_name, corpus_records.c.received_at, corpus_records.c.id)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def max_committed_received_at(self) -> datetime | None:
        async with self.database.engine.connect() as connection:
            return await connection.scalar(
                select(func.max(corpus_records.c.received_at)).where(
                    corpus_records.c.committed.is_(True)
                )
            )

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
                    select(events.c.id).where(events.c.event_name == normalized_name).limit(1)
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
            for other_id in remaining:
                left, right = sorted((record_id, int(other_id)))
                exists = await connection.scalar(
                    select(cannot_links.c.id).where(
                        cannot_links.c.left_record_id == left,
                        cannot_links.c.right_record_id == right,
                    )
                )
                if exists is None:
                    await connection.execute(
                        insert(cannot_links).values(
                            left_record_id=left,
                            right_record_id=right,
                            reason="人工从事件中剔除",
                            source="manual",
                            created_at=datetime.now(UTC),
                        )
                    )
            revision = int(source_event["event_revision"]) + 1
            await connection.execute(
                update(events)
                .where(events.c.id == event_id)
                .values(event_revision=revision, updated_at=datetime.now(UTC))
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

    async def records_for_batch(self, batch_id: str) -> list[dict[str, Any]]:
        async with self.database.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(corpus_records)
                    .join(batch_records, batch_records.c.record_id == corpus_records.c.id)
                    .where(batch_records.c.batch_id == batch_id)
                    .order_by(corpus_records.c.id)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def approved_record_ids_for_batch(self, batch_id: str) -> set[int]:
        joins = (
            corpus_records.join(
                batch_records, batch_records.c.record_id == corpus_records.c.id
            )
            .join(canonical_streets, canonical_streets.c.id == corpus_records.c.street_id)
            .join(canonical_anchors, canonical_anchors.c.id == corpus_records.c.anchor_id)
            .join(canonical_issues, canonical_issues.c.id == corpus_records.c.issue_id)
        )
        async with self.database.engine.connect() as connection:
            values = (
                await connection.execute(
                    select(corpus_records.c.id)
                    .select_from(joins)
                    .where(
                        batch_records.c.batch_id == batch_id,
                        canonical_streets.c.review_status == "approved",
                        canonical_anchors.c.review_status == "approved",
                        canonical_issues.c.review_status == "approved",
                    )
                )
            ).scalars()
        return {int(value) for value in values}

    async def assigned_record_ids(self, record_ids: list[int]) -> set[int]:
        if not record_ids:
            return set()
        async with self.database.engine.connect() as connection:
            values = (
                await connection.execute(
                    select(corpus_event_members.c.record_id).where(
                        corpus_event_members.c.record_id.in_(record_ids)
                    )
                )
            ).scalars()
        return {int(value) for value in values}

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
                    committed_at=timestamp,
                    updated_at=timestamp,
                )
            )
        if not result.rowcount:
            raise KeyError(batch_id)

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
