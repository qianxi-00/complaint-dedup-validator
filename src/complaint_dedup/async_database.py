from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    cast,
    case,
    func,
    delete,
    select,
    update,
)
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert


metadata = MetaData()

jobs = Table(
    "jobs",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("name", String(255), nullable=False, default=""),
    Column("mode", String(16), nullable=False, default="cross"),
    Column("pipeline_version", String(32), nullable=False, default="event_cluster_v2"),
    Column("match_preset", String(16), nullable=False, default="balanced"),
    Column("time_window_days", Integer, nullable=False, default=0),
    Column("status", String(32), nullable=False, default="queued"),
    Column("stage", String(32), nullable=False, default="queued"),
    Column("total_records", Integer, nullable=False, default=0),
    Column("extracted_records", Integer, nullable=False, default=0),
    Column("candidate_count", Integer, nullable=False, default=0),
    Column("judged_count", Integer, nullable=False, default=0),
    Column("extraction_failure_count", Integer, nullable=False, default=0),
    Column("judgement_failure_count", Integer, nullable=False, default=0),
    Column("retry_count", Integer, nullable=False, default=0),
    Column("inflight_batches", Integer, nullable=False, default=0),
    Column("pause_requested", Boolean, nullable=False, default=False),
    Column("lease_owner", String(128)),
    Column("lease_expires_at", DateTime(timezone=True)),
    Column("heartbeat_at", DateTime(timezone=True)),
    Column("error_message", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

records = Table(
    "records",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("job_id", String(64), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
    Column("source", String(1), nullable=False),
    Column("source_row", Integer, nullable=False),
    Column("work_order_id", String(255)),
    Column("received_at", String(64)),
    Column("title", Text),
    Column("category", Text),
    Column("category_level_1", Text),
    Column("category_level_2", Text),
    Column("category_level_3", Text),
    Column("category_level_4", Text),
    Column("normalized_title", Text),
    Column("event_signature", Text),
    Column("normalized_location", Text),
    Column("appeal_text", Text),
    Column("region", String(255)),
    Column("street", String(255)),
    Column("phone_hash", String(64)),
    Column("raw_json", JSON, nullable=False, default=dict),
    Column("extraction_status", String(32), nullable=False, default="pending"),
    Column("extraction_json", JSON),
    Column("embedding_status", String(32), nullable=False, default="pending"),
    UniqueConstraint("job_id", "source", "source_row", name="uq_records_source_row"),
)

candidate_pairs = Table(
    "candidate_pairs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("job_id", String(64), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
    Column("record_a_id", Integer, ForeignKey("records.id", ondelete="CASCADE"), nullable=False),
    Column("record_b_id", Integer, ForeignKey("records.id", ondelete="CASCADE"), nullable=False),
    Column("recall_reasons", JSON, nullable=False, default=list),
    Column("vector_score", Float),
    Column("rerank_score", Float),
    Column("rule_status", String(32), nullable=False, default="candidate"),
    Column("judgement_status", String(32), nullable=False, default="pending"),
    Column("llm_decision", String(32)),
    Column("confidence", Float),
    Column("evidence_json", JSON),
    Column("hard_conflicts_json", JSON),
    Column("event_name_suggestion", Text),
    UniqueConstraint("job_id", "record_a_id", "record_b_id", name="uq_candidate_pair"),
)

reviews = Table(
    "reviews",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("candidate_pair_id", Integer, ForeignKey("candidate_pairs.id", ondelete="CASCADE"), nullable=False, unique=True),
    Column("decision", String(32), nullable=False),
    Column("note", Text),
    Column("reviewed_at", DateTime(timezone=True), nullable=False),
)

event_groups = Table(
    "event_groups",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("job_id", String(64), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
    Column("name", Text, nullable=False),
    Column("region", String(255)),
    Column("first_received_at", String(64)),
    Column("last_received_at", String(64)),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

event_members = Table(
    "event_members",
    metadata,
    Column("event_group_id", Integer, ForeignKey("event_groups.id", ondelete="CASCADE"), primary_key=True),
    Column("record_id", Integer, ForeignKey("records.id", ondelete="CASCADE"), primary_key=True),
)

candidate_events = Table(
    "candidate_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("job_id", String(64), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
    Column("name", Text, nullable=False),
    Column("status", String(32), nullable=False, default="review"),
    Column("confidence", Float),
    Column("evidence_json", JSON),
    Column("region", String(255)),
    Column("street", String(255)),
    Column("category_level_1", Text),
    Column("category_level_2", Text),
    Column("category_level_3", Text),
    Column("category_level_4", Text),
    Column("final_event_group_id", Integer, ForeignKey("event_groups.id", ondelete="SET NULL")),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

candidate_event_members = Table(
    "candidate_event_members",
    metadata,
    Column("candidate_event_id", Integer, ForeignKey("candidate_events.id", ondelete="CASCADE"), primary_key=True),
    Column("record_id", Integer, ForeignKey("records.id", ondelete="CASCADE"), primary_key=True, unique=True),
    Column("confidence", Float),
    Column("role", String(32), nullable=False, default="member"),
    Column("assignment_source", String(32), nullable=False, default="model"),
)

event_review_actions = Table(
    "event_review_actions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("job_id", String(64), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
    Column("candidate_event_id", Integer, ForeignKey("candidate_events.id", ondelete="SET NULL")),
    Column("action", String(32), nullable=False),
    Column("note", Text),
    Column("details_json", JSON),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

job_batches = Table(
    "job_batches",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("job_id", String(64), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
    Column("stage", String(32), nullable=False),
    Column("batch_index", Integer, nullable=False),
    Column("status", String(32), nullable=False, default="pending"),
    Column("attempts", Integer, nullable=False, default=0),
    Column("item_ids", JSON, nullable=False, default=list),
    Column("error_message", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("job_id", "stage", "batch_index", name="uq_job_stage_batch"),
)


class AsyncDatabase:
    def __init__(
        self,
        url: str,
        *,
        pool_size: int = 10,
        max_overflow: int = 10,
    ) -> None:
        options: dict[str, Any] = {"pool_pre_ping": True}
        if not url.startswith("sqlite"):
            options.update(pool_size=pool_size, max_overflow=max_overflow)
        self.engine: AsyncEngine = create_async_engine(url, **options)

    async def initialize(self) -> None:
        from complaint_dedup import corpus_schema  # noqa: F401

        async with self.engine.begin() as connection:
            await connection.run_sync(metadata.create_all)

    async def close(self) -> None:
        await self.engine.dispose()

    async def enqueue_job(
        self,
        job_id: str,
        name: str,
        *,
        mode: Literal["single", "cross"],
        total_records: int,
        match_preset: Literal["strict", "balanced", "loose"] = "balanced",
        time_window_days: int = 0,
        pipeline_version: Literal["pair_v1", "event_cluster_v2"] = "event_cluster_v2",
    ) -> None:
        if pipeline_version not in {"pair_v1", "event_cluster_v2"}:
            raise ValueError("无效的流水线版本")
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            await connection.execute(
                jobs.insert().values(
                    id=job_id,
                    name=name,
                    mode=mode,
                    pipeline_version=pipeline_version,
                    match_preset=match_preset,
                    time_window_days=time_window_days,
                    status="queued",
                    stage="queued",
                    total_records=total_records,
                    pause_requested=False,
                    created_at=now,
                    updated_at=now,
                )
            )

    async def get_job(self, job_id: str) -> dict[str, Any]:
        async with self.engine.connect() as connection:
            row = (await connection.execute(select(jobs).where(jobs.c.id == job_id))).mappings().first()
        if row is None:
            raise KeyError(job_id)
        return dict(row)

    async def list_jobs(self) -> list[dict[str, Any]]:
        async with self.engine.connect() as connection:
            rows = (
                await connection.execute(select(jobs).order_by(jobs.c.created_at.desc()))
            ).mappings().all()
        return [dict(row) for row in rows]

    async def add_records(self, job_id: str, values: list[dict[str, Any]]) -> list[int]:
        record_ids: list[int] = []
        async with self.engine.begin() as connection:
            for value in values:
                source = str(value.get("source", ""))
                if source not in {"S", "A", "B"}:
                    raise ValueError("记录来源必须是 S、A 或 B")
                result = await connection.execute(
                    records.insert().values(
                        job_id=job_id,
                        source=source,
                        source_row=int(value["source_row"]),
                        work_order_id=value.get("work_order_id"),
                        received_at=value.get("received_at"),
                        title=value.get("title"),
                        category=value.get("category"),
                        category_level_1=value.get("category_level_1"),
                        category_level_2=value.get("category_level_2"),
                        category_level_3=value.get("category_level_3"),
                        category_level_4=value.get("category_level_4"),
                        normalized_title=value.get("normalized_title"),
                        event_signature=value.get("event_signature"),
                        normalized_location=value.get("normalized_location"),
                        appeal_text=value.get("appeal_text"),
                        region=value.get("region"),
                        street=value.get("street"),
                        phone_hash=value.get("phone_hash"),
                        raw_json=value.get("raw_json") or {},
                    )
                )
                record_ids.append(int(result.inserted_primary_key[0]))
        return record_ids

    async def list_records(self, job_id: str) -> list[dict[str, Any]]:
        async with self.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(records).where(records.c.job_id == job_id).order_by(records.c.id)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def upsert_candidate_pairs(self, job_id: str, values: list[dict[str, Any]]) -> None:
        if not values:
            return
        async with self.engine.begin() as connection:
            job = (
                await connection.execute(select(jobs.c.mode).where(jobs.c.id == job_id))
            ).scalar_one_or_none()
            if job is None:
                raise KeyError(job_id)
            record_ids = {
                int(value[key])
                for value in values
                for key in ("record_a_id", "record_b_id")
            }
            source_rows = (
                await connection.execute(
                    select(records.c.id, records.c.source).where(
                        records.c.job_id == job_id, records.c.id.in_(record_ids)
                    )
                )
            ).all()
            sources = {int(row.id): row.source for row in source_rows}
            if len(sources) != len(record_ids):
                raise ValueError("候选对包含不属于当前任务的记录")
            for value in values:
                left = int(value["record_a_id"])
                right = int(value["record_b_id"])
                if left == right:
                    raise ValueError("候选记录不能与自身配对")
                if job == "single":
                    if sources[left] != "S" or sources[right] != "S":
                        raise ValueError("单文件任务只能使用 S 来源记录")
                    left, right = sorted((left, right))
                else:
                    pair_sources = {sources[left], sources[right]}
                    if pair_sources != {"A", "B"}:
                        raise ValueError("跨表任务只能生成 A 与 B 的候选对")
                    if sources[left] == "B":
                        left, right = right, left
                reason = str(value.get("recall_reason") or "unknown")
                existing = (
                    await connection.execute(
                        select(candidate_pairs).where(
                            candidate_pairs.c.job_id == job_id,
                            candidate_pairs.c.record_a_id == left,
                            candidate_pairs.c.record_b_id == right,
                        )
                    )
                ).mappings().first()
                if existing:
                    reasons = list(existing["recall_reasons"] or [])
                    if reason not in reasons:
                        reasons.append(reason)
                    await connection.execute(
                        update(candidate_pairs)
                        .where(candidate_pairs.c.id == existing["id"])
                        .values(
                            recall_reasons=reasons,
                            vector_score=max(
                                score
                                for score in (existing["vector_score"], value.get("vector_score"))
                                if score is not None
                            )
                            if existing["vector_score"] is not None or value.get("vector_score") is not None
                            else None,
                            rerank_score=max(
                                score
                                for score in (existing["rerank_score"], value.get("rerank_score"))
                                if score is not None
                            )
                            if existing["rerank_score"] is not None or value.get("rerank_score") is not None
                            else None,
                        )
                    )
                else:
                    await connection.execute(
                        candidate_pairs.insert().values(
                            job_id=job_id,
                            record_a_id=left,
                            record_b_id=right,
                            recall_reasons=[reason],
                            vector_score=value.get("vector_score"),
                            rerank_score=value.get("rerank_score"),
                        )
                    )
            await connection.execute(
                update(jobs)
                .where(jobs.c.id == job_id)
                .values(
                    candidate_count=select(func.count(candidate_pairs.c.id))
                    .where(candidate_pairs.c.job_id == job_id)
                    .scalar_subquery()
                )
            )

    async def list_candidate_pairs(self, job_id: str) -> list[dict[str, Any]]:
        async with self.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(candidate_pairs)
                    .where(candidate_pairs.c.job_id == job_id)
                    .order_by(candidate_pairs.c.id)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def list_pair_details(
        self,
        job_id: str,
        *,
        limit: int = 50,
        offset: int = 0,
        include_not_duplicate: bool = False,
        region: str = "",
        category: str = "",
        recall_reason: str = "",
        model_decision: str = "",
        review_status: str = "",
        min_confidence: float | None = None,
    ) -> list[dict[str, Any]]:
        left = records.alias("left_record")
        right = records.alias("right_record")
        statement = (
            select(
                candidate_pairs,
                reviews.c.decision.label("review_decision"),
                reviews.c.note.label("review_note"),
                left.c.work_order_id.label("a_work_order_id"),
                left.c.title.label("a_title"),
                left.c.region.label("a_region"),
                left.c.street.label("a_street"),
                left.c.category.label("a_category"),
                left.c.appeal_text.label("a_appeal_text"),
                left.c.extraction_json.label("a_extraction_json"),
                right.c.work_order_id.label("b_work_order_id"),
                right.c.title.label("b_title"),
                right.c.region.label("b_region"),
                right.c.street.label("b_street"),
                right.c.category.label("b_category"),
                right.c.appeal_text.label("b_appeal_text"),
                right.c.extraction_json.label("b_extraction_json"),
            )
            .select_from(
                candidate_pairs.join(left, left.c.id == candidate_pairs.c.record_a_id)
                .join(right, right.c.id == candidate_pairs.c.record_b_id)
                .outerjoin(reviews, reviews.c.candidate_pair_id == candidate_pairs.c.id)
            )
            .where(candidate_pairs.c.job_id == job_id)
            .order_by(candidate_pairs.c.id)
            .limit(limit)
            .offset(offset)
        )
        statement = statement.where(
            *_pair_filter_conditions(
                left,
                right,
                include_not_duplicate=include_not_duplicate,
                region=region,
                category=category,
                recall_reason=recall_reason,
                model_decision=model_decision,
                review_status=review_status,
                min_confidence=min_confidence,
            )
        )
        async with self.engine.connect() as connection:
            rows = (await connection.execute(statement)).mappings().all()
        result = []
        for row in rows:
            item = dict(row)
            item["candidate_key"] = "；".join(item.get("recall_reasons") or [])
            item["hard_conflicts"] = item.get("hard_conflicts_json") or []
            item["evidence"] = item.get("evidence_json") or {}
            result.append(item)
        return result

    async def count_pairs(
        self,
        job_id: str,
        *,
        include_not_duplicate: bool = False,
        region: str = "",
        category: str = "",
        recall_reason: str = "",
        model_decision: str = "",
        review_status: str = "",
        min_confidence: float | None = None,
    ) -> int:
        left = records.alias("count_left_record")
        right = records.alias("count_right_record")
        statement = (
            select(func.count(candidate_pairs.c.id))
            .select_from(
                candidate_pairs.join(left, left.c.id == candidate_pairs.c.record_a_id)
                .join(right, right.c.id == candidate_pairs.c.record_b_id)
                .outerjoin(reviews, reviews.c.candidate_pair_id == candidate_pairs.c.id)
            )
            .where(
                candidate_pairs.c.job_id == job_id,
                *_pair_filter_conditions(
                    left,
                    right,
                    include_not_duplicate=include_not_duplicate,
                    region=region,
                    category=category,
                    recall_reason=recall_reason,
                    model_decision=model_decision,
                    review_status=review_status,
                    min_confidence=min_confidence,
                ),
            )
        )
        async with self.engine.connect() as connection:
            return int((await connection.execute(statement)).scalar_one())

    async def list_groups(
        self,
        job_id: str,
        *,
        limit: int | None = None,
        offset: int = 0,
        merged_only: bool = False,
    ) -> list[dict[str, Any]]:
        member_count = func.count(event_members.c.record_id)
        statement = (
            select(
                event_groups.c.id,
                event_groups.c.name,
                event_groups.c.region,
                event_groups.c.first_received_at,
                event_groups.c.last_received_at,
                member_count.label("member_count"),
            )
            .select_from(
                event_groups.join(
                    event_members,
                    event_members.c.event_group_id == event_groups.c.id,
                )
            )
            .where(event_groups.c.job_id == job_id)
            .group_by(event_groups.c.id)
            .order_by(event_groups.c.id)
            .offset(offset)
        )
        if merged_only:
            statement = statement.having(member_count > 1)
        if limit is not None:
            statement = statement.limit(limit)
        async with self.engine.connect() as connection:
            rows = (await connection.execute(statement)).mappings().all()
        return [dict(row) for row in rows]

    async def count_groups(self, job_id: str, *, merged_only: bool = False) -> int:
        member_count = func.count(event_members.c.record_id)
        statement = (
            select(event_groups.c.id)
            .select_from(
                event_groups.join(
                    event_members,
                    event_members.c.event_group_id == event_groups.c.id,
                )
            )
            .where(event_groups.c.job_id == job_id)
            .group_by(event_groups.c.id)
        )
        if merged_only:
            statement = statement.having(member_count > 1)
        async with self.engine.connect() as connection:
            return int(
                (
                    await connection.execute(
                        select(func.count()).select_from(statement.subquery())
                    )
                ).scalar_one()
            )

    async def rename_event_group(self, job_id: str, group_id: int, name: str) -> None:
        normalized = name.strip()
        if not normalized:
            raise ValueError("事件名称不能为空")
        if len(normalized) > 200:
            raise ValueError("事件名称不能超过 200 个字符")
        async with self.engine.begin() as connection:
            result = await connection.execute(
                update(event_groups)
                .where(event_groups.c.job_id == job_id, event_groups.c.id == group_id)
                .values(name=normalized)
            )
        if not result.rowcount:
            raise KeyError(group_id)

    async def list_region_stats(self, job_id: str) -> list[dict[str, Any]]:
        left = records.alias("region_left_record")
        right = records.alias("region_right_record")
        async with self.engine.connect() as connection:
            record_rows = (
                await connection.execute(
                    select(
                        records.c.region,
                        records.c.street,
                        func.count(records.c.id).label("sample_count"),
                    )
                    .where(records.c.job_id == job_id)
                    .group_by(records.c.region, records.c.street)
                )
            ).mappings().all()
            pair_rows = (
                await connection.execute(
                    select(
                        left.c.region,
                        left.c.street,
                        right.c.region.label("right_region"),
                        right.c.street.label("right_street"),
                        candidate_pairs.c.llm_decision,
                        candidate_pairs.c.confidence,
                        reviews.c.decision.label("review_decision"),
                    )
                    .select_from(
                        candidate_pairs.join(left, left.c.id == candidate_pairs.c.record_a_id)
                        .join(right, right.c.id == candidate_pairs.c.record_b_id)
                        .outerjoin(
                            reviews, reviews.c.candidate_pair_id == candidate_pairs.c.id
                        )
                    )
                    .where(candidate_pairs.c.job_id == job_id)
                )
            ).mappings().all()
        stats: dict[tuple[str, str], dict[str, Any]] = {}
        confidences: dict[tuple[str, str], list[float]] = {}
        for row in record_rows:
            region = str(row["region"] or "未知地区")
            street = str(row["street"] or "未知街道")
            key = (region, street)
            stats[key] = {
                "region": region,
                "street": street,
                "sample_count": int(row["sample_count"]),
                "candidate_count": 0,
                "model_duplicate_count": 0,
                "human_duplicate_count": 0,
                "review_count": 0,
                "average_confidence": None,
                "minimum_confidence": None,
            }
            confidences[key] = []
        for row in pair_rows:
            areas = {
                (str(row["region"] or "未知地区"), str(row["street"] or "未知街道")),
                (str(row["right_region"] or "未知地区"), str(row["right_street"] or "未知街道")),
            }
            for key in areas:
                region, street = key
                item = stats.setdefault(
                    key,
                    {
                        "region": region,
                        "street": street,
                        "sample_count": 0,
                        "candidate_count": 0,
                        "model_duplicate_count": 0,
                        "human_duplicate_count": 0,
                        "review_count": 0,
                        "average_confidence": None,
                        "minimum_confidence": None,
                    },
                )
                confidences.setdefault(key, [])
                item["candidate_count"] += 1
                item["model_duplicate_count"] += row["llm_decision"] == "duplicate"
                item["review_count"] += row["llm_decision"] == "review"
                item["human_duplicate_count"] += row["review_decision"] == "duplicate"
                if row["confidence"] is not None:
                    confidences[key].append(float(row["confidence"]))
        for key, values in confidences.items():
            if values:
                stats[key]["average_confidence"] = sum(values) / len(values)
                stats[key]["minimum_confidence"] = min(values)
        return [stats[key] for key in sorted(stats)]

    async def resume_job(self, job_id: str) -> None:
        async with self.engine.begin() as connection:
            status = (
                await connection.execute(select(jobs.c.status).where(jobs.c.id == job_id))
            ).scalar_one_or_none()
            if status is None:
                raise KeyError(job_id)
            if status == "running":
                raise ValueError("运行中的任务不能重复继续")
            if status not in {"paused", "failed", "queued"}:
                raise ValueError("当前任务状态不允许继续")
            await connection.execute(
                update(jobs)
                .where(jobs.c.id == job_id)
                .values(
                    pause_requested=False,
                    status="queued",
                    stage="queued",
                    lease_owner=None,
                    lease_expires_at=None,
                    heartbeat_at=None,
                    updated_at=datetime.now(UTC),
                )
            )

    async def set_job_state(self, job_id: str, *, status: str, stage: str) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(
                update(jobs)
                .where(jobs.c.id == job_id)
                .values(status=status, stage=stage, updated_at=datetime.now(UTC))
            )

    async def save_extractions(self, job_id: str, values: dict[int, dict[str, Any]]) -> None:
        async with self.engine.begin() as connection:
            for record_id, extraction in values.items():
                address = extraction.get("address") or {}
                await connection.execute(
                    update(records)
                    .where(records.c.job_id == job_id, records.c.id == record_id)
                    .values(
                        extraction_status="succeeded",
                        extraction_json=extraction,
                        region=address.get("district") or records.c.region,
                        street=address.get("street") or records.c.street,
                    )
                )
            await connection.execute(
                update(jobs)
                .where(jobs.c.id == job_id)
                .values(
                    extracted_records=select(func.count(records.c.id))
                    .where(records.c.job_id == job_id, records.c.extraction_status == "succeeded")
                    .scalar_subquery(),
                    updated_at=datetime.now(UTC),
                )
            )

    async def save_judgements(self, job_id: str, values: dict[int, dict[str, Any]]) -> None:
        async with self.engine.begin() as connection:
            for pair_id, judgement in values.items():
                await connection.execute(
                    update(candidate_pairs)
                    .where(candidate_pairs.c.job_id == job_id, candidate_pairs.c.id == pair_id)
                    .values(
                        judgement_status="succeeded",
                        llm_decision=judgement["decision"],
                        confidence=judgement["confidence"],
                        evidence_json=judgement.get("evidence"),
                        hard_conflicts_json=judgement.get("hard_conflicts") or [],
                        event_name_suggestion=judgement.get("event_name"),
                    )
                )
            await connection.execute(
                update(jobs)
                .where(jobs.c.id == job_id)
                .values(
                    judged_count=select(func.count(candidate_pairs.c.id))
                    .where(
                        candidate_pairs.c.job_id == job_id,
                        candidate_pairs.c.judgement_status == "succeeded",
                    )
                    .scalar_subquery(),
                    updated_at=datetime.now(UTC),
                )
            )

    async def mark_rule_exclusions(
        self,
        job_id: str,
        values: dict[int, list[str]],
    ) -> None:
        if not values:
            return
        async with self.engine.begin() as connection:
            for pair_id, conflicts in values.items():
                await connection.execute(
                    update(candidate_pairs)
                    .where(candidate_pairs.c.job_id == job_id, candidate_pairs.c.id == pair_id)
                    .values(
                        rule_status="excluded",
                        judgement_status="succeeded",
                        llm_decision="not_duplicate",
                        confidence=1.0,
                        hard_conflicts_json=conflicts,
                        evidence_json={"reason": "程序硬规则排除"},
                    )
                )
            await connection.execute(
                update(jobs)
                .where(jobs.c.id == job_id)
                .values(
                    judged_count=select(func.count(candidate_pairs.c.id))
                    .where(
                        candidate_pairs.c.job_id == job_id,
                        candidate_pairs.c.judgement_status == "succeeded",
                    )
                    .scalar_subquery(),
                    updated_at=datetime.now(UTC),
                )
            )

    async def start_batch(
        self,
        job_id: str,
        stage: str,
        batch_index: int,
        item_ids: list[int],
    ) -> bool:
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            values = {
                "job_id": job_id,
                "stage": stage,
                "batch_index": batch_index,
                "status": "running",
                "attempts": 1,
                "item_ids": item_ids,
                "created_at": now,
                "updated_at": now,
            }
            dialect_insert = (
                postgresql_insert if connection.dialect.name == "postgresql" else sqlite_insert
            )
            inserted = await connection.execute(
                dialect_insert(job_batches)
                .values(**values)
                .on_conflict_do_nothing(
                    index_elements=["job_id", "stage", "batch_index"]
                )
            )
            claimed = bool(inserted.rowcount)
            if not claimed:
                retried = await connection.execute(
                    update(job_batches)
                    .where(
                        job_batches.c.job_id == job_id,
                        job_batches.c.stage == stage,
                        job_batches.c.batch_index == batch_index,
                        job_batches.c.status.in_(("failed", "pending")),
                    )
                    .values(
                        status="running",
                        attempts=job_batches.c.attempts + 1,
                        item_ids=item_ids,
                        error_message=None,
                        updated_at=now,
                    )
                )
                claimed = bool(retried.rowcount)
            if claimed:
                await _refresh_batch_counts(connection, job_id)
            return claimed

    async def finish_batch(self, job_id: str, stage: str, batch_index: int) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(
                update(job_batches)
                .where(
                    job_batches.c.job_id == job_id,
                    job_batches.c.stage == stage,
                    job_batches.c.batch_index == batch_index,
                )
                .values(status="succeeded", error_message=None, updated_at=datetime.now(UTC))
            )
            await _refresh_batch_counts(connection, job_id)

    async def fail_batch(
        self,
        job_id: str,
        stage: str,
        batch_index: int,
        error_message: str,
    ) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(
                update(job_batches)
                .where(
                    job_batches.c.job_id == job_id,
                    job_batches.c.stage == stage,
                    job_batches.c.batch_index == batch_index,
                )
                .values(
                    status="failed",
                    error_message=error_message,
                    updated_at=datetime.now(UTC),
                )
            )
            await _refresh_batch_counts(connection, job_id)

    async def list_batches(self, job_id: str) -> list[dict[str, Any]]:
        async with self.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(job_batches)
                    .where(job_batches.c.job_id == job_id)
                    .order_by(job_batches.c.stage, job_batches.c.batch_index)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def review_pair(
        self,
        job_id: str,
        pair_id: int,
        decision: Literal["duplicate", "not_duplicate"],
        note: str | None = None,
    ) -> None:
        if decision not in {"duplicate", "not_duplicate"}:
            raise ValueError("无效的人工复核结论")
        async with self.engine.begin() as connection:
            pair = (
                await connection.execute(
                    select(candidate_pairs).where(
                        candidate_pairs.c.id == pair_id,
                        candidate_pairs.c.job_id == job_id,
                    )
                )
            ).mappings().first()
            if pair is None:
                raise KeyError(pair_id)
            if decision == "duplicate" and pair["hard_conflicts_json"]:
                raise ValueError("存在硬冲突，不能确认重复")
            if decision == "duplicate":
                pair_rows = (
                    await connection.execute(
                        select(
                            candidate_pairs.c.id,
                            candidate_pairs.c.record_a_id,
                            candidate_pairs.c.record_b_id,
                            candidate_pairs.c.hard_conflicts_json,
                            reviews.c.decision,
                        )
                        .select_from(
                            candidate_pairs.outerjoin(
                                reviews, reviews.c.candidate_pair_id == candidate_pairs.c.id
                            )
                        )
                        .where(candidate_pairs.c.job_id == job_id)
                    )
                ).mappings().all()
                duplicate_edges = [
                    (row["record_a_id"], row["record_b_id"])
                    for row in pair_rows
                    if row["decision"] == "duplicate" and row["id"] != pair_id
                ]
                components = _component_map(duplicate_edges)
                left_members = components.get(pair["record_a_id"], {pair["record_a_id"]})
                right_members = components.get(pair["record_b_id"], {pair["record_b_id"]})
                cannot_links = {
                    frozenset((row["record_a_id"], row["record_b_id"]))
                    for row in pair_rows
                    if row["id"] != pair_id
                    and (row["decision"] == "not_duplicate" or row["hard_conflicts_json"])
                }
                if any(
                    frozenset((left, right)) in cannot_links
                    for left in left_members
                    for right in right_members
                ):
                    raise ValueError("事件组之间存在人工否决关系，不能合并")
            existing = (
                await connection.execute(
                    select(reviews.c.id).where(reviews.c.candidate_pair_id == pair_id)
                )
            ).scalar_one_or_none()
            values = {
                "decision": decision,
                "note": note,
                "reviewed_at": datetime.now(UTC),
            }
            if existing is None:
                await connection.execute(
                    reviews.insert().values(candidate_pair_id=pair_id, **values)
                )
            else:
                await connection.execute(
                    update(reviews).where(reviews.c.id == existing).values(**values)
                )
        await self.rebuild_event_groups(job_id)

    async def rebuild_event_groups(self, job_id: str) -> None:
        async with self.engine.begin() as connection:
            record_rows = (
                await connection.execute(
                    select(records).where(records.c.job_id == job_id).order_by(records.c.id)
                )
            ).mappings().all()
            pair_rows = (
                await connection.execute(
                    select(
                        candidate_pairs.c.id,
                        candidate_pairs.c.record_a_id,
                        candidate_pairs.c.record_b_id,
                        candidate_pairs.c.hard_conflicts_json,
                        reviews.c.decision,
                    )
                    .select_from(
                        candidate_pairs.outerjoin(
                            reviews, reviews.c.candidate_pair_id == candidate_pairs.c.id
                        )
                    )
                    .where(candidate_pairs.c.job_id == job_id)
                    .order_by(candidate_pairs.c.id)
                )
            ).mappings().all()
            existing_name_rows = (
                await connection.execute(
                    select(event_groups.c.name, event_members.c.event_group_id, event_members.c.record_id)
                    .select_from(
                        event_groups.join(
                            event_members,
                            event_members.c.event_group_id == event_groups.c.id,
                        )
                    )
                    .where(event_groups.c.job_id == job_id)
                    .order_by(event_members.c.event_group_id, event_members.c.record_id)
                )
            ).mappings().all()
            existing_members: dict[int, set[int]] = {}
            existing_names: dict[int, str] = {}
            for row in existing_name_rows:
                group_id = int(row["event_group_id"])
                existing_members.setdefault(group_id, set()).add(int(row["record_id"]))
                existing_names[group_id] = str(row["name"])
            names_by_members = {
                frozenset(existing_members[group_id]): name
                for group_id, name in existing_names.items()
            }
            existing_group_ids = select(event_groups.c.id).where(event_groups.c.job_id == job_id)
            await connection.execute(
                delete(event_members).where(event_members.c.event_group_id.in_(existing_group_ids))
            )
            await connection.execute(delete(event_groups).where(event_groups.c.job_id == job_id))
            duplicate_edges = [
                (int(row["record_a_id"]), int(row["record_b_id"]))
                for row in pair_rows
                if row["decision"] == "duplicate" and not row["hard_conflicts_json"]
            ]
            cannot_links = {
                frozenset((int(row["record_a_id"]), int(row["record_b_id"])))
                for row in pair_rows
                if row["decision"] == "not_duplicate" or row["hard_conflicts_json"]
            }
            components = _constrained_components(
                [int(row["id"]) for row in record_rows],
                duplicate_edges,
                cannot_links,
            )
            records_by_id = {int(row["id"]): row for row in record_rows}
            now = datetime.now(UTC)
            for members in components:
                member_rows = [records_by_id[record_id] for record_id in sorted(members)]
                region = next((str(row["region"]) for row in member_rows if row["region"]), None)
                dates = sorted(str(row["received_at"]) for row in member_rows if row["received_at"])
                cursor = await connection.execute(
                    event_groups.insert().values(
                        job_id=job_id,
                        name=names_by_members.get(frozenset(members), _event_name(member_rows)),
                        region=region,
                        first_received_at=dates[0] if dates else None,
                        last_received_at=dates[-1] if dates else None,
                        created_at=now,
                    )
                )
                group_id = int(cursor.inserted_primary_key[0])
                await connection.execute(
                    event_members.insert(),
                    [
                        {"event_group_id": group_id, "record_id": record_id}
                        for record_id in sorted(members)
                    ],
                )

    async def list_event_members(self, job_id: str) -> list[dict[str, Any]]:
        async with self.engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(
                        event_groups.c.id.label("event_group_id"),
                        event_groups.c.name.label("event_name"),
                        event_groups.c.region,
                        event_groups.c.first_received_at,
                        event_groups.c.last_received_at,
                        event_members.c.record_id,
                    )
                    .select_from(
                        event_groups.join(
                            event_members,
                            event_members.c.event_group_id == event_groups.c.id,
                        )
                    )
                    .where(event_groups.c.job_id == job_id)
                    .order_by(event_groups.c.id, event_members.c.record_id)
                )
            ).mappings().all()
        return [dict(row) for row in rows]

    async def replace_candidate_events(
        self, job_id: str, values: list[dict[str, Any]]
    ) -> list[int]:
        member_ids = [
            int(member["record_id"])
            for value in values
            for member in value.get("members", [])
        ]
        if len(member_ids) != len(set(member_ids)):
            raise ValueError("每张工单只能归属一个候选事件")
        now = datetime.now(UTC)
        created_ids: list[int] = []
        async with self.engine.begin() as connection:
            if not await connection.scalar(select(jobs.c.id).where(jobs.c.id == job_id)):
                raise KeyError(job_id)
            if member_ids:
                valid_ids = set(
                    await connection.scalars(
                        select(records.c.id).where(
                            records.c.job_id == job_id, records.c.id.in_(member_ids)
                        )
                    )
                )
                if valid_ids != set(member_ids):
                    raise ValueError("候选事件包含不属于当前任务的工单")
            old_group_ids = list(
                await connection.scalars(
                    select(candidate_events.c.final_event_group_id).where(
                        candidate_events.c.job_id == job_id,
                        candidate_events.c.final_event_group_id.is_not(None),
                    )
                )
            )
            if old_group_ids:
                await connection.execute(
                    delete(event_members).where(event_members.c.event_group_id.in_(old_group_ids))
                )
                await connection.execute(delete(event_groups).where(event_groups.c.id.in_(old_group_ids)))
            old_ids = select(candidate_events.c.id).where(candidate_events.c.job_id == job_id)
            await connection.execute(
                delete(candidate_event_members).where(
                    candidate_event_members.c.candidate_event_id.in_(old_ids)
                )
            )
            await connection.execute(delete(candidate_events).where(candidate_events.c.job_id == job_id))
            for value in values:
                members = value.get("members") or []
                if not members:
                    raise ValueError("候选事件至少需要一张工单")
                status = str(value.get("status") or "review")
                if status not in {"review", "confirmed", "auto_merged", "rejected", "singleton"}:
                    raise ValueError("无效的候选事件状态")
                record_rows = (
                    await connection.execute(
                        select(records).where(
                            records.c.id.in_([int(item["record_id"]) for item in members])
                        )
                    )
                ).mappings().all()
                dimensions = _event_dimensions(record_rows)
                cursor = await connection.execute(
                    candidate_events.insert().values(
                        job_id=job_id,
                        name=_validated_event_name(value.get("name")),
                        status=status,
                        confidence=value.get("confidence"),
                        evidence_json=value.get("evidence") or value.get("evidence_json"),
                        **dimensions,
                        created_at=now,
                        updated_at=now,
                    )
                )
                event_id = int(cursor.inserted_primary_key[0])
                created_ids.append(event_id)
                await connection.execute(
                    candidate_event_members.insert(),
                    [
                        {
                            "candidate_event_id": event_id,
                            "record_id": int(member["record_id"]),
                            "confidence": member.get("confidence"),
                            "role": member.get("role") or "member",
                            "assignment_source": member.get("assignment_source") or "model",
                        }
                        for member in members
                    ],
                )
                if status in {"confirmed", "auto_merged"}:
                    await self._sync_candidate_event_group(connection, job_id, event_id)
                if status == "auto_merged":
                    await _record_event_action(
                        connection, job_id, event_id, "auto_merge"
                    )
        return created_ids

    async def list_candidate_events(
        self,
        job_id: str,
        *,
        limit: int | None = None,
        offset: int = 0,
        region: str = "",
        street: str = "",
        category_level_1: str = "",
        category_level_2: str = "",
        category_level_3: str = "",
        category_level_4: str = "",
        category: str = "",
        event_id: int | None = None,
        status: str = "",
        min_confidence: float | None = None,
        include_singletons: bool = False,
        include_rejected: bool = False,
        show_singletons: bool | None = None,
    ) -> list[dict[str, Any]]:
        if show_singletons is not None:
            include_singletons = show_singletons
        statement = _candidate_event_query(job_id)
        statement = _apply_candidate_event_filters(
            statement,
            region=region,
            street=street,
            category_level_1=category_level_1,
            category_level_2=category_level_2,
            category_level_3=category_level_3,
            category_level_4=category_level_4,
            category=category,
            event_id=event_id,
            status=status,
            min_confidence=min_confidence,
            include_singletons=include_singletons,
            include_rejected=include_rejected,
        ).order_by(candidate_events.c.id).offset(offset)
        if limit is not None:
            statement = statement.limit(limit)
        async with self.engine.connect() as connection:
            rows = (await connection.execute(statement)).mappings().all()
        return [_candidate_event_dict(row) for row in rows]

    async def count_candidate_events(
        self,
        job_id: str,
        *,
        show_singletons: bool | None = None,
        **filters: Any,
    ) -> int:
        if show_singletons is not None:
            filters["include_singletons"] = show_singletons
        statement = _apply_candidate_event_filters(
            _candidate_event_query(job_id), **filters
        ).subquery()
        async with self.engine.connect() as connection:
            return int(await connection.scalar(select(func.count()).select_from(statement)) or 0)

    async def list_candidate_event_stats(self, job_id: str, **filters: Any) -> list[dict[str, Any]]:
        filtered = _apply_candidate_event_filters(
            _candidate_event_query(job_id), **filters
        ).subquery()
        statement = (
            select(
                filtered.c.region,
                filtered.c.street,
                func.sum(filtered.c.member_count).label("sample_count"),
                func.count(filtered.c.id).label("event_count"),
                func.sum(case((filtered.c.status == "auto_merged", 1), else_=0)).label("auto_merged_count"),
                func.sum(case((filtered.c.status == "confirmed", 1), else_=0)).label("confirmed_count"),
                func.sum(case((filtered.c.status == "review", 1), else_=0)).label("review_count"),
                func.avg(filtered.c.confidence).label("average_confidence"),
                func.min(filtered.c.minimum_member_confidence).label("minimum_confidence"),
            )
            .group_by(filtered.c.region, filtered.c.street)
            .order_by(filtered.c.region, filtered.c.street)
        )
        async with self.engine.connect() as connection:
            rows = (await connection.execute(statement)).mappings().all()
        return [dict(row) for row in rows]

    async def get_candidate_event(self, job_id: str, event_id: int) -> dict[str, Any]:
        async with self.engine.connect() as connection:
            event = (
                await connection.execute(
                    _candidate_event_query(job_id).where(candidate_events.c.id == event_id)
                )
            ).mappings().first()
            if event is None:
                raise KeyError(event_id)
            member_rows = (
                await connection.execute(
                    select(
                        records,
                        records.c.id.label("record_id"),
                        candidate_event_members.c.confidence.label("member_confidence"),
                        candidate_event_members.c.role.label("member_role"),
                        candidate_event_members.c.assignment_source,
                    )
                    .select_from(
                        candidate_event_members.join(
                            records, records.c.id == candidate_event_members.c.record_id
                        )
                    )
                    .where(candidate_event_members.c.candidate_event_id == event_id)
                    .order_by(records.c.id)
                )
            ).mappings().all()
        result = _candidate_event_dict(event)
        result["members"] = [dict(row) for row in member_rows]
        return result

    async def list_candidate_event_filter_options(
        self, job_id: str, *, region: str = "", street: str = ""
    ) -> dict[str, Any]:
        async with self.engine.connect() as connection:
            async def distinct(column: Any, *conditions: Any) -> list[str]:
                rows = await connection.scalars(
                    select(column)
                    .where(records.c.job_id == job_id, column.is_not(None), *conditions)
                    .distinct()
                    .order_by(column)
                )
                return [str(value) for value in rows if str(value).strip()]

            region_condition = records.c.region == region.strip() if region.strip() else True
            street_condition = records.c.street == street.strip() if street.strip() else True
            event_conditions = [candidate_events.c.job_id == job_id]
            if region.strip():
                event_conditions.append(candidate_events.c.region == region.strip())
            if street.strip():
                event_conditions.append(candidate_events.c.street == street.strip())
            events_rows = (
                await connection.execute(
                    select(candidate_events.c.id, candidate_events.c.name)
                    .where(*event_conditions)
                    .order_by(candidate_events.c.id)
                )
            ).mappings().all()
            return {
                "regions": await distinct(records.c.region),
                "streets": await distinct(records.c.street, region_condition),
                "category_level_1": await distinct(records.c.category_level_1, region_condition, street_condition),
                "category_level_2": await distinct(records.c.category_level_2, region_condition, street_condition),
                "category_level_3": await distinct(records.c.category_level_3, region_condition, street_condition),
                "category_level_4": await distinct(records.c.category_level_4, region_condition, street_condition),
                "categories": await distinct(records.c.category, region_condition, street_condition),
                "events": [dict(row) for row in events_rows],
            }

    async def confirm_candidate_event(
        self, job_id: str, event_id: int, note: str | None = None
    ) -> None:
        async with self.engine.begin() as connection:
            await self._set_candidate_event_status(connection, job_id, event_id, "confirmed")
            await self._sync_candidate_event_group(connection, job_id, event_id)
            await _record_event_action(connection, job_id, event_id, "confirm", note)

    async def reopen_candidate_event(
        self, job_id: str, event_id: int, note: str | None = None
    ) -> None:
        async with self.engine.begin() as connection:
            await self._clear_candidate_event_group(connection, job_id, event_id)
            await self._set_candidate_event_status(connection, job_id, event_id, "review")
            await _record_event_action(connection, job_id, event_id, "reopen", note)

    async def rename_candidate_event(self, job_id: str, event_id: int, name: str) -> None:
        normalized = _validated_event_name(name)
        async with self.engine.begin() as connection:
            event = await self._candidate_event_for_update(connection, job_id, event_id)
            await connection.execute(
                update(candidate_events)
                .where(candidate_events.c.id == event_id)
                .values(name=normalized, updated_at=datetime.now(UTC))
            )
            if event["final_event_group_id"] is not None:
                await connection.execute(
                    update(event_groups)
                    .where(event_groups.c.id == event["final_event_group_id"])
                    .values(name=normalized)
                )
            await _record_event_action(connection, job_id, event_id, "rename", details={"name": normalized})

    async def split_candidate_event(
        self,
        job_id: str,
        event_id: int,
        record_ids: list[int],
        *,
        name: str,
    ) -> int:
        selected = set(map(int, record_ids))
        if not selected:
            raise ValueError("请选择需要拆分的工单")
        async with self.engine.begin() as connection:
            members = set(await connection.scalars(select(candidate_event_members.c.record_id).where(candidate_event_members.c.candidate_event_id == event_id)))
            if not members:
                raise KeyError(event_id)
            if not selected < members:
                raise ValueError("拆分后原事件和新事件都必须保留工单")
            await self._clear_candidate_event_group(connection, job_id, event_id)
            now = datetime.now(UTC)
            await connection.execute(update(candidate_events).where(candidate_events.c.id == event_id).values(status="review", updated_at=now))
            cursor = await connection.execute(candidate_events.insert().values(job_id=job_id, name=_validated_event_name(name), status="review", created_at=now, updated_at=now))
            new_id = int(cursor.inserted_primary_key[0])
            await connection.execute(update(candidate_event_members).where(candidate_event_members.c.candidate_event_id == event_id, candidate_event_members.c.record_id.in_(selected)).values(candidate_event_id=new_id, assignment_source="manual"))
            await self._refresh_candidate_event_dimensions(connection, event_id)
            await self._refresh_candidate_event_dimensions(connection, new_id)
            await _record_event_action(connection, job_id, event_id, "split", details={"new_event_id": new_id, "record_ids": sorted(selected)})
        return new_id

    async def merge_candidate_events(
        self, job_id: str, event_ids: list[int], *, name: str
    ) -> int:
        ids = sorted(set(map(int, event_ids)))
        if len(ids) < 2:
            raise ValueError("至少选择两个候选事件")
        async with self.engine.begin() as connection:
            rows = (await connection.execute(select(candidate_events).where(candidate_events.c.job_id == job_id, candidate_events.c.id.in_(ids)))).mappings().all()
            if len(rows) != len(ids):
                raise KeyError(next(item for item in ids if item not in {int(row["id"]) for row in rows}))
            for event_id in ids:
                await self._clear_candidate_event_group(connection, job_id, event_id)
            keep = ids[0]
            await connection.execute(update(candidate_event_members).where(candidate_event_members.c.candidate_event_id.in_(ids[1:])).values(candidate_event_id=keep, assignment_source="manual"))
            await connection.execute(delete(candidate_events).where(candidate_events.c.id.in_(ids[1:])))
            await connection.execute(update(candidate_events).where(candidate_events.c.id == keep).values(name=_validated_event_name(name), status="review", confidence=None, updated_at=datetime.now(UTC)))
            await self._refresh_candidate_event_dimensions(connection, keep)
            await _record_event_action(connection, job_id, keep, "merge", details={"merged_event_ids": ids})
        return keep

    async def move_candidate_event_member(
        self,
        job_id: str,
        record_id: int,
        *,
        source_event_id: int,
        target_event_id: int,
    ) -> None:
        if source_event_id == target_event_id:
            raise ValueError("源事件和目标事件不能相同")
        async with self.engine.begin() as connection:
            for event_id in (source_event_id, target_event_id):
                await self._candidate_event_for_update(connection, job_id, event_id)
            source_count = int(await connection.scalar(select(func.count()).select_from(candidate_event_members).where(candidate_event_members.c.candidate_event_id == source_event_id)) or 0)
            if source_count <= 1:
                raise ValueError("不能移动原事件的最后一张工单")
            result = await connection.execute(update(candidate_event_members).where(candidate_event_members.c.candidate_event_id == source_event_id, candidate_event_members.c.record_id == record_id).values(candidate_event_id=target_event_id, assignment_source="manual"))
            if not result.rowcount:
                raise KeyError(record_id)
            for event_id in (source_event_id, target_event_id):
                await self._clear_candidate_event_group(connection, job_id, event_id)
                await connection.execute(update(candidate_events).where(candidate_events.c.id == event_id).values(status="review", updated_at=datetime.now(UTC)))
                await self._refresh_candidate_event_dimensions(connection, event_id)
            await _record_event_action(connection, job_id, target_event_id, "move", details={"record_id": record_id, "source_event_id": source_event_id})

    async def exclude_candidate_event_member(
        self,
        job_id: str,
        event_id: int,
        record_id: int,
        *,
        note: str | None = None,
    ) -> int:
        async with self.engine.begin() as connection:
            await self._candidate_event_for_update(connection, job_id, event_id)
            count = int(await connection.scalar(select(func.count()).select_from(candidate_event_members).where(candidate_event_members.c.candidate_event_id == event_id)) or 0)
            if count <= 1:
                raise ValueError("不能排除事件中的最后一张工单")
            member = (await connection.execute(select(records).select_from(candidate_event_members.join(records, records.c.id == candidate_event_members.c.record_id)).where(candidate_event_members.c.candidate_event_id == event_id, records.c.id == record_id))).mappings().first()
            if member is None:
                raise KeyError(record_id)
            await self._clear_candidate_event_group(connection, job_id, event_id)
            now = datetime.now(UTC)
            cursor = await connection.execute(candidate_events.insert().values(job_id=job_id, name=_event_name([member]), status="rejected", confidence=None, **_event_dimensions([member]), created_at=now, updated_at=now))
            excluded_id = int(cursor.inserted_primary_key[0])
            await connection.execute(update(candidate_event_members).where(candidate_event_members.c.candidate_event_id == event_id, candidate_event_members.c.record_id == record_id).values(candidate_event_id=excluded_id, confidence=None, assignment_source="manual"))
            await connection.execute(update(candidate_events).where(candidate_events.c.id == event_id).values(status="review", updated_at=now))
            await self._refresh_candidate_event_dimensions(connection, event_id)
            await _record_event_action(connection, job_id, event_id, "exclude", note, {"record_id": record_id, "excluded_event_id": excluded_id})
        return excluded_id

    async def list_event_review_actions(self, job_id: str) -> list[dict[str, Any]]:
        async with self.engine.connect() as connection:
            rows = (await connection.execute(select(event_review_actions).where(event_review_actions.c.job_id == job_id).order_by(event_review_actions.c.id))).mappings().all()
        return [dict(row) for row in rows]

    async def _candidate_event_for_update(self, connection: Any, job_id: str, event_id: int) -> Any:
        row = (await connection.execute(select(candidate_events).where(candidate_events.c.job_id == job_id, candidate_events.c.id == event_id))).mappings().first()
        if row is None:
            raise KeyError(event_id)
        return row

    async def _set_candidate_event_status(self, connection: Any, job_id: str, event_id: int, status: str) -> None:
        await self._candidate_event_for_update(connection, job_id, event_id)
        await connection.execute(update(candidate_events).where(candidate_events.c.id == event_id).values(status=status, updated_at=datetime.now(UTC)))

    async def _clear_candidate_event_group(self, connection: Any, job_id: str, event_id: int) -> None:
        event = await self._candidate_event_for_update(connection, job_id, event_id)
        group_id = event["final_event_group_id"]
        if group_id is None:
            return
        await connection.execute(update(candidate_events).where(candidate_events.c.id == event_id).values(final_event_group_id=None))
        await connection.execute(delete(event_members).where(event_members.c.event_group_id == group_id))
        await connection.execute(delete(event_groups).where(event_groups.c.id == group_id))

    async def _sync_candidate_event_group(self, connection: Any, job_id: str, event_id: int) -> None:
        event = await self._candidate_event_for_update(connection, job_id, event_id)
        await self._clear_candidate_event_group(connection, job_id, event_id)
        member_rows = (await connection.execute(select(records).select_from(candidate_event_members.join(records, records.c.id == candidate_event_members.c.record_id)).where(candidate_event_members.c.candidate_event_id == event_id).order_by(records.c.id))).mappings().all()
        if not member_rows:
            raise ValueError("候选事件没有工单成员")
        dates = sorted(str(row["received_at"]) for row in member_rows if row["received_at"])
        cursor = await connection.execute(event_groups.insert().values(job_id=job_id, name=event["name"], region=event["region"], first_received_at=dates[0] if dates else None, last_received_at=dates[-1] if dates else None, created_at=datetime.now(UTC)))
        group_id = int(cursor.inserted_primary_key[0])
        await connection.execute(event_members.insert(), [{"event_group_id": group_id, "record_id": int(row["id"])} for row in member_rows])
        await connection.execute(update(candidate_events).where(candidate_events.c.id == event_id).values(final_event_group_id=group_id, updated_at=datetime.now(UTC)))

    async def _refresh_candidate_event_dimensions(self, connection: Any, event_id: int) -> None:
        rows = (await connection.execute(select(records).select_from(candidate_event_members.join(records, records.c.id == candidate_event_members.c.record_id)).where(candidate_event_members.c.candidate_event_id == event_id))).mappings().all()
        await connection.execute(update(candidate_events).where(candidate_events.c.id == event_id).values(**_event_dimensions(rows), confidence=None, updated_at=datetime.now(UTC)))

    async def claim_jobs(
        self,
        worker_id: str,
        *,
        limit: int,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        claimed_at = now or datetime.now(UTC)
        lease_expires_at = claimed_at + timedelta(seconds=lease_seconds)
        async with self.engine.begin() as connection:
            statement = (
                select(jobs.c.id)
                .where(jobs.c.status == "queued", jobs.c.pause_requested.is_(False))
                .order_by(jobs.c.created_at, jobs.c.id)
                .limit(limit)
            )
            if connection.dialect.name == "postgresql":
                statement = statement.with_for_update(skip_locked=True)
            ids = list((await connection.execute(statement)).scalars())
            if not ids:
                return []
            await connection.execute(
                update(jobs)
                .where(jobs.c.id.in_(ids), jobs.c.status == "queued")
                .values(
                    status="running",
                    lease_owner=worker_id,
                    lease_expires_at=lease_expires_at,
                    heartbeat_at=claimed_at,
                    updated_at=claimed_at,
                )
            )
            rows = (
                await connection.execute(select(jobs).where(jobs.c.id.in_(ids)).order_by(jobs.c.created_at))
            ).mappings().all()
        return [dict(row) for row in rows if row["lease_owner"] == worker_id]

    async def request_pause(self, job_id: str, pause: bool) -> None:
        status = "paused" if pause else "queued"
        async with self.engine.begin() as connection:
            await connection.execute(
                update(jobs)
                .where(jobs.c.id == job_id)
                .values(pause_requested=pause, status=status, updated_at=datetime.now(UTC))
            )

    async def recover_expired_jobs(self, *, now: datetime | None = None) -> int:
        current = now or datetime.now(UTC)
        async with self.engine.begin() as connection:
            result = await connection.execute(
                update(jobs)
                .where(
                    jobs.c.status == "running",
                    jobs.c.pause_requested.is_(False),
                    jobs.c.lease_expires_at < current,
                )
                .values(
                    status="queued",
                    stage="queued",
                    lease_owner=None,
                    lease_expires_at=None,
                    heartbeat_at=None,
                    updated_at=current,
                )
                .returning(jobs.c.id)
            )
            recovered_ids = [str(job_id) for job_id in result.scalars()]
            if recovered_ids:
                await connection.execute(
                    update(job_batches)
                    .where(
                        job_batches.c.job_id.in_(recovered_ids),
                        job_batches.c.status == "running",
                    )
                    .values(
                        status="failed",
                        error_message="任务租约过期，批次等待续跑",
                        updated_at=current,
                    )
                )
                for job_id in recovered_ids:
                    await _refresh_batch_counts(connection, job_id)
        return len(recovered_ids)

    async def heartbeat_job(
        self,
        job_id: str,
        worker_id: str,
        *,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> bool:
        current = now or datetime.now(UTC)
        async with self.engine.begin() as connection:
            result = await connection.execute(
                update(jobs)
                .where(
                    jobs.c.id == job_id,
                    jobs.c.status == "running",
                    jobs.c.lease_owner == worker_id,
                )
                .values(
                    heartbeat_at=current,
                    lease_expires_at=current + timedelta(seconds=lease_seconds),
                    updated_at=current,
                )
            )
        return bool(result.rowcount)

    async def release_lease(self, job_id: str, worker_id: str) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(
                update(jobs)
                .where(jobs.c.id == job_id, jobs.c.lease_owner == worker_id)
                .values(
                    lease_owner=None,
                    lease_expires_at=None,
                    heartbeat_at=None,
                    updated_at=datetime.now(UTC),
                )
            )


def _candidate_event_query(job_id: str) -> Any:
    member_count = func.count(candidate_event_members.c.record_id)
    return (
        select(
            candidate_events,
            member_count.label("member_count"),
            func.min(candidate_event_members.c.confidence).label("minimum_member_confidence"),
        )
        .select_from(
            candidate_events.join(
                candidate_event_members,
                candidate_event_members.c.candidate_event_id == candidate_events.c.id,
            )
        )
        .where(candidate_events.c.job_id == job_id)
        .group_by(candidate_events.c.id)
    )


def _apply_candidate_event_filters(
    statement: Any,
    *,
    region: str = "",
    street: str = "",
    category_level_1: str = "",
    category_level_2: str = "",
    category_level_3: str = "",
    category_level_4: str = "",
    category: str = "",
    event_id: int | None = None,
    status: str = "",
    min_confidence: float | None = None,
    include_singletons: bool = False,
    include_rejected: bool = False,
) -> Any:
    for column, value in (
        (candidate_events.c.region, region),
        (candidate_events.c.street, street),
        (candidate_events.c.category_level_1, category_level_1),
        (candidate_events.c.category_level_2, category_level_2),
        (candidate_events.c.category_level_3, category_level_3),
        (candidate_events.c.category_level_4, category_level_4),
    ):
        if normalized := value.strip():
            statement = statement.where(column == normalized)
    if normalized := category.strip():
        matching_events = (
            select(candidate_event_members.c.candidate_event_id)
            .join(records, records.c.id == candidate_event_members.c.record_id)
            .where(records.c.category == normalized)
        )
        statement = statement.where(candidate_events.c.id.in_(matching_events))
    if event_id is not None:
        statement = statement.where(candidate_events.c.id == event_id)
    if normalized := status.strip():
        statement = statement.where(candidate_events.c.status == normalized)
    if min_confidence is not None:
        statement = statement.where(candidate_events.c.confidence >= min_confidence)
    if not include_rejected:
        statement = statement.where(candidate_events.c.status != "rejected")
    if not include_singletons:
        statement = statement.having(func.count(candidate_event_members.c.record_id) > 1)
    return statement


def _candidate_event_dict(row: Any) -> dict[str, Any]:
    result = dict(row)
    result["evidence"] = result.get("evidence_json") or {}
    return result


def _event_dimensions(record_rows: list[Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name in (
        "region",
        "street",
        "category_level_1",
        "category_level_2",
        "category_level_3",
        "category_level_4",
    ):
        values = {str(row[name]).strip() for row in record_rows if row[name]}
        result[name] = next(iter(values)) if len(values) == 1 else None
    return result


def _validated_event_name(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError("事件名称不能为空")
    if len(normalized) > 200:
        raise ValueError("事件名称不能超过 200 个字符")
    return normalized


async def _record_event_action(
    connection: Any,
    job_id: str,
    event_id: int | None,
    action: str,
    note: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    await connection.execute(
        event_review_actions.insert().values(
            job_id=job_id,
            candidate_event_id=event_id,
            action=action,
            note=note,
            details_json=details,
            created_at=datetime.now(UTC),
        )
    )


def _components(nodes: list[int], edges: list[tuple[int, int]]) -> list[set[int]]:
    parent = {node: node for node in nodes}

    def find(node: int) -> int:
        root = node
        while parent[root] != root:
            root = parent[root]
        while parent[node] != node:
            parent[node], node = root, parent[node]
        return root

    for left, right in edges:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[right_root] = left_root
    grouped: dict[int, set[int]] = {}
    for node in nodes:
        grouped.setdefault(find(node), set()).add(node)
    return list(grouped.values())


def _pair_filter_conditions(
    left: Any,
    right: Any,
    *,
    include_not_duplicate: bool,
    region: str,
    category: str,
    recall_reason: str,
    model_decision: str,
    review_status: str,
    min_confidence: float | None,
) -> list[Any]:
    conditions: list[Any] = []
    if not include_not_duplicate:
        conditions.append(
            (candidate_pairs.c.llm_decision != "not_duplicate")
            | candidate_pairs.c.llm_decision.is_(None)
            | reviews.c.decision.is_not(None)
        )
    if normalized := region.strip():
        conditions.append((left.c.region == normalized) | (right.c.region == normalized))
    if normalized := category.strip():
        pattern = f"%{normalized}%"
        conditions.append(left.c.category.ilike(pattern) | right.c.category.ilike(pattern))
    if normalized := recall_reason.strip():
        conditions.append(cast(candidate_pairs.c.recall_reasons, Text).ilike(f"%{normalized}%"))
    if normalized := model_decision.strip():
        if normalized == "pending":
            conditions.append(candidate_pairs.c.llm_decision.is_(None))
        else:
            conditions.append(candidate_pairs.c.llm_decision == normalized)
    if normalized := review_status.strip():
        if normalized == "pending":
            conditions.append(reviews.c.decision.is_(None))
        elif normalized == "reviewed":
            conditions.append(reviews.c.decision.is_not(None))
        else:
            conditions.append(reviews.c.decision == normalized)
    if min_confidence is not None:
        conditions.append(candidate_pairs.c.confidence >= min_confidence)
    return conditions


def _constrained_components(
    nodes: list[int],
    edges: list[tuple[int, int]],
    cannot_links: set[frozenset[int]],
) -> list[set[int]]:
    parent = {node: node for node in nodes}
    members = {node: {node} for node in nodes}

    def find(node: int) -> int:
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    for left, right in edges:
        left_root, right_root = find(left), find(right)
        if left_root == right_root:
            continue
        left_members = members[left_root]
        right_members = members[right_root]
        if any(
            frozenset((left_member, right_member)) in cannot_links
            for left_member in left_members
            for right_member in right_members
        ):
            continue
        keep, merge = sorted((left_root, right_root))
        parent[merge] = keep
        members[keep] = members[left_root] | members[right_root]
        del members[merge]
    return [members[root] for root in sorted(members, key=lambda item: min(members[item]))]


def _component_map(edges: list[tuple[int, int]]) -> dict[int, set[int]]:
    nodes = sorted({node for edge in edges for node in edge})
    result: dict[int, set[int]] = {}
    for members in _components(nodes, edges):
        for member in members:
            result[member] = members
    return result


def _event_name(member_rows: list[Any]) -> str:
    region = next((str(row["region"]) for row in member_rows if row["region"]), "未知地区")
    subject = None
    location = None
    issue = None
    for row in member_rows:
        extraction = row["extraction_json"] or {}
        subject_data = extraction.get("subject") or {}
        address = extraction.get("address") or {}
        issues = extraction.get("issues") or {}
        subject = subject or subject_data.get("full_name") or subject_data.get("short_name")
        if not subject:
            subject = next(iter(subject_data.get("keys") or []), None)
        if not location:
            location = "".join(
                str(value)
                for value in (
                    address.get("road"),
                    address.get("house_no"),
                    address.get("building"),
                    address.get("shop_no"),
                )
                if value
            ) or None
        issue = issue or issues.get("primary")
    fallback = next(
        (
            str(row["title"] or row["category"])
            for row in member_rows
            if row["title"] or row["category"]
        ),
        "未命名事件",
    )
    parts = [region, subject, location, issue]
    meaningful = [str(part).strip() for part in parts if part and str(part).strip()]
    return "｜".join(meaningful) if len(meaningful) > 1 else f"{region}｜{fallback}"


async def _refresh_batch_counts(connection: Any, job_id: str) -> None:
    await connection.execute(
        update(jobs)
        .where(jobs.c.id == job_id)
        .values(
            inflight_batches=select(func.count(job_batches.c.id))
            .where(job_batches.c.job_id == job_id, job_batches.c.status == "running")
            .scalar_subquery(),
            retry_count=select(
                func.coalesce(func.sum(job_batches.c.attempts - 1), 0)
            )
            .where(job_batches.c.job_id == job_id)
            .scalar_subquery(),
            extraction_failure_count=select(func.count(job_batches.c.id))
            .where(
                job_batches.c.job_id == job_id,
                job_batches.c.stage == "extraction",
                job_batches.c.status == "failed",
            )
            .scalar_subquery(),
            judgement_failure_count=select(func.count(job_batches.c.id))
            .where(
                job_batches.c.job_id == job_id,
                job_batches.c.stage == "judgement",
                job_batches.c.status == "failed",
            )
            .scalar_subquery(),
            updated_at=datetime.now(UTC),
        )
    )
