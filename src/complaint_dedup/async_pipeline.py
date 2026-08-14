import asyncio
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, Literal, TypeVar

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.llm_models import (
    EventClusterResponse,
    ExtractionBatchResponse,
    JudgementBatchResponse,
    validate_event_cluster_coverage,
)
from complaint_dedup.llm_client import LlmResponseError
from complaint_dedup.event_clustering import (
    CandidateEdge,
    build_constrained_components,
    can_auto_merge_event,
)
from complaint_dedup.hard_rules import detect_hard_conflicts
from complaint_dedup.pipeline import InputRecord
from complaint_dedup.prompts import (
    build_event_cluster_messages,
    build_extraction_messages,
    build_judgement_messages,
)
from complaint_dedup.soft_clustering import (
    build_vector_texts,
    extract_record_metadata,
    generate_soft_candidates,
)


T = TypeVar("T")


class AsyncJobProcessor:
    def __init__(
        self,
        *,
        database: AsyncDatabase,
        llm_client: Any,
        extraction_llm_client: Any | None = None,
        judgement_llm_client: Any | None = None,
        embedding_client: Any,
        rerank_client: Any,
        vector_store: Any,
        extraction_batch_size: int,
        judgement_batch_size: int,
        extraction_concurrency: int,
        judgement_concurrency: int,
        vector_top_k: int,
        rerank_top_n: int,
        max_candidates_per_record: int,
        max_inflight_batches_per_job: int,
        embedding_batch_size: int = 16,
        rerank_enabled: bool = True,
        pipeline_version: Literal["pair_v1", "event_cluster_v2"] = "pair_v1",
        event_llm_concurrency: int = 2,
        event_raw_group_limit: int = 20,
        event_component_max_size: int = 200,
        auto_merge_enabled: bool = True,
        auto_merge_confidence: float = 0.95,
        auto_merge_max_members: int = 20,
    ) -> None:
        self.database = database
        self.llm_client = llm_client
        self.extraction_llm_client = extraction_llm_client or llm_client
        self.judgement_llm_client = judgement_llm_client or llm_client
        self.embedding_client = embedding_client
        self.rerank_client = rerank_client
        self.vector_store = vector_store
        self.extraction_batch_size = extraction_batch_size
        self.judgement_batch_size = judgement_batch_size
        self.extraction_concurrency = extraction_concurrency
        self.judgement_concurrency = judgement_concurrency
        self._extraction_semaphore = asyncio.Semaphore(extraction_concurrency)
        self._judgement_semaphore = asyncio.Semaphore(judgement_concurrency)
        self.vector_top_k = vector_top_k
        self.rerank_top_n = rerank_top_n
        self.max_candidates_per_record = max_candidates_per_record
        self.max_inflight_batches_per_job = max_inflight_batches_per_job
        self.embedding_batch_size = embedding_batch_size
        self.rerank_enabled = rerank_enabled
        self.pipeline_version = pipeline_version
        self.event_raw_group_limit = event_raw_group_limit
        self.event_component_max_size = event_component_max_size
        self.auto_merge_enabled = auto_merge_enabled
        self.auto_merge_confidence = auto_merge_confidence
        self.auto_merge_max_members = auto_merge_max_members
        self._event_llm_semaphore = asyncio.Semaphore(event_llm_concurrency)

    async def create_job(
        self,
        name: str,
        records_a: list[InputRecord],
        records_b: list[InputRecord] | None = None,
        *,
        mode: Literal["single", "cross"],
        match_preset: Literal["strict", "balanced", "loose"] = "balanced",
        time_window_days: int = 0,
    ) -> str:
        if mode == "single" and records_b:
            raise ValueError("单文件任务不能包含第二个文件")
        if mode == "cross" and not records_b:
            raise ValueError("跨表任务必须包含两个文件")
        all_records = [*records_a, *(records_b or [])]
        job_id = uuid.uuid4().hex
        await self.database.enqueue_job(
            job_id,
            name,
            mode=mode,
            total_records=len(all_records),
            match_preset=match_preset,
            time_window_days=time_window_days,
            pipeline_version=self.pipeline_version,
        )
        values = []
        for record in all_records:
            value = {
                    "source": "S" if mode == "single" else record.source,
                    "source_row": record.source_row,
                    "work_order_id": record.work_order_id,
                    "received_at": record.received_at,
                    "title": record.title,
                    "category": record.category,
                    "category_level_1": record.category_level_1,
                    "category_level_2": record.category_level_2,
                    "category_level_3": record.category_level_3,
                    "category_level_4": record.category_level_4,
                    "appeal_text": record.appeal_text,
                    "raw_json": record.raw_fields,
                    "raw_fields": record.raw_fields,
                }
            value.update(extract_record_metadata(value))
            values.append(value)
        await self.database.add_records(job_id, values)
        return job_id

    async def process(self, job_id: str) -> None:
        try:
            job = await self.database.get_job(job_id)
            await self.database.set_job_state(job_id, status="running", stage="embedding")
            records = await self.database.list_records(job_id)
            batch_paused = await self._embed_records(job_id, records)
            if batch_paused:
                await self._pause_requested(job_id)
                return
            if await self._pause_requested(job_id):
                return

            if not job["candidate_count"]:
                await self.database.set_job_state(job_id, status="running", stage="generating_candidates")
                candidates = await generate_soft_candidates(
                    records,
                    mode=job["mode"],
                    vector_store=self.vector_store,
                    reranker=self.rerank_client,
                    vector_top_k=self.vector_top_k,
                    rerank_top_n=self.rerank_top_n,
                    max_candidates_per_record=self.max_candidates_per_record,
                    concurrency=self.max_inflight_batches_per_job,
                    match_preset=job["match_preset"],
                    time_window_days=job["time_window_days"],
                    rerank_enabled=self.rerank_enabled,
                )
                await self.database.upsert_candidate_pairs(
                    job_id,
                    [candidate.__dict__ for candidate in candidates],
                )

            await self.database.set_job_state(job_id, status="running", stage="extracting")
            batch_paused = await self._extract_candidate_records(job_id)
            if batch_paused:
                await self._pause_requested(job_id)
                return
            if await self._pause_requested(job_id):
                return
            await apply_hard_rules_to_pairs(self.database, job_id)

            if job["pipeline_version"] == "event_cluster_v2":
                await self.database.set_job_state(job_id, status="running", stage="clustering_events")
                await self._cluster_candidate_events(job_id)
                await self.database.set_job_state(job_id, status="review_ready", stage="review")
                return

            await self.database.set_job_state(job_id, status="running", stage="judging")
            batch_paused = await self._judge_pairs(job_id)
            if batch_paused:
                await self._pause_requested(job_id)
                return
            if await self._pause_requested(job_id):
                return
            await self.database.rebuild_event_groups(job_id)
            await self.database.set_job_state(job_id, status="review_ready", stage="review")
        except Exception:
            await self.database.set_job_state(job_id, status="failed", stage="failed")
            raise

    async def _embed_records(self, job_id: str, records: list[dict[str, Any]]) -> bool:
        batches = _chunks(records, self.embedding_batch_size)

        async def process_batch(batch: list[dict[str, Any]]) -> None:
            texts = [build_vector_texts(record) for record in batch]
            location_vectors, issue_vectors = await asyncio.gather(
                self.embedding_client.embed([text[0] for text in texts]),
                self.embedding_client.embed([text[1] for text in texts]),
            )
            await self.vector_store.upsert_records(
                [
                    {
                        **record,
                        "job_id": job_id,
                        "location_vector": location_vectors[index],
                        "issue_vector": issue_vectors[index],
                    }
                    for index, record in enumerate(batch)
                ]
            )

        return await _run_batches(
            batches,
            process_batch,
            concurrency=self.max_inflight_batches_per_job,
            database=self.database,
            job_id=job_id,
            stage="embedding",
            item_ids=lambda batch: [int(record["id"]) for record in batch],
        )

    async def _extract_candidate_records(self, job_id: str) -> bool:
        records = await self.database.list_records(job_id)
        pairs = await self.database.list_candidate_pairs(job_id)
        candidate_ids = {
            int(pair[key]) for pair in pairs for key in ("record_a_id", "record_b_id")
        }
        pending = [record for record in records if record["id"] in candidate_ids and record["extraction_status"] != "succeeded"]

        async def request_batch(batch: list[dict[str, Any]]) -> None:
            payload = [
                {
                    "record_id": str(record["id"]),
                    "title": record["title"],
                    "category": record["category"],
                    "appeal_text": record["appeal_text"],
                    "received_at": record["received_at"],
                }
                for record in batch
            ]
            async with self._extraction_semaphore:
                response: ExtractionBatchResponse = await self.extraction_llm_client.chat_json(
                    build_extraction_messages(payload),
                    ExtractionBatchResponse,
                )
            returned = {int(item.record_id): item.model_dump(mode="json") for item in response.records}
            missing = {int(record["id"]) for record in batch} - returned.keys()
            if missing:
                raise ValueError(f"模型未返回抽取记录: {sorted(missing)}")
            await self.database.save_extractions(job_id, returned)

        async def process_batch(batch: list[dict[str, Any]]) -> None:
            await _run_with_batch_split(batch, request_batch)

        return await _run_batches(
            _chunks(pending, self.extraction_batch_size),
            process_batch,
            concurrency=min(self.extraction_concurrency, self.max_inflight_batches_per_job),
            database=self.database,
            job_id=job_id,
            stage="extraction",
            item_ids=lambda batch: [int(record["id"]) for record in batch],
        )

    async def _judge_pairs(self, job_id: str) -> bool:
        records = {record["id"]: record for record in await self.database.list_records(job_id)}
        pending = [
            pair
            for pair in await self.database.list_candidate_pairs(job_id)
            if pair["judgement_status"] != "succeeded"
        ]

        async def request_batch(batch: list[dict[str, Any]]) -> None:
            payload = []
            public_to_pair_id: dict[str, int] = {}
            for pair in batch:
                left = records[pair["record_a_id"]]
                right = records[pair["record_b_id"]]
                public_id = f"{pair['record_a_id']}|{pair['record_b_id']}"
                public_to_pair_id[public_id] = int(pair["id"])
                payload.append(
                    {
                        "pair_id": public_id,
                        "candidate_reason": ",".join(pair["recall_reasons"] or []),
                        "a": _judgement_record(left),
                        "b": _judgement_record(right),
                    }
                )
            async with self._judgement_semaphore:
                response: JudgementBatchResponse = await self.judgement_llm_client.chat_json(
                    build_judgement_messages(payload),
                    JudgementBatchResponse,
                )
            returned = {
                public_to_pair_id[item.pair_id]: {
                    "decision": item.decision,
                    "confidence": item.confidence,
                    "hard_conflicts": item.hard_conflicts,
                    "event_name": item.event_name,
                    "evidence": {
                        "evidence_a": item.evidence_a,
                        "evidence_b": item.evidence_b,
                        "reason": item.reason,
                        "matrix": item.matrix.model_dump(),
                    },
                }
                for item in response.pairs
                if item.pair_id in public_to_pair_id
            }
            missing = {int(pair["id"]) for pair in batch} - returned.keys()
            if missing:
                raise ValueError(f"模型未返回候选对: {sorted(missing)}")
            await self.database.save_judgements(job_id, returned)

        async def process_batch(batch: list[dict[str, Any]]) -> None:
            await _run_with_batch_split(batch, request_batch)

        return await _run_batches(
            _chunks(pending, self.judgement_batch_size),
            process_batch,
            concurrency=min(self.judgement_concurrency, self.max_inflight_batches_per_job),
            database=self.database,
            job_id=job_id,
            stage="judgement",
            item_ids=lambda batch: [int(pair["id"]) for pair in batch],
        )

    async def _cluster_candidate_events(self, job_id: str) -> None:
        records = {int(record["id"]): record for record in await self.database.list_records(job_id)}
        pairs = await self.database.list_candidate_pairs(job_id)
        edges = [
            CandidateEdge(
                int(pair["record_a_id"]),
                int(pair["record_b_id"]),
                weight=float(pair.get("rerank_score") or pair.get("vector_score") or 0),
                hard_conflicts=tuple(pair.get("hard_conflicts_json") or ()),
            )
            for pair in pairs
        ]
        cannot_links = {
            frozenset((str(pair["record_a_id"]), str(pair["record_b_id"])))
            for pair in pairs
            if pair.get("hard_conflicts_json") or pair.get("rule_status") == "excluded"
        }
        supporting_edges = {
            tuple(sorted((str(pair["record_a_id"]), str(pair["record_b_id"]))))
            for pair in pairs
            if not pair.get("hard_conflicts_json") and pair.get("rule_status") != "excluded"
        }
        components = build_constrained_components(
            records,
            edges,
            cannot_links={(int(pair["record_a_id"]), int(pair["record_b_id"])) for pair in pairs if pair.get("hard_conflicts_json") or pair.get("rule_status") == "excluded"},
            max_size=self.event_component_max_size,
        )
        values: list[dict[str, Any]] = []
        for component in components:
            if len(component) == 1:
                record = records[component[0]]
                values.append(_singleton_candidate_event(record))
                continue
            for batch in _chunks(component, self.event_raw_group_limit):
                batch_records = [records[record_id] for record_id in batch]
                payload = [_event_record(record) for record in batch_records]
                batch_cannot_links = [
                    tuple(link)
                    for link in cannot_links
                    if link.issubset({str(record_id) for record_id in batch})
                ]
                async with self._event_llm_semaphore:
                    response: EventClusterResponse = await self.judgement_llm_client.chat_json(
                        build_event_cluster_messages(payload, cannot_links=batch_cannot_links),
                        EventClusterResponse,
                    )
                validate_event_cluster_coverage(response, [str(record_id) for record_id in batch])
                extraction_by_id = {
                    str(record_id): records[record_id].get("extraction_json") or {}
                    for record_id in batch
                }
                for event in response.events:
                    auto_merge_eligible = self.auto_merge_enabled and can_auto_merge_event(
                        event,
                        threshold=self.auto_merge_confidence,
                        max_members=self.auto_merge_max_members,
                        cannot_links=cannot_links,
                        extractions=extraction_by_id,
                        supporting_edges=supporting_edges,
                    )
                    auto_merged = False
                    if auto_merge_eligible:
                        async with self._event_llm_semaphore:
                            consistency: EventClusterResponse = await self.judgement_llm_client.chat_json(
                                build_event_cluster_messages(
                                    [_event_record(records[int(member.record_id)]) for member in event.members],
                                    cannot_links=batch_cannot_links,
                                ),
                                EventClusterResponse,
                            )
                        validate_event_cluster_coverage(
                            consistency, [member.record_id for member in event.members]
                        )
                        auto_merged = _same_event_partition(event, consistency)
                    values.append(
                        {
                            "name": event.name,
                            "status": "auto_merged" if auto_merged else "review",
                            "confidence": event.confidence,
                            "evidence": event.evidence,
                            "members": [
                                {
                                    "record_id": int(member.record_id),
                                    "confidence": member.confidence,
                                    "role": member.role,
                                    "assignment_source": "model",
                                }
                                for member in event.members
                            ],
                        }
                    )
                for record_id in response.outliers:
                    values.append(_singleton_candidate_event(records[int(record_id)]))
        await self.database.replace_candidate_events(
            job_id,
            _merge_matching_microclusters(values, supporting_edges, cannot_links),
        )

    async def _pause_requested(self, job_id: str) -> bool:
        job = await self.database.get_job(job_id)
        if not job["pause_requested"]:
            return False
        await self.database.set_job_state(job_id, status="paused", stage=job["stage"])
        return True


async def _run_batches(
    batches: list[list[T]],
    operation: Callable[[list[T]], Awaitable[None]],
    *,
    concurrency: int,
    database: AsyncDatabase | None = None,
    job_id: str | None = None,
    stage: str | None = None,
    item_ids: Callable[[list[T]], list[int]] | None = None,
) -> bool:
    errors: list[BaseException] = []
    next_batch = 0
    next_batch_lock = asyncio.Lock()
    paused = False
    cancelled = False

    async def pause_requested() -> bool:
        if not database or not job_id:
            return False
        return bool((await database.get_job(job_id))["pause_requested"])

    async def worker() -> None:
        nonlocal next_batch, paused, cancelled
        while True:
            if cancelled:
                return
            if await pause_requested():
                paused = True
                return
            async with next_batch_lock:
                if next_batch >= len(batches):
                    return
                batch_index = next_batch
                next_batch += 1
            batch = batches[batch_index]
            if await pause_requested():
                paused = True
                return
            should_run = True
            if database and job_id and stage:
                should_run = await database.start_batch(
                    job_id,
                    stage,
                    batch_index,
                    item_ids(batch) if item_ids else [],
                )
            if not should_run:
                continue
            try:
                await operation(batch)
            except asyncio.CancelledError:
                cancelled = True
                raise
            except BaseException as exc:
                if database and job_id and stage:
                    await database.fail_batch(job_id, stage, batch_index, str(exc))
                errors.append(exc)
            else:
                if database and job_id and stage:
                    await database.finish_batch(job_id, stage, batch_index)

    async with asyncio.TaskGroup() as group:
        for _ in range(min(concurrency, len(batches))):
            group.create_task(worker())
    if cancelled:
        raise asyncio.CancelledError
    if errors:
        raise errors[0]
    return paused


def _chunks(values: list[T], size: int) -> list[list[T]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


async def _run_with_batch_split(
    batch: list[T],
    operation: Callable[[list[T]], Awaitable[None]],
) -> None:
    try:
        await operation(batch)
    except (LlmResponseError, ValueError):
        if len(batch) <= 1:
            raise
        midpoint = len(batch) // 2
        await _run_with_batch_split(batch[:midpoint], operation)
        await _run_with_batch_split(batch[midpoint:], operation)


def _judgement_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "work_order_id": record["work_order_id"],
        "title": record["title"],
        "appeal_text": record["appeal_text"],
        "extraction": record["extraction_json"] or {},
    }


def _event_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "record_id": str(record["id"]),
        "work_order_id": record.get("work_order_id"),
        "title": record.get("title"),
        "category": record.get("category"),
        "category_levels": [
            record.get(f"category_level_{level}") for level in range(1, 5)
        ],
        "appeal_text": record.get("appeal_text"),
        "received_at": record.get("received_at"),
        "region": record.get("region"),
        "street": record.get("street"),
        "extraction": record.get("extraction_json") or {},
    }


def _singleton_candidate_event(record: dict[str, Any]) -> dict[str, Any]:
    extraction = record.get("extraction_json") or {}
    subject = extraction.get("subject") or {}
    address = extraction.get("address") or {}
    issues = extraction.get("issues") or {}
    scope = next(
        (
            value
            for value in (
                subject.get("branch"),
                subject.get("full_name"),
                address.get("landmark"),
                address.get("road"),
                record.get("title"),
            )
            if value
        ),
        "未识别地点",
    )
    issue = issues.get("primary") or record.get("category") or "未识别问题"
    name = "｜".join(
        str(value)
        for value in (record.get("region"), record.get("street"), scope, issue)
        if value
    )
    return {
        "name": name,
        "status": "singleton",
        "confidence": None,
        "evidence": ["未召回到可合并候选，保留为单例事件"],
        "members": [
            {
                "record_id": int(record["id"]),
                "confidence": None,
                "role": "anchor",
                "assignment_source": "singleton",
            }
        ],
    }


def _merge_matching_microclusters(
    values: list[dict[str, Any]],
    supporting_edges: set[tuple[str, str]],
    cannot_links: set[frozenset[str]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for value in values:
        if value.get("status") == "singleton":
            merged.append(value)
            continue
        member_ids = {str(member["record_id"]) for member in value["members"]}
        target = next(
            (
                existing
                for existing in merged
                if existing.get("status") != "singleton"
                and existing.get("name") == value.get("name")
                and _microclusters_connected(existing, member_ids, supporting_edges)
                and not _microclusters_blocked(existing, member_ids, cannot_links)
            ),
            None,
        )
        if target is None:
            merged.append(value)
            continue
        target["members"].extend(value["members"])
        target["confidence"] = min(target.get("confidence") or 0, value.get("confidence") or 0)
        target["status"] = "review"
        target["evidence"] = list(dict.fromkeys([*(target.get("evidence") or []), *(value.get("evidence") or []), "跨微簇存在召回边，名称一致，合并后进入人工复核"]))
    return merged


def _microclusters_connected(
    event: dict[str, Any],
    member_ids: set[str],
    supporting_edges: set[tuple[str, str]],
) -> bool:
    existing_ids = {str(member["record_id"]) for member in event["members"]}
    normalized_edges = {frozenset(edge) for edge in supporting_edges}
    return any(
        frozenset((left, right)) in normalized_edges
        for left in existing_ids
        for right in member_ids
    )


def _microclusters_blocked(
    event: dict[str, Any],
    member_ids: set[str],
    cannot_links: set[frozenset[str]],
) -> bool:
    existing_ids = {str(member["record_id"]) for member in event["members"]}
    return any(
        frozenset((left, right)) in cannot_links
        for left in existing_ids
        for right in member_ids
    )


def _same_event_partition(event: Any, response: EventClusterResponse) -> bool:
    if response.outliers or len(response.events) != 1:
        return False
    expected = {str(member.record_id) for member in event.members}
    returned = {str(member.record_id) for member in response.events[0].members}
    return expected == returned


async def apply_hard_rules_to_pairs(database: AsyncDatabase, job_id: str) -> None:
    records = {record["id"]: record for record in await database.list_records(job_id)}
    exclusions: dict[int, list[str]] = {}
    for pair in await database.list_candidate_pairs(job_id):
        if pair["judgement_status"] == "succeeded":
            continue
        left = records[pair["record_a_id"]]["extraction_json"] or {}
        right = records[pair["record_b_id"]]["extraction_json"] or {}
        conflicts = detect_hard_conflicts(left, right)
        if conflicts:
            exclusions[int(pair["id"])] = conflicts
    await database.mark_rule_exclusions(job_id, exclusions)
