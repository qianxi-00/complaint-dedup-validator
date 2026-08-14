import asyncio
import json
from pathlib import Path

import pytest

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.async_pipeline import AsyncJobProcessor, _run_batches
from complaint_dedup.llm_models import (
    EventClusterResponse,
    ExtractionBatchResponse,
    JudgementBatchResponse,
)
from complaint_dedup.llm_client import LlmResponseError
from complaint_dedup.pipeline import InputRecord


class FakeEmbeddingClient:
    def __init__(self) -> None:
        self.calls = 0

    async def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        return [[float(index + 1), 0.0] for index, _ in enumerate(texts)]


class FakeVectorStore:
    def __init__(self) -> None:
        self.rows: list[dict] = []
        self.search_calls = 0

    async def upsert_records(self, values: list[dict]) -> None:
        self.rows.extend(values)

    async def search(self, *, record_id: int, vector_kind: str, limit: int, target_source: str | None):
        self.search_calls += 1
        ids = [int(row["id"]) for row in self.rows if int(row["id"]) != record_id]
        return [(candidate_id, 0.9 - index * 0.1) for index, candidate_id in enumerate(ids[:limit])]


class FakeReranker:
    async def rerank(self, query: str, documents: list[str]):
        return [(index, 0.95 - index * 0.1) for index in range(len(documents))]


class FakeLlmClient:
    async def chat_json(self, messages, response_model):
        payload = json.loads(messages[-1]["content"].split("\n", 1)[1].rsplit("\n", 1)[0])
        if response_model is ExtractionBatchResponse:
            return response_model.model_validate(
                {
                    "records": [
                        {
                            "record_id": item["record_id"],
                            "subject": {"keys": [item["title"]]},
                            "address": {"district": "江海区", "precision": "coarse"},
                            "issues": {"primary": "拖欠工资"},
                        }
                        for item in payload
                    ]
                }
            )
        assert all("|" in item["pair_id"] for item in payload)
        return JudgementBatchResponse.model_validate(
            {
                "pairs": [
                    {
                        "pair_id": item["pair_id"],
                        "decision": "review",
                        "confidence": 0.7,
                        "subject_relation": "unknown",
                        "address_relation": "coarse",
                        "issue_relation": "same",
                        "new_independent_issue": False,
                        "reason": "需要人工复核",
                        "event_name": "江海区｜拖欠工资",
                    }
                    for item in payload
                ]
            }
        )


class FakeEventLlmClient(FakeLlmClient):
    def __init__(self) -> None:
        self.event_calls = 0

    async def chat_json(self, messages, response_model):
        if response_model is not EventClusterResponse:
            return await super().chat_json(messages, response_model)
        payload = json.loads(messages[-1]["content"].split("\n", 1)[1].rsplit("\n", 1)[0])
        self.event_calls += 1
        record_ids = [str(item["record_id"]) for item in payload["records"]]
        return response_model.model_validate(
            {
                "events": [
                    {
                        "temporary_id": "event-1",
                        "name": "江海区｜外海街道｜甲公司｜拖欠工资",
                        "confidence": 0.97,
                        "members": [
                            {"record_id": record_id, "confidence": 0.97, "role": "member"}
                            for record_id in record_ids
                        ],
                        "evidence": ["主体、地点和核心问题一致"],
                    },
                ],
                "outliers": [],
            }
        )


@pytest.mark.asyncio
async def test_single_job_runs_async_pipeline_to_review_ready(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    embedding = FakeEmbeddingClient()
    vector_store = FakeVectorStore()
    processor = AsyncJobProcessor(
        database=database,
        llm_client=FakeLlmClient(),
        embedding_client=embedding,
        rerank_client=FakeReranker(),
        vector_store=vector_store,
        extraction_batch_size=2,
        judgement_batch_size=2,
        extraction_concurrency=2,
        judgement_concurrency=2,
        vector_top_k=5,
        rerank_top_n=2,
        max_candidates_per_record=2,
        max_inflight_batches_per_job=2,
    )
    records = [
        InputRecord("S", 2, "1", "标题甲", "拖欠工资", "内容甲"),
        InputRecord("S", 3, "2", "标题乙", "拖欠工资", "内容乙"),
        InputRecord("S", 4, "3", "标题丙", "拖欠工资", "内容丙"),
    ]

    job_id = await processor.create_job("单文件测试", records, mode="single")
    await processor.process(job_id)
    await processor.process(job_id)

    job = await database.get_job(job_id)
    pairs = await database.list_candidate_pairs(job_id)
    event_members = await database.list_event_members(job_id)
    assert job["status"] == "review_ready"
    assert job["stage"] == "review"
    assert job["extracted_records"] == 3
    assert job["judged_count"] == len(pairs) == 3
    assert all(pair["llm_decision"] == "review" for pair in pairs)
    assert len(event_members) == 3
    assert embedding.calls == 2
    assert vector_store.search_calls == 6
    assert all(batch["status"] == "succeeded" for batch in await database.list_batches(job_id))
    await database.close()


@pytest.mark.asyncio
async def test_event_cluster_v2_creates_group_level_candidates(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'events.db'}")
    await database.initialize()
    event_client = FakeEventLlmClient()
    processor = AsyncJobProcessor(
        database=database,
        llm_client=event_client,
        embedding_client=FakeEmbeddingClient(),
        rerank_client=FakeReranker(),
        vector_store=FakeVectorStore(),
        extraction_batch_size=2,
        judgement_batch_size=2,
        extraction_concurrency=1,
        judgement_concurrency=1,
        vector_top_k=5,
        rerank_top_n=2,
        max_candidates_per_record=2,
        max_inflight_batches_per_job=1,
        pipeline_version="event_cluster_v2",
        event_llm_concurrency=1,
        event_raw_group_limit=20,
        event_component_max_size=200,
        auto_merge_enabled=True,
        auto_merge_confidence=0.95,
        auto_merge_max_members=20,
    )
    records = [
        InputRecord("S", 2, "1", "江海区外海街道甲公司拖欠工资", "拖欠工资", "地址：甲公司。拖欠工资。"),
        InputRecord("S", 3, "2", "江海区外海街道甲公司欠薪", "拖欠工资", "地址：甲公司。仍未发工资。"),
    ]

    job_id = await processor.create_job("事件簇测试", records, mode="single")
    await processor.process(job_id)

    job = await database.get_job(job_id)
    events = await database.list_candidate_events(job_id, show_singletons=True)
    assert job["pipeline_version"] == "event_cluster_v2"
    assert len(events) == 1
    assert events[0]["member_count"] == 2
    assert events[0]["status"] == "auto_merged"
    assert event_client.event_calls == 2
    assert await database.count_groups(job_id, merged_only=True) == 1
    await database.close()


@pytest.mark.asyncio
async def test_event_cluster_v2_rejoins_matching_microclusters_across_raw_limit(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'large-event.db'}")
    await database.initialize()
    processor = AsyncJobProcessor(
        database=database,
        llm_client=FakeEventLlmClient(),
        embedding_client=FakeEmbeddingClient(),
        rerank_client=FakeReranker(),
        vector_store=FakeVectorStore(),
        extraction_batch_size=3,
        judgement_batch_size=2,
        extraction_concurrency=1,
        judgement_concurrency=1,
        vector_top_k=5,
        rerank_top_n=2,
        max_candidates_per_record=2,
        max_inflight_batches_per_job=1,
        pipeline_version="event_cluster_v2",
        event_llm_concurrency=1,
        event_raw_group_limit=2,
        event_component_max_size=200,
        auto_merge_enabled=False,
    )
    records = [
        InputRecord("S", index + 2, str(index), f"甲公司欠薪{index}", "拖欠工资", "地址：甲公司。拖欠工资。")
        for index in range(3)
    ]

    job_id = await processor.create_job("大簇微簇归并", records, mode="single")
    await processor.process(job_id)

    events = await database.list_candidate_events(job_id, show_singletons=True)
    assert len(events) == 1
    assert events[0]["member_count"] == 3
    await database.close()


@pytest.mark.asyncio
async def test_create_job_persists_match_parameters(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    processor = AsyncJobProcessor(
        database=database,
        llm_client=FakeLlmClient(),
        embedding_client=FakeEmbeddingClient(),
        rerank_client=FakeReranker(),
        vector_store=FakeVectorStore(),
        extraction_batch_size=1,
        judgement_batch_size=1,
        extraction_concurrency=1,
        judgement_concurrency=1,
        vector_top_k=2,
        rerank_top_n=1,
        max_candidates_per_record=1,
        max_inflight_batches_per_job=1,
    )

    job_id = await processor.create_job(
        "严格匹配任务",
        [InputRecord("S", 2, "1", "标题", "事项", "内容")],
        mode="single",
        match_preset="strict",
        time_window_days=14,
    )

    job = await database.get_job(job_id)
    assert job["match_preset"] == "strict"
    assert job["time_window_days"] == 14
    await database.close()


@pytest.mark.asyncio
async def test_create_job_persists_category_levels(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'categories.db'}")
    await database.initialize()
    processor = AsyncJobProcessor(
        database=database,
        llm_client=FakeLlmClient(),
        embedding_client=FakeEmbeddingClient(),
        rerank_client=FakeReranker(),
        vector_store=FakeVectorStore(),
        extraction_batch_size=1,
        judgement_batch_size=1,
        extraction_concurrency=1,
        judgement_concurrency=1,
        vector_top_k=2,
        rerank_top_n=1,
        max_candidates_per_record=1,
        max_inflight_batches_per_job=1,
    )
    record = InputRecord(
        "S", 2, "1", "标题", "最终事项", "正文",
        category_level_1="一级",
        category_level_2="二级",
        category_level_3="三级",
        category_level_4="四级",
    )

    job_id = await processor.create_job("事项字段", [record], mode="single")

    saved = (await database.list_records(job_id))[0]
    assert [saved[f"category_level_{level}"] for level in range(1, 5)] == [
        "一级", "二级", "三级", "四级"
    ]
    await database.close()


class PausingBatchDatabase:
    def __init__(self) -> None:
        self.pause_requested = False
        self.started: list[int] = []
        self.finished: list[int] = []

    async def get_job(self, _job_id: str) -> dict:
        return {"pause_requested": self.pause_requested}

    async def start_batch(
        self, _job_id: str, _stage: str, batch_index: int, _item_ids: list[int]
    ) -> bool:
        self.started.append(batch_index)
        return True

    async def finish_batch(self, _job_id: str, _stage: str, batch_index: int) -> None:
        self.finished.append(batch_index)

    async def fail_batch(
        self, _job_id: str, _stage: str, _batch_index: int, _error: str
    ) -> None:
        raise AssertionError("不应失败")


@pytest.mark.asyncio
async def test_run_batches_stops_claiming_new_batches_after_pause() -> None:
    database = PausingBatchDatabase()
    processed: list[int] = []

    async def operation(batch: list[int]) -> None:
        processed.extend(batch)
        database.pause_requested = True

    paused = await _run_batches(
        [[1], [2], [3]],
        operation,
        concurrency=1,
        database=database,
        job_id="job-1",
        stage="extraction",
        item_ids=lambda batch: batch,
    )

    assert paused is True
    assert processed == [1]
    assert database.started == [0]
    assert database.finished == [0]


@pytest.mark.asyncio
async def test_run_batches_propagates_cancellation_without_recording_failure() -> None:
    class CancellationDatabase(PausingBatchDatabase):
        def __init__(self) -> None:
            super().__init__()
            self.failed = False

        async def fail_batch(self, *_args) -> None:
            self.failed = True

    database = CancellationDatabase()

    async def operation(_batch: list[int]) -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await _run_batches(
            [[1]], operation, concurrency=1, database=database, job_id="job-1", stage="extraction", item_ids=lambda batch: batch
        )
    assert database.failed is False


class ConcurrentExtractionDatabase:
    def __init__(self) -> None:
        self.records = {
            "job-1": [
                {"id": 1, "title": "甲", "category": "事项", "appeal_text": "内容", "received_at": None, "extraction_status": "pending"},
                {"id": 2, "title": "乙", "category": "事项", "appeal_text": "内容", "received_at": None, "extraction_status": "pending"},
            ],
            "job-2": [
                {"id": 3, "title": "丙", "category": "事项", "appeal_text": "内容", "received_at": None, "extraction_status": "pending"},
                {"id": 4, "title": "丁", "category": "事项", "appeal_text": "内容", "received_at": None, "extraction_status": "pending"},
            ],
        }

    async def list_records(self, job_id: str) -> list[dict]:
        return self.records[job_id]

    async def list_candidate_pairs(self, job_id: str) -> list[dict]:
        ids = [record["id"] for record in self.records[job_id]]
        return [{"record_a_id": ids[0], "record_b_id": ids[1]}]

    async def get_job(self, _job_id: str) -> dict:
        return {"pause_requested": False}

    async def start_batch(self, *_args) -> bool:
        return True

    async def finish_batch(self, *_args) -> None:
        return None

    async def fail_batch(self, *_args) -> None:
        raise AssertionError("不应失败")

    async def save_extractions(self, _job_id: str, _values: dict) -> None:
        return None


class ConcurrentExtractionClient:
    def __init__(self) -> None:
        self.active = 0
        self.maximum = 0

    async def chat_json(self, messages, response_model):
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        try:
            await asyncio.sleep(0.02)
            payload = json.loads(messages[-1]["content"].split("\n", 1)[1].rsplit("\n", 1)[0])
            return response_model.model_validate(
                {
                    "records": [
                        {
                            "record_id": item["record_id"],
                            "subject": {"keys": [item["title"]]},
                            "address": {"precision": "coarse"},
                            "issues": {"primary": "事项"},
                        }
                        for item in payload
                    ]
                }
            )
        finally:
            self.active -= 1


@pytest.mark.asyncio
async def test_extraction_concurrency_is_global_across_jobs() -> None:
    database = ConcurrentExtractionDatabase()
    extraction_client = ConcurrentExtractionClient()
    processor = AsyncJobProcessor(
        database=database,
        llm_client=extraction_client,
        extraction_llm_client=extraction_client,
        judgement_llm_client=FakeLlmClient(),
        embedding_client=FakeEmbeddingClient(),
        rerank_client=FakeReranker(),
        vector_store=FakeVectorStore(),
        extraction_batch_size=1,
        judgement_batch_size=1,
        extraction_concurrency=1,
        judgement_concurrency=1,
        vector_top_k=2,
        rerank_top_n=1,
        max_candidates_per_record=1,
        max_inflight_batches_per_job=2,
    )

    await asyncio.gather(
        processor._extract_candidate_records("job-1"),
        processor._extract_candidate_records("job-2"),
    )

    assert extraction_client.maximum == 1


class SplittingExtractionClient(FakeLlmClient):
    def __init__(self) -> None:
        self.batch_sizes: list[int] = []

    async def chat_json(self, messages, response_model):
        payload = json.loads(messages[-1]["content"].split("\n", 1)[1].rsplit("\n", 1)[0])
        self.batch_sizes.append(len(payload))
        if response_model is ExtractionBatchResponse and len(payload) > 2:
            raise LlmResponseError("批量响应过大，无法校验")
        return await super().chat_json(messages, response_model)


@pytest.mark.asyncio
async def test_extraction_splits_failed_batch_and_saves_successful_halves(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'split.db'}")
    await database.initialize()
    await database.enqueue_job("job-split", "拆分测试", mode="single", total_records=4)
    record_ids = await database.add_records(
        "job-split",
        [
            {"source": "S", "source_row": index + 2, "title": f"标题{index}", "category": "事项", "appeal_text": f"内容{index}"}
            for index in range(4)
        ],
    )
    await database.upsert_candidate_pairs(
        "job-split",
        [
            {"record_a_id": record_ids[0], "record_b_id": record_ids[1], "recall_reason": "test"},
            {"record_a_id": record_ids[2], "record_b_id": record_ids[3], "recall_reason": "test"},
        ],
    )
    client = SplittingExtractionClient()
    processor = AsyncJobProcessor(
        database=database,
        llm_client=client,
        embedding_client=FakeEmbeddingClient(),
        rerank_client=FakeReranker(),
        vector_store=FakeVectorStore(),
        extraction_batch_size=4,
        judgement_batch_size=2,
        extraction_concurrency=1,
        judgement_concurrency=1,
        vector_top_k=2,
        rerank_top_n=1,
        max_candidates_per_record=1,
        max_inflight_batches_per_job=1,
    )

    await processor._extract_candidate_records("job-split")

    records = await database.list_records("job-split")
    assert client.batch_sizes == [4, 2, 2]
    assert all(record["extraction_status"] == "succeeded" for record in records)
    assert all(batch["status"] == "succeeded" for batch in await database.list_batches("job-split"))
    await database.close()
