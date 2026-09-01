import json
from pathlib import Path

import pytest
from sqlalchemy import text

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_pipeline import CorpusProcessor
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.llm_models import NormalizationBatchResponse
from complaint_dedup.corpus_models import InputRecord


class FakeNormalizationClient:
    def __init__(self) -> None:
        self.calls = 0

    async def chat_json(self, messages, response_model):
        self.calls += 1
        payload = json.loads(messages[-1]["content"])
        decisions = []
        for item in payload:
            anchor_id = (
                item["anchor_candidates"][0]["id"]
                if item["anchor_candidates"]
                else None
            )
            issue_id = (
                item["issue_candidates"][0]["id"]
                if item["issue_candidates"]
                else None
            )
            decisions.append(
                {
                    "record_id": item["record_id"],
                    "anchor_id": anchor_id,
                    "issue_id": issue_id,
                    "anchor_confidence": 0.98,
                    "issue_confidence": 0.98,
                    "reason": "候选地点和核心问题与历史标准项一致",
                }
            )
        return response_model.model_validate({"decisions": decisions})


def _record(row: int, *, source: str, address: str, issue: str, received: str) -> InputRecord:
    return InputRecord(
        source=source,
        source_row=row,
        work_order_id=f"WO-{source}-{row}",
        received_at=received,
        title=issue,
        category=issue,
        category_level_4=issue,
        appeal_text=f"地址：{address}。\n事项：{issue}。",
        raw_fields={"事发地点": address},
    )


@pytest.mark.asyncio
async def test_daily_unknown_anchor_and_issue_use_llm_and_are_audited(tmp_path: Path):
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'llm.db'}")
    await database.initialize()
    repository = CorpusRepository(database)
    client = FakeNormalizationClient()
    processor = CorpusProcessor(
        repository,
        normalization_llm_client=client,
        normalization_llm_enabled=True,
        normalization_llm_min_confidence=0.9,
        normalization_llm_batch_size=10,
    )

    history = await processor.stage_records(
        name="历史",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="1" * 64,
        records=[
            _record(
                2,
                source="B",
                address="江海区礼乐街道德昌电机门口",
                issue="消费纠纷",
                received="2026-08-12 08:00:00",
            )
        ],
    )
    await processor.approve_bootstrap(history.batch_id, history.dictionary_version_id, approved_by="test")

    daily = await processor.stage_records(
        name="每日",
        batch_type="daily_increment",
        file_name="daily.xlsx",
        file_hash="2" * 64,
        records=[
            _record(
                2,
                source="A",
                address="江海区礼乐街道德昌电机门前",
                issue="消费争议",
                received="2026-08-13 08:00:00",
            )
        ],
    )

    assert client.calls == 1
    row = (await repository.records_for_batch(daily.batch_id))[0]
    assert row["anchor_resolution_status"] == "llm"
    assert row["issue_resolution_status"] == "llm"
    await processor.commit_increment(daily.batch_id)

    async with database.engine.connect() as connection:
        decisions = await connection.execute(
            text("select dimension, method from normalization_decisions where record_id=:id order by dimension"),
            {"id": row["id"]},
        )
        assert {(item.dimension, item.method) for item in decisions} == {
            ("anchor", "llm"),
            ("issue", "llm"),
        }
    assert len(await repository.list_events()) == 1
    assert await repository.event_member_ids((await repository.list_events())[0]["id"]) == [1, 2]
    await database.close()
