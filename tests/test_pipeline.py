import json
from pathlib import Path

import pytest

from complaint_dedup.database import connect_database, initialize_database
from complaint_dedup.llm_models import (
    ExtractionBatchResponse,
    JudgementBatchResponse,
)
from complaint_dedup.pipeline import InputRecord, JobProcessor


class FakeLlmClient:
    def __init__(self, responses: list[object]) -> None:
        self.responses = responses
        self.calls = 0

    async def chat_json(self, messages, response_model):
        response = self.responses[self.calls]
        self.calls += 1
        assert isinstance(response, response_model)
        return response


def extraction(record_id: str, subject: str, address: str, issue: str):
    return {
        "record_id": record_id,
        "subject": {"full_name": subject, "keys": [subject]},
        "address": {
            "precision": "exact",
            "exact_keys": [address],
            "coarse_keys": [address.split("号")[0]],
        },
        "issues": {"primary": issue},
    }


@pytest.mark.asyncio
async def test_job_pipeline_reaches_review_and_confirmed_pair_forms_group(
    tmp_path: Path,
) -> None:
    database = tmp_path / "app.db"
    initialize_database(database)
    fake = FakeLlmClient(
        [
            ExtractionBatchResponse.model_validate(
                {
                    "records": [
                        extraction("A-2", "甲公司", "金瓯路188号", "欠薪"),
                        extraction("B-2", "甲公司", "金瓯路188号", "欠薪"),
                        extraction("B-3", "乙公司", "云沁路83号", "欠薪"),
                    ]
                }
            ),
            JudgementBatchResponse.model_validate(
                {
                    "pairs": [
                        {
                            "pair_id": "1|2",
                            "decision": "duplicate",
                            "confidence": 0.95,
                            "subject_relation": "same",
                            "address_relation": "exact",
                            "issue_relation": "same",
                            "new_independent_issue": False,
                            "hard_conflicts": [],
                            "reason": "主体、地址和问题一致",
                            "event_name": "甲公司｜金瓯路188号｜欠薪",
                        }
                    ]
                }
            ),
        ]
    )
    processor = JobProcessor(
        database,
        fake,
        extraction_batch_size=20,
        judgement_batch_size=20,
        max_candidates_per_record=50,
        broad_key_max_matches=200,
    )
    job_id = processor.create_job(
        "test",
        [InputRecord("A", 2, "A001", "甲公司欠薪", "劳动", "地址：金瓯路188号。事项：欠薪")],
        [
            InputRecord("B", 2, "B001", "甲公司拖欠工资", "劳动", "地址：金瓯路188号。事项：工资未发"),
            InputRecord("B", 3, "B002", "乙公司欠薪", "劳动", "地址：云沁路83号。事项：欠薪"),
        ],
    )

    await processor.process(job_id)

    job = processor.get_job(job_id)
    pairs = processor.list_pairs(job_id)
    assert job["status"] == "review_ready"
    assert job["extracted_records"] == 3
    assert job["candidate_count"] == 1
    assert pairs[0]["llm_decision"] == "duplicate"

    processor.review_pair(pairs[0]["id"], "duplicate", "人工确认")

    groups = processor.list_groups(job_id)
    assert len(groups) == 1
    assert groups[0]["name"] == "甲公司｜金瓯路188号｜欠薪"
    assert groups[0]["member_count"] == 2


@pytest.mark.asyncio
async def test_processing_skips_successfully_extracted_records_on_resume(
    tmp_path: Path,
) -> None:
    database = tmp_path / "app.db"
    initialize_database(database)
    response = ExtractionBatchResponse.model_validate(
        {"records": [extraction("B-2", "乙公司", "云沁路83号", "噪音")]}
    )
    fake = FakeLlmClient([response])
    processor = JobProcessor(database, fake, 20, 20, 50, 200)
    job_id = processor.create_job(
        "resume",
        [InputRecord("A", 2, "A001", "甲公司", "其他", "内容")],
        [InputRecord("B", 2, "B001", "乙公司", "其他", "内容")],
    )
    with connect_database(database) as connection:
        row = connection.execute(
            "SELECT id FROM records WHERE job_id = ? AND source = 'A'", (job_id,)
        ).fetchone()
        connection.execute(
            "UPDATE records SET extraction_status = 'succeeded', extraction_json = ? WHERE id = ?",
            (
                json.dumps(extraction("A-2", "甲公司", "金瓯路188号", "欠薪"), ensure_ascii=False),
                row["id"],
            ),
        )

    await processor.process(job_id)

    assert fake.calls == 1
    assert processor.get_job(job_id)["extracted_records"] == 2
