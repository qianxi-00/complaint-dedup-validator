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


@pytest.mark.asyncio
async def test_extraction_resume_keeps_original_batch_checkpoint(
    tmp_path: Path,
) -> None:
    database = tmp_path / "app.db"
    initialize_database(database)
    fake = FakeLlmClient(
        [
            ExtractionBatchResponse.model_validate(
                {"records": [extraction("B-2", "乙公司", "云沁路83号", "噪音")]}
            )
        ]
    )
    processor = JobProcessor(database, fake, 2, 20, 50, 200)
    job_id = processor.create_job(
        "checkpoint",
        [
            InputRecord("A", 2, "A001", "甲公司", "其他", "内容"),
            InputRecord("A", 3, "A002", "甲公司", "其他", "内容"),
        ],
        [InputRecord("B", 2, "B001", "乙公司", "其他", "内容")],
    )

    with connect_database(database) as connection:
        rows = connection.execute(
            "SELECT id FROM records WHERE job_id = ? ORDER BY id", (job_id,)
        ).fetchall()
        for row in rows[:2]:
            connection.execute(
                "UPDATE records SET extraction_status = 'succeeded', extraction_json = ? WHERE id = ?",
                (json.dumps(extraction("A-2", "甲公司", "金瓯路188号", "欠薪"), ensure_ascii=False), row["id"]),
            )
        connection.execute(
            "INSERT INTO llm_batches (job_id, batch_type, batch_index, status, attempts) VALUES (?, 'extraction', 0, 'succeeded', 1)",
            (job_id,),
        )

    await processor._extract_pending(job_id)

    with connect_database(database) as connection:
        batches = connection.execute(
            "SELECT batch_index, status FROM llm_batches WHERE job_id = ? AND batch_type = 'extraction' ORDER BY batch_index",
            (job_id,),
        ).fetchall()
    assert [(row["batch_index"], row["status"]) for row in batches] == [
        (0, "succeeded"),
        (1, "succeeded"),
    ]


@pytest.mark.asyncio
async def test_judgement_resume_keeps_original_batch_checkpoint(
    tmp_path: Path,
) -> None:
    database = tmp_path / "app.db"
    initialize_database(database)
    fake = FakeLlmClient([])
    processor = JobProcessor(database, fake, 20, 2, 50, 200)
    job_id = processor.create_job(
        "judgement-checkpoint",
        [
            InputRecord("A", 2, "A001", "甲公司", "其他", "内容"),
            InputRecord("A", 3, "A002", "甲公司", "其他", "内容"),
        ],
        [
            InputRecord("B", 2, "B001", "甲公司", "其他", "内容"),
            InputRecord("B", 3, "B002", "甲公司", "其他", "内容"),
        ],
    )

    with connect_database(database) as connection:
        records = connection.execute(
            "SELECT id, source FROM records WHERE job_id = ? ORDER BY id", (job_id,)
        ).fetchall()
        for row in records:
            connection.execute(
                "UPDATE records SET extraction_status = 'succeeded', extraction_json = ? WHERE id = ?",
                (json.dumps(extraction(f"{row['source']}-2", "甲公司", "金瓯路188号", "欠薪"), ensure_ascii=False), row["id"]),
            )
        a_ids = [row["id"] for row in records if row["source"] == "A"]
        b_ids = [row["id"] for row in records if row["source"] == "B"]
        connection.executemany(
            "INSERT INTO candidate_pairs (job_id, record_a_id, record_b_id, candidate_key, rule_status, judgement_status) VALUES (?, ?, ?, 'test', 'candidate', 'succeeded')",
            [(job_id, a_ids[0], b_ids[0]), (job_id, a_ids[1], b_ids[1])],
        )
        connection.execute(
            "UPDATE candidate_pairs SET llm_decision = 'not_duplicate' WHERE job_id = ? AND record_a_id = ?",
            (job_id, a_ids[0]),
        )
        connection.execute(
            "UPDATE candidate_pairs SET judgement_status = 'pending' WHERE job_id = ? AND record_a_id = ?",
            (job_id, a_ids[1]),
        )
        connection.execute(
            "INSERT INTO llm_batches (job_id, batch_type, batch_index, status, attempts) VALUES (?, 'judgement', 0, 'succeeded', 1)",
            (job_id,),
        )

    fake.responses.append(
        JudgementBatchResponse.model_validate(
            {
                "pairs": [
                    {
                        "pair_id": f"{a_ids[1]}|{b_ids[1]}",
                        "decision": "not_duplicate",
                        "confidence": 0.9,
                        "subject_relation": "same",
                        "address_relation": "exact",
                        "issue_relation": "different",
                        "new_independent_issue": False,
                        "hard_conflicts": [],
                        "reason": "测试",
                        "event_name": "测试事件",
                    }
                ]
            }
        )
    )

    await processor._judge_pending(job_id)

    with connect_database(database) as connection:
        batches = connection.execute(
            "SELECT batch_index, status FROM llm_batches WHERE job_id = ? AND batch_type = 'judgement' ORDER BY batch_index",
            (job_id,),
        ).fetchall()
    assert [(row["batch_index"], row["status"]) for row in batches] == [
        (0, "succeeded"),
        (1, "succeeded"),
    ]


def test_review_pair_rejects_cross_job_pair(tmp_path: Path) -> None:
    database = tmp_path / "app.db"
    initialize_database(database)
    processor = JobProcessor(database, FakeLlmClient([]), 20, 20, 50, 200)
    job_a = processor.create_job("a", [], [])
    job_b = processor.create_job(
        "b",
        [InputRecord("A", 2, "A", "甲", "其他", "内容")],
        [InputRecord("B", 2, "B", "甲", "其他", "内容")],
    )
    with connect_database(database) as connection:
        rows = connection.execute(
            "SELECT id FROM records WHERE job_id = ? ORDER BY id", (job_b,)
        ).fetchall()
        connection.execute(
            "INSERT INTO candidate_pairs (job_id, record_a_id, record_b_id, candidate_key, rule_status) VALUES (?, ?, ?, 'test', 'candidate')",
            (job_b, rows[0]["id"], rows[1]["id"]),
        )
        pair_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]

    with pytest.raises(KeyError):
        processor.review_pair(pair_id, "duplicate", job_id=job_a)


def test_failed_batch_preserves_raw_model_response(tmp_path: Path) -> None:
    database = tmp_path / "app.db"
    initialize_database(database)
    processor = JobProcessor(database, FakeLlmClient([]), 20, 20, 50, 200)
    job_id = processor.create_job("failure", [], [])
    processor._start_batch(job_id, "extraction", 0, [])

    processor._fail_batch(job_id, "extraction", 0, "invalid json", "raw model output")

    with connect_database(database) as connection:
        batch = connection.execute(
            "SELECT status, response_json, error_message FROM llm_batches WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    assert dict(batch) == {
        "status": "failed",
        "response_json": "raw model output",
        "error_message": "invalid json",
    }


def test_hard_conflict_cannot_be_confirmed_as_duplicate(tmp_path: Path) -> None:
    database = tmp_path / "app.db"
    initialize_database(database)
    processor = JobProcessor(database, FakeLlmClient([]), 20, 20, 50, 200)
    job_id = processor.create_job(
        "conflict",
        [InputRecord("A", 2, "A", "甲", "其他", "内容")],
        [InputRecord("B", 2, "B", "乙", "其他", "内容")],
    )
    with connect_database(database) as connection:
        rows = connection.execute("SELECT id FROM records ORDER BY id").fetchall()
        cursor = connection.execute(
            """
            INSERT INTO candidate_pairs (
                job_id, record_a_id, record_b_id, judgement_status,
                llm_decision, hard_conflicts_json
            ) VALUES (?, ?, ?, 'succeeded', 'not_duplicate', '["主体不同"]')
            """,
            (job_id, rows[0]["id"], rows[1]["id"]),
        )

    with pytest.raises(ValueError, match="硬冲突"):
        processor.review_pair(cursor.lastrowid, "duplicate", job_id=job_id)
