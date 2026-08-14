from pathlib import Path

import pytest

from complaint_dedup.async_database import AsyncDatabase


async def make_database(tmp_path: Path) -> AsyncDatabase:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    return database


@pytest.mark.asyncio
async def test_single_file_pairs_are_canonical_and_deduplicated(tmp_path: Path) -> None:
    database = await make_database(tmp_path)
    await database.enqueue_job("job-1", "单文件", mode="single", total_records=3)
    record_ids = await database.add_records(
        "job-1",
        [
            {"source": "S", "source_row": 2, "title": "甲"},
            {"source": "S", "source_row": 3, "title": "乙"},
            {"source": "S", "source_row": 4, "title": "丙"},
        ],
    )

    await database.upsert_candidate_pairs(
        "job-1",
        [
            {"record_a_id": record_ids[1], "record_b_id": record_ids[0], "recall_reason": "vector"},
            {"record_a_id": record_ids[0], "record_b_id": record_ids[1], "recall_reason": "subject"},
            {"record_a_id": record_ids[0], "record_b_id": record_ids[2], "recall_reason": "vector"},
        ],
    )

    pairs = await database.list_candidate_pairs("job-1")
    assert len(pairs) == 2
    assert pairs[0]["record_a_id"] == min(record_ids[:2])
    assert pairs[0]["record_b_id"] == max(record_ids[:2])
    assert set(pairs[0]["recall_reasons"]) == {"vector", "subject"}
    assert (await database.get_job("job-1"))["candidate_count"] == 2
    await database.close()


@pytest.mark.asyncio
async def test_single_file_rejects_self_pair(tmp_path: Path) -> None:
    database = await make_database(tmp_path)
    await database.enqueue_job("job-1", "单文件", mode="single", total_records=1)
    record_id = (
        await database.add_records("job-1", [{"source": "S", "source_row": 2, "title": "甲"}])
    )[0]

    with pytest.raises(ValueError, match="自身"):
        await database.upsert_candidate_pairs(
            "job-1",
            [{"record_a_id": record_id, "record_b_id": record_id, "recall_reason": "vector"}],
        )
    await database.close()


@pytest.mark.asyncio
async def test_cross_file_pairs_require_a_and_b_sources(tmp_path: Path) -> None:
    database = await make_database(tmp_path)
    await database.enqueue_job("job-1", "双文件", mode="cross", total_records=3)
    a1, a2, b1 = await database.add_records(
        "job-1",
        [
            {"source": "A", "source_row": 2, "title": "甲"},
            {"source": "A", "source_row": 3, "title": "乙"},
            {"source": "B", "source_row": 2, "title": "丙"},
        ],
    )

    with pytest.raises(ValueError, match="跨表"):
        await database.upsert_candidate_pairs(
            "job-1",
            [{"record_a_id": a1, "record_b_id": a2, "recall_reason": "vector"}],
        )
    await database.upsert_candidate_pairs(
        "job-1",
        [{"record_a_id": b1, "record_b_id": a1, "recall_reason": "vector"}],
    )

    pair = (await database.list_candidate_pairs("job-1"))[0]
    assert pair["record_a_id"] == a1
    assert pair["record_b_id"] == b1
    await database.close()


@pytest.mark.asyncio
async def test_pair_filters_apply_to_list_and_count(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    await database.enqueue_job("job-1", "筛选任务", mode="single", total_records=3)
    first, second, third = await database.add_records(
        "job-1",
        [
            {"source": "S", "source_row": 2, "title": "甲", "region": "江海区", "category": "劳动保障/拖欠工资"},
            {"source": "S", "source_row": 3, "title": "乙", "region": "江海区", "category": "劳动保障/拖欠工资"},
            {"source": "S", "source_row": 4, "title": "丙", "region": "蓬江区", "category": "环境保护/噪音扰民"},
        ],
    )
    await database.upsert_candidate_pairs(
        "job-1",
        [
            {"record_a_id": first, "record_b_id": second, "recall_reason": "hybrid_vector"},
            {"record_a_id": first, "record_b_id": third, "recall_reason": "phone_exact"},
        ],
    )
    pairs = await database.list_candidate_pairs("job-1")
    await database.save_judgements(
        "job-1",
        {
            pairs[0]["id"]: {"decision": "duplicate", "confidence": 0.91},
            pairs[1]["id"]: {"decision": "not_duplicate", "confidence": 0.98},
        },
    )
    await database.review_pair("job-1", pairs[0]["id"], "duplicate")

    assert [row["id"] for row in await database.list_pair_details("job-1")] == [pairs[0]["id"]]
    filters = {
        "include_not_duplicate": True,
        "region": "蓬江区",
        "category": "噪音",
        "recall_reason": "phone",
        "model_decision": "not_duplicate",
        "review_status": "pending",
        "min_confidence": 0.95,
    }
    filtered = await database.list_pair_details("job-1", **filters)
    assert [row["id"] for row in filtered] == [pairs[1]["id"]]
    assert filtered[0]["b_region"] == "蓬江区"
    assert filtered[0]["b_category"] == "环境保护/噪音扰民"
    assert await database.count_pairs("job-1", **filters) == 1
    await database.close()
