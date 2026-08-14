from pathlib import Path

import pytest

from complaint_dedup.async_database import AsyncDatabase


async def prepare_job(tmp_path: Path):
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    await database.enqueue_job("job-1", "分组任务", mode="single", total_records=3)
    record_ids = await database.add_records(
        "job-1",
        [
            {"source": "S", "source_row": 2, "title": "甲公司拖欠工资", "region": "江海区", "street": "外海街道"},
            {"source": "S", "source_row": 3, "title": "甲公司欠薪", "region": "江海区", "street": "外海街道"},
            {"source": "S", "source_row": 4, "title": "乙公司噪音", "region": "江海区", "street": "礼乐街道"},
        ],
    )
    await database.upsert_candidate_pairs(
        "job-1",
        [
            {"record_a_id": record_ids[0], "record_b_id": record_ids[1], "recall_reason": "vector"},
            {"record_a_id": record_ids[1], "record_b_id": record_ids[2], "recall_reason": "vector"},
            {"record_a_id": record_ids[0], "record_b_id": record_ids[2], "recall_reason": "vector"},
        ],
    )
    return database, record_ids, await database.list_candidate_pairs("job-1")


@pytest.mark.asyncio
async def test_event_groups_include_singleton_records(tmp_path: Path) -> None:
    database, record_ids, _ = await prepare_job(tmp_path)

    await database.rebuild_event_groups("job-1")
    groups = await database.list_event_members("job-1")

    assert {row["record_id"] for row in groups} == set(record_ids)
    assert len({row["event_group_id"] for row in groups}) == 3
    assert all(row["event_name"] for row in groups)
    await database.close()


@pytest.mark.asyncio
async def test_merged_group_query_hides_singletons_and_supports_counting(tmp_path: Path) -> None:
    database, record_ids, pairs = await prepare_job(tmp_path)
    first_pair = next(
        pair
        for pair in pairs
        if {pair["record_a_id"], pair["record_b_id"]} == set(record_ids[:2])
    )
    await database.review_pair("job-1", first_pair["id"], "duplicate")

    groups = await database.list_groups(
        "job-1", limit=10, offset=0, merged_only=True
    )

    assert len(groups) == 1
    assert groups[0]["member_count"] == 2
    assert await database.count_groups("job-1", merged_only=True) == 1
    assert await database.count_groups("job-1", merged_only=False) == 2
    await database.close()


@pytest.mark.asyncio
async def test_cannot_link_blocks_transitive_group_merge(tmp_path: Path) -> None:
    database, _, pairs = await prepare_job(tmp_path)
    by_members = {
        frozenset((pair["record_a_id"], pair["record_b_id"])): pair["id"] for pair in pairs
    }
    ids = sorted({value for members in by_members for value in members})
    await database.review_pair("job-1", by_members[frozenset((ids[0], ids[2]))], "not_duplicate")
    await database.review_pair("job-1", by_members[frozenset((ids[0], ids[1]))], "duplicate")

    with pytest.raises(ValueError, match="否决关系"):
        await database.review_pair(
            "job-1", by_members[frozenset((ids[1], ids[2]))], "duplicate"
        )
    await database.close()


@pytest.mark.asyncio
async def test_late_cannot_link_splits_existing_transitive_group(tmp_path: Path) -> None:
    database, record_ids, pairs = await prepare_job(tmp_path)
    by_members = {
        frozenset((pair["record_a_id"], pair["record_b_id"])): pair["id"] for pair in pairs
    }
    first, second, third = record_ids
    await database.review_pair(
        "job-1", by_members[frozenset((first, second))], "duplicate"
    )
    await database.review_pair(
        "job-1", by_members[frozenset((second, third))], "duplicate"
    )

    await database.review_pair(
        "job-1", by_members[frozenset((first, third))], "not_duplicate"
    )

    members = await database.list_event_members("job-1")
    group_by_record = {row["record_id"]: row["event_group_id"] for row in members}
    assert group_by_record[first] != group_by_record[third]
    await database.close()


@pytest.mark.asyncio
async def test_hard_conflict_splits_transitive_duplicate_group(tmp_path: Path) -> None:
    database, record_ids, pairs = await prepare_job(tmp_path)
    by_members = {
        frozenset((pair["record_a_id"], pair["record_b_id"])): pair["id"] for pair in pairs
    }
    first, second, third = record_ids
    await database.review_pair(
        "job-1", by_members[frozenset((first, second))], "duplicate"
    )
    await database.review_pair(
        "job-1", by_members[frozenset((second, third))], "duplicate"
    )
    await database.mark_rule_exclusions(
        "job-1", {by_members[frozenset((first, third))]: ["different_object"]}
    )

    await database.rebuild_event_groups("job-1")

    members = await database.list_event_members("job-1")
    group_by_record = {row["record_id"]: row["event_group_id"] for row in members}
    assert group_by_record[first] != group_by_record[third]
    await database.close()


@pytest.mark.asyncio
async def test_region_stats_separate_model_and_human_decisions(tmp_path: Path) -> None:
    database, _, pairs = await prepare_job(tmp_path)
    await database.save_judgements(
        "job-1",
        {
            pairs[0]["id"]: {"decision": "duplicate", "confidence": 0.92},
            pairs[1]["id"]: {"decision": "review", "confidence": 0.65},
        },
    )
    await database.review_pair("job-1", pairs[0]["id"], "duplicate")

    stats = await database.list_region_stats("job-1")

    assert stats == [
        {
            "region": "江海区",
            "street": "外海街道",
            "sample_count": 2,
            "candidate_count": 3,
            "model_duplicate_count": 1,
            "human_duplicate_count": 1,
            "review_count": 1,
            "average_confidence": pytest.approx(0.785),
            "minimum_confidence": 0.65,
        },
        {
            "region": "江海区",
            "street": "礼乐街道",
            "sample_count": 1,
            "candidate_count": 2,
            "model_duplicate_count": 0,
            "human_duplicate_count": 0,
            "review_count": 1,
            "average_confidence": 0.65,
            "minimum_confidence": 0.65,
        },
    ]
    await database.close()


@pytest.mark.asyncio
async def test_event_name_uses_structured_region_subject_address_and_issue(tmp_path: Path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    await database.enqueue_job("job-1", "命名任务", mode="single", total_records=1)
    record_id = (
        await database.add_records(
            "job-1",
            [{"source": "S", "source_row": 2, "title": "甲公司欠薪"}],
        )
    )[0]
    await database.save_extractions(
        "job-1",
        {
            record_id: {
                "subject": {"full_name": "甲公司", "keys": ["甲公司"]},
                "address": {
                    "district": "江海区",
                    "street": "外海街道",
                    "road": "金瓯路",
                    "house_no": "188号",
                },
                "issues": {"primary": "拖欠一月份工资"},
            }
        },
    )

    await database.rebuild_event_groups("job-1")
    member = (await database.list_event_members("job-1"))[0]
    record = (await database.list_records("job-1"))[0]

    assert record["region"] == "江海区"
    assert record["street"] == "外海街道"
    assert member["event_name"] == "江海区｜甲公司｜金瓯路188号｜拖欠一月份工资"
    await database.close()


@pytest.mark.asyncio
async def test_event_name_can_be_edited_manually(tmp_path: Path) -> None:
    database, _, _ = await prepare_job(tmp_path)
    await database.rebuild_event_groups("job-1")
    group = (await database.list_groups("job-1"))[0]

    await database.rename_event_group("job-1", group["id"], "江海区｜人工修订事件名称")

    updated = next(item for item in await database.list_groups("job-1") if item["id"] == group["id"])
    assert updated["name"] == "江海区｜人工修订事件名称"
    await database.close()


@pytest.mark.asyncio
async def test_manual_event_name_survives_rebuild_when_members_are_unchanged(tmp_path: Path) -> None:
    database, record_ids, pairs = await prepare_job(tmp_path)
    first_pair = next(
        pair
        for pair in pairs
        if {pair["record_a_id"], pair["record_b_id"]} == set(record_ids[:2])
    )
    await database.review_pair("job-1", first_pair["id"], "duplicate")
    group = next(item for item in await database.list_groups("job-1") if item["member_count"] == 2)
    await database.rename_event_group("job-1", group["id"], "江海区｜人工修订事件名称")

    await database.rebuild_event_groups("job-1")

    rebuilt = next(item for item in await database.list_groups("job-1") if item["member_count"] == 2)
    assert rebuilt["name"] == "江海区｜人工修订事件名称"
    await database.close()
