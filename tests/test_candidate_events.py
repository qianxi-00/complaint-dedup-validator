from pathlib import Path

import pytest

from complaint_dedup.async_database import AsyncDatabase


async def prepare_database(tmp_path: Path):
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'events.db'}")
    await database.initialize()
    await database.enqueue_job("job-v2", "事件簇任务", mode="single", total_records=4)
    record_ids = await database.add_records(
        "job-v2",
        [
            {
                "source": "S",
                "source_row": index + 2,
                "title": title,
                "category": "城市管理",
                "category_level_1": "城市管理",
                "category_level_2": "市容环境",
                "category_level_3": "占道经营",
                "category_level_4": "流动摊贩",
                "normalized_title": title.replace("投诉", ""),
                "event_signature": f"江海区|江南街道|{place}|占道经营",
                "normalized_location": place,
                "region": "江海区" if index < 3 else "新会区",
                "street": "江南街道" if index < 3 else "会城街道",
            }
            for index, (title, place) in enumerate(
                [
                    ("投诉保利大都汇南门摊贩", "保利大都汇南门"),
                    ("保利大都汇南门占道", "保利大都汇南门"),
                    ("保利大都汇西门占道", "保利大都汇西门"),
                    ("新会广场噪音", "新会广场"),
                ]
            )
        ],
    )
    return database, record_ids


@pytest.mark.asyncio
async def test_job_pipeline_version_and_record_event_fields_are_persisted(tmp_path: Path) -> None:
    database, record_ids = await prepare_database(tmp_path)
    await database.enqueue_job(
        "job-v1", "旧任务", mode="cross", total_records=0, pipeline_version="pair_v1"
    )

    assert (await database.get_job("job-v2"))["pipeline_version"] == "event_cluster_v2"
    assert (await database.get_job("job-v1"))["pipeline_version"] == "pair_v1"
    record = next(row for row in await database.list_records("job-v2") if row["id"] == record_ids[0])
    assert record["category_level_4"] == "流动摊贩"
    assert record["normalized_location"] == "保利大都汇南门"
    assert record["event_signature"].endswith("|占道经营")
    await database.close()


@pytest.mark.asyncio
async def test_replace_list_count_detail_and_filter_candidate_events(tmp_path: Path) -> None:
    database, record_ids = await prepare_database(tmp_path)
    event_ids = await database.replace_candidate_events(
        "job-v2",
        [
            {
                "name": "江海区｜江南街道｜保利大都汇南门｜占道经营",
                "status": "review",
                "confidence": 0.91,
                "evidence": {"summary": "地点和问题一致"},
                "members": [
                    {"record_id": record_ids[0], "confidence": 0.94, "role": "core"},
                    {"record_id": record_ids[1], "confidence": 0.90, "role": "member"},
                ],
            },
            {
                "name": "江海区｜江南街道｜保利大都汇西门｜占道经营",
                "status": "rejected",
                "confidence": 0.70,
                "members": [{"record_id": record_ids[2], "confidence": 0.70}],
            },
            {
                "name": "新会区｜会城街道｜新会广场｜噪音",
                "status": "review",
                "confidence": 0.82,
                "members": [{"record_id": record_ids[3], "confidence": 0.82}],
            },
        ],
    )

    visible = await database.list_candidate_events("job-v2")
    assert [row["id"] for row in visible] == [event_ids[0]]
    assert visible[0]["member_count"] == 2
    assert visible[0]["minimum_member_confidence"] == pytest.approx(0.90)
    assert await database.count_candidate_events("job-v2") == 1

    filtered = await database.list_candidate_events(
        "job-v2",
        region="江海区",
        street="江南街道",
        category_level_4="流动摊贩",
        category="城市管理",
        status="review",
        min_confidence=0.9,
        include_singletons=True,
        include_rejected=True,
    )
    assert [row["id"] for row in filtered] == [event_ids[0]]

    detail = await database.get_candidate_event("job-v2", event_ids[0])
    assert detail["evidence"] == {"summary": "地点和问题一致"}
    assert [member["record_id"] for member in detail["members"]] == record_ids[:2]
    assert detail["members"][0]["normalized_location"] == "保利大都汇南门"

    options = await database.list_candidate_event_filter_options("job-v2", region="江海区")
    assert set(options["regions"]) == {"江海区", "新会区"}
    assert options["streets"] == ["江南街道"]
    assert "流动摊贩" in options["category_level_4"]
    assert "城市管理" in options["categories"]
    assert options["events"][0]["id"] == event_ids[0]
    await database.close()


@pytest.mark.asyncio
async def test_candidate_event_storage_accepts_singleton_status(tmp_path: Path) -> None:
    database, record_ids = await prepare_database(tmp_path)
    record_id = record_ids[0]

    event_id = (await database.replace_candidate_events(
        "job-v2",
        [{
            "name": "未知地区｜未召回工单｜未识别问题",
            "status": "singleton",
            "members": [{"record_id": record_id, "assignment_source": "singleton"}],
        }],
    ))[0]

    assert (await database.get_candidate_event("job-v2", event_id))["status"] == "singleton"
    await database.close()


@pytest.mark.asyncio
async def test_confirm_and_reopen_candidate_event_sync_final_group(tmp_path: Path) -> None:
    database, record_ids = await prepare_database(tmp_path)
    event_id = (
        await database.replace_candidate_events(
            "job-v2",
            [
                {
                    "name": "江海区｜江南街道｜保利大都汇南门｜占道经营",
                    "status": "review",
                    "confidence": 0.97,
                    "members": [
                        {"record_id": record_ids[0], "confidence": 0.98},
                        {"record_id": record_ids[1], "confidence": 0.97},
                    ],
                }
            ],
        )
    )[0]

    await database.confirm_candidate_event("job-v2", event_id, note="人工确认")

    event = await database.get_candidate_event("job-v2", event_id)
    assert event["status"] == "confirmed"
    assert event["final_event_group_id"] is not None
    members = await database.list_event_members("job-v2")
    assert {row["record_id"] for row in members} == set(record_ids[:2])
    assert len({row["event_group_id"] for row in members}) == 1

    await database.reopen_candidate_event("job-v2", event_id, note="需重新拆分")

    reopened = await database.get_candidate_event("job-v2", event_id)
    assert reopened["status"] == "review"
    assert reopened["final_event_group_id"] is None
    assert await database.list_event_members("job-v2") == []
    assert [item["action"] for item in await database.list_event_review_actions("job-v2")] == [
        "confirm",
        "reopen",
    ]
    await database.close()


@pytest.mark.asyncio
async def test_auto_merged_replacement_syncs_final_group(tmp_path: Path) -> None:
    database, record_ids = await prepare_database(tmp_path)
    event_id = (
        await database.replace_candidate_events(
            "job-v2",
            [
                {
                    "name": "江海区｜江南街道｜自动合并事件",
                    "status": "auto_merged",
                    "confidence": 0.99,
                    "members": [
                        {"record_id": record_ids[0], "confidence": 0.99},
                        {"record_id": record_ids[1], "confidence": 0.98},
                    ],
                }
            ],
        )
    )[0]

    event = await database.get_candidate_event("job-v2", event_id)
    assert event["final_event_group_id"] is not None
    assert {row["record_id"] for row in await database.list_event_members("job-v2")} == set(
        record_ids[:2]
    )
    actions = await database.list_event_review_actions("job-v2")
    assert [item["action"] for item in actions] == ["auto_merge"]
    await database.close()


@pytest.mark.asyncio
async def test_split_merge_move_exclude_and_rename_candidate_events(tmp_path: Path) -> None:
    database, record_ids = await prepare_database(tmp_path)
    first, second = await database.replace_candidate_events(
        "job-v2",
        [
            {
                "name": "待拆分事件",
                "status": "confirmed",
                "confidence": 0.96,
                "members": [{"record_id": item, "confidence": 0.96} for item in record_ids[:3]],
            },
            {
                "name": "目标事件",
                "status": "review",
                "confidence": 0.88,
                "members": [{"record_id": record_ids[3], "confidence": 0.88}],
            },
        ],
    )

    split_id = await database.split_candidate_event(
        "job-v2", first, [record_ids[2]], name="西门独立事件"
    )
    assert [row["record_id"] for row in (await database.get_candidate_event("job-v2", split_id))["members"]] == [
        record_ids[2]
    ]
    assert (await database.get_candidate_event("job-v2", first))["status"] == "review"

    merged_id = await database.merge_candidate_events(
        "job-v2", [split_id, second], name="合并后事件"
    )
    assert {row["record_id"] for row in (await database.get_candidate_event("job-v2", merged_id))["members"]} == {
        record_ids[2],
        record_ids[3],
    }

    await database.move_candidate_event_member(
        "job-v2", record_ids[1], source_event_id=first, target_event_id=merged_id
    )
    await database.rename_candidate_event("job-v2", merged_id, "人工修订名称")
    assert (await database.get_candidate_event("job-v2", merged_id))["name"] == "人工修订名称"

    excluded_id = await database.exclude_candidate_event_member(
        "job-v2", merged_id, record_ids[3], note="不是同一地点"
    )
    excluded = await database.get_candidate_event("job-v2", excluded_id)
    assert excluded["status"] == "rejected"
    assert [row["record_id"] for row in excluded["members"]] == [record_ids[3]]

    actions = [item["action"] for item in await database.list_event_review_actions("job-v2")]
    assert actions == ["split", "merge", "move", "rename", "exclude"]
    await database.close()


@pytest.mark.asyncio
async def test_candidate_event_rejects_records_from_another_job_and_duplicate_membership(tmp_path: Path) -> None:
    database, record_ids = await prepare_database(tmp_path)
    await database.enqueue_job("other", "其他任务", mode="single", total_records=1)
    other_id = (
        await database.add_records("other", [{"source": "S", "source_row": 2, "title": "其他"}])
    )[0]

    with pytest.raises(ValueError, match="不属于当前任务"):
        await database.replace_candidate_events(
            "job-v2", [{"name": "非法事件", "members": [{"record_id": other_id}]}]
        )

    with pytest.raises(ValueError, match="只能归属一个候选事件"):
        await database.replace_candidate_events(
            "job-v2",
            [
                {"name": "事件一", "members": [{"record_id": record_ids[0]}]},
                {"name": "事件二", "members": [{"record_id": record_ids[0]}]},
            ],
        )
    await database.close()
