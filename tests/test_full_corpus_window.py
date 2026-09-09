from datetime import date

import pytest
import pytest_asyncio

from complaint_dedup.corpus_models import InputRecord
from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.full_corpus import FullCorpusService, WindowOverlapError
from complaint_dedup.full_corpus import EventFilters


@pytest_asyncio.fixture
async def full_database(tmp_path):
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'full.db'}")
    await database.initialize()
    yield database
    await database.close()


def record(
    order_id: str,
    received: str,
    *,
    completed: str | None = None,
    title: str = "德昌电机门口积水",
    location: str = "江海区礼乐街道德昌电机门口",
    category: str = "道路积水",
) -> InputRecord:
    return InputRecord(
        source_row=2,
        work_order_id=order_id,
        title=title,
        category=category,
        appeal_text=f"地址：{location}。事项：{category}。",
        received_at=received,
        completed_at=completed,
        location=location,
        raw_fields={"工单编号": order_id, "受理时间": received, "办结时间": completed},
    )


@pytest.mark.asyncio
async def test_full_sync_inserts_updates_and_marks_missing(full_database):
    database = full_database
    service = FullCorpusService(database)

    first = await service.sync_records(
        [record("A", "2026-09-01 08:00:00"), record("B", "2026-09-02 08:00:00")],
        file_name="all-1.xlsx",
    )
    assert first.inserted == 2

    second = await service.sync_records(
        [record("A", "2026-09-01 08:00:00", completed="2026-09-03 12:00:00")],
        file_name="all-2.xlsx",
    )
    assert second.updated == 1
    assert second.missing == 1
    rows = await service.list_current_orders()
    assert {row["work_order_id"] for row in rows} == {"A", "B"}
    row_a = next(row for row in rows if row["work_order_id"] == "A")
    row_b = next(row for row in rows if row["work_order_id"] == "B")
    assert row_a["completed_at"].date() == date(2026, 9, 3)
    assert not row_a["missing_in_latest_upload"]
    assert row_b["missing_in_latest_upload"]
    assert await service.version_count(second.sync_id) == 2


@pytest.mark.asyncio
async def test_empty_full_file_is_rejected(full_database):
    service = FullCorpusService(full_database)
    with pytest.raises(ValueError, match="没有可同步的工单"):
        await service.sync_records([], file_name="empty.xlsx")


@pytest.mark.asyncio
async def test_processing_department_falls_back_to_department(full_database):
    service = FullCorpusService(full_database)
    source = record("A", "2026-09-01 08:00:00")
    source = InputRecord(
        source_row=source.source_row,
        work_order_id=source.work_order_id,
        title=source.title,
        category=source.category,
        appeal_text=source.appeal_text,
        received_at=source.received_at,
        location=source.location,
        raw_fields={"所属部门": "礼乐街道办事处"},
    )
    await service.sync_records([source], file_name="all.xlsx")
    row = (await service.list_current_orders())[0]
    assert row["processing_department"] == "礼乐街道办事处"


@pytest.mark.asyncio
async def test_default_reference_window_is_complement_and_no_overlap(full_database):
    database = full_database
    service = FullCorpusService(database)
    await service.sync_records(
        [
            record("A", "2026-01-02 08:00:00", completed="2026-01-02 10:00:00"),
            record("B", "2026-09-05 08:00:00", completed="2026-09-05 10:00:00"),
            record("C", "2026-12-31 08:00:00", completed="2026-12-31 10:00:00"),
            record("D", "2026-09-06 08:00:00", completed=None),
        ],
        file_name="all.xlsx",
    )
    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 30),
    )
    members = await service.list_comparison_members(comparison.comparison_id)
    assert {row["work_order_id"] for row in members if row["side"] == "target"} == {"B", "D"}
    assert {row["work_order_id"] for row in members if row["side"] == "reference"} == {"A", "C"}
    assert not ({row["work_order_id"] for row in members if row["side"] == "target"} & {row["work_order_id"] for row in members if row["side"] == "reference"})
    assert comparison.missing_time_count == 1

    with pytest.raises(WindowOverlapError):
        await service.compare(
            time_field="completed_at",
            target_from=date(2026, 9, 1),
            target_to=date(2026, 9, 30),
            reference_from=date(2026, 9, 20),
            reference_to=date(2026, 10, 10),
        )
    with pytest.raises(ValueError, match="被比对开始日期不能晚于结束日期"):
        await service.compare(
            time_field="completed_at",
            target_from=date(2026, 9, 1),
            target_to=date(2026, 9, 2),
            reference_from=date(2026, 9, 7),
            reference_to=date(2026, 9, 6),
        )


@pytest.mark.asyncio
async def test_duplicate_full_file_is_idempotent(full_database):
    service = FullCorpusService(full_database)
    rows = [record("A", "2026-09-01 08:00:00")]
    first = await service.sync_records(rows, file_name="all.xlsx")
    second = await service.sync_records(rows, file_name="all.xlsx")
    assert second.sync_id != first.sync_id
    assert second.inserted == 0
    assert second.updated == 0


@pytest.mark.asyncio
async def test_duplicate_work_order_in_one_file_uses_last_row(full_database):
    service = FullCorpusService(full_database)
    await service.sync_records(
        [
            record("A", "2026-09-01 08:00:00", completed="2026-09-01 10:00:00"),
            record("A", "2026-09-01 08:00:00", completed="2026-09-03 10:00:00"),
        ],
        file_name="all.xlsx",
    )
    rows = await service.list_current_orders()
    assert len(rows) == 1
    assert rows[0]["completed_at"].date() == date(2026, 9, 3)


@pytest.mark.asyncio
async def test_fingerprint_order_is_updated_when_only_times_change(full_database):
    service = FullCorpusService(full_database)
    first = record(
        "",
        "2026-09-01 08:00:00",
        completed="2026-09-02 10:00:00",
        title="无编号工单",
        location="江海区礼乐街道测试地点",
    )
    second = record(
        "",
        "2026-09-03 08:00:00",
        completed="2026-09-04 10:00:00",
        title="无编号工单",
        location="江海区礼乐街道测试地点",
    )

    await service.sync_records([first], file_name="all-1.xlsx")
    result = await service.sync_records([second], file_name="all-2.xlsx")

    assert result.inserted == 0
    assert result.updated == 1
    rows = await service.list_current_orders()
    assert len(rows) == 1
    assert rows[0]["received_at"].date() == date(2026, 9, 3)
    assert rows[0]["completed_at"].date() == date(2026, 9, 4)


@pytest.mark.asyncio
async def test_missing_order_reappears_and_clears_missing_flag(full_database):
    service = FullCorpusService(full_database)
    await service.sync_records(
        [
            record("A", "2026-09-01 08:00:00"),
            record("B", "2026-09-02 08:00:00"),
        ],
        file_name="all-1.xlsx",
    )
    await service.sync_records(
        [record("A", "2026-09-01 08:00:00")],
        file_name="all-2.xlsx",
    )
    third = await service.sync_records(
        [
            record("A", "2026-09-01 08:00:00"),
            record("B", "2026-09-02 08:00:00"),
        ],
        file_name="all-3.xlsx",
    )
    rows = await service.list_current_orders()
    assert third.updated == 1
    assert next(row for row in rows if row["work_order_id"] == "B")[
        "missing_in_latest_upload"
    ] is False


@pytest.mark.asyncio
async def test_enterprise_family_groups_same_subject_and_keeps_other_cases_separate(
    full_database,
):
    service = FullCorpusService(full_database)
    await service.sync_records(
        [
            record(
                "A",
                "2026-09-01 08:00:00",
                completed="2026-09-01 10:00:00",
                title="某食品厂食品安全问题",
            ),
            record(
                "B",
                "2026-09-01 09:00:00",
                completed="2026-09-01 11:00:00",
                title="反映某食品厂食品安全问题",
            ),
            record(
                "C",
                "2026-09-01 09:30:00",
                completed="2026-09-01 12:00:00",
                title="某食品厂拖欠工资",
            ),
        ],
        file_name="all.xlsx",
    )
    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 1),
    )
    assert comparison.event_count == 2
    events = await service.list_comparison_events(comparison.comparison_id)
    sizes = sorted(len(event["members"]) for event in events)
    assert sizes == [1, 2]


@pytest.mark.asyncio
async def test_enterprise_family_without_subject_stays_singleton(full_database):
    service = FullCorpusService(full_database)
    await service.sync_records(
        [
            record(
                "A",
                "2026-09-01 08:00:00",
                completed="2026-09-01 10:00:00",
                title="反映食品安全问题",
            ),
            record(
                "B",
                "2026-09-01 09:00:00",
                completed="2026-09-01 11:00:00",
                title="反映食品安全问题",
            ),
        ],
        file_name="all.xlsx",
    )
    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 1),
    )
    assert comparison.event_count == 2
    assert comparison.singleton_count == 2


@pytest.mark.asyncio
async def test_enterprise_family_does_not_merge_across_streets(full_database):
    service = FullCorpusService(full_database)
    await service.sync_records(
        [
            record(
                "A",
                "2026-09-01 08:00:00",
                completed="2026-09-01 10:00:00",
                title="某食品厂食品安全问题",
                location="江海区礼乐街道某食品厂",
            ),
            record(
                "B",
                "2026-09-01 09:00:00",
                completed="2026-09-01 11:00:00",
                title="某食品厂食品安全问题",
                location="江海区外海街道某食品厂",
            ),
        ],
        file_name="all.xlsx",
    )
    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 1),
    )
    assert comparison.event_count == 2


@pytest.mark.asyncio
async def test_unknown_location_or_issue_stays_singleton(full_database):
    service = FullCorpusService(full_database)
    await service.sync_records(
        [
            record(
                "A",
                "2026-09-01 08:00:00",
                completed="2026-09-01 10:00:00",
                title="无法定位的投诉",
                location="江海区礼乐街道",
                category="",
            ),
            record(
                "B",
                "2026-09-01 09:00:00",
                completed="2026-09-01 11:00:00",
                title="无法定位的投诉",
                location="江海区礼乐街道",
                category="",
            ),
        ],
        file_name="all.xlsx",
    )
    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 1),
    )
    assert comparison.event_count == 2


@pytest.mark.asyncio
async def test_comparison_results_are_independent_snapshots(full_database):
    database = full_database
    service = FullCorpusService(database)
    await service.sync_records(
        [record("A", "2026-09-05 08:00:00", completed="2026-09-05 10:00:00"), record("B", "2026-09-06 08:00:00", completed="2026-09-06 10:00:00")],
        file_name="all.xlsx",
    )
    first = await service.compare(time_field="completed_at", target_from=date(2026, 9, 5), target_to=date(2026, 9, 5))
    await service.sync_records([record("A", "2026-09-05 08:00:00", completed="2026-09-07 10:00:00"), record("B", "2026-09-06 08:00:00", completed="2026-09-06 10:00:00")], file_name="all-2.xlsx")
    second = await service.compare(time_field="completed_at", target_from=date(2026, 9, 5), target_to=date(2026, 9, 5))
    first_members = await service.list_comparison_members(first.comparison_id)
    second_members = await service.list_comparison_members(second.comparison_id)
    assert {row["work_order_id"] for row in first_members if row["side"] == "target"} == {"A"}
    assert {row["work_order_id"] for row in second_members if row["side"] == "reference"} == {"A", "B"}
    assert {row["work_order_id"] for row in second_members if row["side"] == "target"} == set()


@pytest.mark.asyncio
async def test_event_library_supports_pagination_and_target_window_filter(full_database):
    service = FullCorpusService(full_database)
    await service.sync_records(
        [
            record("A", "2026-09-01 08:00:00", completed="2026-09-01 10:00:00"),
            record("B", "2026-09-02 08:00:00", completed="2026-09-02 10:00:00", title="另一处积水"),
            record("C", "2026-10-02 08:00:00", completed="2026-10-02 10:00:00", title="十月积水"),
        ],
        file_name="all.xlsx",
    )
    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 2),
        target_to=date(2026, 9, 2),
    )

    page, total = await service.list_event_summaries(
        comparison.comparison_id,
        filters=EventFilters(has_target_records=True),
        limit=1,
        offset=0,
    )
    assert total == 1
    assert len(page) == 1
    assert page[0]["target_count"] == 1


@pytest.mark.asyncio
async def test_event_summary_pagination_reuses_cached_event_members(
    full_database, monkeypatch
):
    service = FullCorpusService(full_database)
    await service.sync_records(
        [
            record(
                str(index),
                "2026-09-01 08:00:00",
                title=f"独立地点积水{index}",
                location=f"江海区礼乐街道独立地点{index}",
                category=f"道路积水{index}",
            )
            for index in range(25)
        ],
        file_name="all.xlsx",
    )
    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 1),
    )

    original_list_events = service.list_comparison_events
    loads = 0

    async def counted_list_events(*args, **kwargs):
        nonlocal loads
        comparison_id = str(args[0])
        if comparison_id not in service._comparison_event_cache:
            loads += 1
        return await original_list_events(*args, **kwargs)

    monkeypatch.setattr(service, "list_comparison_events", counted_list_events)
    page, total = await service.list_event_summaries(
        comparison.comparison_id,
        filters=EventFilters(has_target_records=True),
        limit=20,
        offset=0,
    )
    options = await service.event_filter_options(comparison.comparison_id)

    assert total == 25
    assert len(page) == 20
    assert options["regions"] == ["江海区"]
    assert loads == 1


@pytest.mark.asyncio
async def test_filtered_export_rows_include_all_members_of_matching_events(full_database):
    service = FullCorpusService(full_database)
    await service.sync_records(
        [
            record("A", "2026-09-01 08:00:00", completed="2026-09-01 10:00:00"),
            record("B", "2026-09-02 08:00:00", completed="2026-09-02 10:00:00"),
        ],
        file_name="all.xlsx",
    )
    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 1),
    )
    rows = await service.export_rows(
        comparison.comparison_id,
        filters=EventFilters(has_target_records=True),
    )
    assert {row["work_order_id"] for row in rows} == {"A", "B"}


@pytest.mark.asyncio
async def test_event_review_can_rename_and_move_a_member_to_singleton(full_database):
    service = FullCorpusService(full_database)
    await service.sync_records(
        [
            record("A", "2026-09-01 08:00:00", completed="2026-09-01 10:00:00"),
            record("B", "2026-09-01 09:00:00", completed="2026-09-01 11:00:00"),
        ],
        file_name="all.xlsx",
    )
    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 1),
    )
    event = (await service.list_comparison_events(comparison.comparison_id))[0]
    await service.update_event_name(event["id"], "人工修订事件")
    target_id = await service.exclude_event_member(event["id"], "wo:A")
    moved_event, records, total = await service.list_event_records(target_id)
    assert moved_event["event_name"] == "单例事件"
    assert total == 1
    assert records[0]["record_key"] == "wo:A"
