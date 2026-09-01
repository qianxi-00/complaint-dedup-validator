from datetime import UTC, datetime

import pytest
import pytest_asyncio

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_repository import CorpusRepository, EventFilters


@pytest_asyncio.fixture
async def repository(tmp_path):
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'filters.db'}")
    await database.initialize()
    repo = CorpusRepository(database)
    yield repo
    await database.close()


async def seed_filtered_events(repository):
    batch_id = await repository.create_batch("历史", "bootstrap_history")
    version_id = await repository.create_dictionary_version("dict-filters", status="approved")
    generation_id = await repository.create_generation(batch_id, version_id)
    await repository.activate_generation(generation_id)
    street_id = await repository.create_street("江海区", "礼乐街道", version_id)
    issue_id = await repository.create_issue("食品安全", version_id)
    anchor_ids = [
        await repository.create_anchor(street_id, f"主体{index}", "organization", version_id)
        for index in range(3)
    ]
    event_ids = [
        await repository.get_or_create_event(
            street_id=street_id,
            anchor_id=anchor_id,
            issue_id=issue_id,
            occurrence_key="",
            event_key_version="event-key-v3",
            event_name=f"礼乐街道｜主体{index}｜食品安全",
            generation_id=generation_id,
        )
        for index, anchor_id in enumerate(anchor_ids)
    ]
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="filter-export".ljust(64, "0"),
        source_type="history",
        business_columns=["工单编号", "处理部门", "办结时间"],
        generation_id=generation_id,
    )
    values = []
    specifications = (
        (2, event_ids[0], "市场监管局", datetime(2026, 8, 10, tzinfo=UTC)),
        (3, event_ids[0], "教育局", datetime(2026, 8, 12, tzinfo=UTC)),
        (4, event_ids[1], "市场监管局", None),
        (5, event_ids[2], "住建局", datetime(2026, 7, 1, tzinfo=UTC)),
    )
    for row, event_id, department, completed_at in specifications:
        values.append(
            {
                "source_id": source_id,
                "source_batch_id": batch_id,
                "source_file_hash": "filter-export".ljust(64, "0"),
                "generation_id": generation_id,
                "source_row": row,
                "row_hash": f"record-{row}".ljust(64, "0"),
                "data_source": "history",
                "work_order_id": f"WO-{row}",
                "processing_department": department,
                "completed_at": completed_at,
                "parser_version": "rules-v1",
                "raw_json": {"工单编号": f"WO-{row}", "处理部门": department},
                "committed": True,
            }
        )
    record_ids = await repository.upsert_records(values)
    memberships = (
        (record_ids[0], event_ids[0]),
        (record_ids[1], event_ids[0]),
        (record_ids[2], event_ids[1]),
        (record_ids[3], event_ids[2]),
    )
    for record_id, event_id in memberships:
        await repository.assign_record(event_id, record_id, source="exact_key")
    return generation_id, event_ids


@pytest.mark.asyncio
async def test_events_filter_by_department_completed_range_and_missing_date(repository):
    _, event_ids = await seed_filtered_events(repository)

    by_department, total = await repository.list_event_summaries(
        filters=EventFilters(processing_department="市场监管局")
    )
    assert total == 2
    assert {row["id"] for row in by_department} == set(event_ids[:2])

    by_dates, total = await repository.list_event_summaries(
        filters=EventFilters(
            completed_from=datetime(2026, 8, 9).date(),
            completed_to=datetime(2026, 8, 11).date(),
        )
    )
    assert total == 1
    assert by_dates[0]["id"] == event_ids[0]

    missing_only, total = await repository.list_event_summaries(
        filters=EventFilters(missing_completed=True)
    )
    assert total == 1
    assert missing_only[0]["id"] == event_ids[1]

    assert await repository.count_singleton_events(
        filters=EventFilters(processing_department="住建局")
    ) == 1
    options = await repository.event_filter_options()
    assert set(options["processing_departments"]) == {"住建局", "教育局", "市场监管局"}


@pytest.mark.asyncio
async def test_filtered_export_includes_all_members_of_matching_events(repository):
    _, event_ids = await seed_filtered_events(repository)

    rows = await repository.export_rows(
        filters=EventFilters(completed_from=datetime(2026, 8, 9).date())
    )

    assert {row["event_id"] for row in rows} == {event_ids[0]}
    assert len(rows) == 2
    assert {row["work_order_id"] for row in rows} == {"WO-2", "WO-3"}
    assert all(row["member_count"] == 2 for row in rows)

    missing_rows = await repository.export_rows(
        filters=EventFilters(missing_completed=True)
    )
    assert [row["work_order_id"] for row in missing_rows] == ["WO-4"]

    page_rows, page_total = await repository.list_event_summaries(
        filters=EventFilters(
            region="江海区",
            street="礼乐街道",
            hide_singletons=True,
        )
    )
    exported_rows = await repository.export_rows(
        filters=EventFilters(
            region="江海区",
            street="礼乐街道",
            hide_singletons=True,
        )
    )
    assert page_total == 1
    assert {row["id"] for row in page_rows} == {event_ids[0]}
    assert {row["event_id"] for row in exported_rows} == {event_ids[0]}
