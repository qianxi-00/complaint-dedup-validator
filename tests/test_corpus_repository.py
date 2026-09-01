from datetime import UTC, datetime
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import event, func, select, update

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.corpus_schema import (
    anchor_aliases,
    batch_records,
    canonical_anchors,
    canonical_issues,
    canonical_streets,
    cannot_links,
    corpus_event_members,
    corpus_records,
    daily_batches,
    events,
    record_links,
)


@pytest_asyncio.fixture
async def repository(tmp_path: Path):
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'corpus.db'}")
    await database.initialize()
    repo = CorpusRepository(database)
    yield repo
    await database.close()


@pytest.mark.asyncio
async def test_import_is_idempotent_but_duplicate_work_order_ids_are_allowed(repository):
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="a" * 64,
        source_type="history",
        business_columns=["工单编号"],
    )
    batch_id = await repository.create_batch("历史冷启动", "bootstrap_history")
    record = {
        "source_id": source_id,
        "source_batch_id": batch_id,
        "source_file_hash": "a" * 64,
        "source_row": 2,
        "row_hash": "b" * 64,
        "data_source": "history",
        "work_order_id": "0826081308493182401",
        "parser_version": "rules-v1",
        "raw_json": {"工单编号": "0826081308493182401"},
    }
    first_id = await repository.upsert_record(record)
    assert await repository.upsert_record(record) == first_id

    second = dict(record, source_row=3, row_hash="c" * 64)
    second_id = await repository.upsert_record(second)
    assert second_id != first_id


@pytest.mark.asyncio
async def test_upsert_records_uses_one_bulk_insert_for_many_rows(repository):
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="bulk-upsert".ljust(64, "0"),
        source_type="history",
        business_columns=[],
    )
    batch_id = await repository.create_batch("历史冷启动", "bootstrap_history")
    values = [
        {
            "source_id": source_id,
            "source_batch_id": batch_id,
            "source_file_hash": "bulk-upsert".ljust(64, "0"),
            "source_row": row,
            "row_hash": f"{row:064d}",
            "data_source": "history",
            "raw_json": {},
        }
        for row in range(2, 22)
    ]
    statement_count = 0

    def count_statement(*_args):
        nonlocal statement_count
        statement_count += 1

    event.listen(repository.database.engine.sync_engine, "before_cursor_execute", count_statement)
    try:
        assert len(await repository.upsert_records(values)) == 20
    finally:
        event.remove(
            repository.database.engine.sync_engine,
            "before_cursor_execute",
            count_statement,
        )
    assert statement_count <= 6


@pytest.mark.asyncio
async def test_previous_work_order_links_are_inserted_in_bulk_and_idempotently(repository):
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="1" * 64,
        source_type="history",
        business_columns=["工单编号"],
    )
    batch_id = await repository.create_batch("历史冷启动", "bootstrap_history")
    records = [
        {
            "source_id": source_id,
            "source_batch_id": batch_id,
            "source_file_hash": "1" * 64,
            "source_row": row,
            "row_hash": str(row) * 64,
            "data_source": "history",
            "work_order_id": work_order_id,
            "raw_json": {},
        }
        for row, work_order_id in (
            (2, "HIST-001"),
            (3, "NEW-001"),
            (4, "NEW-002"),
        )
    ]
    target_id, source_one_id, source_two_id = await repository.upsert_records(records)
    links = [
        {"source_record_id": source_one_id, "work_order_ids": ["HIST-001"]},
        {"source_record_id": source_two_id, "work_order_ids": ["MISSING-001"]},
    ]

    await repository.add_previous_work_order_links_bulk(links)
    await repository.add_previous_work_order_links_bulk(links)

    async with repository.database.engine.connect() as connection:
        rows = (
            await connection.execute(select(record_links).order_by(record_links.c.id))
        ).mappings().all()
    assert len(rows) == 2
    assert rows[0]["source_record_id"] == source_one_id
    assert rows[0]["target_record_id"] == target_id
    assert rows[0]["score"] == 1.0
    assert rows[1]["source_record_id"] == source_two_id
    assert rows[1]["target_record_id"] is None
    assert rows[1]["score"] == 0.0


@pytest.mark.asyncio
async def test_exact_event_key_reuses_event_and_record_has_single_membership(repository):
    version_id = await repository.create_dictionary_version("dict-v1", status="approved")
    street_id = await repository.create_street("江海区", "礼乐街道", version_id)
    anchor_id = await repository.create_anchor(
        street_id, "德昌电机门口", "landmark", version_id, direction="门口"
    )
    issue_id = await repository.create_issue("道路积水", version_id)
    event_id = await repository.get_or_create_event(
        street_id=street_id,
        anchor_id=anchor_id,
        issue_id=issue_id,
        event_key_version="key-v1",
        event_name="江海区｜礼乐街道｜德昌电机门口｜道路积水",
    )
    assert await repository.get_or_create_event(
        street_id=street_id,
        anchor_id=anchor_id,
        issue_id=issue_id,
        event_key_version="key-v1",
        event_name="ignored",
    ) == event_id

    source_id = await repository.create_source(
        file_name="daily.xlsx",
        file_hash="d" * 64,
        source_type="daily",
        business_columns=[],
    )
    batch_id = await repository.create_batch("今日", "daily_increment")
    record_id = await repository.upsert_record(
        {
            "source_id": source_id,
            "source_batch_id": batch_id,
            "source_file_hash": "d" * 64,
            "source_row": 2,
            "row_hash": "e" * 64,
            "data_source": "daily",
            "parser_version": "rules-v1",
            "raw_json": {},
        }
    )
    await repository.assign_record(event_id, record_id, source="exact_key")
    assert await repository.event_member_ids(event_id) == [record_id]


@pytest.mark.asyncio
async def test_bulk_exact_event_assignment_uses_constant_database_round_trips(repository):
    version_id = await repository.create_dictionary_version("dict-bulk-events", status="approved")
    batch_id = await repository.create_batch("历史冷启动", "bootstrap_history")
    generation_id = await repository.create_generation(batch_id, version_id)
    street_id = await repository.create_street("江海区", "礼乐街道", version_id)
    anchor_ids = [
        await repository.create_anchor(street_id, name, "landmark", version_id)
        for name in ("德昌电机门口", "文华豪庭门口")
    ]
    issue_id = await repository.create_issue("道路积水", version_id)
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="9" * 64,
        source_type="history",
        business_columns=[],
        generation_id=generation_id,
    )
    record_ids = await repository.upsert_records(
        [
            {
                "source_id": source_id,
                "source_batch_id": batch_id,
                "source_file_hash": "9" * 64,
                "generation_id": generation_id,
                "source_row": row,
                "row_hash": f"{row:064d}",
                "data_source": "history",
                "raw_json": {},
            }
            for row in range(2, 22)
        ]
    )
    rows = [
        {
            "record_id": record_id,
            "street_id": street_id,
            "anchor_id": anchor_ids[index % 2],
            "issue_id": issue_id,
            "event_name": f"礼乐街道｜事件{index % 2}",
            "received_at": datetime(2026, 8, 1 + index % 10, tzinfo=UTC),
        }
        for index, record_id in enumerate(record_ids)
    ]
    statement_count = 0

    def count_statement(*_args):
        nonlocal statement_count
        statement_count += 1

    event.listen(repository.database.engine.sync_engine, "before_cursor_execute", count_statement)
    try:
        await repository.bulk_assign_exact_events(
            rows,
            event_key_version="key-v1",
            frozen=True,
            generation_id=generation_id,
        )
    finally:
        event.remove(
            repository.database.engine.sync_engine,
            "before_cursor_execute",
            count_statement,
        )

    async with repository.database.engine.connect() as connection:
        assert int(
            (await connection.scalar(select(func.count()).select_from(events))) or 0
        ) == 2
        assert int(
            (
                await connection.scalar(
                    select(func.count()).select_from(corpus_event_members)
                )
            )
            or 0
        ) == 20
    assert statement_count <= 6


@pytest.mark.asyncio
async def test_bulk_exact_event_creation_does_not_insert_events_one_by_one(repository):
    version_id = await repository.create_dictionary_version("dict-bulk-exact-unique", status="approved")
    batch_id = await repository.create_batch("历史冷启动", "bootstrap_history")
    generation_id = await repository.create_generation(batch_id, version_id)
    street_id = await repository.create_street("江海区", "礼乐街道", version_id)
    anchor_ids = [
        await repository.create_anchor(street_id, f"地点{index}", "unknown", version_id)
        for index in range(20)
    ]
    issue_id = await repository.create_issue("道路积水", version_id)
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="bulk-exact-unique".ljust(64, "0"),
        source_type="history",
        business_columns=[],
        generation_id=generation_id,
    )
    record_ids = await repository.upsert_records(
        [
            {
                "source_id": source_id,
                "source_batch_id": batch_id,
                "source_file_hash": "bulk-exact-unique".ljust(64, "0"),
                "generation_id": generation_id,
                "source_row": row,
                "row_hash": f"{row + 400:064d}",
                "data_source": "history",
                "raw_json": {},
            }
            for row in range(2, 22)
        ]
    )
    rows = [
        {
            "record_id": record_id,
            "street_id": street_id,
            "anchor_id": anchor_ids[index],
            "issue_id": issue_id,
            "event_name": f"礼乐街道｜地点{index}｜道路积水",
        }
        for index, record_id in enumerate(record_ids)
    ]
    statement_count = 0

    def count_statement(*_args):
        nonlocal statement_count
        statement_count += 1

    event.listen(repository.database.engine.sync_engine, "before_cursor_execute", count_statement)
    try:
        await repository.bulk_assign_exact_events(
            rows,
            event_key_version="key-v1",
            frozen=True,
            generation_id=generation_id,
        )
    finally:
        event.remove(
            repository.database.engine.sync_engine,
            "before_cursor_execute",
            count_statement,
        )
    assert statement_count <= 8


@pytest.mark.asyncio
async def test_linked_and_strong_signal_assignments_use_batched_member_updates(repository):
    version_id = await repository.create_dictionary_version("dict-batched-signals", status="approved")
    batch_id = await repository.create_batch("历史冷启动", "bootstrap_history")
    generation_id = await repository.create_generation(batch_id, version_id)
    street_id = await repository.create_street("江海区", "礼乐街道", version_id)
    anchor_id = await repository.create_anchor(
        street_id, "西屋厨房小家电", "subject", version_id
    )
    issue_id = await repository.create_issue("消费纠纷", version_id)
    event_id = await repository.get_or_create_event(
        street_id=street_id,
        anchor_id=anchor_id,
        issue_id=issue_id,
        event_key_version="key-v1",
        event_name="礼乐街道｜西屋厨房小家电消费纠纷",
        generation_id=generation_id,
    )
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="8" * 64,
        source_type="history",
        business_columns=[],
        generation_id=generation_id,
    )
    record_ids = await repository.upsert_records(
        [
            {
                "source_id": source_id,
                "source_batch_id": batch_id,
                "source_file_hash": "8" * 64,
                "generation_id": generation_id,
                "source_row": row,
                "row_hash": f"{row + 100:064d}",
                "data_source": "history",
                "work_order_id": "HIST-ROOT" if row == 2 else f"NEW-{row}",
                "street_id": street_id,
                "issue_id": issue_id,
                "road": "东海路",
                "house_no": "46号",
                "building": "",
                "direction": "",
                "title_normalized": "西屋消费纠纷",
                "received_at": datetime(2026, 8, 1 + row % 10, tzinfo=UTC),
                "raw_json": {},
            }
            for row in range(2, 23)
        ]
    )
    target_id, linked_ids, strong_ids = record_ids[0], record_ids[1:11], record_ids[11:]
    await repository.assign_record(event_id, target_id, source="exact_key")
    await repository.add_previous_work_order_links_bulk(
        [
            {"source_record_id": record_id, "work_order_ids": ["HIST-ROOT"]}
            for record_id in linked_ids
        ]
    )

    linked_statement_count = 0

    def count_linked(*_args):
        nonlocal linked_statement_count
        linked_statement_count += 1

    event.listen(repository.database.engine.sync_engine, "before_cursor_execute", count_linked)
    try:
        assert await repository.assign_linked_records(batch_id) == set(linked_ids)
    finally:
        event.remove(
            repository.database.engine.sync_engine,
            "before_cursor_execute",
            count_linked,
        )

    strong_statement_count = 0

    def count_strong(*_args):
        nonlocal strong_statement_count
        strong_statement_count += 1

    event.listen(repository.database.engine.sync_engine, "before_cursor_execute", count_strong)
    try:
        assert await repository.assign_strong_signal_records(batch_id) == set(strong_ids)
    finally:
        event.remove(
            repository.database.engine.sync_engine,
            "before_cursor_execute",
            count_strong,
        )

    assert await repository.event_member_ids(event_id) == sorted(record_ids)
    assert linked_statement_count <= 5
    assert strong_statement_count <= 7


@pytest.mark.asyncio
async def test_explicit_order_reconciles_records_already_split_across_events(repository):
    version_id = await repository.create_dictionary_version(
        "dict-order-reconcile", status="approved"
    )
    batch_id = await repository.create_batch("历史冷启动", "bootstrap_history")
    generation_id = await repository.create_generation(batch_id, version_id)
    street_id = await repository.create_street("江海区", "外海街道", version_id)
    first_anchor_id = await repository.create_anchor(
        street_id, "未知商家", "subject", version_id
    )
    second_anchor_id = await repository.create_anchor(
        street_id, "邦民路32号某商家", "subject", version_id
    )
    first_issue_id = await repository.create_issue("食品类", version_id)
    second_issue_id = await repository.create_issue("农资农具", version_id)
    first_event_id = await repository.get_or_create_event(
        street_id=street_id,
        anchor_id=first_anchor_id,
        issue_id=first_issue_id,
        event_key_version="manual-singleton-1",
        event_name="外海街道｜未知商家｜食品类",
        generation_id=generation_id,
    )
    second_event_id = await repository.get_or_create_event(
        street_id=street_id,
        anchor_id=second_anchor_id,
        issue_id=second_issue_id,
        event_key_version="key-v1",
        event_name="外海街道｜邦民路32号某商家｜农资农具",
        generation_id=generation_id,
    )
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="order-reconcile".ljust(64, "0"),
        source_type="history",
        business_columns=[],
        generation_id=generation_id,
    )
    occurrence_key = "order:260617279477021073138"
    record_ids = await repository.upsert_records(
        [
            {
                "source_id": source_id,
                "source_batch_id": batch_id,
                "source_file_hash": "order-reconcile".ljust(64, "0"),
                "generation_id": generation_id,
                "source_row": row,
                "row_hash": f"{row + 500:064d}",
                "data_source": "history",
                "street_id": street_id,
                "anchor_id": anchor_id,
                "issue_id": issue_id,
                "occurrence_key": occurrence_key,
                "raw_json": {},
            }
            for row, anchor_id, issue_id in (
                (2, first_anchor_id, first_issue_id),
                (3, second_anchor_id, second_issue_id),
            )
        ]
    )
    await repository.assign_record(first_event_id, record_ids[0], source="manual_singleton")
    await repository.assign_record(second_event_id, record_ids[1], source="exact_key")

    assert await repository.assign_strong_signal_records(batch_id) == set(record_ids)
    async with repository.database.engine.connect() as connection:
        assigned_event_ids = set(
            (
                await connection.execute(
                    select(corpus_event_members.c.event_id).where(
                        corpus_event_members.c.record_id.in_(record_ids)
                    )
                )
            ).scalars()
        )
    assert len(assigned_event_ids) == 1


@pytest.mark.asyncio
async def test_safe_singletons_are_created_in_bulk_without_merging_records(repository):
    version_id = await repository.create_dictionary_version("dict-singletons", status="candidate")
    batch_id = await repository.create_batch("历史冷启动", "bootstrap_history")
    generation_id = await repository.create_generation(batch_id, version_id)
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="7" * 64,
        source_type="history",
        business_columns=[],
        generation_id=generation_id,
    )
    record_ids = await repository.upsert_records(
        [
            {
                "source_id": source_id,
                "source_batch_id": batch_id,
                "source_file_hash": "7" * 64,
                "generation_id": generation_id,
                "source_row": row,
                "row_hash": f"{row + 200:064d}",
                "data_source": "history",
                "received_at": datetime(2026, 8, 1 + row % 10, tzinfo=UTC),
                "raw_json": {},
            }
            for row in range(2, 22)
        ]
    )
    rows = [
        {
            "record_id": record_id,
            "region": "江海区",
            "street_name": "未知街道",
            "street_id": None,
            "anchor_name": f"未知地点{index}",
            "anchor_type": "unknown",
            "road": None,
            "house_no": None,
            "building": None,
            "shop_no": None,
            "floor": None,
            "direction": None,
            "issue_name": "未识别事项",
            "issue_id": None,
            "event_name": f"未知街道｜未知地点{index}未识别事项",
            "received_at": datetime(2026, 8, 1 + index % 10, tzinfo=UTC),
        }
        for index, record_id in enumerate(record_ids)
    ]
    statement_count = 0

    def count_statement(*_args):
        nonlocal statement_count
        statement_count += 1

    event.listen(repository.database.engine.sync_engine, "before_cursor_execute", count_statement)
    try:
        await repository.bulk_assign_safe_singletons(
            rows,
            dictionary_version_id=version_id,
            generation_id=generation_id,
        )
    finally:
        event.remove(
            repository.database.engine.sync_engine,
            "before_cursor_execute",
            count_statement,
        )

    async with repository.database.engine.connect() as connection:
        statuses = list(
            (
                await connection.execute(
                    select(corpus_records.c.anchor_resolution_status).where(
                        corpus_records.c.id.in_(record_ids)
                    )
                )
            ).scalars()
        )
        event_count = int(
            (await connection.scalar(select(func.count()).select_from(events))) or 0
        )
        member_count = int(
            (
                await connection.scalar(
                    select(func.count()).select_from(corpus_event_members)
                )
            )
            or 0
        )
    assert statuses == ["manual_singleton"] * 20
    assert event_count == 20
    assert member_count == 20
    assert statement_count <= 16


@pytest.mark.asyncio
async def test_committing_batch_marks_staged_records_committed(repository):
    source_id = await repository.create_source(
        file_name="daily.xlsx",
        file_hash="f" * 64,
        source_type="daily",
        business_columns=[],
    )
    batch_id = await repository.create_batch("今日", "daily_increment")
    record_id = await repository.upsert_record(
        {
            "source_id": source_id,
            "source_batch_id": batch_id,
            "source_file_hash": "f" * 64,
            "source_row": 2,
            "row_hash": "1" * 64,
            "data_source": "daily",
            "parser_version": "rules-v1",
            "raw_json": {},
        }
    )
    await repository.commit_batch(batch_id, committed_at=datetime.now(UTC))
    assert (await repository.get_batch(batch_id))["status"] == "committed"
    assert (await repository.get_record(record_id))["committed"] is True


@pytest.mark.asyncio
async def test_dictionary_version_must_be_approved_before_it_is_active(repository):
    version_id = await repository.create_dictionary_version("dict-review")
    assert await repository.active_dictionary_version() is None
    await repository.approve_dictionary_version(version_id, approved_by="tester")
    active = await repository.active_dictionary_version()
    assert active["id"] == version_id
    assert active["status"] == "approved"


@pytest.mark.asyncio
async def test_invalid_dictionary_publish_keeps_current_active_version(repository):
    current_id = await repository.create_dictionary_version(
        "dict-current", status="approved"
    )

    with pytest.raises(KeyError):
        await repository.approve_dictionary_version(999_999, approved_by="tester")

    active = await repository.active_dictionary_version()
    assert active["id"] == current_id


@pytest.mark.asyncio
async def test_history_generation_switch_is_atomic(repository):
    first_batch = await repository.create_batch("旧历史", "bootstrap_history")
    first_generation = await repository.create_generation(first_batch, None)
    await repository.activate_generation(first_generation)

    second_batch = await repository.create_batch("新历史", "bootstrap_history")
    second_generation = await repository.create_generation(second_batch, None)
    assert (await repository.active_generation())["id"] == first_generation

    await repository.activate_generation(second_generation)
    active = await repository.active_generation()
    assert active["id"] == second_generation
    generations = await repository.list_generations()
    states = {row["id"]: row["status"] for row in generations}
    assert states[first_generation] == "archived"
    assert states[second_generation] == "active"


@pytest.mark.asyncio
async def test_failed_history_generation_does_not_replace_active(repository):
    old_batch = await repository.create_batch("旧历史", "bootstrap_history")
    old_generation = await repository.create_generation(old_batch, None)
    await repository.activate_generation(old_generation)

    new_batch = await repository.create_batch("失败历史", "bootstrap_history")
    new_generation = await repository.create_generation(new_batch, None)
    await repository.fail_generation(new_generation, "解析失败")

    active = await repository.active_generation()
    assert active["id"] == old_generation
    failed = await repository.get_generation(new_generation)
    assert failed["status"] == "failed"
    assert failed["error_message"] == "解析失败"


@pytest.mark.asyncio
async def test_building_generation_is_hidden_until_it_becomes_active(repository):
    version_id = await repository.create_dictionary_version("dict-building")
    batch_id = await repository.create_batch("构建中历史", "bootstrap_history")
    generation_id = await repository.create_generation(batch_id, version_id)
    street_id = await repository.create_street(
        "江海区", "礼乐街道", version_id
    )
    anchor_id = await repository.create_anchor(
        street_id, "德昌电机门口", "landmark", version_id
    )
    issue_id = await repository.create_issue("道路积水", version_id)
    event_id = await repository.get_or_create_event(
        street_id=street_id,
        anchor_id=anchor_id,
        issue_id=issue_id,
        event_key_version="event-key-v1",
        event_name="礼乐街道｜德昌电机门口道路积水",
        generation_id=generation_id,
    )
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="building".ljust(64, "0"),
        source_type="history",
        business_columns=["工单编号"],
        generation_id=generation_id,
    )
    record_id = await repository.upsert_record(
        {
            "source_id": source_id,
            "source_batch_id": batch_id,
            "source_file_hash": "building".ljust(64, "0"),
            "generation_id": generation_id,
            "source_row": 2,
            "row_hash": "record".ljust(64, "0"),
            "data_source": "history",
            "work_order_id": "WO-BUILDING",
            "parser_version": "rules-v1",
            "raw_json": {"工单编号": "WO-BUILDING"},
            "committed": True,
        }
    )
    await repository.assign_record(event_id, record_id, source="exact_key")

    assert await repository.list_events() == []
    assert await repository.list_event_summaries() == ([], 0)
    assert await repository.event_filter_options() == {
        "regions": [],
        "streets": [],
        "processing_departments": [],
    }
    assert await repository.export_rows() == []
    assert await repository.export_business_columns() == []
    with pytest.raises(KeyError):
        await repository.get_event(event_id)

    await repository.activate_generation(generation_id)
    assert await repository.count_events() == 1
    assert await repository.search_event_options("德昌") == [
        {"id": event_id, "event_name": "礼乐街道｜德昌电机门口道路积水"}
    ]
    assert len(await repository.event_records(event_id, limit=1)) == 1
    assert await repository.count_event_records(event_id) == 1


@pytest.mark.asyncio
async def test_records_for_batch_can_scope_bootstrap_compare_to_daily_rows(repository):
    version_id = await repository.create_dictionary_version("dict-compare")
    batch_id = await repository.create_batch("首次联合比对", "bootstrap_compare")
    generation_id = await repository.create_generation(batch_id, version_id)
    history_source = await repository.create_source(
        file_name="history.xlsx",
        file_hash="history".ljust(64, "0"),
        source_type="history",
        business_columns=[],
        generation_id=generation_id,
    )
    daily_source = await repository.create_source(
        file_name="daily.xlsx",
        file_hash="daily".ljust(64, "0"),
        source_type="daily",
        business_columns=[],
        generation_id=generation_id,
    )
    await repository.upsert_records(
        [
            {
                "source_id": history_source,
                "source_batch_id": batch_id,
                "source_file_hash": "history".ljust(64, "0"),
                "generation_id": generation_id,
                "source_row": 2,
                "row_hash": "history-row".ljust(64, "0"),
                "data_source": "history",
                "parser_version": "rules-v1",
                "raw_json": {},
            },
            {
                "source_id": daily_source,
                "source_batch_id": batch_id,
                "source_file_hash": "daily".ljust(64, "0"),
                "generation_id": generation_id,
                "source_row": 2,
                "row_hash": "daily-row".ljust(64, "0"),
                "data_source": "daily",
                "parser_version": "rules-v1",
                "raw_json": {},
            },
        ]
    )

    daily_rows = await repository.records_for_batch(
        batch_id, data_source="daily"
    )

    assert len(daily_rows) == 1
    assert daily_rows[0]["data_source"] == "daily"
    assert len(await repository.records_for_batch(batch_id, limit=1)) == 1


@pytest.mark.asyncio
async def test_candidate_alias_is_excluded_from_approved_dictionary_maps(repository):
    version_id = await repository.create_dictionary_version(
        "dict-alias-state", status="approved"
    )
    street_id = await repository.create_street("江海区", "礼乐街道", version_id)
    await repository.create_anchor(
        street_id, "德昌电机门口", "landmark", version_id
    )
    async with repository.database.engine.begin() as connection:
        await connection.execute(
            update(anchor_aliases).values(review_status="candidate")
        )

    mappings = await repository.dictionary_maps(version_id)

    assert mappings["anchors"] == {}


@pytest.mark.asyncio
async def test_event_filter_regions_remain_switchable_when_region_is_selected(repository):
    batch_id = await repository.create_batch("历史", "bootstrap_history")
    version_id = await repository.create_dictionary_version(
        "dict-filter-options", status="approved"
    )
    generation_id = await repository.create_generation(batch_id, version_id)
    await repository.activate_generation(generation_id)
    issue_id = await repository.create_issue("道路积水", version_id)
    event_ids = []
    for region, street_name, anchor_name in (
        ("江海区", "礼乐街道", "德昌电机门口"),
        ("蓬江区", "白沙街道", "胜利市场门口"),
    ):
        street_id = await repository.create_street(region, street_name, version_id)
        anchor_id = await repository.create_anchor(
            street_id, anchor_name, "landmark", version_id
        )
        event_ids.append(
            await repository.get_or_create_event(
                street_id=street_id,
                anchor_id=anchor_id,
                issue_id=issue_id,
                event_key_version="key-v1",
                event_name=f"{region}｜{street_name}｜{anchor_name}｜道路积水",
                generation_id=generation_id,
            )
        )
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="filter-options".ljust(64, "0"),
        source_type="history",
        business_columns=[],
        generation_id=generation_id,
    )
    record_ids = await repository.upsert_records(
        [
            {
                "source_id": source_id,
                "source_batch_id": batch_id,
                "source_file_hash": "filter-options".ljust(64, "0"),
                "generation_id": generation_id,
                "source_row": row,
                "row_hash": f"filter-record-{row}".ljust(64, "0"),
                "data_source": "history",
                "parser_version": "rules-v1",
                "raw_json": {},
            }
            for row in (2, 3, 4)
        ]
    )
    await repository.assign_record(event_ids[0], record_ids[0], source="exact_key")
    await repository.assign_record(event_ids[1], record_ids[1], source="exact_key")
    await repository.assign_record(event_ids[1], record_ids[2], source="exact_key")

    options = await repository.event_filter_options(region="江海区")
    descending, _ = await repository.list_event_summaries(sort="member_count_desc")
    ascending, _ = await repository.list_event_summaries(sort="member_count_asc")

    assert options["regions"] == ["江海区", "蓬江区"]
    assert options["streets"] == ["礼乐街道"]
    assert [row["member_count"] for row in descending] == [2, 1]
    assert [row["member_count"] for row in ascending] == [1, 2]


@pytest.mark.asyncio
async def test_event_summaries_filter_daily_events_and_singletons(repository):
    history_batch = await repository.create_batch("历史", "bootstrap_history")
    daily_batch = await repository.create_batch("今日", "daily_increment")
    version_id = await repository.create_dictionary_version(
        "dict-daily-event-filter", status="approved"
    )
    generation_id = await repository.create_generation(history_batch, version_id)
    await repository.activate_generation(generation_id)
    await repository.update_batch_setup(
        daily_batch,
        total_records=2,
        dictionary_version_id=version_id,
        generation_id=generation_id,
    )
    street_id = await repository.create_street("江海区", "礼乐街道", version_id)
    issue_id = await repository.create_issue("道路积水", version_id)
    anchor_ids = [
        await repository.create_anchor(street_id, name, "landmark", version_id)
        for name in ("纯历史地点", "历史今日地点", "今日单条地点")
    ]
    event_ids = [
        await repository.get_or_create_event(
            street_id=street_id,
            anchor_id=anchor_id,
            issue_id=issue_id,
            event_key_version="key-v1",
            event_name=f"礼乐街道｜地点{index}｜道路积水",
            generation_id=generation_id,
        )
        for index, anchor_id in enumerate(anchor_ids)
    ]
    history_source = await repository.create_source(
        file_name="history.xlsx",
        file_hash="daily-filter-history".ljust(64, "0"),
        source_type="history",
        business_columns=[],
        generation_id=generation_id,
    )
    daily_source = await repository.create_source(
        file_name="daily.xlsx",
        file_hash="daily-filter-new".ljust(64, "0"),
        source_type="daily",
        business_columns=[],
        generation_id=generation_id,
    )
    record_ids = await repository.upsert_records(
        [
            {
                "source_id": history_source,
                "source_batch_id": history_batch,
                "source_file_hash": "daily-filter-history".ljust(64, "0"),
                "generation_id": generation_id,
                "source_row": row,
                "row_hash": f"history-{row}".ljust(64, "0"),
                "data_source": "history",
                "raw_json": {},
            }
            for row in (2, 3)
        ]
        + [
            {
                "source_id": daily_source,
                "source_batch_id": daily_batch,
                "source_file_hash": "daily-filter-new".ljust(64, "0"),
                "generation_id": generation_id,
                "source_row": row,
                "row_hash": f"daily-{row}".ljust(64, "0"),
                "data_source": "daily",
                "raw_json": {},
            }
            for row in (2, 3)
        ]
    )
    await repository.assign_record(event_ids[0], record_ids[0], source="exact_key")
    await repository.assign_record(event_ids[1], record_ids[1], source="exact_key")
    await repository.assign_record(event_ids[1], record_ids[2], source="exact_key")
    await repository.assign_record(event_ids[2], record_ids[3], source="exact_key")
    await repository.commit_batch(daily_batch)

    daily_events, daily_total = await repository.list_event_summaries(
        has_daily_records=True
    )
    multi_daily_events, multi_daily_total = await repository.list_event_summaries(
        has_daily_records=True, hide_singletons=True
    )

    assert daily_total == 2
    assert {row["id"] for row in daily_events} == {event_ids[1], event_ids[2]}
    assert multi_daily_total == 1
    assert [row["id"] for row in multi_daily_events] == [event_ids[1]]
    assert await repository.count_singleton_events() == 2


@pytest.mark.asyncio
async def test_active_event_queries_hide_events_without_members(repository):
    batch_id = await repository.create_batch("历史", "bootstrap_history")
    version_id = await repository.create_dictionary_version(
        "dict-hide-empty-events", status="approved"
    )
    generation_id = await repository.create_generation(batch_id, version_id)
    await repository.activate_generation(generation_id)
    issue_id = await repository.create_issue("道路积水", version_id)

    valid_street = await repository.create_street("江海区", "礼乐街道", version_id)
    valid_anchor = await repository.create_anchor(
        valid_street, "德昌电机门口", "landmark", version_id
    )
    valid_event = await repository.get_or_create_event(
        street_id=valid_street,
        anchor_id=valid_anchor,
        issue_id=issue_id,
        event_key_version="event-key-v1",
        event_name="江海区｜礼乐街道｜德昌电机门口｜道路积水",
        generation_id=generation_id,
    )

    empty_street = await repository.create_street("江海区", "脏街道", version_id)
    empty_anchor = await repository.create_anchor(
        empty_street, "空事件地点", "landmark", version_id
    )
    empty_event = await repository.get_or_create_event(
        street_id=empty_street,
        anchor_id=empty_anchor,
        issue_id=issue_id,
        event_key_version="event-key-v1",
        event_name="江海区｜脏街道｜空事件地点｜道路积水",
        generation_id=generation_id,
    )

    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="hide-empty-events".ljust(64, "0"),
        source_type="history",
        business_columns=[],
        generation_id=generation_id,
    )
    record_id = await repository.upsert_record(
        {
            "source_id": source_id,
            "source_batch_id": batch_id,
            "source_file_hash": "hide-empty-events".ljust(64, "0"),
            "generation_id": generation_id,
            "source_row": 2,
            "row_hash": "valid-record".ljust(64, "0"),
            "data_source": "history",
            "parser_version": "rules-v1",
            "raw_json": {},
        }
    )
    await repository.assign_record(valid_event, record_id, source="exact_key")

    assert empty_event != valid_event
    assert await repository.count_events() == 1
    assert [row["id"] for row in await repository.list_events()] == [valid_event]
    assert await repository.search_event_options("空事件") == []
    summaries, total = await repository.list_event_summaries()
    assert total == 1
    assert [row["id"] for row in summaries] == [valid_event]
    options = await repository.event_filter_options()
    assert "脏街道" not in options["streets"]


@pytest.mark.asyncio
async def test_commit_batch_updates_outcome_counts_and_archives_empty_events(repository):
    history_batch = await repository.create_batch("历史", "bootstrap_history")
    version_id = await repository.create_dictionary_version(
        "dict-batch-outcome", status="approved"
    )
    generation_id = await repository.create_generation(history_batch, version_id)
    await repository.activate_generation(generation_id)
    street_id = await repository.create_street("江海区", "礼乐街道", version_id)
    issue_id = await repository.create_issue("道路积水", version_id)
    anchor_ids = [
        await repository.create_anchor(street_id, name, "landmark", version_id)
        for name in ("历史地点", "新增地点", "空事件地点")
    ]
    event_ids = [
        await repository.get_or_create_event(
            street_id=street_id,
            anchor_id=anchor_id,
            issue_id=issue_id,
            event_key_version="event-key-v1",
            event_name=f"礼乐街道｜地点{index}｜道路积水",
            generation_id=generation_id,
        )
        for index, anchor_id in enumerate(anchor_ids)
    ]

    history_source = await repository.create_source(
        file_name="history.xlsx",
        file_hash="batch-outcome-history".ljust(64, "0"),
        source_type="history",
        business_columns=[],
        generation_id=generation_id,
    )
    historical_record = await repository.upsert_record(
        {
            "source_id": history_source,
            "source_batch_id": history_batch,
            "source_file_hash": "batch-outcome-history".ljust(64, "0"),
            "generation_id": generation_id,
            "source_row": 2,
            "row_hash": "historical-record".ljust(64, "0"),
            "data_source": "history",
            "parser_version": "rules-v1",
            "raw_json": {},
            "committed": True,
        }
    )
    await repository.assign_record(event_ids[0], historical_record, source="exact_key")

    daily_batch = await repository.create_batch("今日", "daily_increment")
    await repository.update_batch_setup(
        daily_batch,
        total_records=2,
        dictionary_version_id=version_id,
        generation_id=generation_id,
    )
    daily_source = await repository.create_source(
        file_name="daily.xlsx",
        file_hash="batch-outcome-daily".ljust(64, "0"),
        source_type="daily",
        business_columns=[],
        generation_id=generation_id,
    )
    daily_records = await repository.upsert_records(
        [
            {
                "source_id": daily_source,
                "source_batch_id": daily_batch,
                "source_file_hash": "batch-outcome-daily".ljust(64, "0"),
                "generation_id": generation_id,
                "source_row": row,
                "row_hash": f"daily-record-{row}".ljust(64, "0"),
                "data_source": "daily",
                "parser_version": "rules-v1",
                "raw_json": {},
            }
            for row in (2, 3)
        ]
    )
    await repository.assign_record(event_ids[0], daily_records[0], source="exact_key")
    await repository.assign_record(event_ids[1], daily_records[1], source="exact_key")

    await repository.commit_batch(daily_batch)

    batch = await repository.get_batch(daily_batch)
    assert batch["matched_records"] == 1
    assert batch["new_events"] == 1
    assert await repository.count_events() == 2
    async with repository.database.engine.connect() as connection:
        empty_status = await connection.scalar(
            select(events.c.status).where(events.c.id == event_ids[2])
        )
    assert empty_status == "archived"


@pytest.mark.asyncio
async def test_dictionary_anchor_candidates_are_inserted_in_bulk(repository):
    version_id = await repository.create_dictionary_version("dict-bulk-anchors")
    anchors = [
        {
            "street_key": ("江海区", "礼乐街道"),
            "canonical_name": f"未知地点{index}",
            "anchor_type": "unknown",
        }
        for index in range(20)
    ]
    statement_count = 0

    def count_statement(*_args):
        nonlocal statement_count
        statement_count += 1

    event.listen(repository.database.engine.sync_engine, "before_cursor_execute", count_statement)
    try:
        await repository.ensure_dictionary_items(
            dictionary_version_id=version_id,
            streets=[("江海区", "礼乐街道")],
            anchors=anchors,
            issues=[{"canonical_name": "未识别事项"}],
            review_status="candidate",
        )
    finally:
        event.remove(
            repository.database.engine.sync_engine,
            "before_cursor_execute",
            count_statement,
        )
    assert statement_count <= 12


@pytest.mark.asyncio
async def test_existing_dictionary_evidence_is_updated_in_bulk(repository):
    version_id = await repository.create_dictionary_version("dict-bulk-evidence")
    streets = [("江海区", f"街道{index}") for index in range(20)]
    anchors = [
        {
            "street_key": street,
            "canonical_name": f"地点{index}",
            "anchor_type": "landmark",
        }
        for index, street in enumerate(streets)
    ]
    issues = [{"canonical_name": f"事项{index}"} for index in range(20)]
    await repository.ensure_dictionary_items(
        dictionary_version_id=version_id,
        streets=streets,
        anchors=anchors,
        issues=issues,
        review_status="candidate",
    )

    statement_count = 0

    def count_statement(*_args):
        nonlocal statement_count
        statement_count += 1

    event.listen(repository.database.engine.sync_engine, "before_cursor_execute", count_statement)
    try:
        await repository.ensure_dictionary_items(
            dictionary_version_id=version_id,
            streets=streets * 2,
            anchors=anchors * 2,
            issues=issues * 2,
            review_status="candidate",
        )
    finally:
        event.remove(
            repository.database.engine.sync_engine,
            "before_cursor_execute",
            count_statement,
        )

    assert statement_count <= 9


@pytest.mark.asyncio
async def test_dictionary_candidates_expose_evidence_and_support_item_review(repository):
    version_id = await repository.create_dictionary_version("dict-items")
    await repository.ensure_dictionary_items(
        dictionary_version_id=version_id,
        streets=[("江海区", "礼乐街道"), ("江海区", "礼乐街道")],
        anchors=[
            {
                "street_key": ("江海区", "礼乐街道"),
                "canonical_name": "德昌电机门口",
                "anchor_type": "landmark",
            },
            {
                "street_key": ("江海区", "礼乐街道"),
                "canonical_name": "德昌电机门口",
                "anchor_type": "landmark",
            },
        ],
        issues=[
            {"canonical_name": "道路积水"},
            {"canonical_name": "道路积水"},
        ],
        review_status="candidate",
    )

    items, total = await repository.list_dictionary_items(
        version_id, dimension="anchor", limit=20, offset=0
    )
    assert total == 1
    assert items[0]["evidence_count"] == 2
    assert items[0]["review_status"] == "candidate"

    await repository.review_dictionary_item(
        version_id,
        dimension="anchor",
        item_id=items[0]["id"],
        action="approve",
        reviewed_by="tester",
    )
    approved, _ = await repository.list_dictionary_items(
        version_id, dimension="anchor", status="approved", limit=20, offset=0
    )
    assert [row["id"] for row in approved] == [items[0]["id"]]
    assert len(await repository.list_dictionary_review_actions(version_id)) == 1


@pytest.mark.asyncio
async def test_dictionary_anchor_can_split_alias_then_merge_into_another_item(repository):
    version_id = await repository.create_dictionary_version("dict-structure")
    street_id = await repository.create_street(
        "江海区", "礼乐街道", version_id, review_status="candidate"
    )
    source_id = await repository.create_anchor(
        street_id,
        "德昌电机",
        "landmark",
        version_id,
        review_status="candidate",
    )
    target_id = await repository.create_anchor(
        street_id,
        "德昌电机正门",
        "landmark",
        version_id,
        review_status="candidate",
    )
    alias_id = await repository.add_dictionary_alias(
        version_id,
        dimension="anchor",
        item_id=source_id,
        alias="德昌电机后门",
        evidence_count=2,
    )

    split_id = await repository.split_dictionary_item(
        version_id,
        dimension="anchor",
        item_id=source_id,
        new_name="德昌电机后门",
        alias_ids=[alias_id],
        reviewed_by="tester",
    )
    split = await repository.get_dictionary_item(
        version_id, dimension="anchor", item_id=split_id
    )
    assert [row["alias"] for row in split["aliases"]] == ["德昌电机后门"]

    await repository.merge_dictionary_item(
        version_id,
        dimension="anchor",
        item_id=split_id,
        target_id=target_id,
        reviewed_by="tester",
    )
    merged = await repository.get_dictionary_item(
        version_id, dimension="anchor", item_id=split_id
    )
    target = await repository.get_dictionary_item(
        version_id, dimension="anchor", item_id=target_id
    )
    assert merged["item"]["review_status"] == "merged"
    assert "德昌电机后门" in [row["alias"] for row in target["aliases"]]


@pytest.mark.asyncio
async def test_long_anchor_alias_uses_hash_identity(repository):
    version_id = await repository.create_dictionary_version("dict-long-alias")
    street_id = await repository.create_street(
        "江海区", "礼乐街道", version_id, review_status="candidate"
    )
    anchor_id = await repository.create_anchor(
        street_id,
        "长地址锚点",
        "landmark",
        version_id,
        review_status="candidate",
    )
    long_alias = "江门市高新区长地址" * 300

    alias_id = await repository.add_dictionary_alias(
        version_id,
        dimension="anchor",
        item_id=anchor_id,
        alias=long_alias,
    )

    async with repository.database.engine.connect() as connection:
        row = (
            await connection.execute(
                select(anchor_aliases).where(anchor_aliases.c.id == alias_id)
            )
        ).mappings().one()
    assert row["alias"] == long_alias
    assert len(row["alias_key_hash"]) == 64


@pytest.mark.asyncio
async def test_event_rename_and_exclude_member_are_audited(repository):
    version_id = await repository.create_dictionary_version("dict-actions", status="approved")
    street_id = await repository.create_street("江海区", "礼乐街道", version_id)
    anchor_id = await repository.create_anchor(
        street_id, "德昌电机门口", "landmark", version_id
    )
    issue_id = await repository.create_issue("道路积水", version_id)
    event_id = await repository.get_or_create_event(
        street_id=street_id,
        anchor_id=anchor_id,
        issue_id=issue_id,
        event_key_version="key-v1",
        event_name="旧名称",
    )
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="2" * 64,
        source_type="history",
        business_columns=[],
    )
    batch_id = await repository.create_batch("历史", "bootstrap_history")
    record_ids = []
    for row in (2, 3):
        record_id = await repository.upsert_record(
            {
                "source_id": source_id,
                "source_batch_id": batch_id,
                "source_file_hash": "2" * 64,
                "source_row": row,
                "row_hash": str(row) * 64,
                "data_source": "history",
                "work_order_id": f"WO-{row}",
                "street_id": street_id,
                "anchor_id": anchor_id,
                "issue_id": issue_id,
                "parser_version": "rules-v1",
                "raw_json": {},
            }
        )
        await repository.assign_record(event_id, record_id, source="exact_key")
        record_ids.append(record_id)

    await repository.rename_event(event_id, "礼乐街道｜德昌电机门口｜道路积水", reviewed_by="tester")
    target_id = await repository.exclude_record(
        event_id, record_ids[1], target_name="德昌电机北侧积水", reviewed_by="tester"
    )
    assert target_id != event_id
    assert await repository.event_member_ids(event_id) == [record_ids[0]]
    assert await repository.event_member_ids(target_id) == [record_ids[1]]
    assert len(await repository.list_event_snapshots(event_id)) >= 2
    assert len(await repository.list_review_actions()) == 2


@pytest.mark.asyncio
async def test_excluding_member_creates_cannot_links_in_bulk(repository):
    version_id = await repository.create_dictionary_version(
        "dict-bulk-exclude", status="approved"
    )
    street_id = await repository.create_street("江海区", "礼乐街道", version_id)
    anchor_id = await repository.create_anchor(
        street_id, "德昌电机门口", "landmark", version_id
    )
    issue_id = await repository.create_issue("道路积水", version_id)
    event_id = await repository.get_or_create_event(
        street_id=street_id,
        anchor_id=anchor_id,
        issue_id=issue_id,
        event_key_version="key-v1",
        event_name="礼乐街道｜德昌电机门口｜道路积水",
    )
    source_id = await repository.create_source(
        file_name="history.xlsx",
        file_hash="bulk-exclude".ljust(64, "0"),
        source_type="history",
        business_columns=[],
    )
    batch_id = await repository.create_batch("历史", "bootstrap_history")
    record_ids = await repository.upsert_records(
        [
            {
                "source_id": source_id,
                "source_batch_id": batch_id,
                "source_file_hash": "bulk-exclude".ljust(64, "0"),
                "source_row": row,
                "row_hash": f"{row:064d}",
                "data_source": "history",
                "work_order_id": f"WO-{row}",
                "street_id": street_id,
                "anchor_id": anchor_id,
                "issue_id": issue_id,
                "parser_version": "rules-v1",
                "raw_json": {},
            }
            for row in range(2, 22)
        ]
    )
    for record_id in record_ids:
        await repository.assign_record(event_id, record_id, source="exact_key")

    statement_count = 0

    def count_statement(*_args):
        nonlocal statement_count
        statement_count += 1

    event.listen(repository.database.engine.sync_engine, "before_cursor_execute", count_statement)
    try:
        await repository.exclude_record(
            event_id,
            record_ids[-1],
            target_name="德昌电机北侧积水",
            reviewed_by="tester",
        )
    finally:
        event.remove(
            repository.database.engine.sync_engine,
            "before_cursor_execute",
            count_statement,
        )

    async with repository.database.engine.connect() as connection:
        cannot_link_count = await connection.scalar(select(func.count(cannot_links.c.id)))
    assert cannot_link_count == 19
    assert statement_count <= 18
