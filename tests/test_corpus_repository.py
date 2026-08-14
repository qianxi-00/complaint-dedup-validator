from datetime import UTC, datetime
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import update

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.corpus_schema import anchor_aliases


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
