from datetime import UTC, datetime
from pathlib import Path
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import insert, select, update

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_models import InputRecord
from complaint_dedup.corpus_pipeline import CorpusProcessor
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.corpus_schema import cannot_links, corpus_records
from complaint_dedup.history_rebuild import rebuild_active_generation


@pytest_asyncio.fixture
async def corpus(tmp_path: Path):
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'rebuild.db'}")
    await database.initialize()
    repository = CorpusRepository(database)
    processor = CorpusProcessor(repository)
    yield repository, processor
    await database.close()


def record(row: int) -> InputRecord:
    return InputRecord(
        source="B",
        source_row=row,
        work_order_id=f"WO-{row}",
        title="德昌电机门口积水",
        category="道路积水",
        category_level_4="道路积水",
        appeal_text="地址：江海区礼乐街道德昌电机门口。\n事项：道路积水。",
        received_at="2026-08-12 08:00:00",
        raw_fields={"事发地点": "江海区礼乐街道德昌电机门口"},
    )


@pytest.mark.asyncio
async def test_rebuild_preserves_records_and_supports_activation_rollback(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="旧历史库",
        batch_type="bootstrap_history",
        file_name="old.xlsx",
        file_hash="a" * 64,
        records=[record(2), record(3)],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )
    old_id = int((await repository.active_generation())["id"])

    _, new_id, report = await rebuild_active_generation(repository.database)
    assert report["status"] == "review_required"
    assert report["old_metrics"]["record_count"] == 2
    assert report["new_metrics"]["record_count"] == 2
    assert (await repository.active_generation())["id"] == old_id

    await repository.activate_generation(new_id)
    assert (await repository.active_generation())["id"] == new_id
    await repository.activate_generation(old_id)
    assert (await repository.active_generation())["id"] == old_id


@pytest.mark.asyncio
async def test_rebuild_isolates_inherited_cannot_link_conflicts(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="旧历史库",
        batch_type="bootstrap_history",
        file_name="conflict.xlsx",
        file_hash="b" * 64,
        records=[record(2), record(3)],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )
    members = [
        row
        for row in await repository.list_events()
        if len(await repository.event_member_ids(row["id"])) == 2
    ]
    left, right = sorted(await repository.event_member_ids(members[0]["id"]))
    async with repository.database.engine.begin() as connection:
        await connection.execute(
            insert(cannot_links).values(
                left_record_id=left,
                right_record_id=right,
                reason="重建测试",
                source="manual",
                    created_at=datetime.now(UTC),
            )
        )

    _, new_id, report = await rebuild_active_generation(repository.database)

    assert report["cannot_link_conflict_splits"] == 2
    assert (await repository.generation_metrics(new_id))["singleton_count"] == 2


@pytest.mark.asyncio
async def test_rebuild_preserves_data_source_and_completion_fields(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="旧历史库",
        batch_type="bootstrap_history",
        file_name="sources.xlsx",
        file_hash="c" * 64,
        records=[record(2), record(3), record(4)],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )
    old_id = int((await repository.active_generation())["id"])
    async with repository.database.engine.begin() as connection:
        old_rows = (
            await connection.execute(
                select(corpus_records.c.id)
                .where(corpus_records.c.generation_id == old_id)
                .order_by(corpus_records.c.id)
            )
        ).scalars().all()
        await connection.execute(
            update(corpus_records)
            .where(corpus_records.c.id == old_rows[1])
            .values(
                data_source="daily",
                processing_department="市场监管局",
                completed_at=datetime(2026, 8, 13, 10, tzinfo=UTC),
                raw_json={
                    "工单编号": "WO-3",
                    "处理部门": "市场监管局",
                    "办结时间": "2026-08-13T10:00:00+00:00",
                },
            )
        )
        await connection.execute(
            update(corpus_records)
            .where(corpus_records.c.id == old_rows[2])
            .values(data_source="correction")
        )

    _, new_id, _ = await rebuild_active_generation(repository.database)
    async with repository.database.engine.connect() as connection:
        rows = (
            await connection.execute(
                select(
                    corpus_records.c.data_source,
                    corpus_records.c.processing_department,
                    corpus_records.c.completed_at,
                    corpus_records.c.raw_json,
                )
                .where(corpus_records.c.generation_id == new_id)
                .order_by(corpus_records.c.id)
            )
        ).mappings().all()

    assert [row["data_source"] for row in rows] == ["history", "daily", "correction"]
    assert rows[1]["processing_department"] == "市场监管局"
    assert rows[1]["completed_at"] is not None
    assert rows[1]["raw_json"]["办结时间"] == "2026-08-13T10:00:00+00:00"


@pytest.mark.asyncio
async def test_unmapped_cannot_link_does_not_split_rebuilt_event(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="旧历史库",
        batch_type="bootstrap_history",
        file_name="unmapped-link.xlsx",
        file_hash="d" * 64,
        records=[record(2), record(3)],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )
    old_id = int((await repository.active_generation())["id"])
    _, new_id, _ = await rebuild_active_generation(repository.database)
    mapping = await repository.generation_record_id_map(old_id, new_id)
    new_record_id = next(iter(mapping.values()))
    old_record_id = next(iter(mapping))
    async with repository.database.engine.begin() as connection:
        await connection.execute(
            insert(cannot_links).values(
                left_record_id=old_record_id,
                right_record_id=new_record_id,
                reason="其他代次关系",
                source="manual",
                created_at=datetime.now(UTC),
            )
        )

    split_count = await repository.split_conflicting_events(new_id, mapping)
    assert split_count == 0
