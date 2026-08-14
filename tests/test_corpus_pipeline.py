from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import func, select

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_pipeline import CorpusProcessor
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.corpus_schema import canonical_anchors
from complaint_dedup.pipeline import InputRecord


@pytest_asyncio.fixture
async def corpus(tmp_path: Path):
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'pipeline.db'}")
    await database.initialize()
    repository = CorpusRepository(database)
    processor = CorpusProcessor(repository)
    yield repository, processor
    await database.close()


def record(
    row: int,
    *,
    address: str,
    title: str,
    issue: str = "道路积水",
    received_at: str = "2026-08-12 08:00:00",
) -> InputRecord:
    return InputRecord(
        source="B",
        source_row=row,
        work_order_id=f"WO-{row}",
        title=title,
        category=issue,
        appeal_text=f"地址：{address}。\n事项：{issue}。",
        received_at=received_at,
        category_level_4=issue,
        raw_fields={"事发地点": address, "联系电话": "13800138000"},
    )


@pytest.mark.asyncio
async def test_bootstrap_requires_dictionary_approval_then_freezes_events(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="a" * 64,
        records=[
            record(2, address="江海区礼乐街道德昌电机门口", title="德昌电机门口积水"),
            record(3, address="江海区礼乐街道德昌电机门口", title="德昌电机门口积水"),
        ],
    )
    assert await repository.active_dictionary_version() is None
    assert (await repository.get_batch(staged.batch_id))["status"] == "reviewing"
    runs = await repository.list_dictionary_extraction_runs(
        staged.dictionary_version_id
    )
    assert len(runs) == 1
    assert runs[0]["source_file_hash"] == "a" * 64
    assert runs[0]["candidate_counts"]["anchors"] == 1
    assert runs[0]["status"] == "completed"

    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )
    events = await repository.list_events()
    assert len(events) == 1
    assert events[0]["is_frozen"] is True
    assert len(await repository.event_member_ids(events[0]["id"])) == 2
    assert (await repository.get_batch(staged.batch_id))["status"] == "committed"


@pytest.mark.asyncio
async def test_same_title_with_different_anchor_creates_separate_events(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="b" * 64,
        records=[
            record(2, address="江海区礼乐街道德昌电机门口", title="道路积水问题"),
            record(3, address="江海区礼乐街道文华豪庭北门", title="道路积水问题"),
        ],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )
    events = await repository.list_events()
    assert len(events) == 2


@pytest.mark.asyncio
async def test_same_brand_at_different_road_addresses_creates_separate_events(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="3" * 64,
        records=[
            record(
                2,
                address="江海区外海街道东海路46号西屋厨房小家电",
                title="西屋消费纠纷",
                issue="消费纠纷",
            ),
            record(
                3,
                address="江海区外海街道金瓯路188号西屋厨房小家电",
                title="西屋消费纠纷",
                issue="消费纠纷",
            ),
        ],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    events = await repository.list_events()
    assert len(events) == 2
    names = {row["event_name"] for row in events}
    assert any("东海路46号" in name for name in names)
    assert any("金瓯路188号" in name for name in names)


@pytest.mark.asyncio
async def test_same_address_different_shop_numbers_are_named_separately(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="shop-scope".ljust(64, "0"),
        records=[
            record(
                2,
                address="江海区外海街道东海路46号西屋厨房小家电商铺A-12",
                title="西屋消费纠纷",
                issue="消费纠纷",
            ),
            record(
                3,
                address="江海区外海街道东海路46号西屋厨房小家电商铺B-8",
                title="西屋消费纠纷",
                issue="消费纠纷",
            ),
        ],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    events = await repository.list_events()
    assert len(events) == 2
    names = {row["event_name"] for row in events}
    assert any("商铺A-12" in name for name in names)
    assert any("商铺B-8" in name for name in names)


@pytest.mark.asyncio
async def test_same_location_common_entity_suffix_variants_share_event(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="2" * 64,
        records=[
            record(
                2,
                address="江海区江南街道东海路46号艾尚梵廷健身中心",
                title="艾尚梵廷消费纠纷",
                issue="消费纠纷",
            ),
            record(
                3,
                address="江海区江南街道东海路46号艾尚梵廷健身房",
                title="艾尚梵廷消费纠纷",
                issue="消费纠纷",
            ),
        ],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    events = await repository.list_events()
    assert len(events) == 1
    assert len(await repository.event_member_ids(events[0]["id"])) == 2


@pytest.mark.asyncio
async def test_same_property_location_with_independent_issues_stays_separate(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="1" * 64,
        records=[
            record(
                2,
                address="江海区江南街道明泰城状元居",
                title="明泰城物业费质价不符",
                issue="物业服务纠纷",
            ),
            record(
                3,
                address="江海区江南街道明泰城状元居",
                title="明泰城地下车库乱停车",
                issue="物业服务纠纷",
            ),
        ],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    assert len(await repository.list_events()) == 2


@pytest.mark.asyncio
async def test_property_fee_wording_variants_share_core_issue(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="0" * 64,
        records=[
            record(
                2,
                address="江海区江南街道明泰城状元居",
                title="明泰城物业费过高",
                issue="物业服务纠纷",
            ),
            record(
                3,
                address="江海区江南街道明泰城状元居",
                title="明泰城物业质价不符",
                issue="物业服务纠纷",
            ),
        ],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    events = await repository.list_events()
    assert len(events) == 1
    assert len(await repository.event_member_ids(events[0]["id"])) == 2


@pytest.mark.asyncio
async def test_review_required_keeps_unapproved_dictionary_items_as_singletons(corpus):
    repository, _ = corpus
    processor = CorpusProcessor(repository, dictionary_review_required=True)
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="5" * 64,
        records=[
            record(2, address="江海区礼乐街道德昌电机门口", title="积水一"),
            record(3, address="江海区礼乐街道德昌电机门口", title="积水二"),
        ],
    )

    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    events = await repository.list_events()
    assert len(events) == 2
    member_counts = [
        len(await repository.event_member_ids(row["id"])) for row in events
    ]
    assert member_counts == [1, 1]


@pytest.mark.asyncio
async def test_reviewed_dictionary_items_can_form_exact_frozen_event(corpus):
    repository, _ = corpus
    processor = CorpusProcessor(repository, dictionary_review_required=True)
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="4" * 64,
        records=[
            record(2, address="江海区礼乐街道德昌电机门口", title="积水一"),
            record(3, address="江海区礼乐街道德昌电机门口", title="积水二"),
        ],
    )
    await repository.bulk_approve_dictionary_items(
        staged.dictionary_version_id, min_evidence=2, reviewed_by="tester"
    )

    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    events = await repository.list_events()
    assert len(events) == 1
    assert len(await repository.event_member_ids(events[0]["id"])) == 2


@pytest.mark.asyncio
async def test_daily_exact_key_appends_to_existing_frozen_event(corpus):
    repository, processor = corpus
    bootstrap = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="c" * 64,
        records=[record(2, address="江海区礼乐街道德昌电机门口", title="积水")],
    )
    await processor.approve_bootstrap(
        bootstrap.batch_id, bootstrap.dictionary_version_id, approved_by="tester"
    )
    event_id = (await repository.list_events())[0]["id"]

    daily = await processor.stage_records(
        name="今日新增",
        batch_type="daily_increment",
        file_name="daily.xlsx",
        file_hash="d" * 64,
        records=[
            record(
                2,
                address="江海区礼乐街道德昌电机门口",
                title="再次反映积水",
                received_at="2026-08-13 08:00:00",
            )
        ],
    )
    await processor.commit_increment(daily.batch_id)
    assert len(await repository.event_member_ids(event_id)) == 2
    assert len(await repository.list_events()) == 1


@pytest.mark.asyncio
async def test_daily_explicit_previous_work_order_link_reuses_historical_event(corpus):
    repository, processor = corpus
    historical_id = "0826081308493182401"
    history_record = InputRecord(
        source="B",
        source_row=2,
        work_order_id=historical_id,
        received_at="2026-08-12 08:00:00",
        title="德昌电机门口积水",
        category="道路积水",
        category_level_4="道路积水",
        appeal_text="地址：江海区礼乐街道德昌电机门口。\n事项：道路积水。",
        raw_fields={"事发地点": "江海区礼乐街道德昌电机门口"},
    )
    bootstrap = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="f" * 64,
        records=[history_record],
    )
    await processor.approve_bootstrap(
        bootstrap.batch_id, bootstrap.dictionary_version_id, approved_by="tester"
    )
    event_id = (await repository.list_events())[0]["id"]
    daily_record = InputRecord(
        source="A",
        source_row=2,
        work_order_id="0826081409000000001",
        received_at="2026-08-13 09:00:00",
        title="再次反映此前问题未解决",
        category="道路积水",
        category_level_4="道路积水",
        appeal_text=f"市民表示此前工单{historical_id}仍未解决。",
        raw_fields={},
    )
    daily = await processor.stage_records(
        name="今日新增",
        batch_type="daily_increment",
        file_name="daily.xlsx",
        file_hash="e" * 64,
        records=[daily_record],
    )

    await processor.commit_increment(daily.batch_id)

    assert await repository.event_member_ids(event_id) == [1, 2]
    assert len(await repository.list_events()) == 1


@pytest.mark.asyncio
async def test_daily_same_title_and_address_scope_reuses_historical_event(corpus):
    repository, processor = corpus
    bootstrap = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="strong-title-history".ljust(64, "0"),
        records=[
            record(
                2,
                address="江海区礼乐街道东海路46号西屋厨房小家电",
                title="西屋消费纠纷",
                issue="消费纠纷",
            )
        ],
    )
    await processor.approve_bootstrap(
        bootstrap.batch_id, bootstrap.dictionary_version_id, approved_by="tester"
    )
    event_id = (await repository.list_events())[0]["id"]

    daily = await processor.stage_records(
        name="今日新增",
        batch_type="daily_increment",
        file_name="daily.xlsx",
        file_hash="strong-title-daily".ljust(64, "0"),
        records=[
            record(
                2,
                address="江海区礼乐街道东海路46号商铺A",
                title="西屋消费纠纷",
                issue="消费纠纷",
                received_at="2026-08-13 08:00:00",
            )
        ],
    )
    await processor.commit_increment(daily.batch_id)

    assert await repository.event_member_ids(event_id) == [1, 2]
    assert (await repository.list_events())[0]["event_name"].endswith("消费纠纷")


@pytest.mark.asyncio
async def test_daily_same_phone_different_issue_does_not_reuse_event(corpus):
    repository, processor = corpus
    bootstrap = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="strong-phone-history".ljust(64, "0"),
        records=[
            record(
                2,
                address="江海区礼乐街道东海路46号西屋厨房小家电",
                title="西屋消费纠纷",
                issue="消费纠纷",
            )
        ],
    )
    await processor.approve_bootstrap(
        bootstrap.batch_id, bootstrap.dictionary_version_id, approved_by="tester"
    )
    event_id = (await repository.list_events())[0]["id"]

    daily = await processor.stage_records(
        name="今日新增",
        batch_type="daily_increment",
        file_name="daily.xlsx",
        file_hash="strong-phone-daily".ljust(64, "0"),
        records=[
            record(
                2,
                address="江海区礼乐街道东海路46号商铺A",
                title="路灯问题",
                issue="路灯故障",
                received_at="2026-08-13 08:00:00",
            )
        ],
    )
    await processor.commit_increment(daily.batch_id)

    assert await repository.event_member_ids(event_id) == [1]
    assert len(await repository.list_events()) == 2


@pytest.mark.asyncio
async def test_daily_unknown_location_commits_as_safe_singleton(corpus):
    repository, processor = corpus
    bootstrap = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="f" * 64,
        records=[record(2, address="江海区礼乐街道德昌电机门口", title="积水")],
    )
    await processor.approve_bootstrap(
        bootstrap.batch_id, bootstrap.dictionary_version_id, approved_by="tester"
    )
    unknown = InputRecord(
        source="A",
        source_row=2,
        work_order_id="NEW-1",
        title="咨询失业保险",
        category="失业保险咨询",
        appeal_text="事项：咨询失业保险待遇。",
        received_at="2026-08-13 10:00:00",
        raw_fields={},
    )
    daily = await processor.stage_records(
        name="今日新增",
        batch_type="daily_increment",
        file_name="daily.xlsx",
        file_hash="0" * 64,
        records=[unknown],
    )
    await processor.commit_increment(daily.batch_id)
    events = await repository.list_events()
    assert len(events) == 2
    daily_record = (await repository.records_for_batch(daily.batch_id))[0]
    assert daily_record["anchor_resolution_status"] == "manual_singleton"
    memberships = [
        await repository.event_member_ids(event["id"]) for event in events
    ]
    assert any(daily_record["id"] in members for members in memberships)
    assert (await repository.get_batch(daily.batch_id))["status"] == "committed"


@pytest.mark.asyncio
async def test_daily_unknown_items_do_not_mutate_published_dictionary(corpus):
    repository, processor = corpus
    bootstrap = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="7" * 64,
        records=[record(2, address="江海区礼乐街道德昌电机门口", title="积水")],
    )
    await processor.approve_bootstrap(
        bootstrap.batch_id, bootstrap.dictionary_version_id, approved_by="tester"
    )
    async with repository.database.engine.connect() as connection:
        before = await connection.scalar(
            select(func.count(canonical_anchors.c.id)).where(
                canonical_anchors.c.dictionary_version_id
                == bootstrap.dictionary_version_id
            )
        )

    daily = await processor.stage_records(
        name="今日新增",
        batch_type="daily_increment",
        file_name="daily.xlsx",
        file_hash="6" * 64,
        records=[
            record(
                2,
                address="江海区外海街道全新地点北门",
                title="全新地点噪声",
                issue="经营噪声",
                received_at="2026-08-13 09:00:00",
            )
        ],
    )
    await processor.commit_increment(daily.batch_id)

    async with repository.database.engine.connect() as connection:
        after = await connection.scalar(
            select(func.count(canonical_anchors.c.id)).where(
                canonical_anchors.c.dictionary_version_id
                == bootstrap.dictionary_version_id
            )
        )
    assert after == before


@pytest.mark.asyncio
async def test_daily_overlap_is_rejected_but_correction_is_allowed(corpus):
    repository, processor = corpus
    bootstrap = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="1" * 64,
        records=[record(2, address="江海区礼乐街道德昌电机门口", title="积水")],
    )
    await processor.approve_bootstrap(
        bootstrap.batch_id, bootstrap.dictionary_version_id, approved_by="tester"
    )
    overlapping = record(
        3, address="江海区礼乐街道德昌电机门口", title="重复日期积水"
    )
    with pytest.raises(ValueError, match="时间范围重叠"):
        await processor.stage_records(
            name="错误每日批次",
            batch_type="daily_increment",
            file_name="daily.xlsx",
            file_hash="3" * 64,
            records=[overlapping],
        )
    correction = await processor.stage_records(
        name="补录批次",
        batch_type="correction",
        file_name="correction.xlsx",
        file_hash="4" * 64,
        records=[overlapping],
    )
    assert (await repository.get_batch(correction.batch_id))["batch_type"] == "correction"
