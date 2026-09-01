from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import event, func, select

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_pipeline import CorpusProcessor
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.corpus_schema import canonical_anchors
from complaint_dedup.corpus_models import InputRecord


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
async def test_correction_mode_is_no_longer_supported(corpus):
    repository, processor = corpus
    with pytest.raises(ValueError, match="批次类型无效"):
        await processor.stage_records(
            name="补录",
            batch_type="correction",
            file_name="correction.xlsx",
            file_hash="c" * 64,
            records=[record(2, address="江海区礼乐街道德昌电机门口", title="积水")],
        )


@pytest.mark.asyncio
async def test_daily_increment_requires_active_history_generation(corpus):
    repository, processor = corpus
    with pytest.raises(ValueError, match="尚未建立活动历史库"):
        await processor.stage_records(
            name="每日新增",
            batch_type="daily_increment",
            file_name="daily.xlsx",
            file_hash="d" * 64,
            records=[record(2, address="江海区礼乐街道德昌电机门口", title="积水")],
        )


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


def transaction_record(
    row: int,
    *,
    address: str,
    title: str,
    facts: str,
    category: str = "消费纠纷",
    received_at: str = "2026-08-12 08:00:00",
) -> InputRecord:
    return InputRecord(
        source="B",
        source_row=row,
        work_order_id=f"TX-{row}",
        title=title,
        category=category,
        category_level_3=category,
        appeal_text=f"地址：{address}。\n事项：{facts}\n备注：请跟进处理。",
        received_at=received_at,
        raw_fields={"事发地点": address, "联系电话": f"1380013{row:04d}"[-11:]},
    )


@pytest.mark.asyncio
async def test_same_merchant_different_orders_do_not_merge(corpus):
    repository, processor = corpus
    address = "江海区外海街道邦民路32号1号厂房自编01江门市西屋厨房小家电有限公司"
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="westinghouse-orders".ljust(64, "0"),
        records=[
            transaction_record(
                2,
                address=address,
                title="西屋破壁机消费纠纷",
                facts="购买破壁机后漏水，订单号：3731137283856671488，要求退款。",
            ),
            transaction_record(
                3,
                address=address,
                title="西屋破壁机消费纠纷",
                facts="购买破壁机后无法启动，订单号：6928579919143468892，要求退款。",
            ),
        ],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    assert len(await repository.list_events()) == 2


@pytest.mark.asyncio
async def test_same_merchant_same_order_merges_across_coarse_categories(corpus):
    repository, processor = corpus
    address = "江海区外海街道邦民路32号01号厂房江门西屋厨房小家电有限公司"
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="westinghouse-same-order".ljust(64, "0"),
        records=[
            transaction_record(
                2,
                address=address,
                title="西屋破壁机漏水",
                facts="订单编号：311242809900，破壁机漏水，要求换货。",
                category="家用电器类",
            ),
            transaction_record(
                3,
                address="江海区外海街道金瓯路341号6幢一楼仓库江门市西屋厨房小家电有限公司",
                title="西屋售后纠纷",
                facts="订单号311242809900，商家拒绝承担换货运费。",
                category="生活、社会服务类",
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
async def test_same_order_converges_when_one_record_first_becomes_singleton(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="same-order-singleton".ljust(64, "0"),
        records=[
            transaction_record(
                2,
                address="广东省",
                title="某商家订单纠纷",
                facts="订单号：260617279477021073138，商家拒绝退款，要求处理。",
                category="食品类",
            ),
            transaction_record(
                3,
                address="江海区外海街道邦民路32号1号厂房某商家",
                title="某商家售后纠纷",
                facts="订单号：260617279477021073138，商家拒绝退款，要求处理。",
                category="农资农具",
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
async def test_same_gym_different_consumers_without_order_stay_separate(corpus):
    repository, processor = corpus
    address = "江海区江南街道东海路46号江海广场三楼艾尚梵廷健身房"
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="gym-consumers".ljust(64, "0"),
        records=[
            transaction_record(
                2,
                address=address,
                title="艾尚梵廷退费",
                facts="4月18日支付4500元购买30节私教课，剩余14节，要求退费。",
            ),
            transaction_record(
                3,
                address=address,
                title="艾尚梵廷退费",
                facts="4月20日支付4800元购买20节私教课，剩余5节，要求退费。",
            ),
        ],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    assert len(await repository.list_events()) == 2


@pytest.mark.asyncio
async def test_culture_center_parking_availability_and_fee_are_separate(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="culture-center-parking".ljust(64, "0"),
        records=[
            transaction_record(
                2,
                address="江海区外海街道中华路外海文化中心停车场",
                title="停车场无法入场",
                facts="明明有剩余车位，入口显示余位为0，车辆无法入场。",
                category="停车场管理",
            ),
            transaction_record(
                3,
                address="江海区外海街道江门市外海文化中心停车场",
                title="停车费与公示不符",
                facts="停放5.5小时被收15元，与公示价格不符，要求退款。",
                category="生活、社会服务类",
            ),
            transaction_record(
                4,
                address="江海区外海街道中华路8号外海中心市场对面外海文化中心停车场",
                title="停车场多收费用",
                facts="停放5小时被收15元，与公示价格不符，要求退回多收费用。",
                category="生活、社会服务类",
            ),
        ],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    events = await repository.list_events()
    assert len(events) == 2
    assert {event["event_name"].split("｜")[-1] for event in events} == {
        "停车场余位与入场",
        "停车收费争议",
    }
    member_counts = [
        len(await repository.event_member_ids(event["id"])) for event in events
    ]
    assert sorted(member_counts) == [1, 2]


@pytest.mark.asyncio
async def test_property_fee_and_parking_facility_issues_use_distinct_names(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="property-issue-rules".ljust(64, "0"),
        records=[
            record(
                2,
                address="江海区江南街道明泰城状元居",
                title="物业管理费质价不符",
                issue="物业服务问题",
            ),
            record(
                3,
                address="江海区江南街道明泰城地下车库",
                title="地下车库消防疏散出口不符合要求",
                issue="物业服务问题",
            ),
            record(
                4,
                address="江海区江南街道明泰城北门",
                title="北门需要增设斑马线",
                issue="小区秩序",
            ),
        ],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    names = {event["event_name"].split("｜")[-1] for event in await repository.list_events()}
    assert names == {"物业收费纠纷", "停车场消防通道", "人行横道设施"}


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
async def test_road_water_wording_variants_share_core_issue(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="water-wording".ljust(64, "0"),
        records=[
            record(
                2,
                address="江海区礼乐街道德昌电机门口上桥路段",
                title="德昌电机门口上桥路段有一滩很大的积水",
                issue="城市道路建设",
            ),
            record(
                3,
                address="江海区礼乐街道德昌电机门口上桥路段",
                title="德昌电机门口上桥路段每逢下大雨均出现水浸",
                issue="道路建设",
            ),
            record(
                4,
                address="江海区礼乐街道德昌电机门口上桥路段",
                title="德昌电机门口上桥路段高架桥积水",
                issue="道路破损",
            ),
        ],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    events = await repository.list_events()
    assert len(events) == 1
    assert events[0]["event_name"].endswith("道路积水")
    assert len(await repository.event_member_ids(events[0]["id"])) == 3


@pytest.mark.asyncio
async def test_bootstrap_auto_approves_backend_dictionary_items(corpus):
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
    assert len(events) == 1
    assert len(await repository.event_member_ids(events[0]["id"])) == 2


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
async def test_daily_new_exact_key_forms_event_and_is_reused_next_day(corpus):
    repository, processor = corpus
    bootstrap = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="new-key-history".ljust(64, "0"),
        records=[record(2, address="江海区礼乐街道德昌电机门口", title="积水")],
    )
    await processor.approve_bootstrap(
        bootstrap.batch_id, bootstrap.dictionary_version_id, approved_by="tester"
    )

    first_daily = await processor.stage_records(
        name="第一天新增",
        batch_type="daily_increment",
        file_name="daily-1.xlsx",
        file_hash="new-key-daily-1".ljust(64, "0"),
        records=[
            record(
                2,
                address="江海区外海街道文化中心停车场北门",
                title="停车场北门噪声一",
                issue="经营噪声",
                received_at="2026-08-13 08:00:00",
            ),
            record(
                3,
                address="江海区外海街道文化中心停车场北门",
                title="停车场北门噪声二",
                issue="经营噪声",
                received_at="2026-08-13 09:00:00",
            ),
        ],
    )
    await processor.commit_increment(first_daily.batch_id)

    events_after_first_day = await repository.list_events()
    assert len(events_after_first_day) == 2
    new_event = None
    for event_row in events_after_first_day:
        if len(await repository.event_member_ids(event_row["id"])) == 2:
            new_event = event_row
            break
    assert new_event is not None

    second_daily = await processor.stage_records(
        name="第二天新增",
        batch_type="daily_increment",
        file_name="daily-2.xlsx",
        file_hash="new-key-daily-2".ljust(64, "0"),
        records=[
            record(
                4,
                address="江海区外海街道文化中心停车场北门",
                title="再次反映停车场北门噪声",
                issue="经营噪声",
                received_at="2026-08-14 08:00:00",
            )
        ],
    )
    await processor.commit_increment(second_daily.batch_id)

    assert len(await repository.list_events()) == 2
    assert len(await repository.event_member_ids(new_event["id"])) == 3


@pytest.mark.asyncio
async def test_daily_safe_singletons_use_batched_repository_path(corpus):
    repository, processor = corpus
    bootstrap = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="batch-singleton-history".ljust(64, "0"),
        records=[record(2, address="江海区礼乐街道德昌电机门口", title="积水")],
    )
    await processor.approve_bootstrap(
        bootstrap.batch_id, bootstrap.dictionary_version_id, approved_by="tester"
    )
    daily = await processor.stage_records(
        name="今日新增",
        batch_type="daily_increment",
        file_name="daily.xlsx",
        file_hash="batch-singleton-daily".ljust(64, "0"),
        records=[
            record(
                row,
                address=f"江海区礼乐街道全新地点{row}号",
                title=f"咨询失业保险{row}",
                issue="失业保险咨询",
                received_at=f"2026-08-13 10:{row:02d}:00",
            )
            for row in range(2, 22)
        ],
    )
    statement_count = 0

    def count_statement(*_args):
        nonlocal statement_count
        statement_count += 1

    event.listen(
        repository.database.engine.sync_engine,
        "before_cursor_execute",
        count_statement,
    )
    try:
        await processor.commit_increment(daily.batch_id)
    finally:
        event.remove(
            repository.database.engine.sync_engine,
            "before_cursor_execute",
            count_statement,
        )

    assert statement_count <= 45
    records = await repository.records_for_batch(daily.batch_id)
    memberships = [
        members
        for event_row in await repository.list_events()
        if (members := await repository.event_member_ids(event_row["id"]))
    ]
    assert all(any(record["id"] in members for members in memberships) for record in records)


@pytest.mark.asyncio
async def test_daily_new_items_extend_dictionary_without_rebuilding_history(corpus):
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
    historical_event_id = (await repository.list_events())[0]["id"]
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
    assert after == before + 1
    assert await repository.event_member_ids(historical_event_id) == [1]
    assert len(await repository.list_events()) == 2


@pytest.mark.asyncio
async def test_daily_overlap_is_rejected_and_correction_is_not_supported(corpus):
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
    with pytest.raises(ValueError, match="批次类型无效"):
        await processor.stage_records(
            name="补录批次",
            batch_type="correction",
            file_name="correction.xlsx",
            file_hash="4" * 64,
            records=[overlapping],
        )
@pytest.mark.asyncio
async def test_bootstrap_compare_commit_only_processes_daily_rows():
    calls: list[tuple[str, str | None]] = []

    class Repository:
        async def get_batch(self, batch_id):
            return {
                "id": batch_id,
                "batch_type": "bootstrap_compare",
                "generation_id": 1,
            }

        async def records_for_batch(self, batch_id, *, data_source=None):
            raise AssertionError("提交阶段不应读取包含正文和 raw_json 的完整工单")

        async def records_for_exact_assignment(
            self, batch_id, *, data_source=None, approved_only=False
        ):
            calls.append(("exact", data_source))
            return []

        async def unassigned_records_for_batch(self, batch_id, *, data_source=None):
            calls.append(("unassigned", data_source))
            return []

        async def bulk_assign_exact_events(self, *args, **kwargs):
            return None

        async def assign_linked_records(self, batch_id, *, data_source=None):
            calls.append(("linked", data_source))
            return set()

        async def assign_strong_signal_records(self, batch_id, *, data_source=None):
            calls.append(("strong", data_source))
            return set()

        async def commit_batch(self, batch_id):
            calls.append(("commit", None))

    processor = CorpusProcessor(Repository(), dictionary_review_required=True)

    await processor.commit_increment("compare-batch")

    assert calls == [
        ("exact", "daily"),
        ("linked", "daily"),
        ("strong", "daily"),
        ("unassigned", "daily"),
        ("commit", None),
    ]
