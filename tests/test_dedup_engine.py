from datetime import date
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import func, select

from complaint_dedup.dedup_engine import _features_conflict

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.corpus_models import InputRecord
from complaint_dedup.corpus_schema import comparison_decisions, comparison_runs
from complaint_dedup.dedup_features import FEATURE_VERSION
from complaint_dedup.full_corpus import FullCorpusService
from complaint_dedup.llm_models import (
    EventCardBatchResponse,
    EventCardGroup,
    EvidenceItem,
)


class FakeLlmClient:
    def __init__(
        self,
        *,
        group_all: bool = True,
        fail: bool = False,
        evidence_field: str = "issue",
    ) -> None:
        self.group_all = group_all
        self.fail = fail
        self.evidence_field = evidence_field
        self.calls: list[list[dict[str, str]]] = []

    async def chat_json(self, messages, response_model):
        self.calls.append(messages)
        if self.fail:
            raise RuntimeError("model unavailable")
        if not self.group_all:
            return EventCardBatchResponse()
        return EventCardBatchResponse(
            groups=[
                EventCardGroup(
                    card_ids=["C0001", "C0002"],
                    confidence=0.96,
                    supporting_evidence=[
                        EvidenceItem(
                            field=self.evidence_field,
                            cards=["C0001", "C0002"],
                            reason="同一地点和同一问题",
                        )
                    ],
                )
            ]
        )


class AllCardsLlmClient:
    async def chat_json(self, messages, response_model):
        raw = messages[1]["content"].split("\n", 1)[1]
        cards = json.loads(raw)
        card_ids = [str(card["card_id"]) for card in cards]
        return EventCardBatchResponse(
            groups=[
                EventCardGroup(
                    card_ids=card_ids,
                    confidence=0.95,
                    supporting_evidence=[
                        EvidenceItem(
                            field="location",
                            cards=card_ids,
                            reason="同一候选桶内地点一致",
                        )
                    ],
                )
            ]
        )


@pytest_asyncio.fixture
async def database(tmp_path):
    value = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'dedup.db'}")
    await value.initialize()
    yield value
    await value.close()


def make_record(
    order_id: str,
    *,
    title: str,
    appeal: str,
    category: str,
    location: str = "江海区礼乐街道东宁路1号",
    received: str = "2026-09-01 08:00:00",
    completed: str = "2026-09-01 10:00:00",
) -> InputRecord:
    return InputRecord(
        source_row=2,
        work_order_id=order_id,
        title=title,
        category=category,
        appeal_text=appeal,
        received_at=received,
        completed_at=completed,
        location=location,
        raw_fields={
            "工单编号": order_id,
            "受理时间": received,
            "办结时间": completed,
            "诉求标题": title,
            "市民诉求": appeal,
            "事项分类": category,
            "事发地点": location,
        },
    )


@pytest.mark.asyncio
async def test_hbd_derived_orders_are_hard_merged(database):
    service = FullCorpusService(database)
    await service.sync_records(
        [
            make_record(
                "BASE001HBD1",
                title="某食品厂食品安全问题",
                appeal="反映某食品厂食品安全问题，地址：江海区礼乐街道东宁路1号。",
                category="食品安全",
            ),
            make_record(
                "BASE001HBD2",
                title="某食品厂食品安全问题",
                appeal="同一诉求转派，地址：江海区礼乐街道东宁路1号。",
                category="食品卫生",
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
            make_record(
                "BASE001",
                title="某食品厂食品安全问题",
                appeal="原始工单，地址：江海区礼乐街道东宁路1号。",
                category="食品安全",
                received="2026-09-03 08:00:00",
                completed="2026-09-03 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 3),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert len(events) == 1
    assert len(events[0]["members"]) == 3
    async with database.engine.connect() as connection:
        audit_count = await connection.scalar(
            select(func.count())
            .select_from(comparison_decisions)
            .where(comparison_decisions.c.comparison_id == comparison.comparison_id)
        )
        run = (
            await connection.execute(
                select(comparison_runs).where(
                    comparison_runs.c.id == comparison.comparison_id
                )
            )
        ).mappings().one()
    assert audit_count >= 1
    assert run["algorithm_version"] == "event-key-v3"
    assert run["feature_version"] == FEATURE_VERSION
    assert run["prompt_version"] == "event-card-v2"


@pytest.mark.asyncio
async def test_same_content_long_text_is_hard_merged(database):
    service = FullCorpusService(database)
    shared = "地址：江海区礼乐街道东宁路1号。事项：路灯连续多日不亮，希望尽快检修。"
    await service.sync_records(
        [
            make_record(
                "A",
                title="东宁路路灯不亮",
                appeal=shared,
                category="路灯故障",
            ),
            make_record(
                "B",
                title="东宁路路灯不亮",
                appeal=shared,
                category="道路照明",
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 3),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert len(events) == 1
    assert len(events[0]["members"]) == 2


@pytest.mark.asyncio
async def test_event_card_model_can_merge_cross_legacy_key_candidates(database):
    llm = FakeLlmClient()
    service = FullCorpusService(database, llm_client=llm)
    shared_location = "江海区礼乐街道东宁路8号"
    await service.sync_records(
        [
            make_record(
                "A",
                title="东宁路8号商铺消费纠纷",
                appeal="地址：江海区礼乐街道东宁路8号。销售商品与约定不一致，要求退款。",
                category="消费纠纷",
                location=shared_location,
            ),
            make_record(
                "B",
                title="反映东宁路8号商铺退款问题",
                appeal="地址：江海区礼乐街道东宁路8号。商家拒绝退款，希望协调处理。",
                category="退款纠纷",
                location=shared_location,
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 2),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert len(events) == 1
    assert len(events[0]["members"]) == 2
    assert comparison.llm_coverage == 1
    assert comparison.decision_count >= 1
    assert llm.calls


@pytest.mark.asyncio
async def test_model_failure_falls_back_to_conservative_rules(database):
    service = FullCorpusService(database, llm_client=FakeLlmClient(fail=True))
    await service.sync_records(
        [
            make_record(
                "A",
                title="某食品厂食品安全问题",
                appeal="地址：江海区礼乐街道东宁路1号。反映某食品厂食品安全问题。",
                category="食品安全",
            ),
            make_record(
                "B",
                title="某食品厂食品安全问题",
                appeal="地址：江海区礼乐街道东宁路1号。同一食品厂再次投诉食品安全。",
                category="食品安全",
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
            make_record(
                "C",
                title="东宁路1号商品质量投诉",
                appeal="地址：江海区礼乐街道东宁路1号。购买的机器存在质量问题。",
                category="产品质量",
                received="2026-09-03 08:00:00",
                completed="2026-09-03 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 3),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert sum(len(event["members"]) for event in events) == 3
    assert comparison.fallback_count >= 1


@pytest.mark.asyncio
async def test_model_merge_is_rejected_when_explicit_orders_conflict(database):
    service = FullCorpusService(database, llm_client=FakeLlmClient())
    await service.sync_records(
        [
            make_record(
                "A",
                title="东宁路8号退款问题",
                appeal="地址：江海区礼乐街道东宁路8号。关联订单号：ORDER-A-123456，要求退款。",
                category="退款纠纷",
            ),
            make_record(
                "B",
                title="东宁路8号退款问题",
                appeal="地址：江海区礼乐街道东宁路8号。关联订单号：ORDER-B-654321，要求退款。",
                category="退款纠纷",
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 2),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert len(events) == 2


@pytest.mark.asyncio
async def test_location_alone_cannot_merge_different_issues(database):
    service = FullCorpusService(
        database,
        llm_client=FakeLlmClient(evidence_field="location"),
    )
    location = "江海区礼乐街道东宁路10号"
    await service.sync_records(
        [
            make_record(
                "A",
                title="东宁路10号噪声扰民",
                appeal="地址：江海区礼乐街道东宁路10号。夜间施工噪声扰民。",
                category="噪声扰民",
                location=location,
            ),
            make_record(
                "B",
                title="东宁路10号垃圾清运",
                appeal="地址：江海区礼乐街道东宁路10号。垃圾长期无人清运。",
                category="环境卫生",
                location=location,
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
            make_record(
                "C",
                title="东宁路10号绿化问题",
                appeal="地址：江海区礼乐街道东宁路10号。公共绿化被损坏。",
                category="园林绿化",
                location=location,
                received="2026-09-03 08:00:00",
                completed="2026-09-03 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 3),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert len(events) == 3


@pytest.mark.asyncio
async def test_fallback_does_not_create_transitive_address_conflict(database):
    service = FullCorpusService(database)
    await service.sync_records(
        [
            make_record(
                "A",
                title="某食品厂食品安全问题",
                appeal="反映某食品厂食品安全问题，希望核查。",
                category="食品安全",
                location="江海区礼乐街道",
            ),
            make_record(
                "B",
                title="某食品厂食品安全问题",
                appeal="地址：江海区礼乐街道东宁路1号。反映食品安全问题。",
                category="食品安全",
                location="江海区礼乐街道东宁路1号",
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
            make_record(
                "C",
                title="某食品厂食品安全问题",
                appeal="地址：江海区礼乐街道东宁路2号。反映食品安全问题。",
                category="食品安全",
                location="江海区礼乐街道东宁路2号",
                received="2026-09-03 08:00:00",
                completed="2026-09-03 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 3),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert len(events) == 2


@pytest.mark.asyncio
async def test_model_prompt_does_not_contain_raw_pii(database):
    llm = FakeLlmClient(group_all=False)
    service = FullCorpusService(database, llm_client=llm)
    await service.sync_records(
        [
            make_record(
                "A",
                title="某小区投诉",
                appeal=(
                    "地址：江海区礼乐街道东宁路1号301室。"
                    "市民张三电话13800138000，身份证440781199311261128。"
                    "要求处理物业服务问题。"
                ),
                category="物业服务",
            ),
            make_record(
                "B",
                title="某小区收费投诉",
                appeal=(
                    "地址：江海区礼乐街道东宁路1号。"
                    "要求核查物业服务收费问题。"
                ),
                category="收费纠纷",
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
            make_record(
                "C",
                title="东宁路1号交通投诉",
                appeal="地址：江海区礼乐街道东宁路1号。道路标识不清，影响通行。",
                category="交通设施",
                received="2026-09-03 08:00:00",
                completed="2026-09-03 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 2),
    )

    assert llm.calls
    prompt = "\n".join(
        message["content"]
        for message in llm.calls[0]
    )
    assert "13800138000" not in prompt
    assert "440781199311261128" not in prompt
    assert "301室" not in prompt
    assert "张三" not in prompt


@pytest.mark.asyncio
async def test_large_candidate_bucket_does_not_lose_records(database):
    service = FullCorpusService(database, llm_client=AllCardsLlmClient())
    location = "江海区礼乐街道东宁路88号"
    await service.sync_records(
        [
            make_record(
                f"LARGE-{index:02d}",
                title=f"东宁路88号问题{index}",
                appeal=f"地址：{location}。反映第{index}项具体问题，要求处理。",
                category="物业服务",
                location=location,
                received=f"2026-09-{index + 1:02d} 08:00:00",
                completed=f"2026-09-{index + 1:02d} 10:00:00",
            )
            for index in range(20)
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 20),
    )
    events = await service.list_comparison_events(comparison.comparison_id)

    assert sum(len(event["members"]) for event in events) == 20
    assert max(len(event["members"]) for event in events) > 1
    async with database.engine.connect() as connection:
        decisions = (
            await connection.execute(
                select(comparison_decisions.c.decision).where(
                    comparison_decisions.c.comparison_id
                    == comparison.comparison_id
                )
            )
        ).scalars().all()
    assert "model_assignment" in decisions


@pytest.mark.asyncio
async def test_category_variants_of_same_family_are_merged(database):
    service = FullCorpusService(database)
    location = "江海区江南街道金瓯路105号明泰城"
    await service.sync_records(
        [
            make_record(
                "FAMILY-A",
                title="明泰城物业费过高",
                appeal="地址：江海区江南街道金瓯路105号明泰城。事项：物业费2.4元过高。",
                category="物业费定价过高",
                location=location,
            ),
            make_record(
                "FAMILY-B",
                title="明泰城物业服务质价不符",
                appeal="地址：江海区江南街道金瓯路105号明泰城。事项：物业服务质价不符。",
                category="（江海）物业服务纠纷",
                location=location,
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 2),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert len(events) == 1
    assert len(events[0]["members"]) == 2


@pytest.mark.asyncio
async def test_different_problem_families_at_same_place_stay_separate(database):
    service = FullCorpusService(database)
    location = "江海区江南街道金瓯路105号明泰城"
    await service.sync_records(
        [
            make_record(
                "FAM-A",
                title="明泰城物业服务问题",
                appeal="地址：江海区江南街道金瓯路105号明泰城。事项：物业服务不到位。",
                category="物业服务纠纷",
                location=location,
            ),
            make_record(
                "FAM-B",
                title="明泰城夜间噪音",
                appeal="地址：江海区江南街道金瓯路105号明泰城。事项：夜间施工噪音扰民。",
                category="生活噪音",
                location=location,
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 2),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert len(events) == 2


@pytest.mark.asyncio
async def test_follow_up_order_referencing_previous_is_merged(database):
    service = FullCorpusService(database)
    location = "江海区礼乐街道东宁路1号"
    await service.sync_records(
        [
            make_record(
                "0826010100000000001",
                title="东宁路路灯不亮",
                appeal="地址：江海区礼乐街道东宁路1号。事项：路灯连续多日不亮。",
                category="路灯故障",
                location=location,
            ),
            make_record(
                "0826020200000000002",
                title="再次反映东宁路路灯未修复",
                appeal=(
                    "地址：江海区礼乐街道东宁路1号。事项：此前工单"
                    "0826010100000000001反映的路灯问题仍未修复，请尽快处理。"
                ),
                category="道路照明",
                location=location,
                received="2026-09-03 08:00:00",
                completed="2026-09-03 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 3),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert len(events) == 1
    assert len(events[0]["members"]) == 2


@pytest.mark.asyncio
async def test_same_appeal_with_different_titles_is_merged(database):
    service = FullCorpusService(database)
    location = "江海区礼乐街道东宁路8号"
    shared_appeal = (
        "地址：江海区礼乐街道东宁路8号。事项：市民反映该处商铺长期占道经营，"
        "影响行人通行，要求尽快处理并加强日常巡查管理。"
    )
    await service.sync_records(
        [
            make_record(
                "APPEAL-A",
                title="东宁路8号占道经营投诉",
                appeal=shared_appeal,
                category="占道经营",
                location=location,
            ),
            make_record(
                "APPEAL-B",
                title="请求处理东宁路商铺摆卖问题",
                appeal=shared_appeal,
                category="占道经营",
                location=location,
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 2),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert len(events) == 1
    assert len(events[0]["members"]) == 2


@pytest.mark.asyncio
async def test_time_span_guard_counts_then_blocks(database):
    location = "江海区江南街道金瓯路105号明泰城"

    async def run(shadow: bool):
        service = FullCorpusService(
            database, settings=SimpleNamespace(dedup_fallback_span_shadow=shadow)
        )
        await service.sync_records(
            [
                make_record(
                    "SPAN-A",
                    title="明泰城物业管理问题A",
                    appeal="地址：江海区江南街道金瓯路105号明泰城。事项：物业管理问题。",
                    category="物业服务纠纷",
                    location=location,
                    received="2026-01-05 08:00:00",
                    completed="2026-01-05 10:00:00",
                ),
                make_record(
                    "SPAN-B",
                    title="明泰城物业管理问题B",
                    appeal="地址：江海区江南街道金瓯路105号明泰城。事项：保洁不到位。",
                    category="物业服务纠纷",
                    location=location,
                    received="2026-08-05 08:00:00",
                    completed="2026-08-05 10:00:00",
                ),
            ],
            file_name="all.xlsx",
        )
        return await service.compare(
            time_field="completed_at",
            target_from=date(2026, 1, 1),
            target_to=date(2026, 8, 31),
        )

    shadowed = await run(shadow=True)
    assert shadowed.span_guard_count >= 1
    shadow_events = await FullCorpusService(database).list_comparison_events(
        shadowed.comparison_id
    )
    assert len(shadow_events) == 1

    enforced = await run(shadow=False)
    assert enforced.span_guard_count >= 1
    enforced_events = await FullCorpusService(database).list_comparison_events(
        enforced.comparison_id
    )
    assert len(enforced_events) == 2


@pytest.mark.asyncio
async def test_same_enterprise_wage_orders_are_merged(database):
    service = FullCorpusService(database)
    await service.sync_records(
        [
            make_record(
                "ENT-WAGE-A",
                title="江海区外海街道云沁路86号安波福电气系统有限公司江门分厂拖欠工资的问题",
                appeal="地址：江海区外海街道云沁路86号安波福电气系统有限公司江门分厂。事项：员工甲被拖欠工资。",
                category="拖欠、克扣工资",
                location="江海区外海街道云沁路86号安波福电气系统有限公司江门分厂",
            ),
            make_record(
                "ENT-WAGE-B",
                title="江海区外海街道云沁路86号安波福电气系统有限公司江门分公司拖欠工资的问题",
                appeal="地址：江海区外海街道云沁路86号安波福电气系统有限公司江门分公司。事项：员工乙被拖欠工资。",
                category="（江海）拖欠、克扣工资",
                location="江海区外海街道云沁路86号安波福电气系统有限公司江门分公司",
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 2),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert len(events) == 1
    assert len(events[0]["members"]) == 2


@pytest.mark.asyncio
async def test_same_subject_outside_enterprise_families_is_not_force_merged(database):
    service = FullCorpusService(database)
    await service.sync_records(
        [
            make_record(
                "ENT-CONSUMER-A",
                title="江海区礼乐街道五邑路119号1幢江门市裕立汽车销售服务有限公司的消费纠纷",
                appeal="地址：江海区礼乐街道五邑路119号1幢江门市裕立汽车销售服务有限公司。事项：购车纠纷。",
                category="消费纠纷",
                location="江海区礼乐街道五邑路119号1幢江门市裕立汽车销售服务有限公司",
            ),
            make_record(
                "ENT-CONSUMER-B",
                title="江海区礼乐街道五邑路200号江门市裕立汽车销售服务有限公司的消费纠纷",
                appeal="地址：江海区礼乐街道五邑路200号江门市裕立汽车销售服务有限公司。事项：退款纠纷。",
                category="消费纠纷",
                location="江海区礼乐街道五邑路200号江门市裕立汽车销售服务有限公司",
                received="2026-09-02 08:00:00",
                completed="2026-09-02 10:00:00",
            ),
        ],
        file_name="all.xlsx",
    )

    comparison = await service.compare(
        time_field="completed_at",
        target_from=date(2026, 9, 1),
        target_to=date(2026, 9, 2),
    )

    events = await service.list_comparison_events(comparison.comparison_id)
    assert len(events) == 2


def test_unknown_street_is_treated_as_missing() -> None:
    left = {"street": "未知街道", "strong_subjects": [], "occurrence_ids": [], "location_keys": []}
    right = {"street": "外海街道", "strong_subjects": [], "occurrence_ids": [], "location_keys": []}

    assert _features_conflict(left, right) is False


def test_subject_alias_variants_do_not_conflict() -> None:
    left = {"strong_subjects": ["安波福电气系统"], "occurrence_ids": [], "location_keys": []}
    right = {"strong_subjects": ["安波福电器系统"], "occurrence_ids": [], "location_keys": []}

    assert _features_conflict(left, right) is False
