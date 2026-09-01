import pytest

from complaint_dedup.corpus_models import InputRecord
from complaint_dedup.corpus_pipeline import (
    _enterprise_issue_family,
    _extract_organization_subject,
    _normalize_organization_name,
)


def complaint(row: int, *, company: str, address: str, facts: str, issue: str):
    return InputRecord(
        source="B",
        source_row=row,
        work_order_id=f"WO-{row}",
        title=f"{company}{issue}问题",
        category=issue,
        category_level_4=issue,
        appeal_text=f"地址：{address}。\n事项：{facts}",
        received_at="2026-08-12 08:00:00",
        raw_fields={"事发地点": address, "处理部门": "市场监管局", "办结时间": "2026-08-13 10:00:00"},
    )


def test_organization_subject_extraction_and_normalization():
    assert (
        _extract_organization_subject(
            "江门市XX食品有限公司食品安全",
            "地址：江海区礼乐街道XX路1号江门市ＸＸ食品有限公司。\n事项：食品变质。",
            None,
        )
        == "江门市XX食品有限公司"
    )
    assert _normalize_organization_name(" 江门市 ＸＸ 食品（有限公司） ") == (
        "江门市xx食品(有限公司)"
    )


@pytest.mark.asyncio
async def test_same_enterprise_same_family_merges_across_orders_and_complainants(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="enterprise-merge".ljust(64, "0"),
        records=[
            complaint(
                2,
                company="江门市XX食品有限公司",
                address="江海区礼乐街道礼东公路XX号江门市XX食品有限公司",
                facts="购买蛋糕发现变质，订单号：1111111111111111111。",
                issue="食品安全",
            ),
            complaint(
                3,
                company="江门市ＸＸ食品有限公司",
                address="江海区礼乐街道另一路9号江门市XX食品有限公司",
                facts="购买饮料发现异物，订单号：2222222222222222222。",
                issue="食品异味",
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
async def test_enterprise_rules_do_not_merge_other_subjects_families_or_streets(corpus):
    repository, processor = corpus
    staged = await processor.stage_records(
        name="历史冷启动",
        batch_type="bootstrap_history",
        file_name="history.xlsx",
        file_hash="enterprise-guard".ljust(64, "0"),
        records=[
            complaint(
                2,
                company="江门市XX食品有限公司",
                address="江海区礼乐街道礼东公路XX号江门市XX食品有限公司",
                facts="蛋糕变质。",
                issue="食品安全",
            ),
            complaint(
                3,
                company="江门市YY食品有限公司",
                address="江海区礼乐街道礼东公路XX号江门市YY食品有限公司",
                facts="饮料异物。",
                issue="食品安全",
            ),
            complaint(
                4,
                company="江门市XX食品有限公司",
                address="江海区礼乐街道礼东公路XX号江门市XX食品有限公司",
                facts="产品开裂。",
                issue="产品质量",
            ),
            complaint(
                5,
                company="江门市XX食品有限公司",
                address="蓬江区白沙街道XX路1号江门市XX食品有限公司",
                facts="蛋糕变质。",
                issue="食品安全",
            ),
        ],
    )
    await processor.approve_bootstrap(
        staged.batch_id, staged.dictionary_version_id, approved_by="tester"
    )

    events = await repository.list_events()
    assert len(events) == 4


def test_enterprise_family_detection_is_explicit():
    context = "拖欠工资 产品开裂 食品过期"
    assert _enterprise_issue_family(context, context) == "食品安全"
