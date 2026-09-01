from datetime import UTC, datetime

from complaint_dedup.ui_labels import format_datetime, label, labels, stage_label


def test_label_translates_internal_values_to_chinese() -> None:
    assert label("review") == "需人工复核"
    assert label("subject+exact_address") == "主体＋精确地址"
    assert label("referenced_work_order_mismatch") == "引用工单不一致"
    assert stage_label("review") == "人工复核"
    assert label("balanced") == "平衡"


def test_label_translates_comma_separated_recall_reasons() -> None:
    assert label("hybrid_vector,same_phone,same_category") == (
        "向量相似召回、联系电话一致、事项分类一致"
    )


def test_label_translates_detailed_hard_conflicts() -> None:
    assert label("different_road") == "道路不同"
    assert label("different_house_no") == "门牌号不同"
    assert label("different_primary_issue") == "核心问题不同"


def test_label_preserves_unknown_text_and_lists() -> None:
    assert label("自定义说明") == "自定义说明"
    assert labels(["different_transaction", "自定义说明"]) == ["交易不同", "自定义说明"]


def test_format_datetime_uses_configured_timezone() -> None:
    value = datetime(2026, 8, 17, 4, 16, 21, tzinfo=UTC)

    assert format_datetime(value, "Asia/Shanghai") == "2026-08-17 12:16:21"
