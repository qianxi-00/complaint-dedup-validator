from complaint_dedup.dedup_features import (
    complaint_fingerprint,
    normalize_work_order_id,
    sanitize_for_model,
)


def test_hbd_suffixes_share_one_canonical_work_order_id() -> None:
    assert normalize_work_order_id("0826081308493182401HBD") == (
        "0826081308493182401"
    )
    assert normalize_work_order_id("0826081308493182401-HBD2") == (
        "0826081308493182401"
    )
    assert normalize_work_order_id("0826081308493182401 hbd3") == (
        "0826081308493182401"
    )


def test_complete_content_fingerprint_ignores_format_noise() -> None:
    first = complaint_fingerprint(
        title="【江海】反映某食品厂食品安全问题",
        appeal_text="地址：江海区礼乐街道。事项：购买食品后发现异物，要求处理。",
        location="江海区礼乐街道",
    )
    second = complaint_fingerprint(
        title="【江海】 反映某食品厂食品安全问题",
        appeal_text="地址: 江海区礼乐街道。 事项: 购买食品后发现异物，要求处理。",
        location="江海区礼乐街道",
    )
    assert first == second


def test_short_duplicate_text_has_no_complete_fingerprint() -> None:
    assert (
        complaint_fingerprint(
            title="积水",
            appeal_text="路面积水",
            location="江海区",
        )
        is None
    )


def test_model_text_sanitizes_phone_id_room_and_name() -> None:
    text = "市民张三电话13800138000，身份证440781199311261128，住在301室。"

    sanitized = sanitize_for_model(text)

    assert "13800138000" not in sanitized
    assert "440781199311261128" not in sanitized
    assert "301室" not in sanitized
    assert "张三" not in sanitized
    assert "[手机号]" in sanitized
    assert "[身份证号]" in sanitized
    assert "[房间号]" in sanitized
