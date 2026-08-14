from complaint_dedup.corpus_parser import (
    clean_title,
    extract_previous_work_order_ids,
    normalize_phone,
    parse_complaint,
    split_issue_segments,
)


def test_parse_uses_address_first_line_and_does_not_pick_later_transfer_street():
    result = parse_complaint(
        title="江海区外海街道东宁路107号11幢鑫汇公司的问题",
        appeal=(
            "地址：江海区外海街道东宁路107号11幢鑫汇塑业有限公司。\n"
            "事项：反映拖欠工资。\n"
            "备注：此事项转办江海区江南街道办事处。"
        ),
        location="江海区外海街道",
    )
    assert result.street == "外海街道"
    assert result.region == "江海区"
    assert result.road == "东宁路"
    assert result.house_no == "107号"
    assert result.anchor_raw == "鑫汇塑业有限公司"
    assert result.anchor_type == "subject"


def test_landmark_keeps_direction_and_does_not_become_subject():
    result = parse_complaint(
        title="江海区礼乐街道德昌电机门口路面积水",
        appeal="地址：江海区礼乐街道德昌电机门口。\n事项：路面积水。",
        location="江海区礼乐街道",
    )
    assert result.anchor_raw == "德昌电机门口"
    assert result.anchor_type == "landmark"
    assert result.direction == "门口"


def test_title_cleaning_removes_template_prefixes_but_keeps_issue():
    assert clean_title("[粤省心]（江海）要求反映东宁路路灯不亮的问题") == "东宁路路灯不亮"


def test_phone_normalization_has_exact_mask_and_invalid_states():
    assert normalize_phone("13800138000") == {
        "phone_exact": "13800138000",
        "phone_mask_pattern": None,
        "phone_is_valid": True,
    }
    assert normalize_phone("1380******000") == {
        "phone_exact": None,
        "phone_mask_pattern": "1380******000",
        "phone_is_valid": False,
    }
    assert normalize_phone("***")["phone_is_valid"] is False


def test_previous_work_order_requires_work_order_context():
    text = "市民表示此前工单0826081308493182401仍未解决，身份证号码440781199311261128。"
    assert extract_previous_work_order_ids(text) == ["0826081308493182401"]


def test_issue_segments_extract_multiple_independent_items():
    segments = split_issue_segments(
        "事项一：物业费过高。事项二：地下车库乱停车。事项三：路灯不亮。"
    )
    assert [item.text for item in segments] == [
        "物业费过高。",
        "地下车库乱停车。",
        "路灯不亮。",
    ]


def test_parse_keeps_structured_address_components():
    result = parse_complaint(
        title="明泰城商铺投诉",
        appeal=(
            "地址：江海区江南街道金瓯路188号26幢2单元301室，商铺A-12，3楼。\n"
            "事项：物业收费纠纷。"
        ),
        location=None,
    )
    assert result.road == "金瓯路"
    assert result.house_no == "188号"
    assert result.building == "26幢"
    assert result.unit == "2单元"
    assert result.room == "301室"
    assert result.shop_no == "A-12"
    assert result.floor == "3楼"
