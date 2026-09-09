from complaint_dedup.corpus_parser import (
    clean_title,
    extract_organization_subject,
    extract_occurrence_identifiers,
    extract_previous_work_order_ids,
    normalize_organization_name,
    normalize_phone,
    parse_complaint,
    split_issue_segments,
)
import pytest


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


def test_inline_issue_marker_is_not_part_of_address_anchor():
    result = parse_complaint(
        title="德昌电机门口积水",
        appeal="地址：江海区礼乐街道德昌电机门口。事项：道路积水。",
        location="江海区礼乐街道德昌电机门口",
    )
    assert result.address_line == "江海区礼乐街道德昌电机门口"
    assert result.anchor_raw == "德昌电机门口"


def test_title_cleaning_removes_template_prefixes_but_keeps_issue():
    assert clean_title("[粤省心]（江海）要求反映东宁路路灯不亮的问题") == "东宁路路灯不亮"


def test_organization_subject_strips_address_and_corporate_suffix():
    extracted = extract_organization_subject(
        "反映某食品厂有限公司食品安全问题",
        "地址：江门市江海区礼乐街道某食品厂有限公司。",
    )
    assert extracted == "某食品厂有限公司"
    assert normalize_organization_name(extracted) == "某食品厂"


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


def test_region_prefers_audited_district_over_city_and_community_suffixes():
    result = parse_complaint(
        title="外海街道道路问题",
        appeal="地址：广东省江门市江海区外海街道麻三村道路。\n事项：道路破损。",
        location=None,
    )
    assert result.region == "江海区"
    assert result.street == "外海街道"


def test_jiang_hai_street_aliases_are_normalized():
    result = parse_complaint(
        title="礼乐道路问题",
        appeal="地址：江海区礼乐街道镇文昌花园门口。\n事项：道路积水。",
        location=None,
    )
    assert result.region == "江海区"
    assert result.street == "礼乐街道"


@pytest.mark.parametrize(
    ("dirty_name", "expected"),
    [
        ("外街道", "外海街道"),
        ("礼街道", "礼乐街道"),
        ("南江街道", "江南街道"),
        ("江海街道", "江南街道"),
    ],
)
def test_known_dirty_jiang_hai_street_names_are_normalized(dirty_name, expected):
    result = parse_complaint(
        title=f"江海区{dirty_name}道路问题",
        appeal=f"地址：江海区{dirty_name}东海路46号。\n事项：道路积水。",
        location=None,
    )

    assert result.street == expected


def test_community_and_department_words_are_not_regions_or_streets():
    community = parse_complaint(
        title="华发四季小区物业问题",
        appeal=None,
        location="华发四季小区",
    )
    department = parse_complaint(
        title="住房和城乡建设业务咨询",
        appeal=None,
        location=None,
    )
    assert community.region is None
    assert community.street is None
    assert department.region is None
    assert department.street is None


def test_repeated_historical_street_name_is_normalized():
    result = parse_complaint(
        title="新会区会城道路问题",
        appeal="地址：新会区圭峰会城会城街道仁义明珠楼17号商铺。\n事项：消费纠纷。",
        location=None,
    )

    assert result.street == "会城街道"


def test_occurrence_identifiers_prefer_order_and_platform_numbers():
    text = (
        "市民反映订单号：6926215322015465430，"
        "并曾通过12315投诉，投诉单号：1440704002026072470944200。"
    )

    assert extract_occurrence_identifiers(text) == [
        "order:6926215322015465430",
        "complaint:1440704002026072470944200",
    ]


def test_multi_address_appeal_uses_first_numbered_address():
    result = parse_complaint(
        title="两处道路问题",
        appeal=(
            "地址一：江海区江南街道金瓯路明泰城路口。\n"
            "事项一：路灯不亮。\n"
            "地址二：江海区礼乐街道六福人家酒楼门口。\n"
            "事项二：道路积水。"
        ),
        location="江海区",
    )

    assert result.street == "江南街道"
    assert result.address_line == "江海区江南街道金瓯路明泰城路口。"


def test_landmark_direction_is_detected_inside_long_location():
    result = parse_complaint(
        title="德昌电机道路积水",
        appeal=(
            "地址：江海区礼乐街道东海路888号德昌电机对面天桥底下。\n"
            "事项：下雨时道路积水。"
        ),
        location=None,
    )

    assert result.anchor_type == "landmark"
    assert result.direction == "对面"


@pytest.mark.parametrize(
    ("address", "expected_building", "expected_floor"),
    [
        ("江海区外海街道邦民路32号01号厂房西屋", "1号厂房", None),
        ("江海区外海街道邦民路32号一号厂房西屋", "1号厂房", None),
        ("江海区江南街道东海路46号江海广场三楼艾尚梵廷", None, "3楼"),
    ],
)
def test_structural_address_numbers_are_normalized(
    address, expected_building, expected_floor
):
    result = parse_complaint(
        title="地点测试",
        appeal=f"地址：{address}。\n事项：消费纠纷。",
        location=None,
    )

    assert result.building == expected_building
    assert result.floor == expected_floor
