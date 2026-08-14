from complaint_dedup.corpus_normalizer import fuzzy_alias_match


def test_fuzzy_alias_matches_common_entity_rewording():
    assert fuzzy_alias_match(
        "艾尚梵廷健身中心", ["艾尚梵廷健身房", "江海广场停车场"]
    ) == "艾尚梵廷健身房"


def test_fuzzy_alias_matches_added_road_and_community_suffix():
    assert fuzzy_alias_match(
        "金瓯路明泰城状元居小区", ["明泰城状元居", "明泰城北门"]
    ) == "明泰城状元居"


def test_fuzzy_alias_rejects_conflicting_direction():
    assert fuzzy_alias_match("德昌电机门口", ["德昌电机对面"]) is None


def test_fuzzy_alias_rejects_conflicting_house_number():
    assert fuzzy_alias_match("东海路46号商铺", ["东海路48号商铺"]) is None
