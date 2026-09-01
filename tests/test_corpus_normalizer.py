from complaint_dedup import corpus_normalizer


def _anchor(name: str) -> dict:
    return {
        "street_key": ("江海区", "礼乐街道"),
        "canonical_name": name,
        "anchor_type": "subject",
        "location_signature": "",
    }


def test_large_anchor_group_uses_only_safe_linear_normalization(monkeypatch):
    fuzzy_calls = 0
    original = corpus_normalizer.fuzzy_alias_match

    def counted(*args, **kwargs):
        nonlocal fuzzy_calls
        fuzzy_calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(corpus_normalizer, "fuzzy_alias_match", counted)
    items = [_anchor(f"独立地点{index:04d}") for index in range(250)]
    items.extend([_anchor("德昌电机"), _anchor("德昌电机有限公司")])

    normalized = corpus_normalizer.canonicalize_anchor_candidates(items)

    names = {
        row["alias"]: row["canonical_name"]
        for row in normalized
        if row["alias"].startswith("德昌电机")
    }
    assert names == {
        "德昌电机": "德昌电机",
        "德昌电机有限公司": "德昌电机",
    }
    assert fuzzy_calls == 0


def test_small_anchor_group_keeps_fuzzy_alias_matching(monkeypatch):
    fuzzy_calls = 0
    original = corpus_normalizer.fuzzy_alias_match

    def counted(*args, **kwargs):
        nonlocal fuzzy_calls
        fuzzy_calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(corpus_normalizer, "fuzzy_alias_match", counted)

    corpus_normalizer.canonicalize_anchor_candidates(
        [_anchor("德昌电机"), _anchor("德昌电机有限公司")]
    )

    assert fuzzy_calls > 0
