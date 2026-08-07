from complaint_dedup.candidates import ExtractedRecord, generate_candidate_pairs


def record(
    record_id: int,
    source: str,
    *,
    subject: tuple[str, ...] = (),
    exact: tuple[str, ...] = (),
    coarse: tuple[str, ...] = (),
    issue: str | None = None,
    received_at: str | None = None,
) -> ExtractedRecord:
    return ExtractedRecord(
        record_id=record_id,
        source=source,
        subject_keys=subject,
        exact_address_keys=exact,
        coarse_address_keys=coarse,
        primary_issue=issue,
        received_at=received_at,
    )


def test_exact_subject_and_address_create_candidate() -> None:
    a = record(1, "A", subject=("甲公司",), exact=("金瓯路188号",), issue="欠薪")
    b = record(2, "B", subject=("甲公司",), exact=("金瓯路188号",), issue="欠薪")

    pairs = generate_candidate_pairs([a], [b], max_per_record=50, broad_key_limit=200)

    assert [(pair.record_a_id, pair.record_b_id) for pair in pairs] == [(1, 2)]
    assert pairs[0].reason == "subject+exact_address"


def test_same_issue_without_subject_or_address_does_not_create_candidate() -> None:
    a = record(1, "A", issue="欠薪")
    b = record(2, "B", issue="欠薪")

    assert generate_candidate_pairs([a], [b], 50, 200) == []


def test_subject_and_coarse_address_require_same_issue() -> None:
    a = record(1, "A", subject=("甲公司",), coarse=("外海街道",), issue="欠薪")
    b = record(2, "B", subject=("甲公司",), coarse=("外海街道",), issue="噪音")

    assert generate_candidate_pairs([a], [b], 50, 200) == []


def test_candidate_count_is_limited_per_a_record() -> None:
    a = record(1, "A", subject=("甲公司",), exact=("金瓯路188号",))
    b_records = [
        record(i, "B", subject=("甲公司",), exact=("金瓯路188号",))
        for i in range(2, 8)
    ]

    pairs = generate_candidate_pairs([a], b_records, max_per_record=3, broad_key_limit=200)

    assert len(pairs) == 3


def test_strict_preset_requires_subject_exact_address_and_issue() -> None:
    a = record(1, "A", subject=("甲公司",), exact=("金瓯路188号",))
    b = record(2, "B", subject=("甲公司",), exact=("金瓯路188号",))

    assert generate_candidate_pairs([a], [b], 50, 200, preset="strict") == []


def test_loose_preset_can_recall_subject_and_issue_with_address_present() -> None:
    a = record(1, "A", subject=("甲公司",), exact=("金瓯路188号",), issue="欠薪")
    b = record(2, "B", subject=("甲公司",), exact=("云沁路83号",), issue="欠薪")

    pairs = generate_candidate_pairs([a], [b], 50, 200, preset="loose")

    assert pairs[0].reason == "subject+issue"


def test_time_window_excludes_records_too_far_apart() -> None:
    a = record(1, "A", subject=("甲公司",), exact=("金瓯路188号",), issue="欠薪", received_at="2026-01-01")
    b = record(2, "B", subject=("甲公司",), exact=("金瓯路188号",), issue="欠薪", received_at="2026-02-01")

    assert generate_candidate_pairs([a], [b], 50, 200, time_window_days=7) == []
