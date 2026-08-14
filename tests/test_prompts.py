from complaint_dedup.prompts import build_extraction_messages, build_judgement_messages


def test_extraction_prompt_contains_compact_output_contract() -> None:
    content = build_extraction_messages([{"record_id": "A-2"}])[0]["content"]

    for field in ("records", "record_id", "subject", "keys", "address", "exact_keys", "issues", "primary", "ambiguities"):
        assert field in content
    assert "脱敏占位主体" in content
    for field in ("transaction_id", "previous_record_ids"):
        assert field in content
    assert "事件地址" in content
    assert "不要输出未列出的字段" in content


def test_judgement_prompt_contains_complete_output_contract() -> None:
    content = build_judgement_messages([{"pair_id": "1|2"}])[0]["content"]

    for field in ("pairs", "pair_id", "decision", "confidence", "hard_conflicts", "evidence_a", "event_name"):
        assert field in content
    for field in (
        "same_legal_subject",
        "same_branch",
        "same_incident_location",
        "same_transaction",
        "same_object",
        "same_fact_chain",
        "references_previous_case",
        "independent_issue",
    ):
        assert field in content
    assert "硬冲突优先" in content
    assert "禁止假设编号笔误" in content
    assert "引用编号与候选B工单编号不一致" in content
    assert "referenced_work_order_mismatch" in content
    assert "排他性硬冲突" in content
