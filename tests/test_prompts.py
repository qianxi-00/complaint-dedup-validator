from complaint_dedup.prompts import build_extraction_messages, build_judgement_messages


def test_extraction_prompt_contains_complete_output_contract() -> None:
    content = build_extraction_messages([{"record_id": "A-2"}])[0]["content"]

    for field in ("records", "record_id", "subject", "keys", "address", "exact_keys", "issues", "primary", "ambiguities"):
        assert field in content
    assert "脱敏占位主体" in content


def test_judgement_prompt_contains_complete_output_contract() -> None:
    content = build_judgement_messages([{"pair_id": "1|2"}])[0]["content"]

    for field in ("pairs", "pair_id", "decision", "confidence", "hard_conflicts", "evidence_a", "event_name"):
        assert field in content
