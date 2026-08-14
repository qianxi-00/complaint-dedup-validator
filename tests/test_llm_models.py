import pytest
from pydantic import ValidationError

from complaint_dedup.llm_models import (
    ExtractionBatchResponse,
    JudgementBatchResponse,
)


def test_judgement_accepts_structured_evidence_items() -> None:
    response = JudgementBatchResponse.model_validate(
        {
            "pairs": [
                {
                    "pair_id": "1|2",
                    "decision": "duplicate",
                    "confidence": 0.9,
                    "subject_relation": "same",
                    "address_relation": "exact",
                    "issue_relation": "same",
                    "new_independent_issue": False,
                    "hard_conflicts": [],
                    "evidence_a": [{"title": "甲公司维修收费"}],
                    "evidence_b": [{"title": "甲公司维修费用"}],
                    "reason": "主体、地址和问题一致",
                }
            ]
        }
    )

    assert response.pairs[0].evidence_a == ['{"title":"甲公司维修收费"}']


def test_extraction_batch_requires_matching_record_ids() -> None:
    payload = {
        "records": [
            {
                "record_id": "A-1",
                "subject": {"full_name": "甲公司", "keys": ["甲公司"]},
                "address": {
                    "precision": "exact",
                    "exact_keys": ["金瓯路188号"],
                    "coarse_keys": ["金瓯路"],
                },
                "issues": {"primary": "欠薪"},
            }
        ]
    }

    response = ExtractionBatchResponse.model_validate(payload)

    assert response.records[0].record_id == "A-1"
    assert response.records[0].address.exact_keys == ["金瓯路188号"]


def test_judgement_rejects_unknown_decision() -> None:
    with pytest.raises(ValidationError):
        JudgementBatchResponse.model_validate(
            {
                "pairs": [
                    {
                        "pair_id": "A-1|B-1",
                        "decision": "maybe",
                        "confidence": 0.5,
                        "subject_relation": "unknown",
                        "address_relation": "unknown",
                        "issue_relation": "unknown",
                        "new_independent_issue": False,
                        "hard_conflicts": [],
                        "reason": "信息不足",
                    }
                ]
            }
            )


def test_extraction_supports_event_fingerprint_fields() -> None:
    response = ExtractionBatchResponse.model_validate(
        {
            "records": [
                {
                    "record_id": "A-1",
                    "subject": {"full_name": "甲公司", "keys": ["甲公司"]},
                    "address": {"precision": "exact", "exact_keys": ["金瓯路188号"]},
                    "event": {
                        "object": "摩托车",
                        "transaction_id": "订单123",
                        "incident_date": "2026-01-01",
                        "transaction_date": "2025-12-01",
                        "previous_record_ids": ["B-9"],
                    },
                    "issues": {"primary": "车辆故障"},
                    "evidence": ["标题明确写明金瓯路188号"],
                }
            ]
        }
    )
    item = response.records[0]
    assert item.event.object == "摩托车"
    assert item.event.previous_record_ids == ["B-9"]


def test_extraction_normalizes_numeric_amount() -> None:
    response = ExtractionBatchResponse.model_validate(
        {
            "records": [
                {
                    "record_id": "A-1",
                    "event": {"amount": 9586},
                }
            ]
        }
    )

    assert response.records[0].event.amount == "9586"


def test_extraction_uses_field_as_text_when_evidence_text_is_missing() -> None:
    response = ExtractionBatchResponse.model_validate(
        {
            "records": [
                {
                    "record_id": "A-1",
                    "evidence": [
                        {
                            "source": "appeal_text",
                            "field": "地址：江海区外海街道金瓯路188号",
                        }
                    ],
                }
            ]
        }
    )

    evidence = response.records[0].evidence[0]
    assert evidence.text == "地址：江海区外海街道金瓯路188号"
    assert evidence.field is None


def test_judgement_requires_decision_matrix_fields() -> None:
    response = JudgementBatchResponse.model_validate(
        {
            "pairs": [
                {
                    "pair_id": "A-1|B-1",
                    "decision": "not_duplicate",
                    "confidence": 0.7,
                    "subject_relation": "same",
                    "address_relation": "exact",
                    "issue_relation": "different",
                    "new_independent_issue": True,
                    "hard_conflicts": ["核心问题不同"],
                    "evidence_a": ["A事实"],
                    "evidence_b": ["B事实"],
                    "reason": "独立问题",
                    "matrix": {
                        "same_legal_subject": True,
                        "same_branch": True,
                        "same_incident_location": True,
                        "same_transaction": False,
                        "same_object": False,
                        "same_fact_chain": False,
                        "same_request": False,
                        "references_previous_case": False,
                        "independent_issue": True,
                    },
                }
            ]
        }
    )
    assert response.pairs[0].matrix.independent_issue is True


def test_judgement_rejects_duplicate_with_independent_issue() -> None:
    with pytest.raises(ValidationError, match="independent_issue"):
        JudgementBatchResponse.model_validate(
            {
                "pairs": [
                    {
                        "pair_id": "A-1|B-1",
                        "decision": "duplicate",
                        "confidence": 0.9,
                        "subject_relation": "same",
                        "address_relation": "exact",
                        "issue_relation": "related",
                        "new_independent_issue": True,
                        "hard_conflicts": [],
                        "evidence_a": ["A事实"],
                        "evidence_b": ["B事实"],
                        "reason": "有新增独立问题",
                        "matrix": {"independent_issue": True},
                    }
                ]
            }
        )


def test_judgement_rejects_duplicate_with_hard_conflict() -> None:
    with pytest.raises(ValidationError, match="hard_conflicts"):
        JudgementBatchResponse.model_validate(
            {
                "pairs": [
                    {
                        "pair_id": "A-1|B-1",
                        "decision": "duplicate",
                        "confidence": 0.9,
                        "subject_relation": "different",
                        "address_relation": "different",
                        "issue_relation": "same",
                        "new_independent_issue": False,
                        "hard_conflicts": ["主体不同"],
                        "evidence_a": ["甲"],
                        "evidence_b": ["乙"],
                        "reason": "硬冲突",
                        "matrix": {},
                    }
                ]
            }
        )
