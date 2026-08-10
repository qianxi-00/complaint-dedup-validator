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
