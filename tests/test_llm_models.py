import pytest
from pydantic import ValidationError

from complaint_dedup.llm_models import NormalizationBatchResponse


def test_normalization_response_validates_confidence_range() -> None:
    response = NormalizationBatchResponse.model_validate(
        {
            "decisions": [
                {
                    "record_id": "1",
                    "anchor_id": 10,
                    "issue_id": 20,
                    "anchor_confidence": 0.96,
                    "issue_confidence": 0.91,
                    "reason": "标准项一致",
                }
            ]
        }
    )

    assert response.decisions[0].anchor_id == 10


def test_normalization_response_rejects_invalid_confidence() -> None:
    with pytest.raises(ValidationError):
        NormalizationBatchResponse.model_validate(
            {
                "decisions": [
                    {
                        "record_id": "1",
                        "anchor_confidence": 1.1,
                    }
                ]
            }
        )
