from pydantic import BaseModel, Field


class NormalizationDecision(BaseModel):
    record_id: str
    anchor_id: int | None = None
    issue_id: int | None = None
    anchor_confidence: float = Field(default=0, ge=0, le=1)
    issue_confidence: float = Field(default=0, ge=0, le=1)
    reason: str = ""


class NormalizationBatchResponse(BaseModel):
    decisions: list[NormalizationDecision] = Field(default_factory=list)
