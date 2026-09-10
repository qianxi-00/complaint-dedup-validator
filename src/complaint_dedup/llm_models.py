from typing import Literal

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


class EvidenceItem(BaseModel):
    field: str = Field(min_length=1)
    cards: list[str] = Field(default_factory=list)
    reason: str = ""


class EventCardGroup(BaseModel):
    card_ids: list[str] = Field(min_length=1)
    decision: Literal["same_event"] = "same_event"
    confidence: float = Field(ge=0, le=1)
    supporting_evidence: list[EvidenceItem] = Field(default_factory=list)
    conflict_evidence: list[EvidenceItem] = Field(default_factory=list)


class EventCardBatchResponse(BaseModel):
    groups: list[EventCardGroup] = Field(default_factory=list)
    unresolved_card_ids: list[str] = Field(default_factory=list)


class ExtractedRecord(BaseModel):
    record_id: str
    subjects: list[str] = Field(default_factory=list)
    addresses: list[str] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)
    occurrence_ids: list[str] = Field(default_factory=list)
    hard_conflicts: list[str] = Field(default_factory=list)
    extraction_confidence: float = Field(default=0, ge=0, le=1)


class ExtractionBatchResponse(BaseModel):
    records: list[ExtractedRecord] = Field(default_factory=list)
