from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


def _list_or_empty(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _string_list(value: Any) -> list[str]:
    return [str(item) for item in _list_or_empty(value) if str(item or "").strip()]


class NormalizationDecision(BaseModel):
    record_id: str
    anchor_id: int | None = None
    issue_id: int | None = None
    anchor_confidence: float = Field(default=0, ge=0, le=1)
    issue_confidence: float = Field(default=0, ge=0, le=1)
    reason: str = ""


class NormalizationBatchResponse(BaseModel):
    decisions: list[NormalizationDecision] = Field(default_factory=list)

    @field_validator("decisions", mode="before")
    @classmethod
    def _coerce_decisions(cls, value: Any) -> list[Any]:
        return _list_or_empty(value)


class EvidenceItem(BaseModel):
    field: str = Field(min_length=1)
    cards: list[str] = Field(default_factory=list)
    reason: str = ""

    @field_validator("cards", mode="before")
    @classmethod
    def _coerce_cards(cls, value: Any) -> list[str]:
        return _string_list(value)

    @field_validator("reason", mode="before")
    @classmethod
    def _coerce_reason(cls, value: Any) -> str:
        return "" if value is None else str(value)


class EventCardGroup(BaseModel):
    card_ids: list[str] = Field(min_length=1)
    decision: Literal["same_event"] = "same_event"
    confidence: float = Field(default=0, ge=0, le=1)
    supporting_evidence: list[EvidenceItem] = Field(default_factory=list)
    conflict_evidence: list[EvidenceItem] = Field(default_factory=list)

    @field_validator("card_ids", mode="before")
    @classmethod
    def _coerce_card_ids(cls, value: Any) -> list[str]:
        return _string_list(value)

    @field_validator("decision", mode="before")
    @classmethod
    def _normalize_decision(cls, value: Any) -> Any:
        text = str(value or "same_event").strip().lower().replace("-", "_")
        if text in {"same_event", "merge", "same", "same event", "同一事件"}:
            return "same_event"
        return value

    @field_validator("supporting_evidence", "conflict_evidence", mode="before")
    @classmethod
    def _coerce_evidence(cls, value: Any) -> list[Any]:
        return _list_or_empty(value)


class EventCardBatchResponse(BaseModel):
    groups: list[EventCardGroup] = Field(default_factory=list)
    unresolved_card_ids: list[str] = Field(default_factory=list)

    @field_validator("groups", mode="before")
    @classmethod
    def _coerce_groups(cls, value: Any) -> list[Any]:
        return _list_or_empty(value)

    @field_validator("unresolved_card_ids", mode="before")
    @classmethod
    def _coerce_unresolved(cls, value: Any) -> list[str]:
        return _string_list(value)


class ExtractedRecord(BaseModel):
    record_id: str
    subjects: list[str] = Field(default_factory=list)
    addresses: list[str] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)
    occurrence_ids: list[str] = Field(default_factory=list)
    hard_conflicts: list[str] = Field(default_factory=list)
    extraction_confidence: float = Field(default=0, ge=0, le=1)

    @field_validator(
        "subjects",
        "addresses",
        "issues",
        "occurrence_ids",
        "hard_conflicts",
        mode="before",
    )
    @classmethod
    def _coerce_lists(cls, value: Any) -> list[str]:
        return _string_list(value)


class ExtractionBatchResponse(BaseModel):
    records: list[ExtractedRecord] = Field(default_factory=list)

    @field_validator("records", mode="before")
    @classmethod
    def _coerce_records(cls, value: Any) -> list[Any]:
        return _list_or_empty(value)
