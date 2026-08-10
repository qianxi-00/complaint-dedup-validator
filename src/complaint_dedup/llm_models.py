import json
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class SubjectExtraction(BaseModel):
    full_name: str | None = None
    short_name: str | None = None
    brand: str | None = None
    branch: str | None = None
    keys: list[str] = Field(default_factory=list)
    confidence: float | None = Field(default=None, ge=0, le=1)


class AddressExtraction(BaseModel):
    district: str | None = None
    street: str | None = None
    road: str | None = None
    community: str | None = None
    house_no: str | None = None
    building: str | None = None
    shop_no: str | None = None
    landmark: str | None = None
    precision: Literal["exact", "coarse", "unknown"] = "unknown"
    exact_keys: list[str] = Field(default_factory=list)
    coarse_keys: list[str] = Field(default_factory=list)


class IssueExtraction(BaseModel):
    primary: str | None = None
    tags: list[str] = Field(default_factory=list)
    secondary: list[str] = Field(default_factory=list)
    facts: list[str] = Field(default_factory=list)
    requests: list[str] = Field(default_factory=list)


class ExtractedComplaint(BaseModel):
    record_id: str
    subject: SubjectExtraction = Field(default_factory=SubjectExtraction)
    address: AddressExtraction = Field(default_factory=AddressExtraction)
    issues: IssueExtraction = Field(default_factory=IssueExtraction)
    ambiguities: list[str] = Field(default_factory=list)


class ExtractionBatchResponse(BaseModel):
    records: list[ExtractedComplaint]


class JudgedPair(BaseModel):
    pair_id: str
    decision: Literal["duplicate", "not_duplicate", "review"]
    confidence: float = Field(ge=0, le=1)
    subject_relation: Literal["same", "different", "unknown"]
    address_relation: Literal["exact", "coarse", "different", "unknown"]
    issue_relation: Literal["same", "related", "different", "unknown"]
    new_independent_issue: bool
    hard_conflicts: list[str] = Field(default_factory=list)
    evidence_a: list[str] = Field(default_factory=list)
    evidence_b: list[str] = Field(default_factory=list)
    reason: str
    event_name: str | None = None

    @field_validator("evidence_a", "evidence_b", mode="before")
    @classmethod
    def normalize_evidence(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            value = [value]
        return [
            item if isinstance(item, str) else json.dumps(item, ensure_ascii=False, separators=(",", ":"))
            for item in value
        ]


class JudgementBatchResponse(BaseModel):
    pairs: list[JudgedPair]
