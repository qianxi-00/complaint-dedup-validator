import json
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


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


class EventExtraction(BaseModel):
    object: str | None = None
    transaction_id: str | None = None
    incident_date: str | None = None
    transaction_date: str | None = None
    previous_record_ids: list[str] = Field(default_factory=list)
    amount: str | None = None
    branch_address: str | None = None

    @field_validator("amount", mode="before")
    @classmethod
    def normalize_amount(cls, value: Any) -> str | None:
        return None if value is None else str(value)


class EvidenceItem(BaseModel):
    text: str
    source: Literal["title", "appeal_text", "category", "extraction"] = "extraction"
    field: str | None = None

    @model_validator(mode="before")
    @classmethod
    def recover_missing_text(cls, value: Any) -> Any:
        if isinstance(value, dict) and "text" not in value and value.get("field"):
            return {**value, "text": value["field"], "field": None}
        return value

    @field_validator("text", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str:
        return value if isinstance(value, str) else str(value)


class ExtractedComplaint(BaseModel):
    record_id: str
    subject: SubjectExtraction = Field(default_factory=SubjectExtraction)
    address: AddressExtraction = Field(default_factory=AddressExtraction)
    issues: IssueExtraction = Field(default_factory=IssueExtraction)
    event: EventExtraction = Field(default_factory=EventExtraction)
    evidence: list[EvidenceItem] = Field(default_factory=list)
    ambiguities: list[str] = Field(default_factory=list)

    @field_validator("evidence", mode="before")
    @classmethod
    def normalize_evidence(cls, value: Any) -> list[Any]:
        if value is None:
            return []
        if not isinstance(value, list):
            value = [value]
        return [{"text": item} if isinstance(item, str) else item for item in value]


class DecisionMatrix(BaseModel):
    same_legal_subject: bool | None = None
    same_branch: bool | None = None
    same_incident_location: bool | None = None
    same_transaction: bool | None = None
    same_object: bool | None = None
    same_fact_chain: bool | None = None
    same_request: bool | None = None
    references_previous_case: bool = False
    independent_issue: bool = False


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
    matrix: DecisionMatrix = Field(default_factory=DecisionMatrix)

    @model_validator(mode="after")
    def enforce_independent_issue_rule(self):
        if self.new_independent_issue or self.matrix.independent_issue:
            if self.decision == "duplicate":
                raise ValueError("independent_issue cannot be duplicate")
        if self.hard_conflicts and self.decision == "duplicate":
            raise ValueError("hard_conflicts cannot be duplicate")
        if self.subject_relation == "different" and self.decision == "duplicate":
            raise ValueError("different subject cannot be duplicate")
        if self.address_relation == "different" and self.decision == "duplicate":
            raise ValueError("different address cannot be duplicate")
        return self

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


class EventClusterMember(BaseModel):
    record_id: str
    confidence: float = Field(ge=0, le=1)
    role: Literal["anchor", "member"] = "member"


class SuggestedEvent(BaseModel):
    temporary_id: str
    name: str
    confidence: float = Field(ge=0, le=1)
    evidence: list[str] = Field(default_factory=list)
    members: list[EventClusterMember]


class EventClusterResponse(BaseModel):
    events: list[SuggestedEvent]
    outliers: list[str] = Field(default_factory=list)


def validate_event_cluster_coverage(
    response: EventClusterResponse,
    expected_record_ids: list[str],
) -> EventClusterResponse:
    expected = [str(record_id) for record_id in expected_record_ids]
    duplicate_expected = sorted(
        record_id for record_id in set(expected) if expected.count(record_id) > 1
    )
    if duplicate_expected:
        raise ValueError(
            "expected record ids contain duplicates: " + ", ".join(duplicate_expected)
        )
    assigned = [
        member.record_id
        for event in response.events
        for member in event.members
    ] + [str(record_id) for record_id in response.outliers]
    counts: dict[str, int] = {}
    for record_id in assigned:
        counts[record_id] = counts.get(record_id, 0) + 1

    duplicate = sorted(record_id for record_id, count in counts.items() if count > 1)
    expected_set = set(expected)
    assigned_set = set(assigned)
    missing = sorted(expected_set - assigned_set)
    unknown = sorted(assigned_set - expected_set)
    if duplicate or missing or unknown:
        details = []
        if duplicate:
            details.append(f"duplicate record ids: {', '.join(duplicate)}")
        if missing:
            details.append(f"missing record ids: {', '.join(missing)}")
        if unknown:
            details.append(f"unknown record ids: {', '.join(unknown)}")
        raise ValueError("; ".join(details))
    return response
