from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any
from unicodedata import normalize as unicode_normalize

from complaint_dedup.corpus_parser import (
    ComplaintParseResult,
    extract_occurrence_identifiers,
    extract_organization_subjects,
    extract_previous_work_order_ids,
    normalize_organization_name,
)

FEATURE_VERSION = "feature-v2"
_HBD_SUFFIX_RE = re.compile(r"(?:[-_\s]*HBD\d*)$", re.IGNORECASE)
_PHONE_RE = re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")
_MASKED_PHONE_RE = re.compile(r"(?<!\d)\d{3,4}\*{2,}\d{3,4}(?!\d)")
_ID_CARD_RE = re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)")
_LONG_NUMBER_RE = re.compile(r"(?<!\d)\d{15,32}(?!\d)")
_ROOM_RE = re.compile(r"\d{1,4}(?:室|房)")
_NAME_RE = re.compile(r"(?:市民|投诉人|联系人|机主)\s*[\u4e00-\u9fff]{2,4}")


@dataclass(frozen=True)
class RecordFeatures:
    canonical_work_order_id: str | None = None
    complaint_fingerprint: str | None = None
    subjects: tuple[str, ...] = ()
    locations: tuple[str, ...] = ()
    issues: tuple[str, ...] = ()
    occurrence_ids: tuple[str, ...] = ()
    previous_work_order_ids: tuple[str, ...] = ()
    explicit_address: str = ""
    source_rows: tuple[int, ...] = ()
    feature_version: str = FEATURE_VERSION

    def to_json(self) -> dict[str, Any]:
        return {
            "canonical_work_order_id": self.canonical_work_order_id,
            "complaint_fingerprint": self.complaint_fingerprint,
            "subjects": list(self.subjects),
            "locations": list(self.locations),
            "issues": list(self.issues),
            "occurrence_ids": list(self.occurrence_ids),
            "previous_work_order_ids": list(self.previous_work_order_ids),
            "explicit_address": self.explicit_address,
            "source_rows": list(self.source_rows),
            "feature_version": self.feature_version,
        }

    @classmethod
    def from_json(cls, value: Any) -> "RecordFeatures":
        payload = value if isinstance(value, dict) else {}
        return cls(
            canonical_work_order_id=_clean(payload.get("canonical_work_order_id")),
            complaint_fingerprint=_clean(payload.get("complaint_fingerprint")),
            subjects=_string_tuple(payload.get("subjects")),
            locations=_string_tuple(payload.get("locations")),
            issues=_string_tuple(payload.get("issues")),
            occurrence_ids=_string_tuple(payload.get("occurrence_ids")),
            previous_work_order_ids=_string_tuple(
                payload.get("previous_work_order_ids")
            ),
            explicit_address=str(payload.get("explicit_address") or ""),
            source_rows=tuple(
                int(item)
                for item in (payload.get("source_rows") or [])
                if str(item).isdigit()
            ),
            feature_version=str(payload.get("feature_version") or ""),
        )


@dataclass(frozen=True)
class FeatureBuildInput:
    work_order_id: str | None
    title: str | None
    appeal_text: str | None
    location: str | None
    category: str | None
    parsed: ComplaintParseResult
    source_row: int
    extra_issues: tuple[str, ...] = field(default_factory=tuple)


def normalize_work_order_id(value: str | None) -> str | None:
    text = unicode_normalize("NFKC", str(value or "")).strip().upper()
    text = re.sub(r"\s+", "", text)
    if not text:
        return None
    base = _HBD_SUFFIX_RE.sub("", text)
    return base or text


def normalize_feature_text(value: str | None, *, keep_punctuation: bool = False) -> str:
    text = unicode_normalize("NFKC", str(value or "")).replace("_x000D_", " ")
    text = text.casefold()
    if keep_punctuation:
        return re.sub(r"\s+", " ", text).strip()
    return re.sub(r"[\W_]+", "", text, flags=re.UNICODE)


def complaint_fingerprint(
    *,
    title: str | None,
    appeal_text: str | None,
    location: str | None,
) -> str | None:
    normalized = "\x1f".join(
        normalize_feature_text(value)
        for value in (title, appeal_text, location)
    )
    if len(normalized.replace("\x1f", "")) < 30:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def build_record_features(payload: FeatureBuildInput) -> RecordFeatures:
    parsed = payload.parsed
    subjects = [
        normalize_organization_name(value)
        for value in extract_organization_subjects(
            payload.title, payload.appeal_text, payload.location
        )
    ]
    subjects = _unique(value for value in subjects if value)

    address_parts = [
        parsed.region,
        parsed.street,
        parsed.road,
        parsed.house_no,
        parsed.building,
        parsed.unit,
        parsed.room,
        parsed.shop_no,
        parsed.floor,
        parsed.anchor_raw,
    ]
    locations = _unique(str(value).strip() for value in address_parts if value)
    issues = _unique(
        normalize_feature_text(value, keep_punctuation=True)
        for value in (
            payload.category,
            *payload.extra_issues,
            *[segment.text for segment in parsed.issue_segments],
        )
        if str(value or "").strip()
    )
    text = "\n".join(
        str(value or "")
        for value in (payload.title, payload.appeal_text, payload.location)
    )
    return RecordFeatures(
        canonical_work_order_id=normalize_work_order_id(payload.work_order_id),
        complaint_fingerprint=complaint_fingerprint(
            title=payload.title,
            appeal_text=payload.appeal_text,
            location=payload.location,
        ),
        subjects=tuple(subjects),
        locations=tuple(locations),
        issues=tuple(issues),
        occurrence_ids=tuple(extract_occurrence_identifiers(text)),
        previous_work_order_ids=tuple(extract_previous_work_order_ids(text)),
        explicit_address="|".join(
            str(value).strip()
            for value in (
                parsed.road,
                parsed.house_no,
                parsed.building,
                parsed.unit,
                parsed.room,
                parsed.shop_no,
                parsed.floor,
            )
            if value
        ),
        source_rows=(int(payload.source_row),),
    )


def sanitize_for_model(value: str | None, *, max_length: int = 240) -> str:
    text = str(value or "")
    text = _PHONE_RE.sub("[手机号]", text)
    text = _MASKED_PHONE_RE.sub("[手机号]", text)
    text = _ID_CARD_RE.sub("[身份证号]", text)
    text = _LONG_NUMBER_RE.sub("[长编号]", text)
    text = _ROOM_RE.sub("[房间号]", text)
    text = _NAME_RE.sub(
        lambda match: match.group(0)[:2] + "[姓名]"
        if match.group(0).startswith(("市民", "投诉人", "联系人", "机主"))
        else "[姓名]",
        text,
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_length]


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(str(item).strip() for item in value if str(item or "").strip())


def _unique(values) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        key = normalize_feature_text(text)
        if text and key and key not in seen:
            result.append(text)
            seen.add(key)
    return result


def _clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
