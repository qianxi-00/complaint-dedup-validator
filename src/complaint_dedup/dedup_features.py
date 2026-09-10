from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any
from unicodedata import normalize as unicode_normalize

from complaint_dedup.category_rules import canonical_category, problem_family_for
from complaint_dedup.corpus_parser import (
    ComplaintParseResult,
    extract_occurrence_identifiers,
    extract_organization_subjects,
    extract_previous_work_order_ids,
    normalize_organization_name,
)

FEATURE_VERSION = "feature-v3"
_HBD_SUFFIX_RE = re.compile(r"(?:[-_\s]*HBD\d*)$", re.IGNORECASE)
_PHONE_RE = re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")
_MASKED_PHONE_RE = re.compile(r"(?<!\d)\d{3,4}\*{2,}\d{3,4}(?!\d)")
_ID_CARD_RE = re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)")
_LONG_NUMBER_RE = re.compile(r"(?<!\d)\d{15,32}(?!\d)")
_ROOM_RE = re.compile(r"\d{1,4}(?:室|房)")
_NAME_RE = re.compile(r"(?:市民|投诉人|联系人|机主)\s*[\u4e00-\u9fff]{2,4}")

# 主体噪声：包含这些词的主体多为正则误抽的句子片段，不参与冲突判定
_SUBJECT_NOISE_RE = re.compile(
    r"(反映|要求|希望|市民|电话|地址|身份证|投诉|咨询|建议|求助|举报|问题|情况|表示|认为)"
)
# 主体别名：同一实体的常见异写，用于模糊匹配
_SUBJECT_ALIASES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"电器系统"), "电气系统"),
    (re.compile(r"厨房家电"), "厨房小家电"),
)
_REGION_SUBJECT_PREFIX_RE = re.compile(
    r"^(?:江门市|江门|江海区|江海|蓬江区|蓬江|广东省|广东)"
)


@dataclass(frozen=True)
class RecordFeatures:
    canonical_work_order_id: str | None = None
    complaint_fingerprint: str | None = None
    # 诉求正文指纹：标题不同但正文完全相同时用于合并
    appeal_fingerprint: str | None = None
    # 内容指纹仅由标题与诉求正文生成；地点指纹单独保存，避免地点写法差异破坏合并
    location_signature: str = ""
    canonical_category: str | None = None
    problem_family: str | None = None
    subjects: tuple[str, ...] = ()
    strong_subjects: tuple[str, ...] = ()
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
            "appeal_fingerprint": self.appeal_fingerprint,
            "location_signature": self.location_signature,
            "canonical_category": self.canonical_category,
            "problem_family": self.problem_family,
            "subjects": list(self.subjects),
            "strong_subjects": list(self.strong_subjects),
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
            appeal_fingerprint=_clean(payload.get("appeal_fingerprint")),
            location_signature=str(payload.get("location_signature") or ""),
            canonical_category=_clean(payload.get("canonical_category")),
            problem_family=_clean(payload.get("problem_family")),
            subjects=_string_tuple(payload.get("subjects")),
            strong_subjects=_string_tuple(payload.get("strong_subjects")),
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


def normalize_subject_for_match(value: str | None) -> str:
    """主体模糊匹配归一：NFKC、去空白、别名替换、去地区前缀与公司后缀。"""
    text = unicode_normalize("NFKC", str(value or "")).casefold()
    text = re.sub(r"\s+", "", text)
    for pattern, replacement in _SUBJECT_ALIASES:
        text = pattern.sub(replacement, text)
    text = _REGION_SUBJECT_PREFIX_RE.sub("", text)
    for suffix in (
        "股份有限公司",
        "有限责任公司",
        "有限公司",
        "集团公司",
        "分公司",
        "分厂",
    ):
        if text.endswith(suffix) and len(text) > len(suffix):
            text = text[: -len(suffix)]
    return text


def complaint_fingerprint(
    *,
    title: str | None,
    appeal_text: str | None,
    location: str | None = None,
) -> str | None:
    """内容指纹：仅标题与诉求正文；location 参数仅为兼容旧调用而保留。"""
    normalized = "\x1f".join(
        normalize_feature_text(value) for value in (title, appeal_text)
    )
    if len(normalized.replace("\x1f", "")) < 30:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def appeal_fingerprint(appeal_text: str | None) -> str | None:
    """诉求正文指纹：仅正文、长度达标（≥40）时生成，用于标题不同但正文相同的合并。"""
    normalized = normalize_feature_text(appeal_text)
    if len(normalized) < 40:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def build_record_features(payload: FeatureBuildInput) -> RecordFeatures:
    parsed = payload.parsed
    canonical = canonical_category(payload.category)
    subjects = [
        normalize_organization_name(value)
        for value in extract_organization_subjects(
            payload.title, payload.appeal_text, payload.location
        )
    ]
    subjects = _unique(value for value in subjects if value)
    strong_subjects = _strong_subjects(subjects)

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
    location_signature = "|".join(
        str(value).strip()
        for value in (
            parsed.region,
            parsed.street,
            parsed.road,
            parsed.house_no,
            parsed.building,
        )
        if value
    )
    return RecordFeatures(
        canonical_work_order_id=normalize_work_order_id(payload.work_order_id),
        complaint_fingerprint=complaint_fingerprint(
            title=payload.title,
            appeal_text=payload.appeal_text,
        ),
        appeal_fingerprint=appeal_fingerprint(payload.appeal_text),
        location_signature=location_signature,
        canonical_category=canonical,
        problem_family=problem_family_for(canonical),
        subjects=tuple(subjects),
        strong_subjects=strong_subjects,
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


def _strong_subjects(subjects: list[str]) -> tuple[str, ...]:
    """过滤句子片段噪声，保留可信主体用于冲突判定。"""
    result: list[str] = []
    for value in subjects:
        text = str(value or "").strip()
        if not text or len(text) > 30:
            continue
        if _SUBJECT_NOISE_RE.search(text):
            continue
        result.append(text)
    return tuple(_unique(result))


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
