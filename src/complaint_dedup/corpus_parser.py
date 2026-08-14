from __future__ import annotations

import re
from dataclasses import dataclass, field


_STREET_RE = re.compile(r"(?P<street>[\u4e00-\u9fff]{2,12}(?:街道|镇|乡))")
_REGION_RE = re.compile(r"(?P<region>[\u4e00-\u9fff]{2,8}(?:区|县|市))")
_ROAD_HOUSE_RE = re.compile(
    r"(?P<road>[\u4e00-\u9fffA-Za-z0-9·]{1,24}(?:路|街|道|巷|大道))"
    r"\s*(?P<house_no>\d+(?:号|號)?)"
)
_BUILDING_RE = re.compile(r"(?P<building>\d{1,4}(?:幢|栋|棟|座))")
_DIRECTION_WORDS = (
    "门口",
    "门前",
    "正门",
    "后门",
    "侧门",
    "北门",
    "南门",
    "东门",
    "西门",
    "对面",
    "旁边",
    "附近",
    "周边",
    "路口",
    "交界处",
    "交叉口",
    "天桥底",
    "河边",
)
_TITLE_NOISE_RE = re.compile(
    r"^\s*(?:\[[^\]]+\]|【[^】]+】|（[^）]+）|\([^)]*\))*"
    r"\s*(?:(?:要求|咨询|反映|投诉|建议|请问|市民反映|此事项转办)\s*)+"
)
_TITLE_SUFFIX_RE = re.compile(r"(?:的)?(?:问题|事项)\s*$")
_PHONE_MASK_RE = re.compile(r"^\d{3,4}\*{2,}\d{3,4}$")
_WORK_ORDER_CONTEXT_RE = re.compile(
    r"(?:工单|单号|编号|此前|之前|历史|重复)[^\dA-Za-z]{0,8}"
    r"(?P<id>\d{16,24}(?:[A-Za-z]{1,4})?)"
)


@dataclass(frozen=True)
class IssueSegment:
    segment_no: int
    text: str
    label: str | None = None
    is_independent: bool = True


@dataclass(frozen=True)
class ComplaintParseResult:
    region: str | None = None
    street: str | None = None
    road: str | None = None
    house_no: str | None = None
    building: str | None = None
    anchor_raw: str | None = None
    anchor_type: str = "unknown"
    direction: str | None = None
    address_raw: str | None = None
    address_line: str | None = None
    parse_source: str | None = None
    parse_confidence: float = 0.0
    normalized_title: str = ""
    previous_work_order_ids: list[str] = field(default_factory=list)
    issue_segments: list[IssueSegment] = field(default_factory=list)


def clean_title(value: str | None) -> str:
    text = _clean_text(value)
    text = _TITLE_NOISE_RE.sub("", text)
    text = re.sub(r"^\s*(?:江海区|蓬江区|新会区|鹤山市)\s*", "", text)
    text = _TITLE_SUFFIX_RE.sub("", text)
    text = re.sub(r"[：:，,。；;、]+$", "", text)
    return text.strip()


def normalize_phone(value: str | None) -> dict[str, str | bool | None]:
    text = re.sub(r"\s+", "", str(value or ""))
    if re.fullmatch(r"1\d{10}", text):
        return {"phone_exact": text, "phone_mask_pattern": None, "phone_is_valid": True}
    if _PHONE_MASK_RE.fullmatch(text):
        return {"phone_exact": None, "phone_mask_pattern": text, "phone_is_valid": False}
    return {"phone_exact": None, "phone_mask_pattern": None, "phone_is_valid": False}


def extract_previous_work_order_ids(text: str | None) -> list[str]:
    if not text:
        return []
    seen: set[str] = set()
    result: list[str] = []
    for match in _WORK_ORDER_CONTEXT_RE.finditer(text):
        value = match.group("id")
        if value not in seen:
            result.append(value)
            seen.add(value)
    return result


def split_issue_segments(text: str | None) -> list[IssueSegment]:
    value = _clean_text(text)
    matches = list(
        re.finditer(
            r"事项(?P<label>[一二三四1-4]?)\s*[:：]\s*",
            value,
        )
    )
    if not matches:
        return [IssueSegment(segment_no=1, text=value, label=None)] if value else []
    segments: list[IssueSegment] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(value)
        segment = value[match.end() : end].strip()
        if segment:
            segments.append(
                IssueSegment(
                    segment_no=index + 1,
                    text=segment,
                    label=match.group("label") or None,
                )
            )
    return segments


def parse_complaint(
    *, title: str | None, appeal: str | None, location: str | None
) -> ComplaintParseResult:
    address_line, source = _extract_address_line(appeal, location, title)
    address = _clean_text(address_line)
    region = _first_region(address)
    street = _normalize_street(_first_street(address))
    scoped_address = address
    if region:
        scoped_address = scoped_address.replace(region, "", 1)
    if street:
        scoped_address = re.sub(re.escape(street) + r"(?:镇|乡)?", "", scoped_address, count=1)
    road = house_no = building = None
    road_match = _ROAD_HOUSE_RE.search(scoped_address)
    if road_match:
        road = road_match.group("road")
        house_no = road_match.group("house_no")
    building_match = _BUILDING_RE.search(scoped_address)
    if building_match:
        building = building_match.group("building")

    anchor = scoped_address
    if road_match:
        anchor = anchor.replace(road_match.group(0), "", 1)
    if building:
        anchor = anchor.replace(building, "", 1)
    anchor = re.sub(r"^[\s,，。:：-]+|[\s,，。:：-]+$", "", anchor)
    anchor = re.sub(r"^(?:地址|事发地点)\s*[:：]?", "", anchor).strip()

    direction = next((word for word in _DIRECTION_WORDS if anchor.endswith(word)), None)
    anchor_type = "landmark" if direction else ("subject" if anchor else "unknown")
    if not anchor:
        anchor = None
    confidence = 0.95 if source == "appeal_address" and street else 0.75 if street else 0.4
    return ComplaintParseResult(
        region=region,
        street=street,
        road=road,
        house_no=house_no,
        building=building,
        anchor_raw=anchor,
        anchor_type=anchor_type,
        direction=direction,
        address_raw=address,
        address_line=address_line,
        parse_source=source,
        parse_confidence=confidence,
        normalized_title=clean_title(title),
        previous_work_order_ids=extract_previous_work_order_ids(appeal),
        issue_segments=split_issue_segments(appeal),
    )


def _extract_address_line(
    appeal: str | None, location: str | None, title: str | None
) -> tuple[str, str]:
    if appeal:
        normalized = str(appeal).replace("_x000D_", "\n")
        match = re.search(r"(?:^|\n)\s*地址\s*[:：]\s*([^\n]+)", normalized)
        if match:
            return match.group(1).strip(), "appeal_address"
    if location and str(location).strip():
        return str(location).strip(), "location"
    if title and str(title).strip():
        return str(title).strip(), "title"
    return "", "unknown"


def _first_region(value: str) -> str | None:
    match = _REGION_RE.search(value)
    return match.group("region") if match else None


def _first_street(value: str) -> str | None:
    scoped = value
    region = _first_region(value)
    if region:
        scoped = value.replace(region, " ", 1)
    match = _STREET_RE.search(scoped)
    return match.group("street") if match else None


def _normalize_street(value: str | None) -> str | None:
    if not value:
        return None
    text = re.sub(r"(街道|镇|乡)(?:街道|镇|乡)+", r"\1", value)
    text = text.replace("街道镇", "街道").replace("街道乡", "街道")
    return text


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\r", "").replace("\n", " ")).strip()
