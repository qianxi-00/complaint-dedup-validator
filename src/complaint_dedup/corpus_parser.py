from __future__ import annotations

import re
from unicodedata import normalize as unicode_normalize
from dataclasses import dataclass, field


_STREET_RE = re.compile(r"(?P<street>[\u4e00-\u9fff]{1,6}?(?:街道|镇))")
_KNOWN_REGIONS = (
    "江海区",
    "蓬江区",
    "新会区",
    "台山市",
    "开平市",
    "恩平市",
    "鹤山市",
    "中山市",
    "东莞市",
    "深圳市",
    "广州市",
    "江门市",
)
_REGION_ALIASES = {
    "江门市高新区": "江海区",
    "高新区": "江海区",
}
_JIANGHAI_STREET_ALIASES = {
    "礼乐街道": (
        "礼乐街道",
        "礼乐镇",
        "礼街道",
        "海区礼乐街道",
        "江海礼乐街道",
    ),
    "外海街道": (
        "外海街道",
        "外海镇",
        "外街道",
        "海区外海街道",
        "江海外海街道",
    ),
    "江南街道": (
        "江南街道",
        "南江街道",
        "江海街道",
        "江海江南街道",
        "江海区江南街道",
    ),
}
_STREET_ALIASES = {
    "圭峰会城会城街道": "会城街道",
}
_ROAD_HOUSE_RE = re.compile(
    r"(?P<road>[\u4e00-\u9fffA-Za-z0-9·]{1,24}(?:路|街|道|巷|大道))"
    r"\s*(?P<house_no>\d+(?:号|號)?)"
)
_CN_NUMBER = "零〇一二两三四五六七八九十百"
_BUILDING_RE = re.compile(
    rf"(?P<building>[0-9{_CN_NUMBER}]{{1,4}}(?:号?厂房|幢|栋|棟|座))"
)
_UNIT_RE = re.compile(r"(?P<unit>\d{1,3}(?:单元|单元楼))")
_ROOM_RE = re.compile(r"(?P<room>\d{1,4}(?:室|房))")
_SHOP_RE = re.compile(
    r"(?:商铺|店铺|铺位)\s*(?P<shop_no>[A-Za-z0-9\u4e00-\u9fff-]+)"
)
_FLOOR_RE = re.compile(
    rf"(?P<floor>[0-9{_CN_NUMBER}]{{1,3}}(?:层|楼)|\d{{1,3}}F)", re.I
)
_DIRECTION_WORDS = (
)
_ORGANIZATION_SUFFIXES = (
    "股份有限公司",
    "有限责任公司",
    "有限公司",
    "集团公司",
    "食品厂",
    "幼儿园",
    "加油站",
    "工厂",
    "学校",
    "大学",
    "学院",
    "医院",
    "超市",
    "商场",
    "酒店",
    "宾馆",
    "餐厅",
    "药店",
)
_ORGANIZATION_RE = re.compile(
    rf"[^\s，。；：、\n\r]{{1,50}}(?:{'|'.join(_ORGANIZATION_SUFFIXES)})"
)
_ORG_ADDRESS_PREFIX_RE = re.compile(
    r"^.*?(?:街道|镇).*?(?:路|大道|街|巷)(?:\d+(?:号|幢|栋|座))?"
)
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
# 标题二次抽取地点锚点：优先“道路+门牌/楼栋”，否则常见场所后缀
_TITLE_LANDMARK_RE = re.compile(
    r"[\u4e00-\u9fffA-Za-z0-9]{2,15}"
    r"(?:小区|花园|大厦|广场|公寓|商城|市场|超市|公园|学校|医院|幼儿园|"
    r"工业园|科技园|村|桥|车站|码头|酒店|宾馆|餐厅|工厂|工地|银行|景区)"
)
_OCCURRENCE_PATTERNS = (
    (
        "order",
        re.compile(
            r"(?:订单(?:号|编号|号码)?|交易(?:单号|号|编号)|支付(?:订单)?号)"
            r"\s*[:：]?\s*(?P<id>[0-9A-Za-z][0-9A-Za-z-]{5,31})",
            re.I,
        ),
    ),
    (
        "complaint",
        re.compile(
            r"(?:投诉(?:工单|单)?号|平台(?:投诉)?单号|工单号)"
            r"\s*[:：]?\s*(?P<id>[0-9A-Za-z][0-9A-Za-z-]{5,31})",
            re.I,
        ),
    ),
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
    unit: str | None = None
    room: str | None = None
    shop_no: str | None = None
    floor: str | None = None
    anchor_raw: str | None = None
    anchor_type: str = "unknown"
    direction: str | None = None
    address_raw: str | None = None
    address_line: str | None = None
    parse_source: str | None = None
    parse_confidence: float = 0.0
    normalized_title: str = ""
    organization_subject: str | None = None
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


def extract_occurrence_identifiers(text: str | None) -> list[str]:
    if not text:
        return []
    seen: set[str] = set()
    result: list[str] = []
    for kind, pattern in _OCCURRENCE_PATTERNS:
        for match in pattern.finditer(text):
            identifier = re.sub(r"[^0-9A-Za-z]", "", match.group("id")).upper()
            value = f"{kind}:{identifier}"
            if value not in seen:
                seen.add(value)
                result.append(value)
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
def normalize_organization_name(value: str | None) -> str:
    text = unicode_normalize("NFKC", str(value or "")).casefold()
    text = re.sub(r"\s+", "", text)
    for suffix in ("股份有限公司", "有限责任公司", "有限公司", "集团公司"):
        if text.endswith(suffix) and len(text) > len(suffix):
            return text[: -len(suffix)]
    return text


def extract_organization_subjects(*texts: str | None) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for text in texts:
        for match in _ORGANIZATION_RE.finditer(str(text or "")):
            candidate = match.group(0)
            address_match = re.search(
                r".*?(?:街道|镇).*?(?:路|大道|街|巷)(?:\d+(?:号|幢|栋|座))?", candidate
            )
            if address_match:
                candidate = candidate[address_match.end() :]
            candidate = re.sub(
                r"^(?:反映|投诉|举报|咨询|要求|关于)",
                "",
                candidate,
            )
            for _ in range(3):
                stripped = re.sub(
                    r"^(?:[\u4e00-\u9fff]{2,8}(?:区|市|县)|"
                    r"[\u4e00-\u9fff]{1,6}(?:街道|镇))",
                    "",
                    candidate,
                )
                if stripped == candidate:
                    break
                candidate = stripped
            if len(candidate) >= 4 and not re.fullmatch(r"(?:某|该|此|相关)公司", candidate):
                normalized = normalize_organization_name(candidate)
                if normalized and normalized not in seen:
                    result.append(candidate)
                    seen.add(normalized)
    return result


def extract_organization_subject(*texts: str | None) -> str | None:
    candidates = extract_organization_subjects(*texts)
    return candidates[0] if candidates else None


def extract_title_anchor(title: str | None) -> str | None:
    """从标题二次抽取地点锚点，用于补足缺失的事发地点。"""
    text = _clean_text(title)
    if not text:
        return None
    road_match = _ROAD_HOUSE_RE.search(text)
    if road_match:
        return road_match.group(0)
    building_match = _BUILDING_RE.search(text)
    if building_match:
        return building_match.group(0)
    landmark = _TITLE_LANDMARK_RE.search(text)
    return landmark.group(0) if landmark else None
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
        building = _normalize_numbered_component(building_match.group("building"))
    unit_match = _UNIT_RE.search(scoped_address)
    unit = unit_match.group("unit") if unit_match else None
    room_match = _ROOM_RE.search(scoped_address)
    room = room_match.group("room") if room_match else None
    shop_match = _SHOP_RE.search(scoped_address)
    shop_no = shop_match.group("shop_no") if shop_match else None
    floor_match = _FLOOR_RE.search(scoped_address)
    floor = (
        _normalize_numbered_component(floor_match.group("floor"))
        if floor_match
        else None
    )

    anchor = scoped_address
    for match in (road_match, building_match, unit_match, room_match, shop_match, floor_match):
        if match:
            anchor = anchor.replace(match.group(0), "", 1)
    anchor = re.sub(r"^[\s,，。:：-]+|[\s,，。:：-]+$", "", anchor)
    anchor = re.sub(r"^(?:地址|事发地点)\s*[:：]?", "", anchor).strip()

    direction = next((word for word in _DIRECTION_WORDS if word in anchor), None)
    organization_subject = extract_organization_subject(title, appeal, address_line)
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
        unit=unit,
        room=room,
        shop_no=shop_no,
        floor=floor,
        anchor_raw=anchor,
        anchor_type=anchor_type,
        direction=direction,
        address_raw=address,
        address_line=address_line,
        parse_source=source,
        parse_confidence=confidence,
        normalized_title=clean_title(title),
        organization_subject=organization_subject,
        previous_work_order_ids=extract_previous_work_order_ids(appeal),
        issue_segments=split_issue_segments(appeal),
    )


def _extract_address_line(
    appeal: str | None, location: str | None, title: str | None
) -> tuple[str, str]:
    if appeal:
        normalized = str(appeal).replace("_x000D_", "\n")
        match = re.search(
            r"(?:^|\n)\s*地址(?:[一二三四1-4])?\s*[:：]\s*([^\n]+)",
            normalized,
        )
        if match:
            address = re.split(
                r"[。；;]\s*(?:事项|诉求|备注)\s*[:：]",
                match.group(1).strip(),
                maxsplit=1,
            )[0]
            return address.strip(), "appeal_address"
    if location and str(location).strip():
        return str(location).strip(), "location"
    if title and str(title).strip():
        return str(title).strip(), "title"
    return "", "unknown"


def _first_region(value: str) -> str | None:
    for region in _KNOWN_REGIONS:
        if region in value:
            return region
    for alias, region in _REGION_ALIASES.items():
        if alias in value:
            return region
    return None


def _first_street(value: str) -> str | None:
    for canonical_name, aliases in _JIANGHAI_STREET_ALIASES.items():
        if any(alias in value for alias in aliases):
            return canonical_name
    scoped = value
    region = _first_region(value)
    if region:
        if region in value:
            scoped = value.split(region, 1)[1]
        else:
            scoped = value
    match = _STREET_RE.search(scoped)
    return match.group("street") if match else None


def _normalize_street(value: str | None) -> str | None:
    if not value:
        return None
    text = re.sub(r"(街道|镇|乡)(?:街道|镇|乡)+", r"\1", value)
    text = text.replace("街道镇", "街道").replace("街道乡", "街道")
    return _STREET_ALIASES.get(text, text)


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\r", "").replace("\n", " ")).strip()


def _normalize_numbered_component(value: str) -> str:
    suffix_match = re.search(r"(?:号?厂房|幢|栋|棟|座|层|楼|F)$", value, re.I)
    if suffix_match is None:
        return value
    number = _chinese_number(value[: suffix_match.start()])
    if number is None:
        return value.replace("棟", "栋")
    suffix = suffix_match.group(0)
    if suffix.lower() == "f":
        suffix = "楼"
    elif suffix == "棟":
        suffix = "栋"
    elif suffix.endswith("厂房"):
        suffix = "号厂房"
    return f"{number}{suffix}"


def _chinese_number(value: str) -> int | None:
    text = value.lstrip("0") or "0"
    if text.isdigit():
        return int(text)
    digits = {
        "零": 0,
        "〇": 0,
        "一": 1,
        "二": 2,
        "两": 2,
        "三": 3,
        "四": 4,
        "五": 5,
        "六": 6,
        "七": 7,
        "八": 8,
        "九": 9,
    }
    if text in digits:
        return digits[text]
    if "十" in text:
        left, _, right = text.partition("十")
        tens = digits.get(left, 1) if left else 1
        ones = digits.get(right, 0) if right else 0
        return tens * 10 + ones
    return None
