from __future__ import annotations

import re
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from typing import Any


_DIRECTIONS = (
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
)
_GENERIC_SUFFIXES = (
    "有限公司",
    "有限责任公司",
    "小区",
    "社区",
    "健身房",
    "健身中心",
)
_MAX_PAIRWISE_FUZZY_GROUP_SIZE = 200


def fuzzy_alias_match(
    query: str | None,
    aliases: list[str],
    *,
    threshold: float = 0.82,
) -> str | None:
    normalized_query = _normalize(query)
    if not normalized_query:
        return None
    best_alias = None
    best_score = 0.0
    for alias in aliases:
        normalized_alias = _normalize(alias)
        if not normalized_alias or _has_hard_conflict(normalized_query, normalized_alias):
            continue
        score = SequenceMatcher(None, normalized_query, normalized_alias).ratio()
        shorter, longer = sorted((normalized_query, normalized_alias), key=len)
        if shorter in longer and len(shorter) / max(len(longer), 1) >= 0.5:
            score = max(score, 0.9)
        base_query = _without_generic_suffix(normalized_query)
        base_alias = _without_generic_suffix(normalized_alias)
        if base_query and base_query == base_alias:
            score = 0.96
        if score > best_score:
            best_score = score
            best_alias = alias
    return best_alias if best_score >= threshold else None


def anchor_location_signature(
    road: str | None,
    house_no: str | None,
    building: str | None,
    direction: str | None,
    shop_no: str | None = None,
    floor: str | None = None,
) -> str:
    return "|".join(
        _normalize_structural(value)
        for value in (road, house_no, building, shop_no, floor, direction)
        if value
    )


def canonicalize_anchor_candidates(
    items: list[dict[str, Any]], *, threshold: float = 0.95
) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        signature = item.get("location_signature") or anchor_location_signature(
            item.get("road"),
            item.get("house_no"),
            item.get("building"),
            item.get("direction"),
            item.get("shop_no"),
            item.get("floor"),
        )
        groups[(item["street_key"], item["anchor_type"], signature)].append(item)

    result: list[dict[str, Any]] = []
    for group in groups.values():
        counts = Counter(str(item["canonical_name"]) for item in group)
        ordered_names = sorted(
            counts, key=lambda value: (-counts[value], len(value), value)
        )
        if len(ordered_names) > _MAX_PAIRWISE_FUZZY_GROUP_SIZE:
            canonical_names = _safe_canonical_names(ordered_names)
        else:
            representatives: list[str] = []
            canonical_names = {}
            for name in ordered_names:
                matched = fuzzy_alias_match(
                    name, representatives, threshold=threshold
                )
                canonical = matched or name
                if matched is None:
                    representatives.append(name)
                canonical_names[name] = canonical
        for item in group:
            value = dict(item)
            raw_name = str(item["canonical_name"])
            value["alias"] = raw_name
            value["canonical_name"] = canonical_names[raw_name]
            result.append(value)
    return result


def _safe_canonical_names(names: list[str]) -> dict[str, str]:
    exact_names: dict[str, str] = {}
    base_names: dict[str, str] = {}
    canonical_names: dict[str, str] = {}
    for name in names:
        normalized = _normalize(name)
        base = _without_generic_suffix(normalized)
        canonical = exact_names.get(normalized)
        if canonical is None and base:
            canonical = base_names.get(base)
        if canonical is None:
            canonical = name
            if normalized:
                exact_names[normalized] = canonical
            if base:
                base_names.setdefault(base, canonical)
        canonical_names[name] = canonical
    return canonical_names


def _normalize(value: str | None) -> str:
    text = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]", "", str(value or ""))
    return text.replace("號", "号").replace("棟", "栋")


def _normalize_structural(value: str | None) -> str:
    text = _normalize(value)
    for source, target in {
        "01号厂房": "1号厂房",
        "一号厂房": "1号厂房",
        "二号厂房": "2号厂房",
        "三号厂房": "3号厂房",
        "一楼": "1楼",
        "二楼": "2楼",
        "三楼": "3楼",
        "四楼": "4楼",
        "五楼": "5楼",
    }.items():
        text = text.replace(source, target)
    return text


def _without_generic_suffix(value: str) -> str:
    for suffix in _GENERIC_SUFFIXES:
        if value.endswith(suffix):
            return value[: -len(suffix)]
    return value


def _has_hard_conflict(left: str, right: str) -> bool:
    left_numbers = set(re.findall(r"\d+", left))
    right_numbers = set(re.findall(r"\d+", right))
    if left_numbers and right_numbers and left_numbers != right_numbers:
        return True
    left_directions = {word for word in _DIRECTIONS if word in left}
    right_directions = {word for word in _DIRECTIONS if word in right}
    if left_directions or right_directions:
        return left_directions != right_directions
    return False
