from __future__ import annotations

import re
from typing import Any


_CULTURE_CENTER_ALIASES = {
    "外海文化中心停车场",
    "江门市外海文化中心停车场",
    "中华路外海文化中心停车场",
    "中华路外海文化中心公益停车场",
    "中华路外海文化中心地上停车场",
}


def apply_audited_anchor_dictionary(
    items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for source in items:
        item = dict(source)
        alias = str(item.get("alias") or item["canonical_name"])
        street_key = item.get("street_key") or (None, None)
        item["canonical_name"] = audited_anchor_name(
            street=str(street_key[1] or ""),
            alias=alias,
            road=str(item.get("road") or ""),
            house_no=str(item.get("house_no") or ""),
            building=str(item.get("building") or ""),
            floor=str(item.get("floor") or ""),
            direction=str(item.get("direction") or ""),
        )
        if item["canonical_name"] == "外海文化中心停车场":
            item["location_signature"] = ""
            item["direction"] = None
            item["anchor_type"] = "facility"
        item["alias"] = alias
        result.append(item)
    return result


def audited_anchor_name(
    *,
    street: str,
    alias: str,
    road: str = "",
    house_no: str = "",
    building: str = "",
    floor: str = "",
    direction: str = "",
) -> str:
    compact = _compact(alias)
    if (
        street == "外海街道"
        and road == "邦民路"
        and house_no == "32号"
        and building == "1号厂房"
        and "西屋" in compact
        and "厨房小家电" in compact
    ):
        return "江门市西屋厨房小家电有限公司｜1号厂房自编01"
    if (
        street == "外海街道"
        and "外海文化中心" in compact
        and "停车场" in compact
    ):
        return "外海文化中心停车场"
    if (
        street == "江南街道"
        and road in {"东海路", ""}
        and house_no in {"46号", ""}
        and floor in {"3楼", ""}
        and any(value in compact for value in ("艾尚梵廷", "爱上尊荟"))
    ):
        return "艾尚梵廷体育发展有限公司（江海广场店）"
    if street == "江南街道" and "明泰城状元居" in compact and not building:
        return "明泰城状元居"
    if (
        street == "礼乐街道"
        and road in {"东海路", "新东海路", ""}
        and house_no in {"888号", ""}
        and not direction
        and "德昌电机" in compact
        and "技术学院" not in compact
    ):
        return "德昌电机（江门）有限公司"
    return alias


def _compact(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]", "", value)
