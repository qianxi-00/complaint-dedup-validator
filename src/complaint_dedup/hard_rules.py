import re
from typing import Any


def detect_hard_conflicts(left: dict[str, Any], right: dict[str, Any]) -> list[str]:
    conflicts: list[str] = []
    left_event = left.get("event") or {}
    right_event = right.get("event") or {}
    left_transaction = _normalize(left_event.get("transaction_id"))
    right_transaction = _normalize(right_event.get("transaction_id"))
    if left_transaction and right_transaction and left_transaction != right_transaction:
        conflicts.append("different_transaction")

    left_subject = left.get("subject") or {}
    right_subject = right.get("subject") or {}
    if (
        _confidence(left_subject) >= 0.9
        and _confidence(right_subject) >= 0.9
        and not (_subject_aliases(left_subject) & _subject_aliases(right_subject))
        and _clearly_different(left_subject.get("full_name"), right_subject.get("full_name"))
    ):
        conflicts.append("different_subject")
    left_brand = _normalize(left_subject.get("brand"))
    right_brand = _normalize(right_subject.get("brand"))
    if (
        left_brand
        and left_brand == right_brand
        and _confidence(left_subject) >= 0.9
        and _confidence(right_subject) >= 0.9
        and _clearly_different(left_subject.get("branch"), right_subject.get("branch"))
    ):
        conflicts.append("different_branch")

    left_address = left.get("address") or {}
    right_address = right.get("address") or {}
    if left_address.get("precision") == right_address.get("precision") == "exact":
        for field, code in (
            ("road", "different_road"),
            ("house_no", "different_house_no"),
            ("building", "different_building"),
            ("shop_no", "different_shop_no"),
        ):
            left_value = _normalize(left_address.get(field))
            right_value = _normalize(right_address.get(field))
            if left_value and right_value and left_value != right_value:
                conflicts.append(code)

    left_object = left_event.get("object")
    right_object = right_event.get("object")
    if (
        _has_explicit_identifier(left_object)
        and _has_explicit_identifier(right_object)
        and _clearly_different(left_object, right_object)
    ):
        conflicts.append("different_object")

    left_issue = (left.get("issues") or {}).get("primary")
    right_issue = (right.get("issues") or {}).get("primary")
    left_family = _issue_family(left_issue)
    right_family = _issue_family(right_issue)
    if left_family and right_family and left_family != right_family:
        conflicts.append("different_primary_issue")
    return conflicts


def _normalize(value: Any) -> str:
    return "".join(str(value).casefold().split()) if value is not None else ""


def _confidence(subject: dict[str, Any]) -> float:
    try:
        return float(subject.get("confidence") or 0)
    except (TypeError, ValueError):
        return 0


def _subject_aliases(subject: dict[str, Any]) -> set[str]:
    values = [subject.get("short_name"), *(subject.get("keys") or [])]
    return {_normalize(value) for value in values if _normalize(value)}


def _clearly_different(left: Any, right: Any) -> bool:
    left_value = _normalize(left)
    right_value = _normalize(right)
    return bool(
        left_value
        and right_value
        and left_value != right_value
        and left_value not in right_value
        and right_value not in left_value
    )


def _has_explicit_identifier(value: Any) -> bool:
    return bool(re.search(r"[0-9a-z]", _normalize(value)))


def _issue_family(value: Any) -> str | None:
    normalized = _normalize(value)
    families = {
        "wage": ("工资", "欠薪", "薪资", "薪酬"),
        "noise": ("噪音", "噪声", "扰民"),
    }
    return next(
        (family for family, keywords in families.items() if any(keyword in normalized for keyword in keywords)),
        None,
    )
