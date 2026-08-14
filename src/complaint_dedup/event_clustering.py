import re
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from complaint_dedup.llm_models import SuggestedEvent


@dataclass(frozen=True)
class CandidateEdge:
    left_id: int
    right_id: int
    weight: float
    hard_conflicts: tuple[str, ...] = ()


def can_auto_merge_event(
    event: SuggestedEvent,
    *,
    threshold: float,
    max_members: int,
    cannot_links: set[frozenset[str]],
    extractions: dict[str, dict[str, Any]],
    supporting_edges: set[tuple[str, str]],
) -> bool:
    member_ids = [str(member.record_id) for member in event.members]
    if not 2 <= len(member_ids) <= max_members:
        return False
    if event.confidence < threshold or any(
        member.confidence < threshold for member in event.members
    ):
        return False
    if any(
        frozenset((left, right)) in cannot_links
        for index, left in enumerate(member_ids)
        for right in member_ids[index + 1 :]
    ):
        return False
    if any(
        (extractions.get(record_id, {}).get("issues") or {}).get("secondary")
        for record_id in member_ids
    ):
        return False
    normalized_edges = {frozenset(edge) for edge in supporting_edges}
    if any(
        frozenset((left, right)) not in normalized_edges
        for index, left in enumerate(member_ids)
        for right in member_ids[index + 1 :]
    ):
        return False
    evidence = "".join(event.evidence)
    has_scope = any(token in evidence for token in ("主体", "地点", "地址", "门店"))
    has_issue = any(token in evidence for token in ("问题", "事项", "事实", "诉求"))
    return has_scope and has_issue


def build_constrained_components(
    record_ids: Iterable[int],
    edges: Sequence[CandidateEdge],
    *,
    cannot_links: Iterable[tuple[int, int]] = (),
    max_size: int,
) -> list[list[int]]:
    if max_size <= 0:
        raise ValueError("max_size must be greater than zero")
    ids = list(dict.fromkeys(int(record_id) for record_id in record_ids))
    known_ids = set(ids)
    parent = {record_id: record_id for record_id in ids}
    members = {record_id: {record_id} for record_id in ids}
    blocked = {
        frozenset((int(left), int(right)))
        for left, right in cannot_links
        if int(left) != int(right)
    }
    blocked.update(
        frozenset((edge.left_id, edge.right_id))
        for edge in edges
        if edge.hard_conflicts and edge.left_id != edge.right_id
    )

    def find(record_id: int) -> int:
        while parent[record_id] != record_id:
            parent[record_id] = parent[parent[record_id]]
            record_id = parent[record_id]
        return record_id

    def can_merge(left_root: int, right_root: int) -> bool:
        if len(members[left_root]) + len(members[right_root]) > max_size:
            return False
        return not any(
            frozenset((left, right)) in blocked
            for left in members[left_root]
            for right in members[right_root]
        )

    eligible_edges = sorted(
        (
            edge
            for edge in edges
            if not edge.hard_conflicts
            and edge.left_id in known_ids
            and edge.right_id in known_ids
            and edge.left_id != edge.right_id
        ),
        key=lambda edge: (-edge.weight, min(edge.left_id, edge.right_id), max(edge.left_id, edge.right_id)),
    )
    for edge in eligible_edges:
        left_root = find(edge.left_id)
        right_root = find(edge.right_id)
        if left_root == right_root or not can_merge(left_root, right_root):
            continue
        keep, remove = sorted((left_root, right_root))
        parent[remove] = keep
        members[keep].update(members.pop(remove))

    components = [sorted(component) for component in members.values()]
    return sorted(components, key=lambda component: component[0])


def normalize_event_title(
    title: str | None,
    *,
    region: str | None = None,
    street: str | None = None,
) -> str:
    normalized = _compact(title)
    if not normalized:
        return ""
    normalized = re.sub(r"^[【\[][^\]】]+[】\]]", "", normalized)
    for scope in (region, street):
        scope_text = _compact(scope)
        if scope_text and normalized.startswith(scope_text):
            normalized = normalized[len(scope_text) :]
    normalized = re.sub(r"^(?:关于|反映|投诉|举报|咨询|求助)+", "", normalized)
    normalized = re.sub(r"(?:的)?(?:投诉|举报|反映|诉求|问题)$", "", normalized)
    return normalized.strip("｜|,，。:：;；-_")


def build_initial_event_signature(
    *,
    region: str | None,
    street: str | None,
    subject: str | None,
    location: str | None,
    core_issue: str | None,
) -> str:
    scope = _compact(subject) or _compact(location)
    return "｜".join(
        value
        for value in (
            _compact(region),
            _compact(street),
            scope,
            _compact(core_issue),
        )
        if value
    )


def _compact(value: str | None) -> str:
    return re.sub(r"\s+", "", str(value or "").strip())
