from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from itertools import product
from typing import Literal


@dataclass(frozen=True)
class ExtractedRecord:
    record_id: int
    source: str
    subject_keys: tuple[str, ...] = ()
    exact_address_keys: tuple[str, ...] = ()
    coarse_address_keys: tuple[str, ...] = ()
    primary_issue: str | None = None
    received_at: str | None = None


@dataclass(frozen=True)
class CandidatePair:
    record_a_id: int
    record_b_id: int
    reason: str


def generate_candidate_pairs(
    records_a: list[ExtractedRecord],
    records_b: list[ExtractedRecord],
    max_per_record: int,
    broad_key_limit: int,
    *,
    preset: Literal["strict", "balanced", "loose"] = "balanced",
    time_window_days: int = 0,
) -> list[CandidatePair]:
    if max_per_record <= 0 or broad_key_limit <= 0:
        raise ValueError("candidate limits must be positive")
    if time_window_days < 0:
        raise ValueError("time_window_days must not be negative")

    indexes = _build_indexes(records_b, broad_key_limit, preset)
    records_b_by_id = {record.record_id: record for record in records_b}
    pairs: list[CandidatePair] = []
    for record_a in records_a:
        matches: dict[int, str] = {}
        for reason, keys in _candidate_keys(record_a, preset):
            index = indexes[reason]
            for key in keys:
                for record_b_id in index.get(key, ()):
                    if not _within_time_window(
                        record_a.received_at,
                        records_b_by_id[record_b_id].received_at,
                        time_window_days,
                    ):
                        continue
                    matches.setdefault(record_b_id, reason)
        for record_b_id, reason in list(matches.items())[:max_per_record]:
            pairs.append(CandidatePair(record_a.record_id, record_b_id, reason))
    return pairs


def _build_indexes(
    records: list[ExtractedRecord],
    broad_key_limit: int,
    preset: Literal["strict", "balanced", "loose"],
) -> dict[str, dict[tuple[str, ...], tuple[int, ...]]]:
    raw: dict[str, dict[tuple[str, ...], list[int]]] = {
        reason: defaultdict(list)
        for reason in (
            "subject+exact_address",
            "subject+exact_address+issue",
            "subject+coarse_address+issue",
            "exact_address+issue",
            "subject+issue",
        )
    }
    for record in records:
        for reason, keys in _candidate_keys(record, preset):
            for key in keys:
                raw[reason][key].append(record.record_id)
    return {
        reason: {
            key: tuple(record_ids)
            for key, record_ids in values.items()
            if len(record_ids) <= broad_key_limit
        }
        for reason, values in raw.items()
    }


def _candidate_keys(
    record: ExtractedRecord,
    preset: Literal["strict", "balanced", "loose"],
) -> list[tuple[str, set[tuple[str, ...]]]]:
    subject = {_normalize(value) for value in record.subject_keys if value.strip()}
    exact = {_normalize(value) for value in record.exact_address_keys if value.strip()}
    coarse = {_normalize(value) for value in record.coarse_address_keys if value.strip()}
    issue = _normalize(record.primary_issue) if record.primary_issue else None

    keys: list[tuple[str, set[tuple[str, ...]]]] = []
    if preset == "strict":
        if issue:
            keys.append(
                (
                    "subject+exact_address+issue",
                    {(subject_key, address_key, issue) for subject_key, address_key in product(subject, exact)},
                )
            )
        return keys
    keys.append(("subject+exact_address", set(product(subject, exact))))
    if issue:
        keys.append(
            (
                "subject+coarse_address+issue",
                {(subject_key, address_key, issue) for subject_key, address_key in product(subject, coarse)},
            )
        )
        keys.append(("exact_address+issue", {(address_key, issue) for address_key in exact}))
        if preset == "loose" or (not exact and not coarse):
            keys.append(("subject+issue", {(subject_key, issue) for subject_key in subject}))
    return keys


def _within_time_window(left: str | None, right: str | None, days: int) -> bool:
    if days == 0 or not left or not right:
        return True
    try:
        left_at = datetime.fromisoformat(str(left).strip().replace("/", "-"))
        right_at = datetime.fromisoformat(str(right).strip().replace("/", "-"))
    except ValueError:
        return True
    return abs((left_at - right_at).total_seconds()) <= days * 86_400


def _normalize(value: str) -> str:
    return "".join(value.casefold().split())
