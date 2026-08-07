from collections import defaultdict
from dataclasses import dataclass
from itertools import product


@dataclass(frozen=True)
class ExtractedRecord:
    record_id: int
    source: str
    subject_keys: tuple[str, ...] = ()
    exact_address_keys: tuple[str, ...] = ()
    coarse_address_keys: tuple[str, ...] = ()
    primary_issue: str | None = None


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
) -> list[CandidatePair]:
    if max_per_record <= 0 or broad_key_limit <= 0:
        raise ValueError("candidate limits must be positive")

    indexes = _build_indexes(records_b, broad_key_limit)
    pairs: list[CandidatePair] = []
    for record_a in records_a:
        matches: dict[int, str] = {}
        for reason, keys in _candidate_keys(record_a):
            index = indexes[reason]
            for key in keys:
                for record_b_id in index.get(key, ()):
                    matches.setdefault(record_b_id, reason)
        for record_b_id, reason in list(matches.items())[:max_per_record]:
            pairs.append(CandidatePair(record_a.record_id, record_b_id, reason))
    return pairs


def _build_indexes(
    records: list[ExtractedRecord],
    broad_key_limit: int,
) -> dict[str, dict[tuple[str, ...], tuple[int, ...]]]:
    raw: dict[str, dict[tuple[str, ...], list[int]]] = {
        reason: defaultdict(list)
        for reason in (
            "subject+exact_address",
            "subject+coarse_address+issue",
            "exact_address+issue",
            "subject+issue",
        )
    }
    for record in records:
        for reason, keys in _candidate_keys(record):
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


def _candidate_keys(record: ExtractedRecord) -> list[tuple[str, set[tuple[str, ...]]]]:
    subject = {_normalize(value) for value in record.subject_keys if value.strip()}
    exact = {_normalize(value) for value in record.exact_address_keys if value.strip()}
    coarse = {_normalize(value) for value in record.coarse_address_keys if value.strip()}
    issue = _normalize(record.primary_issue) if record.primary_issue else None

    keys: list[tuple[str, set[tuple[str, ...]]]] = []
    keys.append(("subject+exact_address", set(product(subject, exact))))
    if issue:
        keys.append(
            (
                "subject+coarse_address+issue",
                {(subject_key, address_key, issue) for subject_key, address_key in product(subject, coarse)},
            )
        )
        keys.append(("exact_address+issue", {(address_key, issue) for address_key in exact}))
        if not exact and not coarse:
            keys.append(("subject+issue", {(subject_key, issue) for subject_key in subject}))
    return keys


def _normalize(value: str) -> str:
    return "".join(value.casefold().split())
