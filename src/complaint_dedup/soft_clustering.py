import asyncio
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol


class VectorStore(Protocol):
    async def search(
        self,
        *,
        record_id: int,
        vector_kind: str,
        limit: int,
        target_source: str | None,
        region: str | None = None,
        street: str | None = None,
    ) -> list[tuple[int, float]]: ...


class Reranker(Protocol):
    async def rerank(self, query: str, documents: list[str]) -> list[tuple[int, float]]: ...


@dataclass(frozen=True)
class SoftCandidate:
    record_a_id: int
    record_b_id: int
    vector_score: float
    rerank_score: float | None
    recall_reason: str = "hybrid_vector"


PHONE_BONUS = 0.15
CATEGORY_BONUS = 0.05
BROAD_BUCKET_LIMIT = 200


def build_vector_texts(record: dict[str, Any]) -> tuple[str, str]:
    title = _text(record.get("title"))
    appeal = _text(record.get("appeal_text"))
    region = _text(record.get("region"))
    street = _text(record.get("street"))
    category = _text(record.get("category"))
    location = "\n".join(
        part
        for part in (
            f"地区：{region}" if region else "",
            f"街道：{street}" if street else "",
            f"标题：{title}" if title else "",
            f"投诉原文：{appeal}" if appeal else "",
        )
        if part
    )
    issue = "\n".join(
        part
        for part in (
            f"事项分类：{category}" if category else "",
            f"标题：{title}" if title else "",
            f"投诉事实与诉求：{appeal}" if appeal else "",
        )
        if part
    )
    return location, issue


def extract_record_metadata(record: dict[str, Any]) -> dict[str, str | None]:
    combined = "\n".join(
        value
        for value in (_text(record.get("title")), _text(record.get("appeal_text")))
        if value
    )
    region_match = re.search(r"([\u4e00-\u9fff]{2,8}(?:区|县|市))", combined)
    street_text = combined[region_match.end() :] if region_match else combined
    street_match = re.search(r"([\u4e00-\u9fff]{2,12}(?:街道|镇|乡))", street_text)
    raw_fields = record.get("raw_fields") or record.get("raw_json") or {}
    phone = next(
        (
            _text(value)
            for key, value in raw_fields.items()
            if any(token in str(key) for token in ("电话", "号码", "手机")) and _text(value)
        ),
        "",
    )
    if not phone:
        phone_match = re.search(r"(?<!\d)1[3-9]\d{9}(?!\d)", combined)
        phone = phone_match.group(0) if phone_match else ""
    return {
        "region": region_match.group(1) if region_match else None,
        "street": street_match.group(1) if street_match else None,
        "phone_hash": hashlib.sha256(phone.encode("utf-8")).hexdigest() if phone else None,
    }


async def generate_soft_candidates(
    records: list[dict[str, Any]],
    *,
    mode: Literal["single", "cross"],
    vector_store: VectorStore,
    reranker: Reranker,
    vector_top_k: int,
    rerank_top_n: int,
    max_candidates_per_record: int,
    concurrency: int,
    match_preset: Literal["strict", "balanced", "loose"] = "balanced",
    time_window_days: int = 0,
    rerank_enabled: bool = True,
) -> list[SoftCandidate]:
    if time_window_days < 0:
        raise ValueError("time_window_days must not be negative")
    by_id = {int(record["id"]): record for record in records}
    anchors = records if mode == "single" else [record for record in records if record["source"] == "A"]
    phone_buckets = _build_buckets(records, "phone_hash")
    category_buckets = _build_buckets(records, "category")

    async def process(record: dict[str, Any]) -> list[SoftCandidate]:
        record_id = int(record["id"])
        target_source = None if mode == "single" else "B"
        region = _text(record.get("region")) or None
        street = _text(record.get("street")) or None
        location, issue = await asyncio.gather(
            _search_preferred_scope(
                vector_store,
                record_id=record_id,
                vector_kind="location",
                limit=vector_top_k,
                target_source=target_source,
                region=region,
                street=street,
                match_preset=match_preset,
            ),
            _search_preferred_scope(
                vector_store,
                record_id=record_id,
                vector_kind="issue",
                limit=vector_top_k,
                target_source=target_source,
                region=region,
                street=street,
                match_preset=match_preset,
            ),
        )
        scores: dict[int, float] = {}
        reasons: dict[int, set[str]] = {}
        for candidate_id, score in location:
            candidate_id = int(candidate_id)
            scores[candidate_id] = scores.get(candidate_id, 0.0) + float(score) * 0.65
            reasons.setdefault(candidate_id, set()).add("hybrid_vector")
        for candidate_id, score in issue:
            candidate_id = int(candidate_id)
            scores[candidate_id] = scores.get(candidate_id, 0.0) + float(score) * 0.35
            reasons.setdefault(candidate_id, set()).add("hybrid_vector")

        phone = _normalized_key(record.get("phone_hash"))
        if phone:
            for candidate_id in _bucket_neighbors(
                phone_buckets.get(phone, []),
                record_id,
                vector_top_k,
                lambda candidate_id: _eligible_pair(
                    record, by_id.get(candidate_id), record_id, candidate_id, mode
                ) and _within_time_window(record, by_id.get(candidate_id), time_window_days),
            ):
                _add_reason(scores, reasons, candidate_id, "same_phone", PHONE_BONUS)
        category = _normalized_key(record.get("category"))
        category_bucket = category_buckets.get(category, []) if category else []
        if category and len(category_bucket) <= BROAD_BUCKET_LIMIT:
            for candidate_id in _bucket_neighbors(
                category_bucket,
                record_id,
                vector_top_k,
                lambda candidate_id: _eligible_pair(
                    record, by_id.get(candidate_id), record_id, candidate_id, mode
                ) and _within_time_window(record, by_id.get(candidate_id), time_window_days),
            ):
                _add_reason(scores, reasons, candidate_id, "same_category", CATEGORY_BONUS)

        for candidate_id in list(scores):
            candidate = by_id.get(candidate_id)
            if candidate is None:
                continue
            if phone and _normalized_key(candidate.get("phone_hash")) == phone:
                _add_reason(scores, reasons, candidate_id, "same_phone", PHONE_BONUS)
            if category and _normalized_key(candidate.get("category")) == category:
                _add_reason(scores, reasons, candidate_id, "same_category", CATEGORY_BONUS)

        eligible = [
            (candidate_id, score)
            for candidate_id, score in scores.items()
            if _eligible_pair(record, by_id.get(candidate_id), record_id, candidate_id, mode)
            and _within_time_window(record, by_id.get(candidate_id), time_window_days)
        ]
        eligible.sort(key=lambda item: item[1], reverse=True)
        candidate_ids = [candidate_id for candidate_id, _ in eligible]
        documents = [_document_text(by_id[candidate_id]) for candidate_id in candidate_ids]
        ranked = (
            await reranker.rerank(_document_text(record), documents)
            if rerank_enabled
            else [(index, score) for index, (_, score) in enumerate(eligible)]
        )
        result: list[SoftCandidate] = []
        score_by_id = dict(eligible)
        for index, rerank_score in ranked[:rerank_top_n]:
            if index < 0 or index >= len(candidate_ids):
                continue
            candidate_id = candidate_ids[index]
            left, right = (record_id, candidate_id)
            if mode == "single":
                left, right = sorted((left, right))
            elif by_id[left]["source"] == "B":
                left, right = right, left
            result.append(
                SoftCandidate(
                    record_a_id=left,
                    record_b_id=right,
                    vector_score=score_by_id[candidate_id],
                    rerank_score=float(rerank_score) if rerank_enabled else None,
                    recall_reason=",".join(sorted(reasons[candidate_id], key=_reason_order)),
                )
            )
            if len(result) >= max_candidates_per_record:
                break
        return result

    batches = await _bounded_map(anchors, process, concurrency)
    merged: dict[tuple[int, int], SoftCandidate] = {}
    for candidate in (item for batch in batches for item in batch):
        key = (candidate.record_a_id, candidate.record_b_id)
        existing = merged.get(key)
        if existing is None:
            merged[key] = candidate
            continue
        merged[key] = SoftCandidate(
            record_a_id=key[0],
            record_b_id=key[1],
            vector_score=max(existing.vector_score, candidate.vector_score),
            rerank_score=max(existing.rerank_score or 0.0, candidate.rerank_score or 0.0),
            recall_reason=",".join(
                sorted(
                    set(existing.recall_reason.split(",")) | set(candidate.recall_reason.split(",")),
                    key=_reason_order,
                )
            ),
        )
    ranked_candidates = sorted(
        merged.values(),
        key=lambda item: (-(item.rerank_score or 0.0), -item.vector_score, item.record_a_id, item.record_b_id),
    )
    counts: dict[int, int] = {}
    limited: list[SoftCandidate] = []
    for candidate in ranked_candidates:
        if counts.get(candidate.record_a_id, 0) >= max_candidates_per_record:
            continue
        if counts.get(candidate.record_b_id, 0) >= max_candidates_per_record:
            continue
        limited.append(candidate)
        counts[candidate.record_a_id] = counts.get(candidate.record_a_id, 0) + 1
        counts[candidate.record_b_id] = counts.get(candidate.record_b_id, 0) + 1
    return sorted(limited, key=lambda item: (item.record_a_id, item.record_b_id))


async def _search_preferred_scope(
    vector_store: VectorStore,
    *,
    record_id: int,
    vector_kind: str,
    limit: int,
    target_source: str | None,
    region: str | None,
    street: str | None,
    match_preset: Literal["strict", "balanced", "loose"],
) -> list[tuple[int, float]]:
    scopes = [(region, street)] if region or street else [(None, None)]
    if street:
        scopes.append((region, None))
    if region and match_preset == "loose":
        scopes.append((None, None))
    for scope_region, scope_street in dict.fromkeys(scopes):
        kwargs = {
            "record_id": record_id,
            "vector_kind": vector_kind,
            "limit": limit,
            "target_source": target_source,
        }
        if scope_region or scope_street:
            kwargs.update(region=scope_region, street=scope_street)
        try:
            result = await vector_store.search(**kwargs)
        except TypeError as exc:
            if "unexpected keyword argument" not in str(exc):
                raise
            kwargs.pop("region", None)
            kwargs.pop("street", None)
            result = await vector_store.search(**kwargs)
        if result:
            return result
    return []


async def _bounded_map(
    values: list[dict[str, Any]],
    operation: Any,
    concurrency: int,
) -> list[list[SoftCandidate]]:
    queue: asyncio.Queue[tuple[int, dict[str, Any]] | None] = asyncio.Queue(
        maxsize=max(concurrency * 2, 1)
    )
    results: list[list[SoftCandidate] | None] = [None] * len(values)

    async def produce() -> None:
        for index, value in enumerate(values):
            await queue.put((index, value))
        for _ in range(concurrency):
            await queue.put(None)

    async def consume() -> None:
        while True:
            item = await queue.get()
            if item is None:
                return
            index, value = item
            results[index] = await operation(value)

    async with asyncio.TaskGroup() as group:
        group.create_task(produce())
        for _ in range(concurrency):
            group.create_task(consume())
    return [result or [] for result in results]


def _build_buckets(records: list[dict[str, Any]], field: str) -> dict[str, list[int]]:
    buckets: dict[str, list[int]] = {}
    for record in records:
        value = _normalized_key(record.get(field))
        if value:
            buckets.setdefault(value, []).append(int(record["id"]))
    return buckets


def _bucket_neighbors(
    values: list[int],
    record_id: int,
    limit: int,
    eligible: Any,
) -> list[int]:
    return [value for value in values if value != record_id and eligible(value)][:limit]


def _add_reason(
    scores: dict[int, float],
    reasons: dict[int, set[str]],
    candidate_id: int,
    reason: str,
    bonus: float,
) -> None:
    candidate_reasons = reasons.setdefault(candidate_id, set())
    if reason in candidate_reasons:
        return
    candidate_reasons.add(reason)
    scores[candidate_id] = scores.get(candidate_id, 0.0) + bonus


def _eligible_pair(
    record: dict[str, Any],
    candidate: dict[str, Any] | None,
    record_id: int,
    candidate_id: int,
    mode: Literal["single", "cross"],
) -> bool:
    if candidate is None or candidate_id == record_id:
        return False
    return mode == "single" or {str(record["source"]), str(candidate["source"])} == {"A", "B"}


def _within_time_window(
    record: dict[str, Any],
    candidate: dict[str, Any] | None,
    days: int,
) -> bool:
    if days == 0 or candidate is None:
        return True
    left = _text(record.get("received_at"))
    right = _text(candidate.get("received_at"))
    if not left or not right:
        return True
    try:
        left_at = datetime.fromisoformat(left.replace("/", "-"))
        right_at = datetime.fromisoformat(right.replace("/", "-"))
    except ValueError:
        return True
    return abs((left_at - right_at).total_seconds()) <= days * 86_400


def _normalized_key(value: Any) -> str:
    return "".join(_text(value).casefold().split())


def _reason_order(reason: str) -> int:
    return {"hybrid_vector": 0, "same_phone": 1, "same_category": 2}.get(reason, 99)


def _document_text(record: dict[str, Any]) -> str:
    return "\n".join(
        value
        for value in (
            _text(record.get("title")),
            _text(record.get("category")),
            _text(record.get("appeal_text")),
        )
        if value
    )


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""
