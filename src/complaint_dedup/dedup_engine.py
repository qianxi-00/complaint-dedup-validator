from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Protocol

from loguru import logger

from complaint_dedup.corpus_parser import parse_complaint
from complaint_dedup.dedup_features import (
    FEATURE_VERSION,
    FeatureBuildInput,
    RecordFeatures,
    build_record_features,
    normalize_feature_text,
    normalize_subject_for_match,
    normalize_work_order_id,
    sanitize_for_model,
)
from complaint_dedup.llm_models import EventCardBatchResponse

ALGORITHM_VERSION = "event-key-v3"
PROMPT_VERSION = "event-card-v2"
SYSTEM_PROMPT = """你是投诉工单判重专家。你只判断输入事件卡是否属于同一具体投诉事件或同一处置链。
必须遵守：
1. 仅因地区、街道、事项大类或电话相同，不得判为同一事件。
2. 同一主体、同一具体问题对象且没有硬冲突时，可以判为同一事件。
3. 明确不同的订单、门牌、楼栋、房间、商品、被投诉对象或核心事实，必须拆分。
4. 同一企业、同一问题族（如欠薪、食品安全、产品质量）的投诉，即使由不同人、不同月份提出，也视为同一事件。
5. 瞬时事项（噪音、占道、交通、单一故障）时间接近是重要的合并证据；持续事项（欠薪、物业、食品、产品质量）可以跨月，以主体和问题对象为准。
6. 证据不足时放入 unresolved_card_ids，不得猜测。
7. 只能输出指定 JSON，不得输出 Markdown、解释前后缀或输入中不存在的字段。
8. 每个 card_id 最多出现在一个分组中。
输出必须严格使用以下结构，不得省略键：
{
  "groups": [
    {
      "card_ids": ["C0001", "C0002"],
      "decision": "same_event",
      "confidence": 0.95,
      "supporting_evidence": [
        {"field": "subject", "cards": ["C0001", "C0002"], "reason": "同一具体主体"}
      ],
      "conflict_evidence": []
    }
  ],
  "unresolved_card_ids": []
}
field 只能使用 subject、location、issue、occurrence、text。
groups 内的组必须互斥；不能确定归组的卡片放入 unresolved_card_ids。
支持证据明确时 confidence 不得低于 0.85；证据不足时不得强行分组。"""


class JsonChatClient(Protocol):
    async def chat_json(self, messages, response_model):
        ...


@dataclass(frozen=True)
class DedupEngineOptions:
    algorithm_version: str = ALGORITHM_VERSION
    prompt_version: str = PROMPT_VERSION
    model_id: str = ""
    llm_enabled: bool = True
    max_candidates: int = 30
    max_cards_per_batch: int = 16
    max_requests: int = 0
    max_concurrency: int = 8
    max_seconds: float = 0
    min_confidence: float = 0.7
    candidate_score_threshold: float = 45.0
    assignment_score_threshold: float = 55.0
    model_review_score_threshold: float = 65.0
    text_duplicate_enabled: bool = True
    text_duplicate_threshold: float = 0.9
    fallback_max_span_days: int = 90
    fallback_span_shadow: bool = True

    @classmethod
    def from_settings(cls, settings: Any | None) -> "DedupEngineOptions":
        if settings is None:
            return cls()
        return cls(
            model_id=str(getattr(settings, "llm_model", "") or ""),
            llm_enabled=bool(getattr(settings, "dedup_llm_enabled", True)),
            max_candidates=int(getattr(settings, "dedup_max_candidates", 30)),
            max_cards_per_batch=int(
                getattr(settings, "dedup_cards_per_batch", 16)
            ),
            max_requests=int(getattr(settings, "dedup_max_requests", 0)),
            max_concurrency=int(
                getattr(settings, "dedup_max_concurrency", 8)
            ),
            max_seconds=float(
                getattr(settings, "dedup_max_seconds", 0)
            ),
            min_confidence=float(
                getattr(settings, "dedup_min_confidence", 0.7)
            ),
            text_duplicate_enabled=bool(
                getattr(settings, "dedup_text_duplicate_enabled", True)
            ),
            text_duplicate_threshold=float(
                getattr(settings, "dedup_text_duplicate_threshold", 0.9)
            ),
            fallback_max_span_days=int(
                getattr(settings, "dedup_fallback_max_span_days", 90)
            ),
            fallback_span_shadow=bool(
                getattr(settings, "dedup_fallback_span_shadow", True)
            ),
        )


@dataclass(frozen=True)
class DedupResult:
    groups: list[list[dict[str, Any]]]
    audits: list[dict[str, Any]]
    algorithm_version: str
    feature_version: str
    prompt_version: str
    model_id: str
    llm_coverage: float
    fallback_count: int
    decision_count: int
    request_count: int = 0
    llm_error_count: int = 0
    span_guard_count: int = 0


@dataclass(frozen=True)
class PreparedRecord:
    row: dict[str, Any]
    features: RecordFeatures
    mapping: dict[str, Any]


@dataclass(eq=False)
class EventCard:
    card_id: str
    records: list[PreparedRecord]
    features: dict[str, Any]
    legacy_keys: set[str]
    text: str
    text_grams: frozenset[str]

    @property
    def record_keys(self) -> list[str]:
        return sorted(str(record.row["record_key"]) for record in self.records)


class _UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size
        self.members = {index: [index] for index in range(size)}

    def find(self, value: int) -> int:
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        if self.rank[left_root] < self.rank[right_root]:
            left_root, right_root = right_root, left_root
        self.parent[right_root] = left_root
        self.members[left_root].extend(self.members.pop(right_root))
        if self.rank[left_root] == self.rank[right_root]:
            self.rank[left_root] += 1

    def grouped(self) -> list[list[int]]:
        result: dict[int, list[int]] = defaultdict(list)
        for index in range(len(self.parent)):
            result[self.find(index)].append(index)
        return list(result.values())


class DedupEngine:
    def __init__(
        self,
        *,
        llm_client: JsonChatClient | None = None,
        options: DedupEngineOptions | None = None,
    ) -> None:
        self.llm_client = llm_client
        self.options = options or DedupEngineOptions()
        self._request_count = 0
        self._llm_error_count = 0
        self._span_guard_count = 0
        self._covered_records = 0
        self._started_at = time.monotonic()

    async def cluster(self, rows: list[dict[str, Any]]) -> DedupResult:
        if not rows:
            return DedupResult(
                groups=[],
                audits=[],
                algorithm_version=self.options.algorithm_version,
                feature_version=FEATURE_VERSION,
                prompt_version=self.options.prompt_version,
                model_id=self.options.model_id,
                llm_coverage=0,
                fallback_count=0,
                decision_count=0,
            )
        prepared = [_prepare_record(row) for row in rows]
        use_llm = bool(self.llm_client and self.options.llm_enabled)
        if not use_llm:
            groups, audits = self._fallback_groups(prepared)
            return self._result(groups, audits)

        cards = _build_event_cards(prepared)
        audits = _hard_merge_audits(cards, self.options)
        components = _candidate_components(cards, self.options)
        final_cards: list[list[EventCard]] = []
        review_components: list[list[EventCard]] = []
        for component in components:
            if _component_rule_safe(component):
                final_cards.append(component)
                audits.extend(_rule_merge_audits(component, self.options))
            elif _should_model_review(component, self.options):
                review_components.append(component)
            else:
                final_cards.extend([[card] for card in component])
                audits.extend(
                    _keep_separate_audits(
                        component,
                        reason="缺少可校验合并证据，保持拆分",
                        options=self.options,
                    )
                )
        review_components.sort(key=len, reverse=True)
        semaphore = asyncio.Semaphore(self.options.max_concurrency)

        async def process_component(component: list[EventCard]):
            async with semaphore:
                return await self._partition_component(component)

        for start in range(0, len(review_components), self.options.max_concurrency):
            if (
                self._request_limit_reached()
                or self._time_limit_reached()
            ):
                for component in review_components[start:]:
                    groups, fallback_audits = self._fallback_card_groups(
                        component,
                        reason="达到模型请求或时间上限，自动回退保守规则",
                    )
                    final_cards.extend(groups)
                    audits.extend(fallback_audits)
                break
            batch = review_components[start : start + self.options.max_concurrency]
            component_results = await asyncio.gather(
                *(process_component(component) for component in batch)
            )
            for component_groups, component_audits in component_results:
                final_cards.extend(component_groups)
                audits.extend(component_audits)
        groups = [
            [record.row for card in group for record in card.records]
            for group in final_cards
        ]
        return self._result(groups, audits)

    async def _partition_component(
        self, cards: list[EventCard]
    ) -> tuple[list[list[EventCard]], list[dict[str, Any]]]:
        if len(cards) <= 1:
            return [cards], []
        if (
            self._request_limit_reached()
            or self._time_limit_reached()
        ):
            groups, audits = self._fallback_card_groups(
                cards, reason="达到模型请求或时间上限，自动回退保守规则"
            )
            return groups, audits
        if len(cards) <= self.options.max_cards_per_batch:
            response, elapsed_ms, error = await self._model_partition(cards)
            if error:
                groups, audits = self._fallback_card_groups(cards, reason=error)
                return groups, audits
            valid, leftovers, audits = self._validate_partition(
                cards, response, elapsed_ms
            )
            fallback_groups, fallback_audits = self._fallback_card_groups(
                leftovers,
                reason="模型未完成分组，自动回退保守规则",
            )
            return valid + fallback_groups, audits + fallback_audits

        representatives = _select_representatives(
            cards, self.options.max_cards_per_batch
        )
        response, elapsed_ms, error = await self._model_partition(representatives)
        if error:
            groups, audits = self._fallback_card_groups(cards, reason=error)
            return groups, audits
        valid, leftover_reps, audits = self._validate_partition(
            representatives, response, elapsed_ms
        )
        if not valid:
            groups, fallback_audits = self._fallback_card_groups(
                cards, reason="模型未形成可用分组，自动回退保守规则"
            )
            return groups, audits + fallback_audits
        remaining = [card for card in cards if card not in representatives]
        groups = [list(group) for group in valid]
        assigned_by_group: dict[int, list[EventCard]] = defaultdict(list)
        unassigned: list[EventCard] = list(leftover_reps)
        for card in remaining:
            target = _best_group_for_card(card, groups, self.options)
            if target is None:
                unassigned.append(card)
            else:
                groups[target].append(card)
                assigned_by_group[target].append(card)
        for group_index, assigned in assigned_by_group.items():
            audits.append(
                _audit(
                    groups[group_index],
                    decision="model_assignment",
                    confidence=0.75,
                    evidence_json=[
                        {
                            "field": "structured_similarity",
                            "cards": group_ids(groups[group_index]),
                            "reason": "代表卡分组通过结构化和文本相似度分配",
                        }
                    ],
                    conflict_json=[],
                    fallback_reason=None,
                    elapsed_ms=None,
                    options=self.options,
                    assigned_cards=assigned,
                )
            )
        if unassigned:
            recursive_groups, recursive_audits = await self._partition_component(
                unassigned
            )
            groups.extend(recursive_groups)
            audits.extend(recursive_audits)
        return groups, audits

    async def _model_partition(
        self, cards: list[EventCard]
    ) -> tuple[EventCardBatchResponse | None, int, str | None]:
        if not self.llm_client:
            return None, 0, "未配置模型，自动回退保守规则"
        self._request_count += 1
        self._covered_records += sum(len(card.records) for card in cards)
        payload = [_model_card_payload(card) for card in cards]
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "请将以下事件卡分成互斥事件组。只返回 JSON。\n"
                    + json.dumps(payload, ensure_ascii=False)
                ),
            },
        ]
        started = time.monotonic()
        try:
            if self.options.max_seconds > 0:
                remaining = self.options.max_seconds - (
                    time.monotonic() - self._started_at
                )
                if remaining <= 0:
                    raise TimeoutError("任务级模型时限已到")
                response = await asyncio.wait_for(
                    self.llm_client.chat_json(
                        messages, EventCardBatchResponse
                    ),
                    timeout=remaining,
                )
            else:
                response = await self.llm_client.chat_json(
                    messages, EventCardBatchResponse
                )
            return response, int((time.monotonic() - started) * 1000), None
        except Exception as exc:
            self._llm_error_count += 1
            logger.warning(
                "事件卡模型裁决失败，自动降级：类型={} 摘要={}",
                type(exc).__name__,
                str(exc)[:200],
            )
            return (
                None,
                int((time.monotonic() - started) * 1000),
                f"模型调用失败：{type(exc).__name__}",
            )

    def _validate_partition(
        self,
        cards: list[EventCard],
        response: EventCardBatchResponse | None,
        elapsed_ms: int,
    ) -> tuple[list[list[EventCard]], list[EventCard], list[dict[str, Any]]]:
        by_id = {card.card_id: card for card in cards}
        valid_groups: list[list[EventCard]] = []
        audits: list[dict[str, Any]] = []
        seen: set[str] = set()
        if response is None:
            return [], list(cards), audits
        for group in response.groups:
            card_ids = list(dict.fromkeys(group.card_ids))
            if any(card_id not in by_id for card_id in group.card_ids):
                audits.append(
                    _audit(
                        [],
                        decision="keep_separate",
                        confidence=group.confidence,
                        evidence_json=[],
                        conflict_json=[],
                        fallback_reason="模型输出包含未知事件卡",
                        elapsed_ms=elapsed_ms,
                        options=self.options,
                    )
                )
                continue
            group_cards = [by_id[card_id] for card_id in card_ids if card_id in by_id]
            rejection = _group_rejection_reason(
                group_cards,
                confidence=group.confidence,
                supporting_evidence=[
                    evidence.model_dump() for evidence in group.supporting_evidence
                ],
                conflict_evidence=[
                    evidence.model_dump() for evidence in group.conflict_evidence
                ],
                duplicate=any(card_id in seen for card_id in card_ids),
                options=self.options,
            )
            if rejection:
                audits.append(
                    _audit(
                        group_cards,
                        decision="keep_separate",
                        confidence=group.confidence,
                        evidence_json=[],
                        conflict_json=[
                            evidence.model_dump()
                            for evidence in group.conflict_evidence
                        ],
                        fallback_reason=rejection,
                        elapsed_ms=elapsed_ms,
                        options=self.options,
                    )
                )
                continue
            seen.update(card.card_id for card in group_cards)
            valid_groups.append(group_cards)
            if len(group_cards) > 1:
                audits.append(
                    _audit(
                        group_cards,
                        decision="model_merge",
                        confidence=group.confidence,
                        evidence_json=[
                            evidence.model_dump()
                            for evidence in group.supporting_evidence
                        ],
                        conflict_json=[],
                        fallback_reason=None,
                        elapsed_ms=elapsed_ms,
                        options=self.options,
                    )
                )
        unresolved_ids = set(response.unresolved_card_ids)
        leftovers = [
            card
            for card in cards
            if card.card_id not in seen and card.card_id in unresolved_ids
        ]
        leftovers.extend(
            card
            for card in cards
            if card.card_id not in seen and card.card_id not in unresolved_ids
        )
        return valid_groups, leftovers, audits

    def _fallback_card_groups(
        self, cards: list[EventCard], *, reason: str
    ) -> tuple[list[list[EventCard]], list[dict[str, Any]]]:
        if not cards:
            return [], []
        groups: list[list[EventCard]] = []
        group_keys: list[set[str]] = []
        for card in cards:
            target_index = None
            for index, group in enumerate(groups):
                if not card.legacy_keys & group_keys[index]:
                    continue
                if all(not _pair_conflict(card, member) for member in group):
                    if self._time_span_guard_blocks(card, group):
                        continue
                    target_index = index
                    break
            if target_index is None:
                groups.append([card])
                group_keys.append(set(card.legacy_keys))
            else:
                groups[target_index].append(card)
                group_keys[target_index] |= card.legacy_keys
        multi_groups = [group for group in groups if len(group) > 1]
        audits = [
            _audit(
                group,
                decision="rule_fallback",
                confidence=0.6,
                evidence_json=[{"field": "legacy_event_key", "cards": group_ids(group), "reason": "沿用现行保守规则"}],
                conflict_json=[],
                fallback_reason=reason,
                elapsed_ms=None,
                options=self.options,
            )
            for group in multi_groups
        ]
        if len(cards) > 1 and not audits:
            audits = [
                _audit(
                    cards,
                    decision="keep_separate",
                    confidence=0.6,
                    evidence_json=[],
                    conflict_json=[],
                    fallback_reason=reason,
                    elapsed_ms=None,
                    options=self.options,
                )
            ]
        return groups, audits

    def _fallback_groups(
        self, records: list[PreparedRecord]
    ) -> tuple[list[list[dict[str, Any]]], list[dict[str, Any]]]:
        cards = _build_event_cards(records)
        grouped_cards, card_audits = self._fallback_card_groups(
            cards, reason="未启用模型，沿用现行保守规则"
        )
        groups = [
            [record.row for card in group for record in card.records]
            for group in grouped_cards
        ]
        audits = _hard_merge_audits(cards, self.options)
        audits.extend(card_audits)
        return groups, audits

    def _result(
        self,
        groups: list[list[dict[str, Any]]],
        audits: list[dict[str, Any]],
    ) -> DedupResult:
        total_records = sum(len(group) for group in groups)
        coverage = (
            min(self._covered_records / total_records, 1.0) if total_records else 0
        )
        fallback_count = sum(1 for audit in audits if audit.get("fallback_reason"))
        return DedupResult(
            groups=groups,
            audits=audits,
            algorithm_version=self.options.algorithm_version,
            feature_version=FEATURE_VERSION,
            prompt_version=self.options.prompt_version,
            model_id=self.options.model_id,
            llm_coverage=coverage,
            fallback_count=fallback_count,
            decision_count=sum(
                1
                for audit in audits
                if audit["decision"]
                in {
                    "hard_merge",
                    "rule_merge",
                    "text_duplicate",
                    "model_merge",
                    "model_assignment",
                    "rule_fallback",
                }
            ),
            request_count=self._request_count,
            llm_error_count=self._llm_error_count,
            span_guard_count=self._span_guard_count,
        )

    def _time_span_guard_blocks(
        self, card: EventCard, group: list[EventCard]
    ) -> bool:
        """回退合并的时间跨度护栏。

        企业问题族按“同一企业=同一事件”处理，不适用护栏；
        普通键合并跨度超过阈值且无强证据时拦截。
        shadow 模式只计数不拦截，便于先观察影响。
        """
        max_days = self.options.fallback_max_span_days
        if max_days <= 0:
            return False
        if any(key.startswith("enterprise|") for key in card.legacy_keys):
            return False
        if self._has_strong_identity(card, group):
            return False
        span = _group_span_days([*group, card])
        if span is None or span <= max_days:
            return False
        self._span_guard_count += 1
        return not self.options.fallback_span_shadow

    @staticmethod
    def _has_strong_identity(card: EventCard, group: list[EventCard]) -> bool:
        for member in group:
            if _intersects(
                card.features.get("canonical_work_order_id"),
                member.features.get("canonical_work_order_id"),
            ):
                return True
            if _intersects(
                card.features.get("complaint_fingerprint"),
                member.features.get("complaint_fingerprint"),
            ):
                return True
            if _intersects(
                card.features.get("appeal_fingerprint"),
                member.features.get("appeal_fingerprint"),
            ):
                return True
            if _set_intersection(
                card.features.get("occurrence_ids"),
                member.features.get("occurrence_ids"),
            ):
                return True
        return False

    def _request_limit_reached(self) -> bool:
        return (
            self.options.max_requests > 0
            and self._request_count >= self.options.max_requests
        )

    def _time_limit_reached(self) -> bool:
        return (
            self.options.max_seconds > 0
            and time.monotonic() - self._started_at
            >= self.options.max_seconds
        )


def _prepare_record(row: dict[str, Any]) -> PreparedRecord:
    features = RecordFeatures.from_json(row.get("feature_json"))
    if features.feature_version != FEATURE_VERSION:
        parsed = parse_complaint(
            title=row.get("title_raw"),
            appeal=row.get("appeal_text"),
            location=row.get("location"),
        )
        features = build_record_features(
            FeatureBuildInput(
                work_order_id=row.get("work_order_id"),
                title=row.get("title_raw"),
                appeal_text=row.get("appeal_text"),
                location=row.get("location"),
                category=row.get("category"),
                parsed=parsed,
                source_row=int(row.get("source_row") or 0),
            )
        )
    return PreparedRecord(
        row=row,
        features=features,
        mapping=_record_feature_mapping(row, features),
    )


def _record_feature_mapping(
    row: dict[str, Any], features: RecordFeatures
) -> dict[str, Any]:
    return {
        **asdict(features),
        "location_keys": (
            [features.explicit_address]
            if features.explicit_address
            else []
        ),
        "region": row.get("region"),
        "street": row.get("street"),
        "anchor": row.get("anchor"),
        "issue_family": row.get("issue_family"),
        "title": row.get("title_raw"),
        "appeal": row.get("appeal_text"),
    }


def _build_event_cards(records: list[PreparedRecord]) -> list[EventCard]:
    if not records:
        return []
    finder = _UnionFind(len(records))
    canonical_index: dict[str, list[int]] = defaultdict(list)
    fingerprint_index: dict[str, list[int]] = defaultdict(list)
    appeal_index: dict[str, list[int]] = defaultdict(list)
    occurrence_index: dict[str, list[int]] = defaultdict(list)
    work_order_index: dict[str, list[int]] = defaultdict(list)
    for index, record in enumerate(records):
        if record.features.canonical_work_order_id:
            canonical_index[record.features.canonical_work_order_id].append(index)
            work_order_index[record.features.canonical_work_order_id].append(index)
        if record.features.complaint_fingerprint:
            fingerprint_index[record.features.complaint_fingerprint].append(index)
        if record.features.appeal_fingerprint:
            appeal_index[record.features.appeal_fingerprint].append(index)
        for occurrence in record.features.occurrence_ids:
            occurrence_index[occurrence].append(index)
    for indexes in canonical_index.values():
        first = indexes[0]
        for index in indexes[1:]:
            finder.union(first, index)
    for indexes in fingerprint_index.values():
        for group in _compatible_index_groups(indexes, records):
            for index in group[1:]:
                _safe_union(finder, records, group[0], index)
    for indexes in appeal_index.values():
        for group in _compatible_index_groups(indexes, records):
            for index in group[1:]:
                _safe_union(finder, records, group[0], index)
    for indexes in occurrence_index.values():
        for group in _compatible_index_groups(indexes, records):
            for index in group[1:]:
                _safe_union(finder, records, group[0], index)
    # 跟进单/复查单：诉求中引用的历史工单号指向库内工单时合并
    for index, record in enumerate(records):
        for previous in record.features.previous_work_order_ids:
            previous_norm = normalize_work_order_id(previous)
            if not previous_norm:
                continue
            for other in work_order_index.get(previous_norm, []):
                if other != index:
                    _safe_union(finder, records, index, other)

    clusters = [[records[index] for index in indexes] for indexes in finder.grouped()]
    clusters.sort(key=lambda group: min(str(record.row["record_key"]) for record in group))
    return [
        _make_event_card(f"C{index:04d}", cluster)
        for index, cluster in enumerate(clusters, start=1)
    ]


def _compatible_index_groups(
    indexes: list[int], records: list[PreparedRecord]
) -> list[list[int]]:
    groups: list[list[int]] = []
    for index in indexes:
        target = None
        for group in groups:
            if all(
                not _features_conflict(records[index].mapping, records[other].mapping)
                for other in group
            ):
                target = group
                break
        if target is None:
            groups.append([index])
        else:
            target.append(index)
    return groups


def _safe_union(
    finder: _UnionFind,
    records: list[PreparedRecord],
    left: int,
    right: int,
) -> None:
    left_root = finder.find(left)
    right_root = finder.find(right)
    if left_root == right_root:
        return
    left_members = finder.members[left_root]
    right_members = finder.members[right_root]
    if any(
        _features_conflict(records[a].mapping, records[b].mapping)
        for a in left_members
        for b in right_members
    ):
        return
    finder.union(left_root, right_root)


def _make_event_card(card_id: str, records: list[PreparedRecord]) -> EventCard:
    features = [record.features for record in records]
    representative = max(records, key=_record_completeness)
    title = max(
        (str(record.row.get("title_raw") or "") for record in records),
        key=len,
        default="",
    )
    appeal = max(
        (str(record.row.get("appeal_text") or "") for record in records),
        key=len,
        default="",
    )
    text = " ".join((title, appeal[:500]))
    return EventCard(
        card_id=card_id,
        records=records,
        features={
            "canonical_work_order_id": _first_nonempty(
                item.canonical_work_order_id for item in features
            ),
            "complaint_fingerprint": _first_nonempty(
                item.complaint_fingerprint for item in features
            ),
            "subjects": _merged_values(item.subjects for item in features),
            "locations": _merged_values(item.locations for item in features),
            "location_keys": _merged_values(
                item.explicit_address for item in features if item.explicit_address
            ),
            "issues": _merged_values(item.issues for item in features),
            "occurrence_ids": _merged_values(
                item.occurrence_ids for item in features
            ),
            "previous_work_order_ids": _merged_values(
                item.previous_work_order_ids for item in features
            ),
            "region": representative.row.get("region"),
            "street": representative.row.get("street"),
            "anchor": representative.row.get("anchor"),
            "issue_family": representative.row.get("issue_family"),
            "title": title,
            "appeal": appeal,
            "time_bounds": _time_bounds(records),
        },
        legacy_keys={
            str(record.row.get("event_key") or "")
            for record in records
            if record.row.get("event_key")
        },
        text=text,
        text_grams=frozenset(_ngrams(text)),
    )


def _candidate_components(
    cards: list[EventCard], options: DedupEngineOptions
) -> list[list[EventCard]]:
    if len(cards) <= 1:
        return [cards] if cards else []
    finder = _UnionFind(len(cards))
    exact_indexes: dict[str, list[int]] = defaultdict(list)
    gram_indexes: dict[str, list[int]] = defaultdict(list)
    for index, card in enumerate(cards):
        for key in _candidate_keys(card):
            exact_indexes[key].append(index)
        for gram in card.text_grams:
            if len(gram_indexes[gram]) < 120:
                gram_indexes[gram].append(index)
    for indexes in exact_indexes.values():
        first = indexes[0]
        for index in indexes[1:]:
            finder.union(first, index)

    seen_pairs: set[tuple[int, int]] = set()
    root_sizes: Counter[int] = Counter(finder.find(index) for index in range(len(cards)))
    fuzzy_edges: list[tuple[float, int, int]] = []
    for index, card in enumerate(cards):
        if root_sizes[finder.find(index)] > 1:
            continue
        candidates: set[int] = set()
        grams = sorted(
            card.text_grams,
            key=lambda gram: len(gram_indexes.get(gram, ())),
        )[:8]
        for gram in grams:
            candidates.update(gram_indexes.get(gram, ()))
        scored: list[tuple[float, int]] = []
        for other in candidates:
            if other == index:
                continue
            key = (min(index, other), max(index, other))
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            other_card = cards[other]
            score = _card_score(card, other_card)
            if score >= options.candidate_score_threshold and not _pair_conflict(
                card, other_card
            ):
                scored.append((score, other))
        scored.sort(reverse=True)
        for score, other in scored[: options.max_candidates]:
            fuzzy_edges.append((score, index, other))
    fuzzy_edges.sort(reverse=True)
    for _, left, right in fuzzy_edges:
        left_root = finder.find(left)
        right_root = finder.find(right)
        if left_root == right_root:
            continue
        if root_sizes[left_root] + root_sizes[right_root] > 8:
            continue
        finder.union(left_root, right_root)
        merged_root = finder.find(left_root)
        root_sizes[merged_root] = root_sizes[left_root] + root_sizes[right_root]
    result = [
        [cards[index] for index in indexes]
        for indexes in finder.grouped()
    ]
    return result


def _candidate_keys(card: EventCard) -> set[str]:
    features = card.features
    keys: set[str] = set()
    canonical = features.get("canonical_work_order_id")
    fingerprint = features.get("complaint_fingerprint")
    if canonical:
        keys.add(f"canonical:{canonical}")
    if fingerprint:
        keys.add(f"fingerprint:{fingerprint}")
    appeal = features.get("appeal_fingerprint")
    if appeal:
        keys.add(f"appeal:{appeal}")
    for occurrence in features.get("occurrence_ids") or []:
        keys.add(f"occurrence:{occurrence}")
    street = features.get("street")
    family = features.get("problem_family") or features.get("issue_family")
    subjects = features.get("strong_subjects") or features.get("subjects") or []
    for subject in subjects:
        if family:
            keys.add(f"subject-family:{street}:{subject}:{family}")
    for location in features.get("location_keys") or []:
        parts = [part for part in str(location).split("|") if part]
        if len(parts) >= 2:
            keys.add(f"location:{street}:{'|'.join(parts[:2])}")
        elif parts:
            keys.add(f"location:{street}:{parts[0]}")
    for issue in features.get("issues") or []:
        keys.add(f"anchor-issue:{street}:{features.get('anchor')}:{issue}")
    for legacy in card.legacy_keys:
        keys.add(f"legacy:{legacy}")
    return keys


def _card_score(left: EventCard, right: EventCard) -> float:
    left_features = left.features
    right_features = right.features
    if _pair_conflict(left, right):
        return 0
    score = 0.0
    if _intersects(
        left_features.get("canonical_work_order_id"),
        right_features.get("canonical_work_order_id"),
    ):
        score += 100
    if _intersects(
        left_features.get("complaint_fingerprint"),
        right_features.get("complaint_fingerprint"),
    ):
        score += 95
    if _intersects(
        left_features.get("appeal_fingerprint"),
        right_features.get("appeal_fingerprint"),
    ):
        score += 90
    if _set_intersection(
        left_features.get("occurrence_ids"), right_features.get("occurrence_ids")
    ):
        score += 90
    for left_subject in left_features.get("strong_subjects") or left_features.get("subjects") or []:
        for right_subject in right_features.get("strong_subjects") or right_features.get("subjects") or []:
            if _subjects_match(left_subject, right_subject):
                score += 75
                break
    if _set_intersection(
        left_features.get("location_keys"), right_features.get("location_keys")
    ):
        score += 55
    if _set_intersection(
        left_features.get("locations"), right_features.get("locations")
    ):
        score += 35
    score += 60 * _text_similarity_from_grams(
        left.text_grams, right.text_grams
    )
    score += _time_bonus(left, right)
    return score


def _pair_has_merge_evidence(left: EventCard, right: EventCard) -> bool:
    if _intersects(
        left.features.get("canonical_work_order_id"),
        right.features.get("canonical_work_order_id"),
    ):
        return True
    if _intersects(
        left.features.get("complaint_fingerprint"),
        right.features.get("complaint_fingerprint"),
    ):
        return True
    if _intersects(
        left.features.get("appeal_fingerprint"),
        right.features.get("appeal_fingerprint"),
    ):
        return True
    if _set_intersection(
        left.features.get("occurrence_ids"), right.features.get("occurrence_ids")
    ):
        return True
    for left_subject in left.features.get("strong_subjects") or left.features.get("subjects") or []:
        for right_subject in right.features.get("strong_subjects") or right.features.get("subjects") or []:
            if _subjects_match(left_subject, right_subject):
                left_family = left.features.get("problem_family") or left.features.get("issue_family")
                right_family = right.features.get("problem_family") or right.features.get("issue_family")
                if left_family and left_family == right_family:
                    return True
    if _set_intersection(
        left.features.get("location_keys"), right.features.get("location_keys")
    ):
        return (
            _issue_similarity(left, right) >= 0.25
            or _text_similarity_from_grams(left.text_grams, right.text_grams)
            >= 0.78
        )
    return (
        _text_similarity_from_grams(left.text_grams, right.text_grams)
        >= 0.78
    )


def _pair_rule_safe(left: EventCard, right: EventCard) -> bool:
    if _pair_conflict(left, right):
        return False
    if _intersects(
        left.features.get("canonical_work_order_id"),
        right.features.get("canonical_work_order_id"),
    ):
        return True
    if _intersects(
        left.features.get("complaint_fingerprint"),
        right.features.get("complaint_fingerprint"),
    ):
        return True
    if _set_intersection(
        left.features.get("occurrence_ids"), right.features.get("occurrence_ids")
    ):
        return True
    if (
        left.features.get("issue_family")
        and left.features.get("issue_family")
        == right.features.get("issue_family")
        and any(
            _subjects_match(left_subject, right_subject)
            for left_subject in left.features.get("subjects") or []
            for right_subject in right.features.get("subjects") or []
        )
    ):
        return True
    if not _set_intersection(
        left.features.get("location_keys"), right.features.get("location_keys")
    ):
        return False
    return (
        _text_similarity_from_grams(left.text_grams, right.text_grams) >= 0.72
    )


def _component_rule_safe(cards: list[EventCard]) -> bool:
    if len(cards) <= 1:
        return True
    if len(cards) > 32:
        return False
    for index, left in enumerate(cards):
        for right in cards[index + 1 :]:
            if not _pair_rule_safe(left, right):
                return False
    return True


def _should_model_review(
    cards: list[EventCard], options: DedupEngineOptions
) -> bool:
    if len(cards) <= 1:
        return False
    if len(cards) == 2:
        return _card_score(cards[0], cards[1]) >= options.model_review_score_threshold
    return True


def _pair_conflict(left: EventCard, right: EventCard) -> bool:
    return _features_conflict(left.features, right.features)


_UNKNOWN_VALUES = frozenset(
    {"", "未知地点", "未知街道", "未知", "不详", "无", "其他", "其它", "-", "--"}
)


def _known_value(value: Any) -> str:
    """把“未知/空”占位统一视为缺失，返回空字符串。"""
    text = str(value or "").strip()
    return "" if text in _UNKNOWN_VALUES else text


def _features_conflict(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if left.get("issue_family") and right.get("issue_family"):
        if left["issue_family"] != right["issue_family"]:
            return True
    # “未知街道/未知地点”等占位值视为缺失，不参与冲突判定
    left_street = _known_value(left.get("street"))
    right_street = _known_value(right.get("street"))
    if left_street and right_street and left_street != right_street:
        return True
    left_subjects = [str(value) for value in (left.get("strong_subjects") or [])]
    right_subjects = [str(value) for value in (right.get("strong_subjects") or [])]
    if left_subjects and right_subjects and not any(
        _subjects_match(a, b) for a in left_subjects for b in right_subjects
    ):
        return True
    left_occurrences = set(left.get("occurrence_ids") or [])
    right_occurrences = set(right.get("occurrence_ids") or [])
    if left_occurrences and right_occurrences and not left_occurrences & right_occurrences:
        return True
    left_address = set(str(value) for value in (left.get("location_keys") or []))
    right_address = set(str(value) for value in (right.get("location_keys") or []))
    if left_address and right_address and not (
        left_address <= right_address or right_address <= left_address
    ):
        return True
    return False


def _group_rejection_reason(
    cards: list[EventCard],
    *,
    confidence: float,
    supporting_evidence: list[dict[str, Any]],
    conflict_evidence: list[dict[str, Any]],
    duplicate: bool,
    options: DedupEngineOptions,
) -> str | None:
    if duplicate:
        return "模型输出存在重复卡片，已取消该组自动合并"
    if not cards:
        return "模型输出了未知卡片"
    if len(cards) == 1:
        return None
    if confidence < options.min_confidence:
        return f"模型置信度低于 {options.min_confidence:.2f}"
    if conflict_evidence:
        return "模型返回冲突证据"
    if not supporting_evidence:
        return "模型未返回可追溯支持证据"
    card_ids = {card.card_id for card in cards}
    evidence_cards = {
        card_id
        for evidence in supporting_evidence
        for card_id in evidence.get("cards", [])
    }
    if len(evidence_cards & card_ids) < 2:
        return "模型支持证据无法覆盖同一组内至少两张事件卡"
    for index, left in enumerate(cards):
        for right in cards[index + 1 :]:
            if _pair_conflict(left, right):
                return "候选事件卡存在硬冲突"
            if not _pair_has_merge_evidence(
                left, right
            ) and not _supporting_evidence_covers_pair(
                supporting_evidence, left.card_id, right.card_id
            ):
                return "候选事件卡缺少可校验的结构化或文本证据"
    return None


def _supporting_evidence_covers_pair(
    supporting_evidence: list[dict[str, Any]],
    left_card_id: str,
    right_card_id: str,
) -> bool:
    semantic_fields = {"subject", "issue", "occurrence", "text"}
    for evidence in supporting_evidence:
        if str(evidence.get("field") or "") not in semantic_fields:
            continue
        card_ids = set(str(value) for value in evidence.get("cards") or [])
        if left_card_id in card_ids and right_card_id in card_ids:
            return True
    return False


def _best_group_for_card(
    card: EventCard,
    groups: list[list[EventCard]],
    options: DedupEngineOptions,
) -> int | None:
    best_index = None
    best_score = 0.0
    for index, group in enumerate(groups):
        if any(_pair_conflict(card, member) for member in group):
            continue
        if not all(_pair_has_merge_evidence(card, member) for member in group):
            continue
        score = sum(_card_score(card, member) for member in group) / len(group)
        if score >= options.assignment_score_threshold and score > best_score:
            best_index = index
            best_score = score
    return best_index


def _select_representatives(
    cards: list[EventCard], limit: int
) -> list[EventCard]:
    if len(cards) <= limit:
        return list(cards)
    selected = [max(cards, key=lambda card: len(_card_text(card)))]
    remaining = [card for card in cards if card is not selected[0]]
    while remaining and len(selected) < limit:
        candidate = min(
            remaining,
            key=lambda card: max(_card_score(card, chosen) for chosen in selected),
        )
        selected.append(candidate)
        remaining.remove(candidate)
    return selected


def _model_card_payload(card: EventCard) -> dict[str, Any]:
    features = card.features
    return {
        "card_id": card.card_id,
        "base_order_token": _token(features.get("canonical_work_order_id")),
        "subjects": [
            sanitize_for_model(value, max_length=80)
            for value in (features.get("subjects") or [])[:6]
        ],
        "location": [
            sanitize_for_model(value, max_length=100)
            for value in (features.get("locations") or [])[:8]
        ],
        "issue": [
            sanitize_for_model(value, max_length=120)
            for value in (features.get("issues") or [])[:8]
        ],
        "occurrence": [
            _token(value) for value in (features.get("occurrence_ids") or [])[:6]
        ],
        "time_range": list(features.get("time_bounds") or []),
        "representative_title": sanitize_for_model(features.get("title")),
        "member_count": len(card.records),
        "evidence_snippets": _evidence_snippets(features.get("appeal")),
    }


def _evidence_snippets(value: Any) -> list[str]:
    text = sanitize_for_model(value, max_length=600)
    snippets = [
        item.strip()
        for item in re.split(r"[。；;\n]+", text)
        if len(item.strip()) >= 6
    ]
    return snippets[:3]


def _card_text(card: EventCard) -> str:
    return card.text


def _ngrams(value: str) -> set[str]:
    text = normalize_feature_text(value)[:1200]
    if len(text) < 3:
        return set()
    return {text[index : index + 3] for index in range(len(text) - 2)}


def _text_similarity(left: str, right: str) -> float:
    left_grams = _ngrams(left)
    right_grams = _ngrams(right)
    return _text_similarity_from_grams(left_grams, right_grams)


def _text_similarity_from_grams(
    left_grams: set[str] | frozenset[str],
    right_grams: set[str] | frozenset[str],
) -> float:
    if not left_grams or not right_grams:
        return 0.0
    return len(left_grams & right_grams) / len(left_grams | right_grams)


def _issue_similarity(left: EventCard, right: EventCard) -> float:
    return _text_similarity_from_grams(
        _ngrams(_issue_text(left)),
        _ngrams(_issue_text(right)),
    )


def _issue_text(card: EventCard) -> str:
    values = [
        str(card.features.get("title") or ""),
        *[str(value) for value in card.features.get("issues") or []],
    ]
    text = normalize_feature_text(" ".join(values))
    location_values = [
        *[str(value) for value in card.features.get("locations") or []],
        *[
            part
            for value in card.features.get("location_keys") or []
            for part in str(value).split("|")
        ],
        str(card.features.get("region") or ""),
        str(card.features.get("street") or ""),
        str(card.features.get("anchor") or ""),
    ]
    for value in location_values:
        normalized = normalize_feature_text(value)
        if normalized:
            text = text.replace(normalized, "")
    return text


def _subjects_match(left: str | None, right: str | None) -> bool:
    """主体模糊匹配：归一化相等、包含关系或字符 3-gram 相似度达标。"""
    left_text = normalize_subject_for_match(left)
    right_text = normalize_subject_for_match(right)
    if not left_text or not right_text:
        return False
    if left_text == right_text:
        return True
    if min(len(left_text), len(right_text)) >= 3 and (
        left_text in right_text or right_text in left_text
    ):
        return True
    return _text_similarity(left_text, right_text) >= 0.85


def _time_bonus(left: EventCard, right: EventCard) -> float:
    left_bounds = left.features.get("time_bounds") or []
    right_bounds = right.features.get("time_bounds") or []
    if len(left_bounds) != 2 or len(right_bounds) != 2:
        return 0.0
    left_start, left_end = _parse_date(left_bounds[0]), _parse_date(left_bounds[1])
    right_start, right_end = _parse_date(right_bounds[0]), _parse_date(right_bounds[1])
    if not all((left_start, left_end, right_start, right_end)):
        return 0.0
    distance = max(
        0,
        max(
            (right_start - left_end).days,
            (left_start - right_end).days,
        ),
    )
    return max(0.0, 10.0 - distance / 30)


def _group_span_days(cards: list[EventCard]) -> int | None:
    """事件卡组内最早到最晚时间的跨度（天）；不足两条日期时返回 None。"""
    dates: list[date] = []
    for card in cards:
        bounds = card.features.get("time_bounds") or []
        if len(bounds) != 2:
            continue
        for value in bounds:
            parsed = _parse_date(value)
            if parsed:
                dates.append(parsed)
    if len(dates) < 2:
        return None
    return (max(dates) - min(dates)).days


def _time_bounds(records: list[PreparedRecord]) -> tuple[str | None, str | None]:
    values: list[date] = []
    for record in records:
        for field in ("received_at", "completed_at"):
            value = record.row.get(field)
            if isinstance(value, datetime):
                values.append(value.date())
            elif isinstance(value, date):
                values.append(value)
    if not values:
        return (None, None)
    return (min(values).isoformat(), max(values).isoformat())


def _parse_date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _audit(
    cards: list[EventCard],
    *,
    decision: str,
    confidence: float,
    evidence_json: list[dict[str, Any]],
    conflict_json: list[dict[str, Any]],
    fallback_reason: str | None,
    elapsed_ms: int | None,
    options: DedupEngineOptions,
    assigned_cards: list[EventCard] | None = None,
) -> dict[str, Any]:
    return {
        "card_id": "+".join(group_ids(cards)) or "empty",
        "card_ids": group_ids(cards),
        "assigned_card_ids": group_ids(assigned_cards or []),
        "decision": decision,
        "confidence": confidence,
        "evidence_json": evidence_json,
        "conflict_json": conflict_json,
        "rule_version": options.algorithm_version,
        "prompt_version": (
            options.prompt_version
            if decision in {"model_merge", "model_assignment"}
            else None
        ),
        "model_id": (
            options.model_id
            if decision in {"model_merge", "model_assignment"}
            else None
        ),
        "fallback_reason": fallback_reason,
        "elapsed_ms": elapsed_ms,
        "record_keys": [
            record_key
            for card in cards
            for record_key in card.record_keys
        ],
    }


def _hard_merge_audits(
    cards: list[EventCard], options: DedupEngineOptions
) -> list[dict[str, Any]]:
    return [
        _audit(
            [card],
            decision="hard_merge",
            confidence=1,
            evidence_json=[{"field": "hard_identity", "cards": [card.card_id], "reason": "基础工单号、内容/正文指纹、发生对象或历史工单号硬匹配"}],
            conflict_json=[],
            fallback_reason=None,
            elapsed_ms=None,
            options=options,
        )
        for card in cards
        if len(card.records) > 1
    ]


def _rule_merge_audits(
    cards: list[EventCard], options: DedupEngineOptions
) -> list[dict[str, Any]]:
    if len(cards) <= 1:
        return []
    return [
        _audit(
            cards,
            decision="rule_merge",
            confidence=0.9,
            evidence_json=[
                {
                    "field": "deterministic_features",
                    "cards": group_ids(cards),
                    "reason": "命中基础编号、内容指纹、发生对象、同主体问题族或高置信地点文本规则",
                }
            ],
            conflict_json=[],
            fallback_reason=None,
            elapsed_ms=None,
            options=options,
        )
    ]


def _keep_separate_audits(
    cards: list[EventCard],
    *,
    reason: str,
    options: DedupEngineOptions,
) -> list[dict[str, Any]]:
    return [
        _audit(
            cards,
            decision="keep_separate",
            confidence=0,
            evidence_json=[],
            conflict_json=[],
            fallback_reason=reason,
            elapsed_ms=None,
            options=options,
        )
    ]


def group_ids(cards: list[EventCard]) -> list[str]:
    return [card.card_id for card in cards]


def _record_completeness(record: PreparedRecord) -> int:
    row = record.row
    return sum(
        len(str(row.get(field) or ""))
        for field in ("title_raw", "appeal_text", "location", "category")
    )


def _first_nonempty(values) -> Any:
    return next((value for value in values if value not in (None, "")), None)


def _merged_values(values) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for group in values:
        if isinstance(group, str):
            items = (group,)
        else:
            items = group or ()
        for item in items:
            text = str(item or "").strip()
            key = normalize_feature_text(text)
            if text and key and key not in seen:
                result.append(text)
                seen.add(key)
    return result


def _intersects(left: Any, right: Any) -> bool:
    return bool(left and right and str(left) == str(right))


def _set_intersection(left: Any, right: Any) -> set[str]:
    return set(left or []) & set(right or [])


def _token(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    kind = "编号"
    if text.startswith("order:"):
        kind = "订单"
    elif text.startswith("complaint:"):
        kind = "投诉单"
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:10].upper()
    return f"{kind}_{digest}"
