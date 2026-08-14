import asyncio
from dataclasses import dataclass, field

import pytest

from complaint_dedup.soft_clustering import (
    build_vector_texts,
    extract_record_metadata,
    generate_soft_candidates,
)


def test_build_vector_texts_separates_location_and_issue() -> None:
    location, issue = build_vector_texts(
        {
            "title": "江海区金瓯路188号某公司拖欠工资",
            "category": "劳动保障/拖欠工资",
            "appeal_text": "地址：金瓯路188号。市民反映公司拖欠一月份工资。",
            "region": "江海区",
            "street": "外海街道",
        }
    )

    assert "金瓯路188号" in location
    assert "江海区" in location
    assert "拖欠工资" in issue
    assert "劳动保障" in issue


def test_extract_record_metadata_finds_region_street_and_hashes_phone() -> None:
    metadata = extract_record_metadata(
        {
            "title": "江海区外海街道金瓯路188号甲公司欠薪",
            "appeal_text": "联系电话：13800138000",
            "raw_fields": {"来电号码": "13800138000"},
        }
    )

    assert metadata["region"] == "江海区"
    assert metadata["street"] == "外海街道"
    assert len(metadata["phone_hash"]) == 64
    assert "13800138000" not in metadata["phone_hash"]


@dataclass
class FakeVectorStore:
    results: dict[tuple[int, str], list[tuple[int, float]]]
    calls: list[dict] = field(default_factory=list)

    async def search(
        self,
        *,
        record_id: int,
        vector_kind: str,
        limit: int,
        target_source: str | None,
        region: str | None = None,
        street: str | None = None,
    ) -> list[tuple[int, float]]:
        self.calls.append(
            {
                "record_id": record_id,
                "vector_kind": vector_kind,
                "region": region,
                "street": street,
            }
        )
        return self.results.get((record_id, vector_kind), [])[:limit]


class FakeReranker:
    async def rerank(self, query: str, documents: list[str]) -> list[tuple[int, float]]:
        return [(index, 1.0 - index * 0.1) for index in range(len(documents))]


class FailingReranker:
    async def rerank(self, query: str, documents: list[str]) -> list[tuple[int, float]]:
        raise AssertionError("关闭重排时不应调用 Rerank 服务")


@pytest.mark.asyncio
async def test_rerank_can_be_disabled_and_vector_order_is_retained() -> None:
    records = [
        {"id": 1, "source": "S", "title": "甲"},
        {"id": 2, "source": "S", "title": "乙"},
        {"id": 3, "source": "S", "title": "丙"},
    ]
    store = FakeVectorStore({(1, "location"): [(2, 0.9), (3, 0.7)]})

    pairs = await generate_soft_candidates(
        records,
        mode="single",
        vector_store=store,
        reranker=FailingReranker(),
        vector_top_k=5,
        rerank_top_n=2,
        max_candidates_per_record=2,
        concurrency=1,
        rerank_enabled=False,
    )

    assert [(pair.record_a_id, pair.record_b_id) for pair in pairs] == [(1, 2), (1, 3)]
    assert all(pair.rerank_score is None for pair in pairs)


@pytest.mark.asyncio
async def test_single_mode_deduplicates_reverse_neighbors_and_removes_self() -> None:
    records = [
        {"id": 1, "source": "S", "title": "甲", "appeal_text": "甲内容"},
        {"id": 2, "source": "S", "title": "乙", "appeal_text": "乙内容"},
        {"id": 3, "source": "S", "title": "丙", "appeal_text": "丙内容"},
    ]
    store = FakeVectorStore(
        {
            (1, "location"): [(1, 1.0), (2, 0.9), (3, 0.8)],
            (1, "issue"): [(2, 0.7)],
            (2, "location"): [(1, 0.9)],
            (2, "issue"): [(1, 0.7)],
        }
    )

    pairs = await generate_soft_candidates(
        records,
        mode="single",
        vector_store=store,
        reranker=FakeReranker(),
        vector_top_k=10,
        rerank_top_n=2,
        max_candidates_per_record=2,
        concurrency=2,
    )

    assert [(pair.record_a_id, pair.record_b_id) for pair in pairs] == [(1, 2), (1, 3)]
    assert pairs[0].vector_score == pytest.approx(0.83)


@pytest.mark.asyncio
async def test_cross_mode_only_returns_a_to_b_pairs() -> None:
    records = [
        {"id": 1, "source": "A", "title": "甲", "appeal_text": "甲内容"},
        {"id": 2, "source": "A", "title": "乙", "appeal_text": "乙内容"},
        {"id": 3, "source": "B", "title": "丙", "appeal_text": "丙内容"},
    ]
    store = FakeVectorStore(
        {
            (1, "location"): [(2, 0.99), (3, 0.8)],
            (1, "issue"): [(3, 0.6)],
            (2, "location"): [(1, 0.99), (3, 0.7)],
        }
    )

    pairs = await generate_soft_candidates(
        records,
        mode="cross",
        vector_store=store,
        reranker=FakeReranker(),
        vector_top_k=10,
        rerank_top_n=2,
        max_candidates_per_record=2,
        concurrency=2,
    )

    assert {(pair.record_a_id, pair.record_b_id) for pair in pairs} == {(1, 3), (2, 3)}


@pytest.mark.asyncio
async def test_vector_search_prefers_region_and_street_without_filtering_category() -> None:
    records = [
        {
            "id": 1,
            "source": "S",
            "title": "甲",
            "appeal_text": "甲内容",
            "region": "江海区",
            "street": "外海街道",
            "category": "劳动保障",
        },
        {"id": 2, "source": "S", "title": "乙", "appeal_text": "乙内容"},
    ]
    store = FakeVectorStore({(1, "location"): [(2, 0.9)]})

    await generate_soft_candidates(
        records,
        mode="single",
        vector_store=store,
        reranker=FakeReranker(),
        vector_top_k=10,
        rerank_top_n=2,
        max_candidates_per_record=2,
        concurrency=1,
    )

    first_calls = [call for call in store.calls if call["record_id"] == 1][:2]
    assert all(call["region"] == "江海区" for call in first_calls)
    assert all(call["street"] == "外海街道" for call in first_calls)
    assert all("category" not in call for call in first_calls)


@pytest.mark.asyncio
async def test_phone_and_category_rules_merge_with_vector_reasons() -> None:
    records = [
        {
            "id": 1,
            "source": "S",
            "title": "甲",
            "appeal_text": "甲内容",
            "phone_hash": "same-phone",
            "category": "劳动保障/欠薪",
        },
        {
            "id": 2,
            "source": "S",
            "title": "乙",
            "appeal_text": "乙内容",
            "phone_hash": "same-phone",
            "category": "劳动保障/欠薪",
        },
    ]
    store = FakeVectorStore({(1, "location"): [(2, 0.8)]})

    pairs = await generate_soft_candidates(
        records,
        mode="single",
        vector_store=store,
        reranker=FakeReranker(),
        vector_top_k=10,
        rerank_top_n=5,
        max_candidates_per_record=5,
        concurrency=1,
    )

    assert len(pairs) == 1
    assert set(pairs[0].recall_reason.split(",")) == {
        "hybrid_vector",
        "same_phone",
        "same_category",
    }
    assert pairs[0].vector_score > 0.8 * 0.65


@pytest.mark.asyncio
async def test_phone_rule_can_recall_candidate_but_does_not_make_a_decision() -> None:
    records = [
        {"id": 1, "source": "S", "title": "甲", "phone_hash": "same-phone"},
        {"id": 2, "source": "S", "title": "乙", "phone_hash": "same-phone"},
    ]

    pairs = await generate_soft_candidates(
        records,
        mode="single",
        vector_store=FakeVectorStore({}),
        reranker=FakeReranker(),
        vector_top_k=10,
        rerank_top_n=5,
        max_candidates_per_record=5,
        concurrency=1,
    )

    assert [(pair.record_a_id, pair.record_b_id) for pair in pairs] == [(1, 2)]
    assert pairs[0].recall_reason == "same_phone"
    assert not hasattr(pairs[0], "decision")


@pytest.mark.asyncio
async def test_high_frequency_category_does_not_generate_all_pairs() -> None:
    records = [
        {"id": record_id, "source": "S", "title": str(record_id), "category": "高频事项"}
        for record_id in range(1, 202)
    ]

    pairs = await generate_soft_candidates(
        records,
        mode="single",
        vector_store=FakeVectorStore({}),
        reranker=FakeReranker(),
        vector_top_k=5,
        rerank_top_n=2,
        max_candidates_per_record=2,
        concurrency=3,
    )

    assert pairs == []


@pytest.mark.asyncio
async def test_high_frequency_category_still_boosts_ann_candidates() -> None:
    records = [
        {"id": record_id, "source": "S", "title": str(record_id), "category": "高频事项"}
        for record_id in range(1, 202)
    ]
    store = FakeVectorStore({(1, "issue"): [(2, 0.8)]})

    pairs = await generate_soft_candidates(
        records,
        mode="single",
        vector_store=store,
        reranker=FakeReranker(),
        vector_top_k=5,
        rerank_top_n=2,
        max_candidates_per_record=2,
        concurrency=3,
    )

    pair = next(item for item in pairs if (item.record_a_id, item.record_b_id) == (1, 2))
    assert set(pair.recall_reason.split(",")) == {"hybrid_vector", "same_category"}
    assert pair.vector_score == pytest.approx(0.8 * 0.35 + 0.05)


@pytest.mark.asyncio
async def test_cross_rule_candidates_filter_source_before_applying_limit() -> None:
    records = [
        {"id": 1, "source": "A", "title": "甲", "phone_hash": "same-phone"},
        {"id": 2, "source": "A", "title": "乙", "phone_hash": "same-phone"},
        {"id": 3, "source": "B", "title": "丙", "phone_hash": "same-phone"},
    ]

    pairs = await generate_soft_candidates(
        records,
        mode="cross",
        vector_store=FakeVectorStore({}),
        reranker=FakeReranker(),
        vector_top_k=1,
        rerank_top_n=1,
        max_candidates_per_record=1,
        concurrency=1,
    )

    assert [(pair.record_a_id, pair.record_b_id) for pair in pairs] == [(1, 3)]
    assert pairs[0].recall_reason == "same_phone"


@pytest.mark.asyncio
async def test_candidate_generation_does_not_gather_every_anchor(monkeypatch) -> None:
    original_gather = asyncio.gather

    async def bounded_gather(*awaitables, **kwargs):
        assert len(awaitables) <= 2
        return await original_gather(*awaitables, **kwargs)

    monkeypatch.setattr("complaint_dedup.soft_clustering.asyncio.gather", bounded_gather)
    records = [
        {"id": record_id, "source": "S", "title": str(record_id)}
        for record_id in range(1, 20)
    ]

    await generate_soft_candidates(
        records,
        mode="single",
        vector_store=FakeVectorStore({}),
        reranker=FakeReranker(),
        vector_top_k=5,
        rerank_top_n=2,
        max_candidates_per_record=2,
        concurrency=3,
    )


@pytest.mark.asyncio
async def test_time_window_filters_vector_and_rule_candidates() -> None:
    records = [
        {
            "id": 1,
            "source": "S",
            "title": "甲",
            "received_at": "2026-01-01 09:00:00",
            "phone_hash": "same-phone",
        },
        {
            "id": 2,
            "source": "S",
            "title": "乙",
            "received_at": "2026-02-01 09:00:00",
            "phone_hash": "same-phone",
        },
    ]

    pairs = await generate_soft_candidates(
        records,
        mode="single",
        vector_store=FakeVectorStore({(1, "location"): [(2, 0.9)]}),
        reranker=FakeReranker(),
        vector_top_k=5,
        rerank_top_n=2,
        max_candidates_per_record=2,
        concurrency=1,
        time_window_days=7,
    )

    assert pairs == []


@pytest.mark.asyncio
async def test_strict_preset_does_not_fall_back_outside_known_region() -> None:
    class ScopedVectorStore(FakeVectorStore):
        async def search(self, **kwargs):
            self.calls.append(kwargs)
            if kwargs.get("region") == "江海区":
                return []
            return [(2, 0.9)]

    records = [
        {"id": 1, "source": "S", "title": "甲", "region": "江海区"},
        {"id": 2, "source": "S", "title": "乙", "region": "蓬江区"},
    ]
    strict_store = ScopedVectorStore({})
    loose_store = ScopedVectorStore({})

    strict_pairs = await generate_soft_candidates(
        records,
        mode="single",
        vector_store=strict_store,
        reranker=FakeReranker(),
        vector_top_k=5,
        rerank_top_n=2,
        max_candidates_per_record=2,
        concurrency=1,
        match_preset="strict",
    )
    loose_pairs = await generate_soft_candidates(
        records,
        mode="single",
        vector_store=loose_store,
        reranker=FakeReranker(),
        vector_top_k=5,
        rerank_top_n=2,
        max_candidates_per_record=2,
        concurrency=1,
        match_preset="loose",
    )

    assert strict_pairs == []
    assert [(pair.record_a_id, pair.record_b_id) for pair in loose_pairs] == [(1, 2)]
