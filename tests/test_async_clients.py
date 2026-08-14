import asyncio
import json

import httpx
import pytest

from complaint_dedup.embedding_client import EmbeddingClient
from complaint_dedup.llm_client import LlmClient
from complaint_dedup.rerank_client import RerankClient


class SlowTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.01)
        self.active -= 1
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": '{"records": []}'}}]},
            request=request,
        )


@pytest.mark.asyncio
async def test_llm_client_limits_concurrent_requests() -> None:
    transport = SlowTransport()
    client = LlmClient(
        base_url="http://model.local/v1",
        api_key="",
        model="local-model",
        timeout_seconds=1,
        max_retries=1,
        concurrency=2,
        transport=transport,
    )

    await asyncio.gather(
        *[
            client.chat_json(
                [{"role": "user", "content": "extract"}],
                __import__("complaint_dedup.llm_models", fromlist=["ExtractionBatchResponse"]).ExtractionBatchResponse,
            )
            for _ in range(6)
        ]
    )

    assert transport.max_active == 2
    await client.aclose()


@pytest.mark.asyncio
async def test_embedding_client_parses_vectors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/embeddings"
        payload = json.loads(request.content)
        assert payload["model"] == "bge-m3"
        assert payload["input"] == ["甲", "乙"]
        return httpx.Response(
            200,
            json={"data": [{"index": 1, "embedding": [2.0]}, {"index": 0, "embedding": [1.0]}]},
            request=request,
        )

    client = EmbeddingClient(
        url="http://model.local/v1/embeddings",
        model="bge-m3",
        timeout_seconds=1,
        max_retries=1,
        transport=httpx.MockTransport(handler),
    )

    assert await client.embed(["甲", "乙"]) == [[1.0], [2.0]]
    await client.aclose()


@pytest.mark.asyncio
async def test_rerank_client_returns_sorted_results() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/rerank"
        return httpx.Response(
            200,
            json={"results": [{"index": 1, "relevance_score": 0.4}, {"index": 0, "relevance_score": 0.9}]},
            request=request,
        )

    client = RerankClient(
        url="http://model.local/v1/rerank",
        model="bge-reranker-v2-m3",
        timeout_seconds=1,
        max_retries=1,
        transport=httpx.MockTransport(handler),
    )

    assert await client.rerank("query", ["a", "b"]) == [(0, 0.9), (1, 0.4)]
    await client.aclose()
