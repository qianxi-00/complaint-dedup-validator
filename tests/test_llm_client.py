import json

import httpx
import pytest

from complaint_dedup.llm_client import LlmClient, LlmResponseError
from complaint_dedup.llm_models import ExtractionBatchResponse


@pytest.mark.asyncio
async def test_chat_json_retries_invalid_json_then_returns_valid_payload() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        content = "not json"
        if attempts == 3:
            content = json.dumps({"records": []})
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": content}}]},
        )

    client = LlmClient(
        base_url="http://model.local/v1",
        api_key="",
        model="local-model",
        timeout_seconds=1,
        max_retries=3,
        transport=httpx.MockTransport(handler),
    )

    result = await client.chat_json(
        [{"role": "user", "content": "extract"}],
        ExtractionBatchResponse,
    )

    assert result.records == []
    assert attempts == 3
    await client.aclose()


@pytest.mark.asyncio
async def test_chat_json_raises_after_retry_budget_is_exhausted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": "invalid"}}]},
        )

    client = LlmClient(
        base_url="http://model.local/v1",
        api_key="",
        model="local-model",
        timeout_seconds=1,
        max_retries=2,
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(LlmResponseError):
        await client.chat_json(
            [{"role": "user", "content": "extract"}],
            ExtractionBatchResponse,
        )
    await client.aclose()
