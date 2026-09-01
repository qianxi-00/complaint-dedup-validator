import json

import httpx
import pytest

from complaint_dedup.llm_client import LlmClient, LlmResponseError
from complaint_dedup.llm_models import NormalizationBatchResponse


@pytest.mark.asyncio
async def test_chat_json_retries_invalid_json_then_returns_valid_payload() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        content = "not json"
        if attempts == 3:
            content = json.dumps({"decisions": []})
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
        NormalizationBatchResponse,
    )

    assert result.decisions == []
    assert attempts == 3
    await client.aclose()


@pytest.mark.asyncio
async def test_chat_json_omits_unsupported_thinking_flag_when_disabled() -> None:
    payload = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal payload
        payload = json.loads(request.content)
        return httpx.Response(200, json={"choices": [{"message": {"content": '{"decisions":[]}'}}]})

    client = LlmClient(
        base_url="http://model.local/v1",
        api_key="",
        model="local-model",
        timeout_seconds=1,
        max_retries=1,
        max_tokens=2048,
        enable_thinking=False,
        send_enable_thinking=False,
        transport=httpx.MockTransport(handler),
    )

    await client.chat_json([{"role": "user", "content": "extract"}], NormalizationBatchResponse)

    assert payload["max_tokens"] == 2048
    assert "enable_thinking" not in payload
    await client.aclose()


@pytest.mark.asyncio
async def test_chat_json_sends_thinking_flag_when_enabled() -> None:
    payload = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal payload
        payload = json.loads(request.content)
        return httpx.Response(200, json={"choices": [{"message": {"content": '{"decisions":[]}'}}]})

    client = LlmClient(
        base_url="http://model.local/v1",
        api_key="",
        model="local-model",
        timeout_seconds=1,
        max_retries=1,
        enable_thinking=True,
        transport=httpx.MockTransport(handler),
    )

    await client.chat_json([{"role": "user", "content": "extract"}], NormalizationBatchResponse)

    assert payload["enable_thinking"] is True
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

    with pytest.raises(LlmResponseError) as error:
        await client.chat_json(
            [{"role": "user", "content": "extract"}],
            NormalizationBatchResponse,
        )
    assert error.value.raw_response == "invalid"
    await client.aclose()
