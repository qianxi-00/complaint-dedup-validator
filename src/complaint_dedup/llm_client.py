import json
import re
import asyncio
from collections.abc import Sequence
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel, ValidationError


ResponseModel = TypeVar("ResponseModel", bound=BaseModel)


class LlmResponseError(RuntimeError):
    def __init__(self, message: str, *, raw_response: str | None = None) -> None:
        super().__init__(message)
        self.raw_response = raw_response


class LlmClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_seconds: float,
        max_retries: int,
        temperature: float = 0,
        max_tokens: int = 4096,
        enable_thinking: bool = False,
        send_enable_thinking: bool = True,
        concurrency: int = 2,
        max_connections: int = 24,
        max_keepalive_connections: int = 12,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._model = model
        self._max_retries = max_retries
        self._temperature = temperature
        self._max_tokens = max_tokens
        self._enable_thinking = enable_thinking
        self._send_enable_thinking = send_enable_thinking
        self._semaphore = asyncio.Semaphore(concurrency)
        self._client = httpx.AsyncClient(
            base_url=f"{base_url.rstrip('/')}/",
            headers=headers,
            timeout=timeout_seconds,
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive_connections,
            ),
            transport=transport,
        )

    async def chat_json(
        self,
        messages: Sequence[dict[str, str]],
        response_model: type[ResponseModel],
    ) -> ResponseModel:
        last_error: Exception | None = None
        raw_response: str | None = None
        async with self._semaphore:
            for attempt in range(self._max_retries):
                try:
                    payload = {
                        "model": self._model,
                        "messages": list(messages),
                        "temperature": self._temperature,
                        "max_tokens": self._max_tokens,
                    }
                    if self._send_enable_thinking:
                        payload["enable_thinking"] = self._enable_thinking
                    response = await self._client.post(
                        "chat/completions",
                        json=payload,
                    )
                    response.raise_for_status()
                    content = response.json()["choices"][0]["message"]["content"]
                    raw_response = str(content)
                    payload = json.loads(_strip_code_fence(content))
                    return response_model.model_validate(payload)
                except (httpx.HTTPError, KeyError, TypeError, json.JSONDecodeError, ValidationError) as exc:
                    last_error = exc
                    if isinstance(exc, httpx.HTTPStatusError):
                        raw_response = exc.response.text
                    if attempt + 1 < self._max_retries:
                        await asyncio.sleep(_retry_delay(attempt, response if "response" in locals() else None))
        raise LlmResponseError(
            "模型响应无法通过结构化校验", raw_response=raw_response
        ) from last_error

    async def test_connection(self) -> str:
        if not self._model:
            raise LlmResponseError("未配置 LLM_MODEL")
        response = await self._client.post(
            "chat/completions",
            json={
                "model": self._model,
                "messages": [{"role": "user", "content": "只回复 OK"}],
                "temperature": 0,
            },
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        return f"连接成功：{str(content).strip()[:80]}"

    async def aclose(self) -> None:
        await self._client.aclose()


def _strip_code_fence(content: Any) -> str:
    text = str(content).strip()
    match = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    return match.group(1) if match else text


def _retry_delay(attempt: int, response: httpx.Response | None) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                pass
    return min(2**attempt, 8) * 0.25
