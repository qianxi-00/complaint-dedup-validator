import json
import re
from collections.abc import Sequence
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel, ValidationError


ResponseModel = TypeVar("ResponseModel", bound=BaseModel)


class LlmResponseError(RuntimeError):
    pass


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
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._model = model
        self._max_retries = max_retries
        self._temperature = temperature
        self._client = httpx.AsyncClient(
            base_url=f"{base_url.rstrip('/')}/",
            headers=headers,
            timeout=timeout_seconds,
            transport=transport,
        )

    async def chat_json(
        self,
        messages: Sequence[dict[str, str]],
        response_model: type[ResponseModel],
    ) -> ResponseModel:
        last_error: Exception | None = None
        for _ in range(self._max_retries):
            try:
                response = await self._client.post(
                    "chat/completions",
                    json={
                        "model": self._model,
                        "messages": list(messages),
                        "temperature": self._temperature,
                    },
                )
                response.raise_for_status()
                content = response.json()["choices"][0]["message"]["content"]
                payload = json.loads(_strip_code_fence(content))
                return response_model.model_validate(payload)
            except (httpx.HTTPError, KeyError, TypeError, json.JSONDecodeError, ValidationError) as exc:
                last_error = exc
        raise LlmResponseError("模型响应无法通过结构化校验") from last_error

    async def aclose(self) -> None:
        await self._client.aclose()


def _strip_code_fence(content: Any) -> str:
    text = str(content).strip()
    match = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    return match.group(1) if match else text
