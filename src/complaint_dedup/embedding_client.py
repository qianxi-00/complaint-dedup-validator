import asyncio
from typing import Any

import httpx

from complaint_dedup.llm_client import _retry_delay


class EmbeddingClient:
    def __init__(
        self,
        *,
        url: str,
        model: str,
        timeout_seconds: float,
        max_retries: int,
        concurrency: int = 4,
        max_connections: int = 24,
        max_keepalive_connections: int = 12,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._url = url
        self._model = model
        self._max_retries = max_retries
        self._semaphore = asyncio.Semaphore(concurrency)
        self._client = httpx.AsyncClient(
            timeout=timeout_seconds,
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive_connections,
            ),
            transport=transport,
        )

    async def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        async with self._semaphore:
            response = await self._post_with_retry({"model": self._model, "input": texts})
        rows = response.get("data")
        if not isinstance(rows, list) or len(rows) != len(texts):
            raise ValueError("Embedding 响应数量与输入不一致")
        ordered = sorted(rows, key=lambda row: int(row.get("index", 0)))
        vectors = [row.get("embedding") for row in ordered]
        if any(not isinstance(vector, list) for vector in vectors):
            raise ValueError("Embedding 响应缺少向量")
        return vectors

    async def _post_with_retry(self, payload: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            response: httpx.Response | None = None
            try:
                response = await self._client.post(self._url, json=payload)
                response.raise_for_status()
                result = response.json()
                if not isinstance(result, dict):
                    raise ValueError("Embedding 响应不是对象")
                return result
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                last_error = exc
                if attempt + 1 < self._max_retries:
                    await asyncio.sleep(_retry_delay(attempt, response))
        raise RuntimeError("Embedding 请求失败") from last_error

    async def aclose(self) -> None:
        await self._client.aclose()
