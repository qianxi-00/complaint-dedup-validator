import asyncio
from typing import Any

import httpx

from complaint_dedup.llm_client import _retry_delay


class RerankClient:
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

    async def rerank(self, query: str, documents: list[str]) -> list[tuple[int, float]]:
        if not documents:
            return []
        async with self._semaphore:
            result = await self._post_with_retry(
                {"model": self._model, "query": query, "documents": documents}
            )
        rows = result.get("results")
        if not isinstance(rows, list):
            raise ValueError("Rerank 响应缺少 results")
        parsed = [(int(row["index"]), float(row["relevance_score"])) for row in rows]
        return sorted(parsed, key=lambda item: item[1], reverse=True)

    async def _post_with_retry(self, payload: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            response: httpx.Response | None = None
            try:
                response = await self._client.post(self._url, json=payload)
                response.raise_for_status()
                result = response.json()
                if not isinstance(result, dict):
                    raise ValueError("Rerank 响应不是对象")
                return result
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                last_error = exc
                if attempt + 1 < self._max_retries:
                    await asyncio.sleep(_retry_delay(attempt, response))
        raise RuntimeError("Rerank 请求失败") from last_error

    async def aclose(self) -> None:
        await self._client.aclose()
