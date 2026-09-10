import asyncio
import json
import random
import re
import time
from collections import Counter
from collections.abc import Sequence
from typing import Any, TypeVar

import httpx
from loguru import logger
from pydantic import BaseModel, ValidationError


ResponseModel = TypeVar("ResponseModel", bound=BaseModel)

_THINK_BLOCK_RE = re.compile(
    r"<think(?:ing)?>.*?</think(?:ing)?>", re.DOTALL | re.IGNORECASE
)
_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)
_REPAIR_INSTRUCTION = (
    "上一次输出无法解析为要求的 JSON（错误：{error}）。"
    "请只输出修正后的 JSON 对象，不要输出解释、Markdown 代码围栏或思考过程。"
)


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
        json_mode: str = "prompt",
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
        self._json_mode = json_mode
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
        # 调用统计：requests/success/rate_limited/timeout/transport_error/http_error/invalid_output
        self.stats: Counter[str] = Counter()

    async def chat_json(
        self,
        messages: Sequence[dict[str, str]],
        response_model: type[ResponseModel],
    ) -> ResponseModel:
        base_messages = list(messages)
        request_messages = base_messages
        last_error: Exception | None = None
        raw_response: str | None = None
        started = time.monotonic()
        async with self._semaphore:
            for attempt in range(self._max_retries):
                response: httpx.Response | None = None
                self.stats["requests"] += 1
                try:
                    payload = {
                        "model": self._model,
                        "messages": list(request_messages),
                        "temperature": self._temperature,
                        "max_tokens": self._max_tokens,
                    }
                    if self._send_enable_thinking:
                        payload["enable_thinking"] = self._enable_thinking
                    _apply_json_mode(payload, response_model, self._json_mode)
                    logger.debug(
                        "LLM 请求 model={} attempt={}/{} 批量大小={}",
                        self._model,
                        attempt + 1,
                        self._max_retries,
                        len(payload["messages"]),
                    )
                    response = await self._client.post(
                        "chat/completions",
                        json=payload,
                    )
                    response.raise_for_status()
                    data = response.json()
                    choices = data.get("choices") or []
                    message = (choices[0].get("message") if choices else {}) or {}
                    content = message.get("content")
                    if not content:
                        # 部分思考型模型把有效内容放在 reasoning_content 字段
                        content = message.get("reasoning_content")
                    raw_response = str(content or "")
                    parsed = json.loads(
                        _strip_code_fence(_strip_thinking(raw_response))
                    )
                    result = response_model.model_validate(parsed)
                    self.stats["success"] += 1
                    logger.info(
                        "LLM 调用成功 model={} attempt={} 决策数={} 耗时={:.1f}s",
                        self._model,
                        attempt + 1,
                        len(getattr(result, "groups", []) or []),
                        time.monotonic() - started,
                    )
                    return result
                except (json.JSONDecodeError, ValidationError) as exc:
                    # 无效输出：下一次尝试携带修复提示，而不是简单重发
                    self.stats["invalid_output"] += 1
                    last_error = exc
                    request_messages = _repair_messages(
                        base_messages, raw_response, exc
                    )
                    if attempt + 1 < self._max_retries:
                        logger.warning(
                            "LLM 输出无效,准备修复重试 model={} attempt={}/{} 类型={} 摘要={}",
                            self._model,
                            attempt + 1,
                            self._max_retries,
                            type(exc).__name__,
                            str(exc)[:200],
                        )
                except httpx.TimeoutException as exc:
                    self.stats["timeout"] += 1
                    last_error = exc
                    if attempt + 1 < self._max_retries:
                        logger.warning(
                            "LLM 请求超时,准备重试 model={} attempt={}/{}",
                            self._model,
                            attempt + 1,
                            self._max_retries,
                        )
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 429:
                        self.stats["rate_limited"] += 1
                    else:
                        self.stats["http_error"] += 1
                    last_error = exc
                    raw_response = exc.response.text
                    if attempt + 1 < self._max_retries:
                        logger.warning(
                            "LLM 调用失败,准备重试 model={} attempt={}/{} 状态码={}",
                            self._model,
                            attempt + 1,
                            self._max_retries,
                            exc.response.status_code,
                        )
                except (httpx.HTTPError, KeyError, TypeError) as exc:
                    self.stats["transport_error"] += 1
                    last_error = exc
                    if attempt + 1 < self._max_retries:
                        logger.warning(
                            "LLM 调用失败,准备重试 model={} attempt={}/{} 类型={} 摘要={}",
                            self._model,
                            attempt + 1,
                            self._max_retries,
                            type(exc).__name__,
                            str(exc)[:200],
                        )
                if attempt + 1 < self._max_retries:
                    await asyncio.sleep(_retry_delay(attempt, response))
        snippet = (raw_response or "")[:400].replace("\n", " ")
        logger.error(
            "LLM 调用最终失败(已尝试 {} 次) model={} 类型={} 错误={} 响应摘要={} 统计={}",
            self._max_retries,
            self._model,
            type(last_error).__name__ if last_error else "-",
            str(last_error)[:200],
            snippet,
            dict(self.stats),
        )
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


def build_llm_client(
    settings: Any,
    *,
    model: str | None = None,
    concurrency: int | None = None,
) -> LlmClient:
    resolved_concurrency = concurrency or settings.llm_concurrency
    max_connections = max(
        settings.http_max_connections,
        resolved_concurrency + 4,
    )
    max_keepalive_connections = min(
        max_connections,
        max(settings.http_max_keepalive_connections, resolved_concurrency),
    )
    return LlmClient(
        base_url=str(settings.llm_base_url),
        api_key=settings.llm_api_key,
        model=model or settings.llm_model,
        timeout_seconds=settings.llm_timeout_seconds,
        max_retries=settings.llm_max_retries,
        temperature=settings.llm_temperature,
        max_tokens=settings.llm_max_tokens,
        enable_thinking=settings.llm_enable_thinking,
        send_enable_thinking=settings.llm_send_enable_thinking,
        json_mode=settings.llm_json_mode,
        concurrency=resolved_concurrency,
        max_connections=max_connections,
        max_keepalive_connections=max_keepalive_connections,
    )


def _apply_json_mode(
    payload: dict[str, Any],
    response_model: type[BaseModel],
    json_mode: str,
) -> None:
    if json_mode == "json_object":
        payload["response_format"] = {"type": "json_object"}
    elif json_mode == "guided_json":
        payload["guided_json"] = response_model.model_json_schema()


def _strip_thinking(content: Any) -> str:
    text = _THINK_BLOCK_RE.sub("", str(content or "")).strip()
    return text


def _strip_code_fence(content: Any) -> str:
    text = str(content).strip()
    match = _CODE_FENCE_RE.fullmatch(text)
    return match.group(1) if match else text


def _repair_messages(
    base_messages: list[dict[str, str]],
    raw_response: str | None,
    error: Exception,
) -> list[dict[str, str]]:
    snippet = (raw_response or "").strip()[:2000]
    return list(base_messages) + [
        {"role": "assistant", "content": snippet},
        {
            "role": "user",
            "content": _REPAIR_INSTRUCTION.format(
                error=f"{type(error).__name__}: {str(error)[:300]}"
            ),
        },
    ]


def _retry_delay(attempt: int, response: httpx.Response | None) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                pass
    base = min(2**attempt, 8) * 0.25
    return base + random.uniform(0, 0.25)
