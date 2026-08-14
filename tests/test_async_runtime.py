from pathlib import Path

import pytest

from complaint_dedup import async_runtime
from complaint_dedup.config import Settings


class FakeLlmClient:
    instances: list["FakeLlmClient"] = []

    def __init__(self, **kwargs) -> None:
        self.concurrency = kwargs["concurrency"]
        self.model = kwargs["model"]
        self.closed = False
        self.instances.append(self)

    async def aclose(self) -> None:
        self.closed = True


class FakeVectorStore:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_runtime_uses_independent_global_llm_clients(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    FakeLlmClient.instances = []
    vector_store = FakeVectorStore()

    async def connect_vector_store(**_kwargs):
        return vector_store

    monkeypatch.setattr(async_runtime, "LlmClient", FakeLlmClient)
    monkeypatch.setattr(async_runtime.MilvusVectorStore, "connect", connect_vector_store)
    settings = Settings(
        _env_file=None,
        database_mode="sqlite",
        database_path=tmp_path / "app.db",
        llm_model="test-model",
        llm_extraction_model="fast-extractor",
        llm_judgement_model="strong-judge",
        llm_extraction_concurrency=1,
        llm_judgement_concurrency=3,
    )

    runtime = await async_runtime.create_runtime(settings)

    assert [client.concurrency for client in FakeLlmClient.instances] == [1, 3]
    assert [client.model for client in FakeLlmClient.instances] == ["fast-extractor", "strong-judge"]
    assert runtime.processor.extraction_llm_client is FakeLlmClient.instances[0]
    assert runtime.processor.judgement_llm_client is FakeLlmClient.instances[1]

    await runtime.close()
    assert all(client.closed for client in FakeLlmClient.instances)
    assert vector_store.closed is True
