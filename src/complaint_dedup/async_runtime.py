from dataclasses import dataclass

from sqlalchemy.engine import URL

from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.async_pipeline import AsyncJobProcessor
from complaint_dedup.config import Settings
from complaint_dedup.embedding_client import EmbeddingClient
from complaint_dedup.llm_client import LlmClient
from complaint_dedup.milvus_store import MilvusVectorStore
from complaint_dedup.rerank_client import RerankClient


@dataclass
class AsyncRuntime:
    database: AsyncDatabase
    processor: AsyncJobProcessor
    llm_client: LlmClient
    judgement_llm_client: LlmClient
    embedding_client: EmbeddingClient
    rerank_client: RerankClient
    vector_store: MilvusVectorStore

    async def close(self) -> None:
        await self.vector_store.close()
        await self.rerank_client.aclose()
        await self.embedding_client.aclose()
        await self.judgement_llm_client.aclose()
        await self.llm_client.aclose()
        await self.database.close()


async def create_runtime(settings: Settings) -> AsyncRuntime:
    database = AsyncDatabase(
        database_url(settings),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )
    await database.initialize()
    llm_options = {
        "base_url": str(settings.llm_base_url),
        "api_key": settings.llm_api_key,
        "timeout_seconds": settings.llm_timeout_seconds,
        "max_retries": settings.llm_max_retries,
        "temperature": settings.llm_temperature,
        "max_tokens": settings.llm_max_tokens,
        "enable_thinking": settings.llm_enable_thinking,
        "send_enable_thinking": settings.llm_send_enable_thinking,
        "max_connections": settings.http_max_connections,
        "max_keepalive_connections": settings.http_max_keepalive_connections,
    }
    extraction_llm = LlmClient(
        **llm_options,
        model=settings.llm_extraction_model or settings.llm_model,
        concurrency=settings.llm_extraction_concurrency,
    )
    judgement_llm = LlmClient(
        **llm_options,
        model=settings.llm_judgement_model or settings.llm_model,
        concurrency=settings.llm_judgement_concurrency,
    )
    embedding = EmbeddingClient(
        url=str(settings.embedding_url),
        model=settings.embedding_model,
        timeout_seconds=settings.embedding_timeout,
        max_retries=settings.embedding_retries,
        concurrency=settings.embedding_concurrency,
        max_connections=settings.http_max_connections,
        max_keepalive_connections=settings.http_max_keepalive_connections,
    )
    rerank = RerankClient(
        url=str(settings.rerank_url),
        model=settings.rerank_model,
        timeout_seconds=settings.rerank_timeout,
        max_retries=settings.rerank_retries,
        concurrency=settings.rerank_concurrency,
        max_connections=settings.http_max_connections,
        max_keepalive_connections=settings.http_max_keepalive_connections,
    )
    vector_store = await MilvusVectorStore.connect(
        host=settings.milvus_host,
        port=settings.milvus_port,
        user=settings.milvus_user,
        password=settings.milvus_password,
        db_name=settings.milvus_db,
        collection_prefix=settings.milvus_collection,
        dimension=settings.embedding_dimension,
        concurrency=settings.milvus_concurrency,
    )
    processor = AsyncJobProcessor(
        database=database,
        llm_client=extraction_llm,
        extraction_llm_client=extraction_llm,
        judgement_llm_client=judgement_llm,
        embedding_client=embedding,
        rerank_client=rerank,
        vector_store=vector_store,
        extraction_batch_size=settings.llm_extraction_batch_size,
        judgement_batch_size=settings.llm_judgement_batch_size,
        extraction_concurrency=settings.llm_extraction_concurrency,
        judgement_concurrency=settings.llm_judgement_concurrency,
        vector_top_k=settings.event_vector_top_k if settings.pipeline_version == "event_cluster_v2" else settings.vector_top_k,
        rerank_top_n=settings.event_rerank_top_n if settings.pipeline_version == "event_cluster_v2" else settings.rerank_top_n,
        max_candidates_per_record=settings.max_candidates_per_record,
        max_inflight_batches_per_job=settings.max_inflight_batches_per_job,
        embedding_batch_size=settings.embedding_batch_size,
        rerank_enabled=settings.rerank_enabled,
        pipeline_version=settings.pipeline_version,
        event_llm_concurrency=settings.event_llm_concurrency,
        event_raw_group_limit=settings.event_raw_group_limit,
        event_component_max_size=settings.event_component_max_size,
        auto_merge_enabled=settings.auto_merge_enabled,
        auto_merge_confidence=settings.auto_merge_confidence,
        auto_merge_max_members=settings.auto_merge_max_members,
    )
    return AsyncRuntime(
        database,
        processor,
        extraction_llm,
        judgement_llm,
        embedding,
        rerank,
        vector_store,
    )


def database_url(settings: Settings) -> str:
    if settings.database_mode == "sqlite":
        return f"sqlite+aiosqlite:///{settings.database_path}"
    return URL.create(
        drivername="postgresql+asyncpg",
        username=settings.db_user,
        password=settings.db_password,
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
    ).render_as_string(hide_password=False)
