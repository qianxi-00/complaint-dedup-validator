import asyncio
from typing import Any, Literal

from pymilvus import AsyncMilvusClient, DataType


class MilvusVectorStore:
    def __init__(
        self,
        client: Any,
        *,
        collection_prefix: str,
        concurrency: int,
    ) -> None:
        self._client = client
        self._collection_prefix = collection_prefix
        self._semaphore = asyncio.Semaphore(concurrency)

    @classmethod
    async def connect(
        cls,
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        db_name: str,
        collection_prefix: str,
        dimension: int,
        concurrency: int,
    ) -> "MilvusVectorStore":
        uri = f"http://{host}:{port}"
        admin = AsyncMilvusClient(uri=uri, user=user, password=password)
        databases = await admin.list_databases()
        if db_name not in databases:
            await admin.create_database(db_name)
        await admin.close()
        client = AsyncMilvusClient(
            uri=uri,
            user=user,
            password=password,
            db_name=db_name,
        )
        store = cls(client, collection_prefix=collection_prefix, concurrency=concurrency)
        await store.initialize(dimension)
        return store

    async def initialize(self, dimension: int) -> None:
        for kind in ("location", "issue"):
            collection = self._collection(kind)
            if await self._client.has_collection(collection):
                await self._ensure_street_field(collection)
                continue
            schema = self._client.create_schema(auto_id=False, enable_dynamic_field=False)
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field("job_id", DataType.VARCHAR, max_length=64)
            schema.add_field("source", DataType.VARCHAR, max_length=1)
            schema.add_field("region", DataType.VARCHAR, max_length=255)
            schema.add_field("street", DataType.VARCHAR, max_length=255)
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=dimension)
            indexes = self._client.prepare_index_params()
            indexes.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="COSINE",
                params={"M": 16, "efConstruction": 200},
            )
            try:
                await self._client.create_collection(
                    collection_name=collection,
                    schema=schema,
                    index_params=indexes,
                )
            except Exception as exc:
                if not _is_already_exists(exc):
                    raise

    async def _ensure_street_field(self, collection: str) -> None:
        description = await self._client.describe_collection(collection)
        fields = description.get("fields") or []
        if any(field.get("name") == "street" for field in fields):
            return
        try:
            await self._client.add_collection_field(
                collection,
                "street",
                DataType.VARCHAR,
                max_length=255,
                nullable=True,
            )
        except Exception as exc:
            if not _is_already_exists(exc):
                raise

    async def upsert_records(self, values: list[dict[str, Any]]) -> None:
        for kind in ("location", "issue"):
            rows = [
                {
                    "id": int(value["id"]),
                    "job_id": str(value["job_id"]),
                    "source": str(value["source"]),
                    "region": str(value.get("region") or ""),
                    "street": str(value.get("street") or ""),
                    "vector": value[f"{kind}_vector"],
                }
                for value in values
            ]
            if rows:
                async with self._semaphore:
                    await self._client.upsert(self._collection(kind), rows)

    async def search(
        self,
        *,
        record_id: int,
        vector_kind: Literal["location", "issue"],
        limit: int,
        target_source: str | None,
        region: str | None = None,
        street: str | None = None,
    ) -> list[tuple[int, float]]:
        collection = self._collection(vector_kind)
        async with self._semaphore:
            rows = await self._client.get(
                collection,
                ids=[record_id],
                output_fields=["job_id", "vector"],
            )
            if not rows:
                return []
            job_id = _escape_filter(str(rows[0]["job_id"]))
            expression = f'job_id == "{job_id}"'
            if target_source:
                expression += f' and source == "{_escape_filter(target_source)}"'
            if region:
                expression += f' and region == "{_escape_filter(region)}"'
            if street:
                expression += f' and street == "{_escape_filter(street)}"'
            result = await self._client.search(
                collection,
                data=[rows[0]["vector"]],
                filter=expression,
                limit=limit,
                output_fields=["id"],
                search_params={"metric_type": "COSINE", "params": {"ef": max(limit, 64)}},
            )
        hits = result[0] if result else []
        return [(int(hit["id"]), float(hit.get("distance", hit.get("score", 0.0)))) for hit in hits]

    async def close(self) -> None:
        await self._client.close()

    def _collection(self, kind: str) -> str:
        return f"{self._collection_prefix}_{kind}"


def _escape_filter(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _is_already_exists(exc: Exception) -> bool:
    message = str(exc).casefold().replace("_", " ")
    return "already exist" in message
