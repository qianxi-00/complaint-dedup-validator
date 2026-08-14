import pytest

from complaint_dedup.milvus_store import MilvusVectorStore


class FakeMilvusClient:
    def __init__(self) -> None:
        self.filters: list[str] = []
        self.upserts: list[tuple[str, list[dict]]] = []

    async def get(self, collection_name: str, ids: list[int], output_fields: list[str]):
        return [{"id": ids[0], "job_id": "job-1", "vector": [0.1, 0.2]}]

    async def search(self, collection_name: str, **kwargs):
        self.filters.append(kwargs["filter"])
        return [[{"id": 2, "distance": 0.91}, {"id": 3, "distance": 0.72}]]

    async def upsert(self, collection_name: str, data: list[dict]):
        self.upserts.append((collection_name, data))

    async def close(self):
        return None


@pytest.mark.asyncio
async def test_milvus_search_filters_job_and_target_source() -> None:
    client = FakeMilvusClient()
    store = MilvusVectorStore(client, collection_prefix="records", concurrency=2)

    result = await store.search(
        record_id=1,
        vector_kind="location",
        limit=10,
        target_source="B",
        region=None,
        street=None,
    )

    assert result == [(2, 0.91), (3, 0.72)]
    assert client.filters == ['job_id == "job-1" and source == "B"']


@pytest.mark.asyncio
async def test_milvus_search_adds_optional_region_and_street_filters() -> None:
    client = FakeMilvusClient()
    store = MilvusVectorStore(client, collection_prefix="records", concurrency=2)

    await store.search(
        record_id=1,
        vector_kind="location",
        limit=10,
        target_source=None,
        region='江海区"测试',
        street="外海街道",
    )

    assert client.filters == [
        'job_id == "job-1" and region == "江海区\\"测试" and street == "外海街道"'
    ]


@pytest.mark.asyncio
async def test_milvus_upserts_both_vector_kinds() -> None:
    client = FakeMilvusClient()
    store = MilvusVectorStore(client, collection_prefix="records", concurrency=2)

    await store.upsert_records(
        [
            {
                "id": 1,
                "job_id": "job-1",
                "source": "S",
                "region": "江海区",
                "street": "外海街道",
                "location_vector": [0.1],
                "issue_vector": [0.2],
            }
        ]
    )

    assert [name for name, _ in client.upserts] == ["records_location", "records_issue"]
    assert client.upserts[0][1][0]["vector"] == [0.1]
    assert client.upserts[0][1][0]["street"] == "外海街道"
    assert client.upserts[1][1][0]["vector"] == [0.2]


class FakeSchema:
    def __init__(self) -> None:
        self.fields: list[str] = []

    def add_field(self, name: str, *_args, **_kwargs) -> None:
        self.fields.append(name)


class FakeIndexParams:
    def add_index(self, **_kwargs) -> None:
        return None


class RacingMilvusClient(FakeMilvusClient):
    def __init__(self) -> None:
        super().__init__()
        self.schemas: list[FakeSchema] = []

    async def has_collection(self, _collection_name: str) -> bool:
        return False

    def create_schema(self, **_kwargs) -> FakeSchema:
        schema = FakeSchema()
        self.schemas.append(schema)
        return schema

    def prepare_index_params(self) -> FakeIndexParams:
        return FakeIndexParams()

    async def create_collection(self, **_kwargs) -> None:
        raise RuntimeError("collection already exists")


@pytest.mark.asyncio
async def test_milvus_initialize_tolerates_concurrent_collection_creation() -> None:
    client = RacingMilvusClient()
    store = MilvusVectorStore(client, collection_prefix="records", concurrency=2)

    await store.initialize(1024)

    assert len(client.schemas) == 2
    assert all("street" in schema.fields for schema in client.schemas)


class ExistingCollectionClient(RacingMilvusClient):
    def __init__(self) -> None:
        super().__init__()
        self.added_fields: list[tuple[str, str]] = []

    async def has_collection(self, _collection_name: str) -> bool:
        return True

    async def describe_collection(self, _collection_name: str) -> dict:
        return {"fields": [{"name": "id"}, {"name": "region"}, {"name": "vector"}]}

    async def add_collection_field(
        self, collection_name: str, field_name: str, *_args, **_kwargs
    ) -> None:
        self.added_fields.append((collection_name, field_name))


@pytest.mark.asyncio
async def test_milvus_initialize_adds_street_to_existing_collections() -> None:
    client = ExistingCollectionClient()
    store = MilvusVectorStore(client, collection_prefix="records", concurrency=2)

    await store.initialize(1024)

    assert client.added_fields == [
        ("records_location", "street"),
        ("records_issue", "street"),
    ]
