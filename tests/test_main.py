from complaint_dedup.config import Settings
from complaint_dedup.main import build_app


def test_main_uses_full_corpus_app_with_sqlite(tmp_path) -> None:
    app = build_app(
        Settings(
            database_mode="sqlite",
            database_path=tmp_path / "app.db",
            pipeline_mode="corpus_incremental",
            _env_file=None,
        )
    )

    assert app.title == "投诉全量工单比对系统"


def test_main_does_not_require_legacy_model_or_vector_runtime(tmp_path) -> None:
    app = build_app(
        Settings(
            database_mode="sqlite",
            database_path=tmp_path / "app.db",
            llm_model="",
            _env_file=None,
        )
    )

    assert app.title == "投诉全量工单比对系统"
