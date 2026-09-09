import uvicorn

from complaint_dedup.config import get_settings
from complaint_dedup.full_corpus_web import create_full_corpus_app
from complaint_dedup import licensing
from complaint_dedup.logging_setup import setup_logging


settings = get_settings()


def build_app(config):
    return create_full_corpus_app(config)


app = build_app(settings)


def run() -> None:
    licensing.ensure_not_expired()
    runtime_settings = get_settings()
    setup_logging(
        log_dir=runtime_settings.log_dir,
        process_name="api",
        level=runtime_settings.log_level,
        retention_days=runtime_settings.log_retention_days,
    )
    uvicorn.run(app, host=runtime_settings.app_host, port=runtime_settings.app_port)


if __name__ == "__main__":
    run()
