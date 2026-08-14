import webbrowser

import uvicorn

from complaint_dedup.config import get_settings
from complaint_dedup.corpus_web import create_corpus_app


settings = get_settings()


def build_app(config):
    return create_corpus_app(config)


app = build_app(settings)


def run() -> None:
    runtime_settings = get_settings()
    webbrowser.open(f"http://{runtime_settings.app_host}:{runtime_settings.app_port}")
    uvicorn.run(app, host=runtime_settings.app_host, port=runtime_settings.app_port)


if __name__ == "__main__":
    run()
