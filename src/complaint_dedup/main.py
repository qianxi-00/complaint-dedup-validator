import webbrowser

import uvicorn

from complaint_dedup.config import get_settings
from complaint_dedup.web import create_app


app = create_app()


def run() -> None:
    settings = get_settings()
    webbrowser.open(f"http://{settings.app_host}:{settings.app_port}")
    uvicorn.run(app, host=settings.app_host, port=settings.app_port)


if __name__ == "__main__":
    run()
