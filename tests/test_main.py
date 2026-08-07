from complaint_dedup.config import Settings
from complaint_dedup import main


def test_run_uses_host_and_port_from_settings(monkeypatch) -> None:
    calls = {}
    configured = Settings(app_host="127.0.0.2", app_port=9876, llm_model="test")
    monkeypatch.setattr(main, "get_settings", lambda: configured)
    monkeypatch.setattr(main.webbrowser, "open", lambda url: calls.setdefault("url", url))
    monkeypatch.setattr(main.uvicorn, "run", lambda app, **kwargs: calls.update(kwargs))

    main.run()

    assert calls == {"url": "http://127.0.0.2:9876", "host": "127.0.0.2", "port": 9876}
