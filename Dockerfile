FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.9.18 /uv /uvx /bin/

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /build

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

RUN groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app

WORKDIR /app
USER app

EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8765/', timeout=5)"]

CMD ["uvicorn", "complaint_dedup.main:app", "--host", "0.0.0.0", "--port", "8765"]
