$ErrorActionPreference = "Stop"
uv sync
uv run python -m complaint_dedup.main
