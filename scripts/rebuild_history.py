from __future__ import annotations

import asyncio

from complaint_dedup.history_rebuild import run as cli


if __name__ == "__main__":
    raise SystemExit(asyncio.run(cli()))