# -*- coding: utf-8 -*-
"""打包部署源码:排除本地产物,文本文件统一 LF。"""
import io
import os
import sys
import tarfile

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = r"C:\Users\QianXi\AppData\Local\Temp\opencode\deploy-account-system.tar.gz"

EXCLUDE_DIRS = {".git", ".venv", "runtime", "output", "build", ".pytest_cache",
                ".playwright-cli", ".superpowers", "__pycache__", "node_modules"}
EXCLUDE_FILES = {".env", "tmp_return.db", "realtest.db"}
EXCLUDE_SUFFIX = {".pyc", ".db", ".db-wal", ".db-shm", ".log", ".tar.gz"}
TEXT_SUFFIX = {".py", ".md", ".toml", ".yaml", ".yml", ".html", ".css", ".js",
               ".ini", ".cfg", ".txt", ".sh", ".ps1", ".mako", ".example", ""}


def is_text(name: str) -> bool:
    return os.path.splitext(name)[1].lower() in TEXT_SUFFIX or name.startswith(".")

with tarfile.open(OUT, "w:gz") as tar:
    for root, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for f in files:
            if f in EXCLUDE_FILES or os.path.splitext(f)[1].lower() in EXCLUDE_SUFFIX:
                continue
            full = os.path.join(root, f)
            rel = os.path.relpath(full, ROOT).replace("\\", "/")
            arcname = "app/" + rel
            if is_text(f):
                with open(full, "rb") as fh:
                    data = fh.read()
                try:
                    text_data = data.decode("utf-8")
                    data = text_data.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")
                except UnicodeDecodeError:
                    pass
                info = tarfile.TarInfo(arcname)
                info.size = len(data)
                import time as _time
                info.mtime = int(_time.time())
                tar.addfile(info, io.BytesIO(data))
            else:
                tar.add(full, arcname=arcname)
print("打包完成:", OUT, f"{os.path.getsize(OUT)/1e6:.1f} MB")
