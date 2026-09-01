import importlib
import sys
from pathlib import Path


def test_history_rebuild_script_is_importable():
    repository_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repository_root))
    module = importlib.import_module("scripts.rebuild_history")
    assert callable(module.cli)
