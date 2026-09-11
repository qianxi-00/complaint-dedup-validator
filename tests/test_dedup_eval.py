# -*- coding: utf-8 -*-
"""评估集回归门禁。

在冻结评估集上以纯规则模式运行判重引擎，校验基线指标不退化，
并对已知 bad case 的执行结果做断言。缺少冻结集时自动跳过。
"""
from __future__ import annotations

import importlib.util
from functools import lru_cache
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "eval_dedup_script", ROOT / "scripts" / "eval_dedup.py"
)
eval_dedup = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(eval_dedup)  # type: ignore[union-attr]

DATASET = ROOT / "tests" / "fixtures" / "dedup_eval"

pytestmark = pytest.mark.skipif(
    not (DATASET / "records.jsonl").exists(),
    reason="冻结评估集不存在，请先运行 scripts/build_eval_set.py",
)


@lru_cache(maxsize=1)
def _run_ruleonly():
    records = eval_dedup.load_jsonl(DATASET / "records.jsonl")
    pairs = eval_dedup.load_jsonl(DATASET / "pairs.jsonl")
    rows = eval_dedup.build_rows(records)
    result, _, _ = eval_dedup.run_engine(
        rows, use_llm=False, model_override=None, concurrency=None
    )
    predicted = {
        str(row["record_key"]): index
        for index, group in enumerate(result.groups)
        for row in group
    }
    titles = {record["record_key"]: record["title"] for record in records}
    metrics = {
        "pair": eval_dedup.pair_metrics(pairs, predicted, titles),
        "event": eval_dedup.event_metrics(result, rows),
    }
    return records, metrics, predicted


def test_ruleonly_metrics_do_not_regress() -> None:
    # 冻结下限来自 v3.0 复核后基线（规则模式 P=0.860/R=0.953），留 2-3 个点余量
    _, metrics, _ = _run_ruleonly()

    assert metrics["pair"]["precision"] >= 0.83
    assert metrics["pair"]["recall"] >= 0.93
    assert metrics["pair"]["severe_false_merges"] == []


def test_known_enterprise_wage_pair_is_merged() -> None:
    _, _, predicted = _run_ruleonly()
    left, right = "wo:0826020513110775301", "wo:0826031014471635201"

    assert left in predicted and right in predicted
    assert predicted[left] == predicted[right]


def test_known_content_duplicate_pair_is_merged() -> None:
    _, _, predicted = _run_ruleonly()
    left, right = "wo:0826021109574389201", "wo:0826031814480782501"
    if left not in predicted or right not in predicted:
        pytest.skip("该案例不在当前冻结集中")

    assert predicted[left] == predicted[right]


def test_frozen_dataset_uses_real_ids() -> None:
    records, _, _ = _run_ruleonly()

    assert len(records) >= 1000
    assert all(record["work_order_id"] for record in records)
    assert (DATASET / "CHECKSUMS.txt").exists()
