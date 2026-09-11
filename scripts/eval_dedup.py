# -*- coding: utf-8 -*-
"""判重评估工具。

加载冻结评估集（records/pairs/clusters.jsonl），离线调用 `DedupEngine`，
计算成对指标、BCubed、严重误合并、事件规模与模型运行指标，并生成独立报告。

用法：
    uv run python scripts/eval_dedup.py --dataset tests/fixtures/dedup_eval \
        --report docs/判重评估报告-基线.md
    uv run python scripts/eval_dedup.py --dataset tests/fixtures/dedup_eval --no-llm
    uv run python scripts/eval_dedup.py --dataset tests/fixtures/dedup_eval \
        --compare runtime/eval/eval_<commit>.json
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import platform
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from complaint_dedup.config import Settings  # noqa: E402
from complaint_dedup.corpus_models import InputRecord  # noqa: E402
from complaint_dedup.dedup_engine import (  # noqa: E402
    PROMPT_VERSION,
    DedupEngine,
    DedupEngineOptions,
)
from complaint_dedup.dedup_features import FEATURE_VERSION  # noqa: E402
from complaint_dedup.full_corpus import _normalize_record  # noqa: E402
from complaint_dedup.llm_client import build_llm_client  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[1]


# ------------------------------- 数据加载 -----------------------------------
def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def build_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for index, record in enumerate(records):
        raw = {
            "工单编号": record.get("work_order_id"),
            "受理时间": record.get("received_at"),
            "办结时间": record.get("completed_at"),
            "诉求标题": record.get("title"),
            "市民诉求": record.get("appeal_text"),
            "事项分类": record.get("category"),
            "事发地点": record.get("location"),
            "处理部门": record.get("processing_department"),
            "所属部门": record.get("department"),
        }
        payload = _normalize_record(
            InputRecord(
                source_row=index + 2,
                work_order_id=record.get("work_order_id") or None,
                title=record.get("title"),
                category=record.get("category") or None,
                appeal_text=record.get("appeal_text"),
                received_at=record.get("received_at") or None,
                completed_at=record.get("completed_at") or None,
                location=record.get("location") or None,
                processing_department=record.get("processing_department") or None,
                raw_fields={key: value for key, value in raw.items() if value},
            )
        )
        payload.pop("raw_json", None)
        rows.append(payload)
    return rows


# ------------------------------- 评估核心 -----------------------------------
def run_engine(
    rows: list[dict[str, Any]],
    *,
    use_llm: bool,
    model_override: str | None,
    concurrency: int | None,
) -> tuple[Any, Settings, dict[str, Any]]:
    settings = Settings()
    options = DedupEngineOptions.from_settings(settings)
    if model_override:
        options = DedupEngineOptions(
            **{**options.__dict__, "model_id": model_override}
        )
    client = None
    if use_llm and (model_override or settings.llm_model):
        client = build_llm_client(
            settings,
            model=model_override,
            concurrency=concurrency or settings.dedup_max_concurrency,
        )
    result = asyncio.run(DedupEngine(llm_client=client, options=options).cluster(rows))
    return result, settings, options.__dict__


def pair_metrics(
    pairs: list[dict[str, Any]],
    predicted: dict[str, int],
    titles: dict[str, str] | None = None,
) -> dict[str, Any]:
    tp = fp = fn = tn = 0
    severe: list[dict[str, Any]] = []
    false_positives: list[dict[str, Any]] = []
    false_negatives: list[dict[str, Any]] = []
    slices: dict[str, Counter[str]] = defaultdict(Counter)
    titles = titles or {}

    def detail(pair: dict[str, Any]) -> dict[str, Any]:
        left, right = pair["left"], pair["right"]
        return {
            "pair_id": pair["pair_id"],
            "left": left,
            "right": right,
            "left_title": titles.get(left, "")[:60],
            "right_title": titles.get(right, "")[:60],
            "reason_code": pair.get("reason_code"),
            "conflict_fields": pair.get("hard_conflict_fields") or [],
            "left_event": predicted.get(left),
            "right_event": predicted.get(right),
        }

    for pair in pairs:
        left, right = pair["left"], pair["right"]
        if left not in predicted or right not in predicted:
            continue
        gold_same = pair["label"] == "same"
        pred_same = predicted[left] == predicted[right]
        if gold_same and pred_same:
            tp += 1
        elif gold_same and not pred_same:
            fn += 1
            false_negatives.append(detail(pair))
        elif not gold_same and pred_same:
            fp += 1
            item = detail(pair)
            false_positives.append(item)
            if item["conflict_fields"]:
                severe.append(item)
        else:
            tn += 1
        bucket = slices[pair.get("reason_code") or "UNKNOWN"]
        bucket["tp" if gold_same and pred_same else
               "fn" if gold_same else
               "fp" if pred_same else "tn"] += 1
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    slice_rows = []
    for reason, counts in sorted(slices.items(), key=lambda item: -sum(item[1].values())):
        total = sum(counts.values())
        slice_precision = (
            counts["tp"] / (counts["tp"] + counts["fp"]) if counts["tp"] + counts["fp"] else None
        )
        slice_recall = (
            counts["tp"] / (counts["tp"] + counts["fn"]) if counts["tp"] + counts["fn"] else None
        )
        slice_rows.append(
            {
                "reason_code": reason,
                "pairs": total,
                "tp": counts["tp"],
                "fp": counts["fp"],
                "fn": counts["fn"],
                "tn": counts["tn"],
                "precision": slice_precision,
                "recall": slice_recall,
            }
        )
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "severe_false_merges": severe,
        "false_positives": false_positives,
        "false_negatives": false_negatives,
        "slices": slice_rows,
    }


def bcubed_metrics(
    clusters: list[dict[str, Any]], predicted: dict[str, int]
) -> dict[str, Any]:
    gold_of = {
        member: cluster["cluster_id"]
        for cluster in clusters
        for member in cluster["members"]
        if member in predicted
    }
    keys = list(gold_of)
    if not keys:
        return {"items": 0, "precision": None, "recall": None, "f1": None}
    pred_groups: dict[int, set[str]] = defaultdict(set)
    gold_groups: dict[str, set[str]] = defaultdict(set)
    for key in keys:
        pred_groups[predicted[key]].add(key)
        gold_groups[gold_of[key]].add(key)
    precisions, recalls = [], []
    for key in keys:
        pred_members = pred_groups[predicted[key]]
        gold_members = gold_groups[gold_of[key]]
        intersection = len(pred_members & gold_members)
        precisions.append(intersection / len(pred_members))
        recalls.append(intersection / len(gold_members))
    precision = sum(precisions) / len(precisions)
    recall = sum(recalls) / len(recalls)
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "items": len(keys),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }


def event_metrics(result: Any, rows: list[dict[str, Any]]) -> dict[str, Any]:
    sizes = [len(group) for group in result.groups]
    total = sum(sizes)
    largest = max(result.groups, key=len) if result.groups else []
    distinct_fingerprints = len(
        {
            (row.get("feature_json") or {}).get("complaint_fingerprint")
            for row in largest
            if (row.get("feature_json") or {}).get("complaint_fingerprint")
        }
    )
    return {
        "records": total,
        "events": len(sizes),
        "singletons": sum(1 for size in sizes if size == 1),
        "singleton_rate": round(sum(1 for size in sizes if size == 1) / len(sizes), 4)
        if sizes
        else 0.0,
        "max_event_size": max(sizes) if sizes else 0,
        "max_event_distinct_fingerprints": distinct_fingerprints,
        "size_ge_10": sum(1 for size in sizes if size >= 10),
        "size_ge_50": sum(1 for size in sizes if size >= 50),
        "llm_coverage": round(result.llm_coverage, 4),
        "fallback_count": result.fallback_count,
        "decision_count": result.decision_count,
        "request_count": result.request_count,
        "llm_error_count": result.llm_error_count,
        "span_guard_count": result.span_guard_count,
    }


# ------------------------------- 版本信息 -----------------------------------
def _run_git(*args: str) -> str:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
        return completed.stdout.strip() if completed.returncode == 0 else ""
    except (OSError, subprocess.SubprocessError):
        return ""


def _git_dirty_tracked() -> bool:
    # core.quotepath=false 保证中文路径按字面输出，便于过滤生成的报告文件
    output = _run_git(
        "-c", "core.quotepath=false", "status", "--porcelain", "--untracked-files=no"
    )
    lines = [
        line
        for line in output.splitlines()
        if "判重评估报告" not in line
    ]
    return bool(lines)


def collect_versions(settings: Settings, db_path: str | None) -> dict[str, Any]:
    import importlib.metadata as metadata

    def package_version(name: str) -> str:
        try:
            return metadata.version(name)
        except metadata.PackageNotFoundError:
            return "不可核验"

    versions: dict[str, Any] = {
        "code": {
            "commit": _run_git("rev-parse", "HEAD"),
            "branch": _run_git("rev-parse", "--abbrev-ref", "HEAD"),
            "tag": _run_git("tag", "--points-at", "HEAD"),
            # 只统计已跟踪代码修改；生成的评估报告本身不算代码工作区不干净
            "dirty": _git_dirty_tracked(),
        },
        "algorithm": {
            "feature_version": FEATURE_VERSION,
            "prompt_version": PROMPT_VERSION,
        },
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "packages": {
            name: package_version(name)
            for name in ("fastapi", "httpx", "pydantic", "sqlalchemy", "loguru")
        },
        "config": {
            "llm_model": settings.llm_model,
            "llm_base_url_host": str(settings.llm_base_url).split("/")[2],
            "llm_concurrency": settings.llm_concurrency,
            "llm_timeout_seconds": settings.llm_timeout_seconds,
            "llm_json_mode": settings.llm_json_mode,
            "dedup_max_concurrency": settings.dedup_max_concurrency,
            "dedup_max_requests": settings.dedup_max_requests,
            "dedup_max_seconds": settings.dedup_max_seconds,
            "dedup_cards_per_batch": settings.dedup_cards_per_batch,
            "dedup_min_confidence": settings.dedup_min_confidence,
            "dedup_text_duplicate_enabled": settings.dedup_text_duplicate_enabled,
            "dedup_fallback_max_span_days": settings.dedup_fallback_max_span_days,
            "dedup_fallback_span_shadow": settings.dedup_fallback_span_shadow,
        },
        "model_service": {"status": "未查询"},
    }
    try:
        import httpx

        response = httpx.get(
            f"{str(settings.llm_base_url).rstrip('/')}/models",
            headers={"Authorization": f"Bearer {settings.llm_api_key}"}
            if settings.llm_api_key
            else {},
            timeout=30,
        )
        models = (
            sorted(str(item.get("id")) for item in (response.json().get("data") or []))
            if response.status_code == 200
            else []
        )
        versions["model_service"] = {
            "status": response.status_code,
            "models": models,
            "note": "服务未暴露 vLLM 版本/启动参数，相关项不可核验",
        }
    except Exception as exc:  # noqa: BLE001
        versions["model_service"] = {
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
        }
    if db_path:
        path = Path(db_path)
        if path.exists():
            stat = path.stat()
            versions["database"] = {
                "path": str(path),
                "size_bytes": stat.st_size,
                "mtime": stat.st_mtime,
                "sha256_head": _file_sha256(path, limit=8 << 20),
            }
    return versions


def _file_sha256(path: Path, *, limit: int | None = None) -> str:
    digest = hashlib.sha256()
    read = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
            read += len(chunk)
            if limit and read >= limit:
                break
    return digest.hexdigest()


# ------------------------------- 报告输出 -----------------------------------
def build_report(
    *,
    dataset_dir: Path,
    meta: dict[str, Any],
    versions: dict[str, Any],
    metrics: dict[str, Any],
    engine_params: dict[str, Any],
    comparison: dict[str, Any] | None,
) -> str:
    pair = metrics["pair"]
    bcubed = metrics["bcubed"]
    events = metrics["event"]
    lines = [
        "# 判重评估报告",
        "",
        "## 1. 基础信息",
        f"- 评估时间（UTC）：{datetime.now(timezone.utc).isoformat()}",
        f"- 环境：Python {versions['python']} / {versions['platform']}",
        f"- 数据集：`{dataset_dir}`；工单 {meta.get('record_count')} 条，"
        f"标注对 {meta.get('labeled_pair_count')}，复核队列 {meta.get('review_queue_count')}",
        f"- 数据库：{json.dumps(meta.get('database'), ensure_ascii=False)}",
        "",
        "## 2. 模型信息",
        f"- 当前模型：`{versions['config']['llm_model']}`",
        f"- 服务状态：{versions['model_service'].get('status')}；"
        f"实测可用模型数：{len(versions['model_service'].get('models') or [])}；"
        f"当前模型可用：{versions['config']['llm_model'] in (versions['model_service'].get('models') or [])}",
        f"- 服务地址主机：{versions['config']['llm_base_url_host']}；"
        f"并发 {versions['config']['dedup_max_concurrency']}，"
        f"超时 {versions['config']['llm_timeout_seconds']}s，"
        f"JSON 模式 {versions['config']['llm_json_mode']}",
        "- vLLM 版本、启动参数、模型路径：不可核验（服务未提供）",
        "",
        "## 3. 代码版本",
        f"- 分支：{versions['code']['branch']}；提交：`{versions['code']['commit']}`；"
        f"标签：{versions['code']['tag'] or '无'}；工作区未提交：{versions['code']['dirty']}",
        f"- 特征版本：{versions['algorithm']['feature_version']}；"
        f"提示词版本：{versions['algorithm']['prompt_version']}",
        f"- 依赖：{json.dumps(versions['packages'], ensure_ascii=False)}",
        "",
        "## 4. 评估集来源",
        f"- 生成种子：{meta.get('seed')}；标签来源：{meta.get('label_method')}",
        f"- 标签分布：{json.dumps(meta.get('label_counts'), ensure_ascii=False)}；"
        f"划分：{json.dumps(meta.get('split_counts'), ensure_ascii=False)}",
        f"- 分层：{json.dumps(meta.get('strata_counts'), ensure_ascii=False)}",
        f"- 一致性校验：{json.dumps(meta.get('consistency_check'), ensure_ascii=False)}",
        "",
        "## 5. 算法说明",
        f"- 参数：{json.dumps(engine_params, ensure_ascii=False)}",
        "- 流程：硬匹配（基础编号/内容指纹/正文指纹/订单号/历史工单）→ 多路候选召回 → "
        "事件卡 LLM 裁决 → 证据校验与保守回退；普通键按“地点 + 问题族”聚合。",
        "",
        "## 6. 指标结果",
        "| 指标 | 值 |",
        "| --- | --- |",
        f"| Pair 精确率 | {pair['precision']}（TP={pair['tp']}, FP={pair['fp']}） |",
        f"| Pair 召回率 | {pair['recall']}（FN={pair['fn']}） |",
        f"| Pair F1 | {pair['f1']} |",
        f"| 严重误合并 | {len(pair['severe_false_merges'])} |",
        f"| BCubed P/R/F1 | {bcubed['precision']} / {bcubed['recall']} / {bcubed['f1']}"
        f"（样本 {bcubed['items']}） |",
        f"| 事件数/单例率 | {events['events']} / {events['singleton_rate']} |",
        f"| 最大事件规模 | {events['max_event_size']}"
        f"（去重内容指纹 {events.get('max_event_distinct_fingerprints', 0)}） |",
        f"| LLM 覆盖率 | {events['llm_coverage']} |",
        f"| 回退数/决策数 | {events['fallback_count']} / {events['decision_count']} |",
        f"| 模型请求/失败 | {events['request_count']} / {events['llm_error_count']} |",
        f"| 时间护栏命中（shadow） | {events['span_guard_count']} |",
        "",
        "### 指标原因分析",
        _metric_analysis(pair, events),
        "",
        "## 7. 分类切片",
        "| 原因码 | 对数 | TP | FP | FN | TN | 精确率 | 召回率 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in pair["slices"]:
        lines.append(
            f"| {row['reason_code']} | {row['pairs']} | {row['tp']} | {row['fp']} | "
            f"{row['fn']} | {row['tn']} | {row['precision']} | {row['recall']} |"
        )
    lines += ["", "## 8. 与上一次评估对比"]
    if comparison:
        lines.append(_comparison_table(comparison, metrics))
    else:
        lines.append("无历史评估可对比；本次结果作为后续回归的基线。")
    conflict_counter: Counter[str] = Counter()
    for item in pair["false_positives"]:
        for field in item["conflict_fields"] or ["<none>"]:
            conflict_counter[field] += 1
    lines += [
        "",
        "## 9. Bad case 分类统计",
        f"- 误合并（FP）：{len(pair['false_positives'])} 条；"
        f"漏合并（FN）：{len(pair['false_negatives'])} 条；"
        f"其中严重误合并：{len(pair['severe_false_merges'])} 条",
        "",
        "### 9.1 误合并冲突字段分布",
        "| 冲突字段 | 条数 |",
        "| --- | --- |",
    ]
    for field, count in conflict_counter.most_common():
        lines.append(f"| {field} | {count} |")
    lines += ["", "### 9.2 误合并明细（最多 25 条）"]
    for item in pair["false_positives"][:25]:
        lines.append(
            f"- `{item['pair_id']}` `{item['left']}` vs `{item['right']}` "
            f"（冲突：{item['conflict_fields'] or '无'}；原因码：{item['reason_code']}）"
        )
        lines.append(f"  - 左：{item['left_title']}")
        lines.append(f"  - 右：{item['right_title']}")
    lines += ["", "### 9.3 漏合并明细（最多 25 条）"]
    for item in pair["false_negatives"][:25]:
        lines.append(
            f"- `{item['pair_id']}` `{item['left']}` vs `{item['right']}` "
            f"（原因码：{item['reason_code']}）"
        )
        lines.append(f"  - 左：{item['left_title']}")
        lines.append(f"  - 右：{item['right_title']}")
    lines += [
        "",
        "## 10. 结论与下一步",
        "- 本报告由评估工具自动生成，案例编号均来自冻结数据集，可用记录定位到数据库。",
        "- 下一轮优化按误合并/漏合并原因码排序，逐项补回归测试并重跑本评估对比。",
        "",
    ]
    return "\n".join(lines) + "\n"


def _metric_analysis(pair: dict[str, Any], events: dict[str, Any]) -> str:
    parts = [
        "标注说明：same 标签主要来自确定性身份规则（与引擎硬合并同源），"
        "因此召回率会被高估，解释时应以精确率和误合并明细为准。"
    ]
    if pair["recall"] and pair["recall"] < 0.9:
        parts.append("召回率偏低，重点检查分类/地点归一与候选召回。")
    if pair["precision"] and pair["precision"] < 0.95:
        parts.append("精确率偏低，检查回退合并与 LLM 证据校验。")
    if pair["false_positives"]:
        fields: Counter[str] = Counter()
        for item in pair["false_positives"]:
            for field in item["conflict_fields"] or ["<none>"]:
                fields[field] += 1
        parts.append(
            "误合并冲突字段分布："
            + "、".join(f"{name}={count}" for name, count in fields.most_common())
        )
    parts.append(
        f"LLM 覆盖率 {events['llm_coverage']}：仅灰区组件进入模型，"
        "规则可确定的部分不产生请求，属预期。"
    )
    if events["llm_error_count"]:
        parts.append(f"模型失败 {events['llm_error_count']} 次，关注限流与超时。")
    if events["fallback_count"]:
        parts.append(f"回退决策 {events['fallback_count']} 条，评估回退质量。")
    parts.append(
        "标注外的灰区样本已进入复核队列（review_queue），需人工/AI 复核后再纳入精确率统计；"
        "最大事件规模仍受 HBD 派生与联名投诉影响，需结合成员内容抽检。"
    )
    return "；".join(parts)


def _comparison_table(previous: dict[str, Any], current: dict[str, Any]) -> str:
    rows = [
        ("Pair 精确率", previous["pair"]["precision"], current["pair"]["precision"]),
        ("Pair 召回率", previous["pair"]["recall"], current["pair"]["recall"]),
        ("Pair F1", previous["pair"]["f1"], current["pair"]["f1"]),
        (
            "严重误合并",
            len(previous["pair"]["severe_false_merges"]),
            len(current["pair"]["severe_false_merges"]),
        ),
        ("BCubed 精确率", previous["bcubed"]["precision"], current["bcubed"]["precision"]),
        ("BCubed 召回率", previous["bcubed"]["recall"], current["bcubed"]["recall"]),
        (
            "最大事件规模",
            previous["event"]["max_event_size"],
            current["event"]["max_event_size"],
        ),
        ("事件数", previous["event"]["events"], current["event"]["events"]),
        (
            "单例率",
            previous["event"]["singleton_rate"],
            current["event"]["singleton_rate"],
        ),
    ]
    lines = ["| 指标 | 上次 | 本次 | 变化 |", "| --- | --- | --- | --- |"]
    for name, before, after in rows:
        try:
            delta = round(float(after) - float(before), 4)
        except (TypeError, ValueError):
            delta = "-"
        lines.append(f"| {name} | {before} | {after} | {delta} |")
    return "\n".join(lines)


# ---------------------------------- 入口 ------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="判重评估工具")
    parser.add_argument("--dataset", default="tests/fixtures/dedup_eval")
    parser.add_argument("--db", default="", help="可选：记录数据库指纹")
    parser.add_argument("--report", default="", help="Markdown 报告输出路径")
    parser.add_argument("--json-out", default="", help="指标 JSON 输出路径")
    parser.add_argument("--compare", default="", help="上一次评估的 JSON 指标")
    parser.add_argument("--no-llm", action="store_true", help="只用规则，不调用模型")
    parser.add_argument("--model", default="", help="覆盖模型名称")
    parser.add_argument("--concurrency", type=int, default=0, help="模型并发")
    args = parser.parse_args()

    dataset_dir = Path(args.dataset)
    records = load_jsonl(dataset_dir / "records.jsonl")
    pairs = load_jsonl(dataset_dir / "pairs.jsonl")
    clusters = load_jsonl(dataset_dir / "clusters.jsonl")
    meta_path = dataset_dir / "meta.json"
    if not meta_path.exists():
        meta_path = dataset_dir.parent / "meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}

    rows = build_rows(records)
    result, settings, engine_params = run_engine(
        rows,
        use_llm=not args.no_llm,
        model_override=args.model or None,
        concurrency=args.concurrency or None,
    )
    predicted = {
        str(row["record_key"]): index
        for index, group in enumerate(result.groups)
        for row in group
    }
    titles = {record["record_key"]: record["title"] for record in records}
    metrics = {
        "pair": pair_metrics(pairs, predicted, titles),
        "bcubed": bcubed_metrics(clusters, predicted),
        "event": event_metrics(result, rows),
    }
    versions = collect_versions(settings, args.db or meta.get("database", {}).get("path"))
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    commit = (versions["code"]["commit"] or "nocommit")[:8]
    json_out = Path(args.json_out) if args.json_out else Path("runtime/eval") / f"eval_{commit}_{timestamp}.json"
    json_out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset": str(dataset_dir),
        "versions": versions,
        "engine_params": engine_params,
        "metrics": metrics,
    }
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    comparison = None
    if args.compare:
        loaded = json.loads(Path(args.compare).read_text(encoding="utf-8"))
        comparison = loaded.get("metrics", loaded)
    report = build_report(
        dataset_dir=dataset_dir,
        meta=meta,
        versions=versions,
        metrics=metrics,
        engine_params=engine_params,
        comparison=comparison,
    )
    report_path = Path(args.report) if args.report else Path("runtime/eval") / f"report_{commit}_{timestamp}.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report, encoding="utf-8")

    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    print(f"\n指标 JSON: {json_out}")
    print(f"评估报告: {report_path}")


if __name__ == "__main__":
    main()
