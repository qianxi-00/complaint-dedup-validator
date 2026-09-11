# -*- coding: utf-8 -*-
"""评估集复核脚本：规则优先 + LLM 批量复核，并把接受的标签写回冻结集。

流程：
1. 读取 review_queue.jsonl（构建评估集时无法自动判定的困难对）；
2. 先用确定性规则判定明确同/不同，其余批量交给模型复核；
3. 接受规则结论与高置信模型结论（label_source=rule_review/ai_reviewed），
   低置信或模型失败保持 uncertain；
4. --apply 时更新冻结集 pairs/clusters/meta/CHECKSUMS，供评估工具使用。

用法：
    uv run python scripts/review_eval_queue.py --apply
    uv run python scripts/review_eval_queue.py --no-llm --limit 50   # 只看规则部分
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from complaint_dedup.config import Settings  # noqa: E402
from complaint_dedup.corpus_models import InputRecord  # noqa: E402
from complaint_dedup.full_corpus import _normalize_record  # noqa: E402
from complaint_dedup.llm_client import build_llm_client  # noqa: E402
from complaint_dedup.llm_models import PairReviewBatchResponse  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REVIEW_PROMPT = """你是投诉工单判重复核员，判断每对工单是否属于同一具体投诉事件或同一处置链。
判定规则（按优先级）：
1. 同一基础工单号、同一标题+诉求指纹、同一订单号 → same。
2. 同一企业 + 同一企业问题族（欠薪/食品安全/产品质量），即使员工不同、月份不同 → same。
3. 同一地点 + 同一具体问题（例如都是"物业费质价不符"、同一条道路积水）→ same。
4. 同一地点但不同具体问题（漏水 vs 物业费 vs 门禁 vs 噪音）→ different。
5. 不同订单号、不同门牌楼栋、不同消费者/车辆/商品对象、不同具体事故 → different。
6. 仅地区/街道/事项大类相同，或证据不足 → uncertain。
只输出 JSON：
{"decisions":[{"pair_id":"...","label":"same|different|uncertain","confidence":0.0,
"reason_code":"简短英文原因码","reason":"中文依据，引用输入中的事实"}]}"""

UNKNOWN_ANCHORS = {"未知地点", "未知街道", "未知", "不详", "无", "其他", "其它"}


def norm_text(value: str | None) -> str:
    return re.sub(r"[\W_]+", "", str(value or "").casefold())


def text_grams(value: str | None, size: int = 3) -> set[str]:
    text = norm_text(value)[:800]
    return {text[i : i + size] for i in range(max(0, len(text) - size + 1))}


def similarity(left: str | None, right: str | None) -> float:
    left_grams, right_grams = text_grams(left), text_grams(right)
    if not left_grams or not right_grams:
        return 0.0
    return len(left_grams & right_grams) / len(left_grams | right_grams)


def _subject_key(value: str | None) -> str:
    text = norm_text(value)
    for old, new in (("电器", "电气"), ("小家电", "家电"), ("分厂", ""), ("分公司", "")):
        text = text.replace(old, new)
    return text


def _subjects_match(left: list[str], right: list[str]) -> bool:
    for a in (key for key in map(_subject_key, left) if key):
        for b in (key for key in map(_subject_key, right) if key):
            if a == b or (min(len(a), len(b)) >= 3 and (a in b or b in a)):
                return True
    return False


def _house_number(record: dict[str, Any]) -> str:
    match = re.search(r"(\d+)(?:号|號|幢|栋|座|室|房)", str(record.get("location") or ""))
    return match.group(1) if match else ""


def _known(value: str | None) -> str:
    text = str(value or "").strip()
    return "" if text in UNKNOWN_ANCHORS else text


def rule_review(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any] | None:
    """确定性复核；无法判定返回 None，交给模型。"""
    left_key, right_key = left["record_key"], right["record_key"]
    if left.get("canonical_work_order_id") and (
        left["canonical_work_order_id"] == right.get("canonical_work_order_id")
    ):
        return _decision(left_key, right_key, "same", "HARD_IDENTITY", 1.0, "基础工单号相同")
    if left.get("complaint_fingerprint") and (
        left["complaint_fingerprint"] == right.get("complaint_fingerprint")
    ):
        return _decision(left_key, right_key, "same", "CONTENT_FINGERPRINT", 1.0, "标题+诉求指纹相同")
    if left.get("appeal_fingerprint") and (
        left["appeal_fingerprint"] == right.get("appeal_fingerprint")
    ):
        return _decision(left_key, right_key, "same", "APPEAL_FINGERPRINT", 1.0, "诉求正文指纹相同")

    conflicts: list[str] = []
    left_house, right_house = _house_number(left), _house_number(right)
    if left_house and right_house and left_house != right_house:
        conflicts.append("address")
    left_orders = set(left.get("occurrence_ids") or [])
    right_orders = set(right.get("occurrence_ids") or [])
    if left_orders and right_orders and not (left_orders & right_orders):
        conflicts.append("order")
    left_subjects = left.get("strong_subjects") or left.get("subjects") or []
    right_subjects = right.get("strong_subjects") or right.get("subjects") or []
    if left_subjects and right_subjects and not _subjects_match(left_subjects, right_subjects):
        conflicts.append("subject")
    left_street, right_street = _known(left.get("street")), _known(right.get("street"))
    if left_street and right_street and left_street != right_street:
        conflicts.append("street")
    left_anchor, right_anchor = _known(left.get("anchor")), _known(right.get("anchor"))
    if (
        left_anchor
        and right_anchor
        and left_anchor != right_anchor
        and left_anchor not in right_anchor
        and right_anchor not in left_anchor
    ):
        conflicts.append("anchor")
    if conflicts:
        return _decision(
            left_key, right_key, "different", "FIELD_CONFLICT", 0.95, f"硬冲突：{conflicts}"
        )

    sim = similarity(
        f"{left.get('title')} {left.get('appeal_text')}",
        f"{right.get('title')} {right.get('appeal_text')}",
    )
    if norm_text(left.get("title")) == norm_text(right.get("title")) and sim >= 0.85:
        return _decision(left_key, right_key, "same", "SAME_TITLE_TEXT", 0.9, "标题相同且正文高度相似")
    if sim >= 0.95:
        return _decision(left_key, right_key, "same", "HIGH_TEXT_SIMILARITY", 0.9, "正文高度相似且无冲突")
    return None


def _decision(
    left: str,
    right: str,
    label: str,
    reason_code: str,
    confidence: float,
    reason: str,
) -> dict[str, Any]:
    return {
        "pair_id": "P" + hashlib.sha256("\x1f".join(sorted((left, right))).encode()).hexdigest()[:10].upper(),
        "left": left,
        "right": right,
        "label": label,
        "reason_code": reason_code,
        "confidence": confidence,
        "reason": reason,
    }


def _pair_id(left: str, right: str) -> str:
    return "P" + hashlib.sha256("\x1f".join(sorted((left, right))).encode()).hexdigest()[:10].upper()


def pair_payload(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    def brief(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "work_order_id": record.get("work_order_id"),
            "title": record.get("title"),
            "appeal": str(record.get("appeal_text") or "")[:260],
            "category": record.get("category"),
            "street": record.get("street"),
            "anchor": record.get("anchor"),
            "problem_family": record.get("problem_family"),
            "received_at": record.get("received_at"),
        }

    return {
        "pair_id": _pair_id(left["record_key"], right["record_key"]),
        "left": brief(left),
        "right": brief(right),
    }


async def llm_review(
    payloads: list[dict[str, Any]],
    *,
    settings: Settings,
    model: str | None,
    concurrency: int,
    batch_size: int,
) -> tuple[dict[str, dict[str, Any]], Counter]:
    client = build_llm_client(settings, model=model, concurrency=concurrency)
    semaphore = asyncio.Semaphore(concurrency)
    results: dict[str, dict[str, Any]] = {}
    stats: Counter = Counter()

    async def run_batch(batch: list[dict[str, Any]]) -> None:
        async with semaphore:
            try:
                response = await client.chat_json(
                    [
                        {"role": "system", "content": REVIEW_PROMPT},
                        {
                            "role": "user",
                            "content": "请复核以下工单对，只返回 JSON。\n"
                            + json.dumps(batch, ensure_ascii=False),
                        },
                    ],
                    PairReviewBatchResponse,
                )
            except Exception:  # noqa: BLE001
                stats["llm_failed"] += len(batch)
                return
        for decision in response.decisions:
            results[decision.pair_id] = {
                "label": decision.label,
                "confidence": decision.confidence,
                "reason_code": decision.reason_code or "AI_REVIEW",
                "reason": decision.reason,
            }

    batches = [payloads[i : i + batch_size] for i in range(0, len(payloads), batch_size)]
    await asyncio.gather(*(run_batch(batch) for batch in batches))
    await client.aclose()
    return results, stats


_COMPANY_FAMILY_RE = re.compile(
    r"company|enterprise|entity|ent_|family", re.IGNORECASE
)


def enrich_records(records: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """为复核记录重算特征，用于业务护栏判断（企业问题族、订单号、门牌）。"""
    for key, record in records.items():
        if "issue_family" in record and "feature_json" in record:
            continue
        payload = _normalize_record(
            InputRecord(
                source_row=0,
                work_order_id=record.get("work_order_id"),
                title=record.get("title"),
                category=record.get("category"),
                appeal_text=record.get("appeal_text"),
                received_at=record.get("received_at") or None,
                completed_at=record.get("completed_at") or None,
                location=record.get("location") or None,
                raw_fields={},
            )
        )
        record["issue_family"] = payload["issue_family"]
        record["feature_json"] = payload["feature_json"]
    return records


def refine_review_results(
    results: list[dict[str, Any]], records: dict[str, dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[str]]:
    """业务护栏：把“同企业+问题族”误用到消费纠纷/产品的结论改判为不同事件。

    规则：reason 含 company/family 且判 same 的对，若不属于引擎三类企业问题族
    （wage/food_safety/product_quality），则要求订单号相交或正文相似度≥0.75，
    否则视为不同事件（不同消费者/商品/事故）。
    """
    changed: list[str] = []
    for item in results:
        if item.get("label") != "same":
            continue
        if not _COMPANY_FAMILY_RE.search(str(item.get("reason_code") or "")):
            continue
        left_record = records.get(item["left"], {})
        right_record = records.get(item["right"], {})
        left_issue = left_record.get("issue_family")
        right_issue = right_record.get("issue_family")
        if left_issue and left_issue == right_issue:
            continue
        left_features = left_record.get("feature_json") or {}
        right_features = right_record.get("feature_json") or {}
        left_orders = set(left_features.get("occurrence_ids") or [])
        right_orders = set(right_features.get("occurrence_ids") or [])
        sim = similarity(
            f"{left_record.get('title')} {left_record.get('appeal_text')}",
            f"{right_record.get('title')} {right_record.get('appeal_text')}",
        )
        if left_orders & right_orders or sim >= 0.75:
            continue
        item["label"] = "different"
        item["reason_code"] = "REFINE_NOT_ENTERPRISE_FAMILY"
        item["notes"] = (
            "业务护栏：非三类企业问题族，且无同订单/高文本相似，按不同消费者/问题拆分"
        )
        changed.append(item["pair_id"])
    return results, changed


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.open(encoding="utf-8") if line.strip()]


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def assign_split(pair_id: str) -> str:
    bucket = int(hashlib.sha1(pair_id.encode()).hexdigest(), 16) % 100
    return "train" if bucket < 40 else "dev" if bucket < 60 else "test"


def derive_clusters(pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    parent: dict[str, str] = {}

    def find(value: str) -> str:
        parent.setdefault(value, value)
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    for pair in pairs:
        if pair["label"] != "same":
            continue
        root_left, root_right = find(pair["left"]), find(pair["right"])
        if root_left != root_right:
            parent[root_right] = root_left
    groups: dict[str, list[str]] = defaultdict(list)
    for key in list(parent):
        groups[find(key)].append(key)
    return [
        {
            "cluster_id": "C" + hashlib.sha256("\x1f".join(sorted(members)).encode()).hexdigest()[:10].upper(),
            "members": sorted(members),
            "label": "same_event",
            "basis": "reviewed_same_pairs",
        }
        for members in groups.values()
        if len(members) > 1
    ]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="评估集复核脚本")
    parser.add_argument("--dataset", default="tests/fixtures/dedup_eval")
    parser.add_argument("--queue", default="runtime/eval/eval_set/review_queue.jsonl")
    parser.add_argument("--results", default="runtime/eval/review_results.jsonl")
    parser.add_argument("--no-llm", action="store_true", help="只跑规则复核")
    parser.add_argument("--apply", action="store_true", help="把接受的标签写回冻结集")
    parser.add_argument("--model", default="")
    parser.add_argument("--concurrency", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--min-confidence", type=float, default=0.8)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--refine", default="", help="对已有复核结果做业务护栏修订并输出")
    parser.add_argument("--from-results", default="", help="直接使用已有复核结果，不再调用规则/模型")
    args = parser.parse_args()

    dataset_dir = Path(args.dataset)
    records = {row["record_key"]: row for row in load_jsonl(dataset_dir / "records.jsonl")}
    queue_path = Path(args.queue)
    if not queue_path.exists():
        raise SystemExit(f"复核队列不存在: {queue_path}")
    queue = load_jsonl(queue_path)
    if args.limit:
        queue = queue[: args.limit]
    if not queue:
        raise SystemExit("复核队列为空")

    if args.refine:
        results = load_jsonl(Path(args.refine))
        enrich_records(records)
        refined, changed = refine_review_results(results, records)
        write_jsonl(Path(args.results), refined)
        print(
            json.dumps(
                {"refined": len(refined), "changed": len(changed), "changed_ids": changed},
                ensure_ascii=False,
                indent=1,
            )
        )
        return

    settings = Settings()
    if args.from_results:
        loaded = load_jsonl(Path(args.from_results))
        decisions: dict[str, dict[str, Any]] = {
            item["pair_id"]: item
            for item in loaded
            if item.get("label") in {"same", "different"}
        }
        stats: Counter = Counter(total=len(queue), from_results=len(loaded))
        stats["accepted"] = len(decisions)
        stats["accepted_same"] = sum(
            1 for item in decisions.values() if item["label"] == "same"
        )
        stats["accepted_different"] = sum(
            1 for item in decisions.values() if item["label"] == "different"
        )
        stats["remaining_uncertain"] = stats["total"] - stats["accepted"]
    else:
        decisions = {}
        pending: list[dict[str, Any]] = []
        for pair in queue:
            left = records.get(pair["left"])
            right = records.get(pair["right"])
            if left is None or right is None:
                continue
            result = rule_review(left, right)
            if result is not None:
                result["label_source"] = "rule_review"
                decisions[result["pair_id"]] = result
            else:
                payload = pair_payload(left, right)
                payload["_path"] = pair
                pending.append(payload)

        stats = Counter(
            total=len(queue), rule_decided=len(decisions), pending_llm=len(pending)
        )
        llm_stats: Counter = Counter()
        if pending and not args.no_llm:
            payloads = [{k: v for k, v in item.items() if k != "_path"} for item in pending]
            llm_results, llm_stats = asyncio.run(
                llm_review(
                    payloads,
                    settings=settings,
                    model=args.model or None,
                    concurrency=args.concurrency or settings.dedup_max_concurrency,
                    batch_size=args.batch_size,
                )
            )
            path_by_id = {item["pair_id"]: item["_path"] for item in pending}
            for pair_id, result in llm_results.items():
                path = path_by_id.get(pair_id)
                if (
                    path is None
                    or result["confidence"] < args.min_confidence
                    or result["label"] == "uncertain"
                ):
                    continue
                decisions[pair_id] = {
                    "pair_id": pair_id,
                    "left": path["left"],
                    "right": path["right"],
                    "label": result["label"],
                    "reason_code": result["reason_code"],
                    "confidence": result["confidence"],
                    "reason": result["reason"],
                    "label_source": "ai_reviewed",
                }
        stats["accepted"] = len(decisions)
        stats["accepted_same"] = sum(
            1 for item in decisions.values() if item["label"] == "same"
        )
        stats["accepted_different"] = sum(
            1 for item in decisions.values() if item["label"] == "different"
        )
        stats["remaining_uncertain"] = stats["total"] - stats["accepted"]
        stats.update(llm_stats)

    results_path = Path(args.results)
    write_jsonl(
        results_path,
        sorted(decisions.values(), key=lambda item: item["pair_id"]),
    )
    print(json.dumps(stats, ensure_ascii=False, indent=1))

    if not args.apply:
        print(f"复核结果已写入 {results_path}（未写回冻结集，使用 --apply 生效）")
        return

    pairs_path = dataset_dir / "pairs.jsonl"
    pairs = load_jsonl(pairs_path)
    existing = {pair["pair_id"] for pair in pairs}
    added = 0
    for item in decisions.values():
        if item["pair_id"] in existing:
            continue
        pairs.append(
            {
                "pair_id": item["pair_id"],
                "left": item["left"],
                "right": item["right"],
                "label": item["label"],
                "reason_code": item["reason_code"],
                "label_source": item["label_source"],
                "hard_conflict_fields": [],
                "notes": item.get("reason", "")[:200],
                "split": assign_split(item["pair_id"]),
            }
        )
        added += 1
    write_jsonl(pairs_path, pairs)
    clusters = derive_clusters(pairs)
    write_jsonl(dataset_dir / "clusters.jsonl", clusters)

    meta_path = dataset_dir / "meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    meta["label_counts"] = {
        "same": sum(1 for pair in pairs if pair["label"] == "same"),
        "different": sum(1 for pair in pairs if pair["label"] == "different"),
        "uncertain": stats["remaining_uncertain"],
    }
    meta["labeled_pair_count"] = len(pairs)
    meta["split_counts"] = dict(Counter(pair.get("split", assign_split(pair["pair_id"])) for pair in pairs))
    meta["cluster_count"] = len(clusters)
    meta["review"] = {
        "reviewed_at": datetime.now(timezone.utc).isoformat(),
        "model": args.model or settings.llm_model,
        "method": "rule_review + ai_reviewed（min_confidence=%s）" % args.min_confidence,
        "accepted_same": stats["accepted_same"],
        "accepted_different": stats["accepted_different"],
        "remaining_uncertain": stats["remaining_uncertain"],
        "llm_failed": stats.get("llm_failed", 0),
        "added_pairs": added,
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    checksums = []
    for name in ("records.jsonl", "pairs.jsonl", "clusters.jsonl"):
        checksums.append(f"{file_sha256(dataset_dir / name)}  {name}")
    (dataset_dir / "CHECKSUMS.txt").write_text("\n".join(checksums) + "\n", encoding="utf-8")
    print(f"已写回冻结集：新增 {added} 对，总计 {len(pairs)} 对，事件簇 {len(clusters)}")


if __name__ == "__main__":
    main()
