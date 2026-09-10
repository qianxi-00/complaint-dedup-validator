# -*- coding: utf-8 -*-
"""评估集构建脚本。

从本地 SQLite 全量库按分层抽样生成判重评估集：
- records.jsonl：脱敏后的真实工单（保留工单编号，姓名/手机号/身份证掩码）；
- pairs.jsonl：标注对（same/different/uncertain），标注来源与原因码可追溯；
- clusters.jsonl：由确定同事件边推导的黄金事件集合；
- review_queue.jsonl：需要人工/AI 复核的困难对；
- meta.json / summary.md：数据源、规模、比例、分层统计与一致性校验结果。

用法：
    uv run python scripts/build_eval_set.py --db runtime/local-test-20260910.db \
        --out runtime/eval/eval_set --frozen tests/fixtures/dedup_eval \
        --seed 20260910 --target-records 1200
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from complaint_dedup.corpus_models import InputRecord  # noqa: E402
from complaint_dedup.full_corpus import _normalize_record  # noqa: E402

# ----------------------------- 脱敏（保留案件标识） -----------------------------
_PHONE_RE = re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")
_ID_CARD_RE = re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)")
_NAME_RE = re.compile(
    r"(?:姓名|市民|投诉人|联系人|机主)\s*[:：]?\s*[\u4e00-\u9fff]{2,4}"
)


def mask_personal_info(text: str | None) -> str:
    """掩码姓名/手机号/身份证；保留订单号、工单号、地址等案件标识。"""
    value = str(text or "")
    value = _PHONE_RE.sub("[手机号]", value)
    value = _ID_CARD_RE.sub("[身份证号]", value)
    value = _NAME_RE.sub("[姓名]", value)
    return value


def norm_text(value: str | None) -> str:
    return re.sub(r"[\W_]+", "", str(value or "").casefold())


def normalize_for_compare(value: str | None) -> str:
    """一致性校验用归一：折叠空白与换行，避免仅空白差异被判为内容不一致。"""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _text_grams(value: str | None, size: int = 3) -> set[str]:
    text = norm_text(value)[:800]
    return {text[i : i + size] for i in range(max(0, len(text) - size + 1))}


def text_similarity(left: str | None, right: str | None) -> float:
    left_grams, right_grams = _text_grams(left), _text_grams(right)
    if not left_grams or not right_grams:
        return 0.0
    return len(left_grams & right_grams) / len(left_grams | right_grams)


def stable_hash(*values: str) -> str:
    return hashlib.sha256("\x1f".join(values).encode("utf-8")).hexdigest()


# ------------------------------- 数据访问层 ---------------------------------
WORK_ORDER_FIELDS = (
    "record_key, work_order_id, canonical_work_order_id, complaint_fingerprint, "
    "received_at, completed_at, title_raw, appeal_text, category, "
    "processing_department, department, location, region, street, anchor, "
    "organization_subject, issue_family, event_key, feature_json, feature_version, "
    "raw_json, missing_in_latest_upload"
)


def load_work_orders(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in conn.execute(f"SELECT {WORK_ORDER_FIELDS} FROM work_orders"):
        item = dict(zip([c.strip() for c in WORK_ORDER_FIELDS.split(",")], row))
        rows[item["record_key"]] = item
    return rows


def load_features(rows: dict[str, dict[str, Any]]) -> None:
    for row in rows.values():
        try:
            row["features"] = json.loads(row.get("feature_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            row["features"] = {}


def recompute_features(
    rows: dict[str, dict[str, Any]], keys: Iterable[str]
) -> None:
    """用当前代码重算抽样记录的特征，避免沿用旧版 feature_json 影响标注。"""
    for key in keys:
        row = rows.get(key)
        if row is None:
            continue
        raw_fields = row.get("raw_json") or {}
        if isinstance(raw_fields, str):
            try:
                raw_fields = json.loads(raw_fields)
            except json.JSONDecodeError:
                raw_fields = {}
        payload = _normalize_record(
            InputRecord(
                source_row=int(row.get("source_row") or 0),
                work_order_id=row.get("work_order_id"),
                title=row.get("title_raw"),
                category=row.get("category"),
                appeal_text=row.get("appeal_text"),
                received_at=row.get("received_at"),
                completed_at=row.get("completed_at"),
                location=row.get("location"),
                processing_department=row.get("processing_department"),
                raw_fields=raw_fields,
            )
        )
        row["features"] = payload["feature_json"]
        row["canonical_work_order_id"] = payload["canonical_work_order_id"]
        row["complaint_fingerprint"] = payload["complaint_fingerprint"]
        row["issue_family"] = payload["issue_family"]


def load_latest_comparison(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT id, algorithm_version, feature_version, prompt_version, model_id, "
        "target_count, reference_count, event_count, singleton_count, "
        "llm_coverage, fallback_count, decision_count, created_at "
        "FROM comparison_runs ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    keys = (
        "id, algorithm_version, feature_version, prompt_version, model_id, "
        "target_count, reference_count, event_count, singleton_count, "
        "llm_coverage, fallback_count, decision_count, created_at"
    ).split(", ")
    return dict(zip(keys, row))


def load_events(conn: sqlite3.Connection, comparison_id: str) -> list[dict[str, Any]]:
    events = []
    for event_id, event_name, event_key in conn.execute(
        "SELECT id, event_name, event_key FROM comparison_events "
        "WHERE comparison_id=? ORDER BY id",
        (comparison_id,),
    ):
        members = [
            key
            for (key,) in conn.execute(
                "SELECT record_key FROM comparison_event_members WHERE event_id=?",
                (event_id,),
            )
        ]
        events.append({"id": event_id, "name": event_name, "members": members})
    return events


def load_record_decisions(conn: sqlite3.Connection, comparison_id: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for decision, record_keys in conn.execute(
        "SELECT decision, record_keys FROM comparison_decisions WHERE comparison_id=?",
        (comparison_id,),
    ):
        try:
            keys = json.loads(record_keys) if record_keys else []
        except json.JSONDecodeError:
            keys = []
        for key in keys:
            result.setdefault(key, decision)
    return result


# ------------------------------- 分层抽样 -----------------------------------
def _add(selected: dict[str, set[str]], key: str, tag: str) -> None:
    if key:
        selected.setdefault(key, set()).add(tag)


def sample_records(
    rows: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
    record_decisions: dict[str, str],
    rng: random.Random,
    target_records: int,
) -> dict[str, set[str]]:
    selected: dict[str, set[str]] = {}
    valid = set(rows)

    def add_members(members: Iterable[str], tag: str, limit: int) -> int:
        count = 0
        for key in members:
            if key in valid:
                _add(selected, key, tag)
                count += 1
            if count >= limit:
                break
        return count

    # L1 HBD 同源组
    canonical_groups: dict[str, list[str]] = defaultdict(list)
    for key, row in rows.items():
        cid = row.get("canonical_work_order_id")
        if cid:
            canonical_groups[cid].append(key)
    hbd_groups = [
        group
        for group in canonical_groups.values()
        if len(group) >= 2
        and any("HBD" in str(rows[key].get("work_order_id") or "").upper() for key in group)
    ]
    rng.shuffle(hbd_groups)
    for group in hbd_groups:
        add_members(group, "HBD", target_records // 8)
        if len(selected) >= target_records // 8:
            break

    # L2/L3/L4 事件层
    hard_events, fallback_events, large_events = [], [], []
    for event in events:
        decisions = Counter(record_decisions.get(key, "-") for key in event["members"])
        size = len(event["members"])
        if size >= 2 and decisions.get("hard_merge"):
            hard_events.append(event)
        elif size > 10:
            large_events.append(event)
        elif 2 <= size <= 10 and decisions.get("rule_fallback"):
            fallback_events.append(event)
    rng.shuffle(hard_events)
    rng.shuffle(fallback_events)
    rng.shuffle(large_events)
    for event in hard_events:
        add_members(event["members"], "hard_event", 200)
        if len([1 for tag in selected.values() if "hard_event" in tag]) >= 120:
            break
    for event in fallback_events:
        add_members(event["members"], "fallback_event", 240)
        if len([1 for tag in selected.values() if "fallback_event" in tag]) >= 160:
            break
    for event in large_events[:8]:
        add_members(event["members"], "large_event", 200)

    # L5 漏合并疑点：同标题跨事件 / 同订单跨事件
    title_bucket: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for event in events:
        for key in event["members"]:
            if key not in valid:
                continue
            title = norm_text(rows[key].get("title_raw"))
            if len(title) >= 8:
                title_bucket[title][str(event["id"])].add(key)
    miss_keys: list[str] = []
    for buckets in title_bucket.values():
        if len(buckets) < 2:
            continue
        keys = [key for group in buckets.values() for key in group]
        miss_keys.extend(keys[:6])
    rng.shuffle(miss_keys)
    for key in miss_keys:
        _add(selected, key, "missed_merge")
        if len([1 for tag in selected.values() if "missed_merge" in tag]) >= 200:
            break

    # L6 未知地点
    unknown = [
        key
        for key, row in rows.items()
        if row.get("anchor") == "未知地点" and row.get("title_raw")
    ]
    rng.shuffle(unknown)
    for key in unknown:
        _add(selected, key, "unknown_anchor")
        if len([1 for tag in selected.values() if "unknown_anchor" in tag]) >= 150:
            break

    # L7 随机层：按月份/街道/问题族分层
    buckets: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for key, row in rows.items():
        month = str(row.get("received_at") or "")[:7]
        street = str(row.get("street") or "")
        family = str((row.get("features") or {}).get("problem_family") or "")
        buckets[(month, street, family)].append(key)
    bucket_keys = sorted(buckets)
    rng.shuffle(bucket_keys)
    while bucket_keys and len([1 for tag in selected.values() if "random" in tag]) < 200:
        bucket = buckets[bucket_keys.pop()]
        for key in rng.sample(bucket, min(2, len(bucket))):
            _add(selected, key, "random")
    return selected


# ------------------------------- 标注对生成 ---------------------------------
def _explicit_address(row: dict[str, Any]) -> str:
    return str((row.get("features") or {}).get("explicit_address") or "")


def _house_number(row: dict[str, Any]) -> str:
    match = re.search(r"(\d+)(?:号|號|幢|栋|栋|座|室|房)", _explicit_address(row))
    return match.group(1) if match else ""


def _strong_subjects(row: dict[str, Any]) -> list[str]:
    return list((row.get("features") or {}).get("strong_subjects") or [])


def _subject_key(value: str) -> str:
    """主体别名归一（评估标注用）：电器=电气、去掉分厂/分公司等后缀。"""
    text = norm_text(value)
    for old, new in (
        ("电器", "电气"),
        ("小家电", "家电"),
        ("分厂", ""),
        ("分公司", ""),
        ("分店", ""),
    ):
        text = text.replace(old, new)
    return text


# 业务口径中“同一企业=同一事件”仅适用于三类企业问题族
ENTERPRISE_FAMILIES = {"欠薪", "食品安全", "产品质量"}


def _subjects_alias_match(left: list[str], right: list[str]) -> bool:
    left_keys = [key for key in map(_subject_key, left) if key]
    right_keys = [key for key in map(_subject_key, right) if key]
    for left_key in left_keys:
        for right_key in right_keys:
            if left_key == right_key:
                return True
            if min(len(left_key), len(right_key)) >= 3 and (
                left_key in right_key or right_key in left_key
            ):
                return True
    return False


def classify_pair(
    left: dict[str, Any], right: dict[str, Any]
) -> tuple[str, str, list[str]]:
    """对两条工单给出规则标注（label, reason_code, hard_conflict_fields）。"""
    lf, rf = left.get("features") or {}, right.get("features") or {}
    conflicts: list[str] = []

    if left.get("canonical_work_order_id") and (
        left["canonical_work_order_id"] == right.get("canonical_work_order_id")
    ):
        return "same", "HBD_OR_SAME_BASE_ORDER", conflicts
    if left.get("complaint_fingerprint") and (
        left["complaint_fingerprint"] == right.get("complaint_fingerprint")
    ):
        return "same", "SAME_CONTENT_FINGERPRINT", conflicts
    if lf.get("appeal_fingerprint") and lf["appeal_fingerprint"] == rf.get(
        "appeal_fingerprint"
    ):
        return "same", "SAME_APPEAL_FINGERPRINT", conflicts
    if set(lf.get("occurrence_ids") or []) & set(rf.get("occurrence_ids") or []):
        return "same", "SAME_OCCURRENCE_ID", conflicts

    left_subjects, right_subjects = _strong_subjects(left), _strong_subjects(right)
    left_family, right_family = lf.get("problem_family"), rf.get("problem_family")
    left_issue, right_issue = left.get("issue_family"), right.get("issue_family")
    if (
        left_subjects
        and right_subjects
        and left_issue
        and left_issue == right_issue
        and _subjects_alias_match(left_subjects, right_subjects)
    ):
        # 业务口径：同一企业 + 同一企业问题族 = 同一事件（跨月/跨网点不拆）
        return "same", "SAME_ENTERPRISE_FAMILY", conflicts

    left_house, right_house = _house_number(left), _house_number(right)
    if left_house and right_house and left_house != right_house:
        conflicts.append("address")
    left_orders = set(lf.get("occurrence_ids") or [])
    right_orders = set(rf.get("occurrence_ids") or [])
    if left_orders and right_orders and not (left_orders & right_orders):
        conflicts.append("order")
    if left_subjects and right_subjects and not _subjects_alias_match(
        left_subjects, right_subjects
    ):
        conflicts.append("subject")
    left_street = norm_text(left.get("street"))
    right_street = norm_text(right.get("street"))
    if left_street and right_street and left_street != right_street:
        conflicts.append("street")
    unknown_anchor = norm_text("未知地点")
    left_anchor = norm_text(left.get("anchor"))
    right_anchor = norm_text(right.get("anchor"))
    if left_anchor == unknown_anchor:
        left_anchor = ""
    if right_anchor == unknown_anchor:
        right_anchor = ""
    if (
        left_anchor
        and right_anchor
        and left_anchor != right_anchor
        and left_anchor not in right_anchor
        and right_anchor not in left_anchor
    ):
        conflicts.append("anchor")
    if conflicts:
        return "different", "FIELD_CONFLICT", conflicts

    left_title = norm_text(left.get("title_raw"))
    if (
        left_title
        and left_title == norm_text(right.get("title_raw"))
        and len(left_title) >= 10
        and text_similarity(left.get("appeal_text"), right.get("appeal_text")) >= 0.85
    ):
        # 标题相同且正文高度相似、无硬冲突：视为同一事件（分类口径差异不应拆分）
        return "same", "SAME_TITLE_TEXT", conflicts

    if (
        lf.get("problem_family")
        and rf.get("problem_family")
        and lf["problem_family"] != rf["problem_family"]
    ):
        return "different", "DIFFERENT_PROBLEM_FAMILY", conflicts
    return "uncertain", "INSUFFICIENT_EVIDENCE", conflicts


def build_pairs(
    selected: dict[str, set[str]],
    rows: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
    rng: random.Random,
) -> list[dict[str, Any]]:
    allowed = set(selected)
    pairs: dict[tuple[str, str], dict[str, Any]] = {}

    def add_pair(
        left: str,
        right: str,
        *,
        reason_override: str | None = None,
        label_override: str | None = None,
        context: str = "",
    ) -> None:
        if left == right or left not in allowed or right not in allowed:
            return
        key = tuple(sorted((left, right)))
        if key in pairs:
            return
        label, reason, conflicts = classify_pair(rows[left], rows[right])
        if label_override:
            label = label_override
        if reason_override:
            reason = reason_override
        if (
            context == "missed_merge"
            and label == "different"
            and set(conflicts) <= {"street", "anchor"}
        ):
            # 同标题跨事件：地点/街道解析差异不足以直接否定，转人工复核
            label, reason = "uncertain", "ANCHOR_OR_STREET_VARIANCE"
        pairs[key] = {
            "pair_id": "P" + stable_hash(*key)[:10].upper(),
            "left": key[0],
            "right": key[1],
            "label": label,
            "reason_code": reason,
            "label_source": "rule_auto",
            "hard_conflict_fields": conflicts,
            "notes": "",
        }

    # 1) 确定同事件：canonical / 指纹 / 正文指纹 / 订单号
    for field, reason in (
        ("canonical_work_order_id", "HBD_OR_SAME_BASE_ORDER"),
        ("complaint_fingerprint", "SAME_CONTENT_FINGERPRINT"),
        ("appeal_fingerprint", "SAME_APPEAL_FINGERPRINT"),
    ):
        bucket: dict[str, list[str]] = defaultdict(list)
        for key in allowed:
            value = (rows[key].get("features") or {}).get(field) or rows[key].get(field)
            if value:
                bucket[value].append(key)
        for keys in bucket.values():
            for left, right in combinations(sorted(keys)[:8], 2):
                add_pair(left, right, reason_override=reason, label_override="same")
    order_bucket: dict[str, list[str]] = defaultdict(list)
    for key in allowed:
        for occurrence in (rows[key].get("features") or {}).get("occurrence_ids") or []:
            order_bucket[occurrence].append(key)
    for keys in order_bucket.values():
        for left, right in combinations(sorted(set(keys))[:8], 2):
            add_pair(left, right, reason_override="SAME_OCCURRENCE_ID", label_override="same")

    # 2) 事件内对：确定合、字段冲突、或灰区
    for event in events:
        members = [key for key in event["members"] if key in allowed]
        if len(members) < 2:
            continue
        if len(members) > 8:
            members = rng.sample(members, 8)
        for left, right in combinations(sorted(members), 2):
            add_pair(left, right)

    # 3) 漏合并疑点：同标题跨事件且正文/订单支持
    by_title: dict[str, list[tuple[str, str]]] = defaultdict(list)
    event_of = {
        key: str(event["id"]) for event in events for key in event["members"]
    }
    for key in allowed:
        title = norm_text(rows[key].get("title_raw"))
        if len(title) >= 8:
            by_title[title].append((key, event_of.get(key, "")))
    for entries in by_title.values():
        event_ids = {entry[1] for entry in entries}
        if len(event_ids) < 2:
            continue
        for (left, left_event), (right, right_event) in combinations(entries[:6], 2):
            if left_event == right_event:
                continue
            add_pair(left, right, context="missed_merge")

    # 4) 随机跨事件对（补充负例）
    keys = sorted(allowed)
    for _ in range(240):
        left, right = rng.sample(keys, 2)
        add_pair(left, right)
    return list(pairs.values())


def assign_split(pair_id: str) -> str:
    bucket = int(hashlib.sha1(pair_id.encode()).hexdigest(), 16) % 100
    if bucket < 40:
        return "train"
    if bucket < 60:
        return "dev"
    return "test"


def derive_clusters(
    pairs: list[dict[str, Any]], records: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    """由确定 same 边推导黄金事件（连通分量）。"""
    parent: dict[str, str] = {}

    def find(value: str) -> str:
        parent.setdefault(value, value)
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        root_left, root_right = find(left), find(right)
        if root_left != root_right:
            parent[root_right] = root_left

    for pair in pairs:
        if pair["label"] == "same":
            union(pair["left"], pair["right"])
    groups: dict[str, list[str]] = defaultdict(list)
    for key in list(parent):
        groups[find(key)].append(key)
    clusters = []
    for members in groups.values():
        if len(members) < 2:
            continue
        members = sorted(members)
        clusters.append(
            {
                "cluster_id": "C" + stable_hash(*members)[:10].upper(),
                "members": members,
                "label": "same_event",
                "basis": "deterministic_same_pairs",
            }
        )
    return clusters


# ------------------------------- 输出与校验 ---------------------------------
def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def consistency_check(
    source_xlsx: Path | None, records: list[dict[str, Any]], limit: int = 30
) -> dict[str, Any]:
    if source_xlsx is None:
        return {"status": "skipped", "reason": "未提供 --source-xlsx"}
    if not source_xlsx.exists():
        return {"status": "skipped", "reason": f"文件不存在: {source_xlsx}"}
    try:
        import pandas as pd

        frame = pd.read_excel(source_xlsx, dtype=object)
    except Exception as exc:  # noqa: BLE001
        return {"status": "failed", "reason": f"{type(exc).__name__}: {exc}"}
    frame.columns = [str(column).strip() for column in frame.columns]
    id_column = next(
        (column for column in frame.columns if column in {"工单编号", "受理编号", "编号"}),
        None,
    )
    if id_column is None:
        return {"status": "failed", "reason": "源文件缺少工单编号列"}
    by_id = {str(value).strip(): row for value, row in zip(frame[id_column], frame.to_dict("records"))}
    checked = matched = whitespace_only = 0
    mismatches = []
    step = max(1, len(records) // limit)
    sample = records[::step][:limit]
    for record in sample:
        source = by_id.get(str(record["work_order_id"]))
        checked += 1
        if source is None:
            mismatches.append({"work_order_id": record["work_order_id"], "reason": "源文件未找到"})
            continue
        title = mask_personal_info(source.get("诉求标题"))
        appeal = mask_personal_info(source.get("市民诉求"))
        exact = title == record["title"] and appeal == record["appeal_text"]
        normalized = (
            normalize_for_compare(title) == normalize_for_compare(record["title"])
            and normalize_for_compare(appeal) == normalize_for_compare(record["appeal_text"])
        )
        if not normalized:
            mismatches.append({"work_order_id": record["work_order_id"], "reason": "标题或诉求内容不一致"})
        elif not exact:
            whitespace_only += 1
            matched += 1
        else:
            matched += 1
    return {
        "status": "ok",
        "checked": checked,
        "matched": matched,
        "whitespace_only_differences": whitespace_only,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:10],
        "method": "按值比较（掩码后先精确、再折叠空白）；仅空白差异单独计数",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="构建判重评估集")
    parser.add_argument("--db", required=True, help="SQLite 数据库路径")
    parser.add_argument("--out", default="runtime/eval/eval_set", help="原始输出目录（不提交）")
    parser.add_argument("--frozen", default="tests/fixtures/dedup_eval", help="冻结数据集目录（可提交）")
    parser.add_argument("--seed", type=int, default=20260910, help="随机种子")
    parser.add_argument("--target-records", type=int, default=1200, help="目标工单条数")
    parser.add_argument("--source-xlsx", default="", help="原始全量 Excel，用于逐值一致性校验")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"数据库不存在: {db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    rows = load_work_orders(conn)
    load_features(rows)
    comparison = load_latest_comparison(conn)
    if comparison is None:
        raise SystemExit("数据库中没有 comparison_runs 记录")
    events = load_events(conn, comparison["id"])
    record_decisions = load_record_decisions(conn, comparison["id"])
    conn.close()

    rng = random.Random(args.seed)
    selected = sample_records(
        rows, events, record_decisions, rng, args.target_records
    )
    if len(selected) < 400:
        raise SystemExit(f"抽样记录过少: {len(selected)}")
    recompute_features(rows, selected)
    pairs = build_pairs(selected, rows, events, rng)
    for pair in pairs:
        pair["split"] = assign_split(pair["pair_id"])
    clusters = derive_clusters(pairs, rows)

    records = []
    for key in sorted(selected):
        row = rows[key]
        records.append(
            {
                "record_key": key,
                "work_order_id": row.get("work_order_id") or "",
                "canonical_work_order_id": row.get("canonical_work_order_id") or "",
                "title": mask_personal_info(row.get("title_raw")),
                "appeal_text": mask_personal_info(row.get("appeal_text")),
                "location": row.get("location") or "",
                "category": row.get("category") or "",
                "received_at": str(row.get("received_at") or ""),
                "completed_at": str(row.get("completed_at") or ""),
                "processing_department": row.get("processing_department") or "",
                "department": row.get("department") or "",
                "strata": sorted(selected[key]),
                "problem_family": (row.get("features") or {}).get("problem_family"),
                "event_id": next(
                    (
                        event["id"]
                        for event in events
                        if key in event["members"]
                    ),
                    None,
                ),
            }
        )
    record_map = {record["record_key"]: record for record in records}

    label_counts = Counter(pair["label"] for pair in pairs)
    review_queue = [pair for pair in pairs if pair["label"] == "uncertain"]
    labeled_pairs = [pair for pair in pairs if pair["label"] in {"same", "different"}]
    out_dir = Path(args.out)
    frozen_dir = Path(args.frozen)
    write_jsonl(out_dir / "records.jsonl", records)
    write_jsonl(out_dir / "pairs.jsonl", pairs)
    write_jsonl(out_dir / "clusters.jsonl", clusters)
    write_jsonl(out_dir / "review_queue.jsonl", review_queue)
    write_jsonl(frozen_dir / "records.jsonl", records)
    write_jsonl(frozen_dir / "pairs.jsonl", labeled_pairs)
    write_jsonl(frozen_dir / "clusters.jsonl", clusters)

    consistency = consistency_check(
        Path(args.source_xlsx) if args.source_xlsx else None, records
    )
    db_stat = db_path.stat()
    meta = {
        "generated_at": __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ).isoformat(),
        "seed": args.seed,
        "database": {
            "path": str(db_path),
            "size_bytes": db_stat.st_size,
            "mtime": db_stat.st_mtime,
            "sha256": _file_sha256(db_path),
        },
        "comparison": comparison,
        "target_records": args.target_records,
        "record_count": len(records),
        "strata_counts": dict(Counter(tag for record in records for tag in record["strata"])),
        "pair_count": len(pairs),
        "labeled_pair_count": len(labeled_pairs),
        "review_queue_count": len(review_queue),
        "label_counts": dict(label_counts),
        "split_counts": dict(Counter(pair["split"] for pair in pairs)),
        "cluster_count": len(clusters),
        "consistency_check": consistency,
        "label_method": "rule_auto + AI 初标；困难集进入 review_queue 待人工复核",
    }
    (out_dir / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (frozen_dir / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (frozen_dir / "README.md").write_text(_frozen_readme(meta), encoding="utf-8")
    checksums = []
    for name in ("records.jsonl", "pairs.jsonl", "clusters.jsonl"):
        digest = _file_sha256(frozen_dir / name)
        checksums.append(f"{digest}  {name}")
    (frozen_dir / "CHECKSUMS.txt").write_text("\n".join(checksums) + "\n", encoding="utf-8")
    (out_dir / "summary.md").write_text(_summary(meta, records, label_counts), encoding="utf-8")

    print(json.dumps(meta, ensure_ascii=False, indent=2))
    print(f"\n原始输出: {out_dir}")
    print(f"冻结数据集: {frozen_dir}")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _summary(meta: dict[str, Any], records: list[dict[str, Any]], labels: Counter) -> str:
    lines = [
        "# 判重评估集构建摘要",
        "",
        f"- 数据库：`{meta['database']['path']}`（{meta['database']['size_bytes']} bytes）",
        f"- 比对任务：`{meta['comparison']['id']}`",
        f"- 评估工单：{meta['record_count']} 条",
        f"- 标注对：{meta['pair_count']} 对（same={labels.get('same',0)}，"
        f"different={labels.get('different',0)}，uncertain={labels.get('uncertain',0)}）",
        f"- 冻结标注对：{meta['labeled_pair_count']}（uncertain 进入复核队列）",
        f"- 黄金事件簇：{meta['cluster_count']}",
        f"- 分层统计：{json.dumps(meta['strata_counts'], ensure_ascii=False)}",
        f"- 一致性校验：{json.dumps(meta['consistency_check'], ensure_ascii=False)}",
        "",
        "> 标注方式：强规则自动标注 + AI 初标；不确定样本进入 review_queue 待人工复核。",
    ]
    return "\n".join(lines) + "\n"


def _frozen_readme(meta: dict[str, Any]) -> str:
    return (
        "# 判重评估冻结集\n\n"
        "由 `scripts/build_eval_set.py` 从本地全量库分层抽样生成；文本已掩码姓名/手机号/身份证，"
        "保留工单编号与订单号等案件标识。\n\n"
        f"- 生成时间：{meta['generated_at']}\n"
        f"- 工单数：{meta['record_count']}，标注对：{meta['labeled_pair_count']}，"
        f"黄金事件簇：{meta['cluster_count']}\n"
        f"- 标签分布：{json.dumps(meta['label_counts'], ensure_ascii=False)}\n"
        f"- 划分：{json.dumps(meta['split_counts'], ensure_ascii=False)}\n\n"
        "字段说明：\n"
        "- `records.jsonl`：record_key/work_order_id/title/appeal_text/category/received_at/strata 等；\n"
        "- `pairs.jsonl`：left/right/label(same|different)/reason_code/label_source/split；\n"
        "- `clusters.jsonl`：黄金事件成员。\n\n"
        "运行评估：`uv run python scripts/eval_dedup.py --dataset tests/fixtures/dedup_eval`\n"
    )


if __name__ == "__main__":
    main()
