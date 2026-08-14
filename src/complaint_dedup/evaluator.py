from collections.abc import Sequence
import json
from pathlib import Path
from typing import Any

import pandas as pd

from complaint_dedup.llm_models import JudgedPair, JudgementBatchResponse
from complaint_dedup.prompts import build_judgement_messages


async def evaluate_explicit_pairs(
    llm_client: Any,
    pairs: Sequence[dict],
    *,
    batch_size: int = 20,
) -> list[JudgedPair]:
    """Send exactly the requested pairs to the judge, bypassing candidate recall."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    results: list[JudgedPair] = []
    for start in range(0, len(pairs), batch_size):
        batch = list(pairs[start : start + batch_size])
        response: JudgementBatchResponse = await llm_client.chat_json(
            build_judgement_messages(batch), JudgementBatchResponse
        )
        expected = {str(item["pair_id"]) for item in batch}
        returned = {item.pair_id for item in response.pairs}
        missing = expected - returned
        if missing:
            raise ValueError(f"模型未返回指定候选对: {', '.join(sorted(missing))}")
        results.extend(response.pairs)
    return results


PAIR_COLUMNS = {
    "pair_id": ("pair_id", "配对编号", "候选对编号"),
    "a_work_order_id": ("A工单编号", "a_work_order_id"),
    "a_title": ("A标题", "a_title"),
    "a_content": ("A内容", "A市民诉求", "a_content", "a_appeal_text"),
    "b_work_order_id": ("B工单编号", "b_work_order_id"),
    "b_title": ("B标题", "b_title"),
    "b_content": ("B内容", "B市民诉求", "b_content", "b_appeal_text"),
    "reference_label": ("参考标签", "人工标签", "reference_label"),
}


def load_explicit_pairs(path: str | Path) -> list[dict]:
    input_path = Path(path)
    if input_path.suffix.lower() == ".csv":
        frame = pd.read_csv(input_path, dtype=object)
    elif input_path.suffix.lower() in {".xlsx", ".xls"}:
        engine = "openpyxl" if input_path.suffix.lower() == ".xlsx" else "xlrd"
        frame = pd.read_excel(input_path, dtype=object, engine=engine)
    else:
        raise ValueError("评测文件仅支持 xlsx、xls、csv")
    frame = frame.where(pd.notna(frame), None)
    columns = {str(column).strip(): column for column in frame.columns}

    def find(key: str) -> str | None:
        return next((columns[name] for name in PAIR_COLUMNS[key] if name in columns), None)

    required = ("a_title", "a_content", "b_title", "b_content")
    missing = [key for key in required if find(key) is None]
    if missing:
        raise ValueError(f"缺少评测字段: {', '.join(missing)}")
    rows: list[dict] = []
    for index, row in frame.iterrows():
        def value(key: str):
            column = find(key)
            return row.get(column) if column is not None else None

        rows.append(
            {
                "pair_id": str(value("pair_id") or f"P{index + 1:04d}"),
                "a": {
                    "work_order_id": value("a_work_order_id"),
                    "title": value("a_title"),
                    "appeal_text": value("a_content"),
                },
                "b": {
                    "work_order_id": value("b_work_order_id"),
                    "title": value("b_title"),
                    "appeal_text": value("b_content"),
                },
                "reference_label": value("reference_label"),
            }
        )
    return rows


def export_evaluation(
    source_pairs: Sequence[dict],
    results: Sequence[JudgedPair],
    output_path: str | Path,
) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    source_by_id = {str(item["pair_id"]): item for item in source_pairs}
    rows = []
    for result in results:
        source = source_by_id[result.pair_id]
        row = {
            "pair_id": result.pair_id,
            "参考标签": source.get("reference_label"),
            "模型判定": result.decision,
            "置信度": result.confidence,
            "主体关系": result.subject_relation,
            "地址关系": result.address_relation,
            "问题关系": result.issue_relation,
            "新增独立问题": result.new_independent_issue,
            "硬冲突": "；".join(result.hard_conflicts),
            "A证据": "；".join(result.evidence_a),
            "B证据": "；".join(result.evidence_b),
            "判断理由": result.reason,
            "事件名称": result.event_name,
            "A工单编号": source["a"].get("work_order_id"),
            "A标题": source["a"].get("title"),
            "A内容": source["a"].get("appeal_text"),
            "B工单编号": source["b"].get("work_order_id"),
            "B标题": source["b"].get("title"),
            "B内容": source["b"].get("appeal_text"),
            "决策矩阵": json.dumps(result.matrix.model_dump(), ensure_ascii=False),
        }
        rows.append(row)
    details = pd.DataFrame(rows)
    labeled = details[details["参考标签"].notna()] if not details.empty else details
    correct = (
        int((labeled["参考标签"].astype(str) == labeled["模型判定"]).sum())
        if not labeled.empty
        else 0
    )
    summary = pd.DataFrame(
        [
            {"指标": "评测对数", "值": len(details)},
            {"指标": "有参考标签", "值": len(labeled)},
            {"指标": "标签一致数", "值": correct},
            {"指标": "标签一致率", "值": correct / len(labeled) if len(labeled) else None},
            {"指标": "模型判重复", "值": int((details["模型判定"] == "duplicate").sum())},
            {"指标": "模型判不重复", "值": int((details["模型判定"] == "not_duplicate").sum())},
            {"指标": "模型判复核", "值": int((details["模型判定"] == "review").sum())},
        ]
    )
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="评测总览", index=False)
        details.to_excel(writer, sheet_name="逐对结果", index=False)
    return output
