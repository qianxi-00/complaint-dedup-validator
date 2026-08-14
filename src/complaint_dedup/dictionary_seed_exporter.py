from __future__ import annotations

import asyncio
from collections import Counter
from pathlib import Path
from typing import Any

import xlsxwriter

from complaint_dedup.corpus_repository import CorpusRepository


async def export_dictionary_seed(
    repository: CorpusRepository, version_id: int, output_path: str | Path
) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    dimensions: dict[str, list[dict[str, Any]]] = {}
    for dimension in ("street", "anchor", "issue"):
        rows, _ = await repository.list_dictionary_items(
            version_id, dimension=dimension, limit=100_000, offset=0
        )
        dimensions[dimension] = rows
    runs = await repository.list_dictionary_extraction_runs(version_id)
    await asyncio.to_thread(_write_workbook, output, dimensions, runs)
    return output


def _write_workbook(
    output: Path,
    dimensions: dict[str, list[dict[str, Any]]],
    runs: list[dict[str, Any]],
) -> None:
    workbook = xlsxwriter.Workbook(output)
    header = workbook.add_format(
        {"bold": True, "bg_color": "#DCE6F1", "border": 1, "align": "center"}
    )
    cell = workbook.add_format({"border": 1, "valign": "top"})
    risk = workbook.add_format(
        {"border": 1, "valign": "top", "bg_color": "#FFF2CC", "font_color": "#7F6000"}
    )
    anchor_names = Counter(row["canonical_name"] for row in dimensions["anchor"])

    street_headers = ["ID", "地区", "标准街道", "证据数", "别名数", "审核状态", "风险标记"]
    street_rows = [
        [
            row["id"],
            row.get("region"),
            row["canonical_name"],
            row["evidence_count"],
            row["alias_count"],
            row["review_status"],
            "低频候选" if int(row["evidence_count"]) < 2 else "",
        ]
        for row in dimensions["street"]
    ]
    _write_sheet(workbook, "标准街道", street_headers, street_rows, header, cell, risk)

    anchor_headers = [
        "ID",
        "街道ID",
        "标准锚点",
        "锚点类型",
        "道路",
        "门牌",
        "楼栋",
        "方位",
        "证据数",
        "别名数",
        "审核状态",
        "风险标记",
    ]
    anchor_rows = []
    for row in dimensions["anchor"]:
        flags = []
        if anchor_names[row["canonical_name"]] > 1:
            flags.append("同名多地点")
        if int(row["evidence_count"]) < 2:
            flags.append("低频候选")
        anchor_rows.append(
            [
                row["id"],
                row.get("street_id"),
                row["canonical_name"],
                row.get("anchor_type"),
                row.get("road"),
                row.get("house_no"),
                row.get("building"),
                row.get("direction"),
                row["evidence_count"],
                row["alias_count"],
                row["review_status"],
                "、".join(flags),
            ]
        )
    _write_sheet(workbook, "标准锚点", anchor_headers, anchor_rows, header, cell, risk)

    issue_headers = [
        "ID",
        "标准问题",
        "事项一级",
        "事项二级",
        "事项三级",
        "事项四级",
        "证据数",
        "别名数",
        "审核状态",
        "风险标记",
    ]
    issue_rows = [
        [
            row["id"],
            row["canonical_name"],
            row.get("category_level_1"),
            row.get("category_level_2"),
            row.get("category_level_3"),
            row.get("category_level_4"),
            row["evidence_count"],
            row["alias_count"],
            row["review_status"],
            "低频候选" if int(row["evidence_count"]) < 2 else "",
        ]
        for row in dimensions["issue"]
    ]
    _write_sheet(workbook, "标准问题", issue_headers, issue_rows, header, cell, risk)

    queue_headers = ["维度", "ID", "标准名称", "证据数", "审核状态", "风险标记"]
    queue_rows = []
    for dimension, rows in dimensions.items():
        for row in rows:
            if row["review_status"] not in {"candidate", "uncertain"}:
                continue
            flags = []
            if int(row["evidence_count"]) < 2:
                flags.append("低频候选")
            if dimension == "anchor" and anchor_names[row["canonical_name"]] > 1:
                flags.append("同名多地点")
            queue_rows.append(
                [
                    {"street": "街道", "anchor": "锚点", "issue": "问题"}[dimension],
                    row["id"],
                    row["canonical_name"],
                    row["evidence_count"],
                    row["review_status"],
                    "、".join(flags),
                ]
            )
    queue_rows.sort(key=lambda row: (not bool(row[5]), -int(row[3]), row[0], row[2]))
    _write_sheet(workbook, "审核队列", queue_headers, queue_rows, header, cell, risk)

    stats_headers = ["抽取运行", "文件哈希", "规则版本", "状态", "候选统计"]
    stats_rows = [
        [
            row["id"],
            row["source_file_hash"],
            row["parser_version"],
            row["status"],
            str(row.get("candidate_counts") or {}),
        ]
        for row in runs
    ]
    _write_sheet(workbook, "抽取统计", stats_headers, stats_rows, header, cell, risk)
    workbook.close()


def _write_sheet(
    workbook: xlsxwriter.Workbook,
    name: str,
    headers: list[str],
    rows: list[list[Any]],
    header_format: Any,
    cell_format: Any,
    risk_format: Any,
) -> None:
    sheet = workbook.add_worksheet(name)
    sheet.freeze_panes(1, 0)
    sheet.autofilter(0, 0, max(len(rows), 1), len(headers) - 1)
    for column, value in enumerate(headers):
        sheet.write(0, column, value, header_format)
    risk_column = headers.index("风险标记") if "风险标记" in headers else -1
    for row_index, values in enumerate(rows, start=1):
        for column, value in enumerate(values):
            fmt = risk_format if column == risk_column and value else cell_format
            sheet.write(row_index, column, value, fmt)
    for column, value in enumerate(headers):
        width = 14
        if "名称" in value or "锚点" in value or "统计" in value:
            width = 28
        elif "哈希" in value:
            width = 68
        sheet.set_column(column, column, width)
