from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import xlsxwriter

from complaint_dedup.full_corpus import EventFilters


async def export_comparison_workbook(
    service,
    comparison_id: str,
    output: str | Path,
    *,
    filters: EventFilters | None = None,
) -> Path:
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    rows = await service.export_rows(comparison_id, filters=filters)
    columns = await service.sync_business_columns(comparison_id)
    if not columns:
        columns = [
            "工单编号",
            "受理时间",
            "办结时间",
            "诉求标题",
            "事项分类",
            "所属部门",
            "处理部门",
            "事发地点",
            "市民诉求",
        ]
    await asyncio.to_thread(_write, output, rows, columns)
    return output


def _write(output: Path, rows: list[dict[str, Any]], business_columns: list[str]) -> None:
    workbook = xlsxwriter.Workbook(
        output,
        {
            "constant_memory": True,
            "strings_to_formulas": False,
            "strings_to_urls": False,
            "tmpdir": str(output.parent),
        },
    )
    header = workbook.add_format(
        {
            "bold": True,
            "font_color": "#FFFFFF",
            "bg_color": "#1F4E78",
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True,
            "border": 1,
            "border_color": "#D9E0E8",
        }
    )
    fills = (
        workbook.add_format({"bg_color": "#EAF2FB", "valign": "top", "text_wrap": True}),
        workbook.add_format({"bg_color": "#FFF8E7", "valign": "top", "text_wrap": True}),
    )
    event_counts: dict[int, int] = {}
    for row in rows:
        event_id = int(row["event_id"])
        event_counts[event_id] = event_counts.get(event_id, 0) + 1
    headers = ["数据侧", "事件名称", *business_columns]
    for sheet_name, data in (
        ("重复项", [row for row in rows if event_counts[int(row["event_id"])] > 1]),
        ("孤立工单", [row for row in rows if event_counts[int(row["event_id"])] == 1]),
    ):
        sheet = workbook.add_worksheet(sheet_name)
        sheet.write_row(0, 0, headers, header)
        sheet.set_row(0, 30)
        previous_event_id: int | None = None
        color_index = -1
        for row_index, row in enumerate(data, start=1):
            event_id = int(row["event_id"])
            if event_id != previous_event_id:
                color_index += 1
                previous_event_id = event_id
            values = [
                "待比对" if row.get("side") == "target" else "被比对",
                row.get("event_name"),
                *[_safe(_raw_value(row, column)) for column in business_columns],
            ]
            sheet.write_row(row_index, 0, values, fills[color_index % len(fills)])
        sheet.freeze_panes(1, 0)
        sheet.autofilter(0, 0, max(len(data), 1), len(headers) - 1)
        _set_column_widths(sheet, headers)
    workbook.close()


def _raw_value(row: dict[str, Any], column: str) -> Any:
    raw = row.get("raw_json") or {}
    if column in raw:
        return raw[column]
    mapping = {
        "工单编号": row.get("work_order_id"),
        "受理时间": row.get("received_at"),
        "办结时间": row.get("completed_at"),
        "诉求标题": row.get("title_raw"),
        "事项分类": row.get("category"),
        "所属部门": row.get("department"),
        "处理部门": row.get("processing_department"),
        "事发地点": row.get("location"),
        "市民诉求": row.get("appeal_text"),
    }
    return mapping.get(column)


def _safe(value: Any) -> Any:
    if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


def _set_column_widths(sheet, headers: list[str]) -> None:
    wide = {"市民诉求", "回复内容", "事实认定", "解决方式"}
    medium = {"诉求标题", "事发地点", "所属部门", "处理部门"}
    for index, header in enumerate(headers):
        if header == "事件名称":
            width = 42
        elif header == "数据侧":
            width = 12
        elif header in wide:
            width = 60
        elif header in medium:
            width = 34
        else:
            width = max(12, min(24, len(str(header)) * 2 + 4))
        sheet.set_column(index, index, width)
