from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import xlsxwriter

from complaint_dedup.corpus_repository import CorpusRepository, EventFilters


async def export_corpus(
    repository: CorpusRepository,
    output_path: str | Path,
    *,
    filters: EventFilters | None = None,
) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    columns = await repository.export_business_columns()
    rows = await repository.export_rows(filters=filters)
    await asyncio.to_thread(_write_workbook, output, columns, rows)
    return output


def _write_workbook(
    output: Path, business_columns: list[str], rows: list[dict[str, Any]]
) -> None:
    workbook = xlsxwriter.Workbook(
        output,
        {
            "constant_memory": True,
            "strings_to_formulas": False,
            "strings_to_urls": False,
            "tmpdir": str(output.parent),
        },
    )
    header_format = workbook.add_format(
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
    group_formats = (
        workbook.add_format(
            {
                "font_color": "#000000",
                "bg_color": "#EAF2FB",
                "valign": "top",
                "text_wrap": True,
            }
        ),
        workbook.add_format(
            {
                "font_color": "#000000",
                "bg_color": "#FFF8E7",
                "valign": "top",
                "text_wrap": True,
            }
        ),
    )
    duplicate_rows = [row for row in rows if int(row.get("member_count") or 0) > 1]
    singleton_rows = [row for row in rows if int(row.get("member_count") or 0) <= 1]
    for name, data in (("重复项", duplicate_rows), ("孤立工单", singleton_rows)):
        worksheet = workbook.add_worksheet(name)
        headers = ["数据来源", "事件名称", *business_columns]
        worksheet.write_row(0, 0, headers, header_format)
        worksheet.set_row(0, 30)

        color_index = -1
        previous_event_id: int | None = None
        for row_index, item in enumerate(data, start=1):
            event_id = int(item["event_id"])
            if event_id != previous_event_id:
                color_index += 1
                previous_event_id = event_id
            raw = item.get("raw_json") or {}
            values = [
                _data_source_label(item.get("data_source")),
                item.get("event_name"),
            ]
            values.extend(_safe_excel_value(raw.get(column)) for column in business_columns)
            worksheet.write_row(
                row_index,
                0,
                values,
                group_formats[color_index % len(group_formats)],
            )

        worksheet.freeze_panes(1, 0)
        worksheet.autofilter(0, 0, max(len(data), 1), len(headers) - 1)
        _set_column_widths(worksheet, headers)
    workbook.close()


def _safe_excel_value(value: Any) -> Any:
    if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


def _data_source_label(value: Any) -> str:
    return {
        "daily": "今日新增",
        "history": "历史表",
        "correction": "补录",
    }.get(str(value or ""), "未知")


def _set_column_widths(worksheet, headers: list[str]) -> None:
    wide_columns = {"市民诉求", "回复内容", "事实认定", "解决方式"}
    medium_columns = {"诉求标题", "事发地点", "所属部门", "处理部门"}
    for index, header in enumerate(headers, start=1):
        normalized_header = str(header).strip()
        if normalized_header == "事件名称":
            width = 42
        elif normalized_header == "数据来源":
            width = 12
        elif normalized_header in wide_columns:
            width = 60
        elif normalized_header in medium_columns:
            width = 34
        else:
            width = max(12, min(24, len(str(header)) * 2 + 4))
        worksheet.set_column(index - 1, index - 1, width)
