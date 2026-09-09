from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import xlsxwriter


async def export_comparison_workbook(service, comparison_id: str, output: str | Path) -> Path:
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    events = await service.list_comparison_events(comparison_id)
    members = await service.list_comparison_members(comparison_id)
    snapshots = {row["record_key"]: row["snapshot_json"] or {} for row in members}
    rows: list[dict[str, Any]] = []
    for event in events:
        for member in event["members"]:
            row = dict(snapshots.get(member["record_key"], {}))
            row.update(event_name=event["event_name"], event_id=event["id"], side=member["side"])
            rows.append(row)
    await asyncio.to_thread(_write, output, rows)
    return output


def _write(output: Path, rows: list[dict[str, Any]]) -> None:
    workbook = xlsxwriter.Workbook(output, {"constant_memory": True, "strings_to_formulas": False, "strings_to_urls": False})
    header = workbook.add_format({"bold": True, "font_color": "#FFFFFF", "bg_color": "#1F4E78", "text_wrap": True})
    fills = [workbook.add_format({"bg_color": "#EAF2FB", "text_wrap": True}), workbook.add_format({"bg_color": "#FFF8E7", "text_wrap": True})]
    event_counts: dict[int, int] = {}
    for row in rows:
        event_id = int(row["event_id"])
        event_counts[event_id] = event_counts.get(event_id, 0) + 1
    duplicate = [row for row in rows if event_counts[int(row["event_id"])] > 1]
    singleton = [row for row in rows if event_counts[int(row["event_id"])] == 1]
    for sheet_name, data in (("重复项", duplicate), ("孤立工单", singleton)):
        sheet = workbook.add_worksheet(sheet_name)
        columns = ["数据侧", "事件名称", "工单编号", "受理时间", "办结时间", "诉求标题", "事项分类", "处理部门", "事发地点", "市民诉求"]
        sheet.write_row(0, 0, columns, header)
        color_index = -1
        previous_event_id: int | None = None
        for index, row in enumerate(data, start=1):
            event_id = int(row["event_id"])
            if event_id != previous_event_id:
                color_index += 1
                previous_event_id = event_id
            values = [
                "待比对" if row.get("side") == "target" else "被比对",
                row.get("event_name"),
                row.get("work_order_id"),
                row.get("received_at"),
                row.get("completed_at"),
                row.get("title_raw"),
                row.get("category"),
                row.get("processing_department"),
                row.get("location"),
                row.get("appeal_text"),
            ]
            sheet.write_row(
                index,
                0,
                [_safe(value) for value in values],
                fills[color_index % len(fills)],
            )
        sheet.freeze_panes(1, 0)
        sheet.autofilter(0, 0, max(1, len(data)), len(columns) - 1)
        sheet.set_column(0, 1, 24)
        sheet.set_column(2, 4, 18)
        sheet.set_column(5, 8, 28)
        sheet.set_column(9, 9, 60)
    workbook.close()


def _safe(value: Any) -> Any:
    if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
        return "'" + value
    return value
