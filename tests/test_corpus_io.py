from pathlib import Path

from openpyxl import Workbook

from complaint_dedup.corpus_io import load_records_auto


def test_loader_handles_historical_headers_with_trailing_spaces(tmp_path: Path):
    path = tmp_path / "history.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["工单编号 ", "受理时间 ", "诉求标题", "事项分类", "市民诉求"])
    sheet.append(
        [
            "0826081308493182401",
            "2026-08-13 08:49:31",
            "测试标题",
            "道路积水",
            "地址：江海区礼乐街道德昌电机门口。\n事项：道路积水。",
        ]
    )
    workbook.save(path)

    row = load_records_auto(path, source="B")[0]
    assert row.work_order_id == "0826081308493182401"
    assert row.received_at == "2026-08-13 08:49:31"
