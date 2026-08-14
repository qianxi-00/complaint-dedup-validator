from pathlib import Path

import pandas as pd
import pytest
import xlwt

from complaint_dedup.file_inspection import (
    RowLimitExceededError,
    UnsupportedFileFormatError,
    inspect_input_file,
    suggest_field_mapping,
    validate_combined_row_limit,
)


COLUMNS = ["工单编号", "受理时间", "诉求标题", "事项分类四级", "市民诉求"]


def sample_frame(rows: int = 2) -> pd.DataFrame:
    return pd.DataFrame(
        [
            [f"WO-{index}", "2026-01-01", f"标题{index}", "劳动纠纷", f"诉求{index}"]
            for index in range(1, rows + 1)
        ],
        columns=COLUMNS,
    )


def test_inspects_xlsx_with_multiple_sheets(tmp_path: Path) -> None:
    path = tmp_path / "input.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        sample_frame(2).to_excel(writer, sheet_name="A表", index=False)
        sample_frame(3).to_excel(writer, sheet_name="B表", index=False)

    inspection = inspect_input_file(path, preview_rows=1)

    assert inspection.format == "xlsx"
    assert inspection.total_rows == 5
    assert [sheet.name for sheet in inspection.sheets] == ["A表", "B表"]
    assert [sheet.row_count for sheet in inspection.sheets] == [2, 3]
    assert len(inspection.sheets[0].preview) == 1
    assert inspection.sheets[0].columns == COLUMNS


def test_inspects_legacy_xls(tmp_path: Path) -> None:
    path = tmp_path / "input.xls"
    workbook = xlwt.Workbook()
    worksheet = workbook.add_sheet("Sheet1")
    frame = sample_frame(2)
    for column_index, column in enumerate(frame.columns):
        worksheet.write(0, column_index, column)
    for row_index, row in enumerate(frame.itertuples(index=False), start=1):
        for column_index, value in enumerate(row):
            worksheet.write(row_index, column_index, value)
    workbook.save(str(path))

    inspection = inspect_input_file(path)

    assert inspection.format == "xls"
    assert inspection.total_rows == 2
    assert inspection.sheets[0].columns == COLUMNS


@pytest.mark.parametrize("encoding", ["utf-8-sig", "gb18030"])
def test_inspects_csv_and_detects_encoding(tmp_path: Path, encoding: str) -> None:
    path = tmp_path / f"input-{encoding}.csv"
    sample_frame(2).to_csv(path, index=False, encoding=encoding)

    inspection = inspect_input_file(path)

    assert inspection.format == "csv"
    assert inspection.total_rows == 2
    assert inspection.encoding is not None
    assert inspection.sheets[0].columns == COLUMNS


def test_csv_encoding_can_be_overridden(tmp_path: Path) -> None:
    path = tmp_path / "input.csv"
    sample_frame(1).to_csv(path, index=False, encoding="gb18030")

    inspection = inspect_input_file(path, encoding="gb18030")

    assert inspection.encoding == "gb18030"


def test_suggests_canonical_field_mapping() -> None:
    mapping = suggest_field_mapping(
        [
            "工单号",
            "受理日期",
            "工单标题",
            "事项分类一级",
            "事项分类二级",
            "事项分类三级",
            "事项分类四级",
            "事项分类",
            "投诉内容",
            "备注",
        ]
    )

    assert mapping == {
        "work_order_id": "工单号",
        "received_at": "受理日期",
        "title": "工单标题",
        "category_level_1": "事项分类一级",
        "category_level_2": "事项分类二级",
        "category_level_3": "事项分类三级",
        "category_level_4": "事项分类四级",
        "category": "事项分类",
        "appeal_text": "投诉内容",
    }


def test_mapping_marks_unknown_fields_as_none() -> None:
    mapping = suggest_field_mapping(["其他列"])

    assert set(mapping) == {
        "work_order_id",
        "received_at",
        "title",
        "category_level_1",
        "category_level_2",
        "category_level_3",
        "category_level_4",
        "category",
        "appeal_text",
    }
    assert all(value is None for value in mapping.values())


def test_rejects_unsupported_format(tmp_path: Path) -> None:
    path = tmp_path / "input.txt"
    path.write_text("hello", encoding="utf-8")

    with pytest.raises(UnsupportedFileFormatError, match="txt"):
        inspect_input_file(path)


def test_validates_combined_row_limit(tmp_path: Path) -> None:
    first_path = tmp_path / "a.csv"
    second_path = tmp_path / "b.csv"
    sample_frame(2).to_csv(first_path, index=False, encoding="utf-8")
    sample_frame(3).to_csv(second_path, index=False, encoding="utf-8")
    inspections = [inspect_input_file(first_path), inspect_input_file(second_path)]

    assert validate_combined_row_limit(inspections, max_rows=5) == 5
    with pytest.raises(RowLimitExceededError, match="5.*4"):
        validate_combined_row_limit(inspections, max_rows=4)
