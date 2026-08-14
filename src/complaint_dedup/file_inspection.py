from dataclasses import dataclass
from pathlib import Path
import re
from collections.abc import Sequence
from typing import Any

import chardet
import pandas as pd


class UnsupportedFileFormatError(ValueError):
    pass


class RowLimitExceededError(ValueError):
    pass


@dataclass(frozen=True)
class SheetInspection:
    name: str
    columns: list[str]
    row_count: int
    preview: list[dict[str, Any]]
    suggested_mapping: dict[str, str | None]


@dataclass(frozen=True)
class FileInspection:
    path: Path
    format: str
    sheets: list[SheetInspection]
    total_rows: int
    encoding: str | None = None


FIELD_ALIASES = {
    "work_order_id": ("work_order_id", "工单编号", "工单号", "受理编号", "编号"),
    "received_at": ("received_at", "受理时间", "受理日期", "创建时间", "来件时间"),
    "title": ("title", "诉求标题", "工单标题", "投诉标题", "标题"),
    "category_level_1": ("category_level_1", "事项分类一级", "一级事项分类"),
    "category_level_2": ("category_level_2", "事项分类二级", "二级事项分类"),
    "category_level_3": ("category_level_3", "事项分类三级", "三级事项分类"),
    "category_level_4": ("category_level_4", "事项分类四级", "四级事项分类"),
    "category": ("category", "事项分类", "最终事项分类", "诉求分类", "分类"),
    "appeal_text": (
        "appeal_text",
        "市民诉求",
        "诉求内容",
        "投诉内容",
        "反映内容",
        "工单内容",
    ),
}


def inspect_input_file(
    path: str | Path,
    *,
    encoding: str | None = None,
    preview_rows: int = 5,
) -> FileInspection:
    input_path = Path(path)
    file_format = input_path.suffix.lower().lstrip(".")
    if file_format not in {"xlsx", "xls", "csv"}:
        raise UnsupportedFileFormatError(f"不支持的文件格式: {file_format or '无扩展名'}")
    if preview_rows < 0:
        raise ValueError("preview_rows must not be negative")

    if file_format == "csv":
        selected_encoding = encoding or _detect_csv_encoding(input_path)
        frame = pd.read_csv(input_path, encoding=selected_encoding, dtype=object)
        sheets = [_inspect_frame("CSV", frame, preview_rows)]
        return FileInspection(
            path=input_path,
            format=file_format,
            sheets=sheets,
            total_rows=len(frame),
            encoding=selected_encoding,
        )

    engine = "openpyxl" if file_format == "xlsx" else "xlrd"
    workbook = pd.ExcelFile(input_path, engine=engine)
    sheets = [
        _inspect_frame(
            sheet_name,
            workbook.parse(sheet_name=sheet_name, dtype=object),
            preview_rows,
        )
        for sheet_name in workbook.sheet_names
    ]
    return FileInspection(
        path=input_path,
        format=file_format,
        sheets=sheets,
        total_rows=sum(sheet.row_count for sheet in sheets),
    )


def suggest_field_mapping(columns: Sequence[str]) -> dict[str, str | None]:
    normalized_columns = {_normalize_column(column): column for column in columns}
    result: dict[str, str | None] = {}
    for canonical, aliases in FIELD_ALIASES.items():
        result[canonical] = next(
            (
                normalized_columns[normalized_alias]
                for alias in aliases
                if (normalized_alias := _normalize_column(alias)) in normalized_columns
            ),
            None,
        )
    return result


def validate_combined_row_limit(
    inspections: Sequence[FileInspection],
    max_rows: int,
) -> int:
    if max_rows <= 0:
        raise ValueError("max_rows must be positive")
    total_rows = sum(inspection.total_rows for inspection in inspections)
    if total_rows > max_rows:
        raise RowLimitExceededError(
            f"合计 {total_rows} 行，超过上限 {max_rows} 行"
        )
    return total_rows


def _detect_csv_encoding(path: Path) -> str:
    sample = path.read_bytes()[:1_000_000]
    if sample.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    try:
        sample.decode("utf-8")
    except UnicodeDecodeError:
        detected = chardet.detect(sample).get("encoding")
        return detected or "gb18030"
    return "utf-8"


def _inspect_frame(name: str, frame: pd.DataFrame, preview_rows: int) -> SheetInspection:
    columns = [str(column).strip() for column in frame.columns]
    frame = frame.copy()
    frame.columns = columns
    preview_frame = frame.head(preview_rows).where(pd.notna(frame), None)
    return SheetInspection(
        name=name,
        columns=columns,
        row_count=len(frame),
        preview=preview_frame.to_dict(orient="records"),
        suggested_mapping=suggest_field_mapping(columns),
    )


def _normalize_column(value: str) -> str:
    return re.sub(r"[\s_\-()（）]+", "", str(value)).casefold()
