from __future__ import annotations

from pathlib import Path

import pandas as pd

from complaint_dedup.file_inspection import inspect_input_file
from complaint_dedup.corpus_models import InputRecord


def load_records_auto(path: str | Path, *, source: str) -> list[InputRecord]:
    input_path = Path(path)
    inspection = inspect_input_file(input_path, preview_rows=0)
    selected = inspection.sheets[0]
    mapping = selected.suggested_mapping
    required = ("work_order_id", "title", "appeal_text")
    missing = [name for name in required if not mapping.get(name)]
    if missing:
        raise ValueError(f"无法自动识别必要字段：{', '.join(missing)}")

    if input_path.suffix.lower() == ".csv":
        frame = pd.read_csv(input_path, encoding=inspection.encoding, dtype=object)
    else:
        engine = "openpyxl" if input_path.suffix.lower() == ".xlsx" else "xlrd"
        frame = pd.read_excel(
            input_path,
            sheet_name=selected.name,
            engine=engine,
            dtype=object,
        )
    frame.columns = [str(column) for column in frame.columns]
    frame = frame.where(pd.notna(frame), None)

    def value(row: pd.Series, key: str):
        column = mapping.get(key)
        return row.get(column) if column else None

    result: list[InputRecord] = []
    for index, row in frame.iterrows():
        raw = {
            str(key): _text(item)
            for key, item in row.to_dict().items()
            if str(key).strip() != "Unnamed: 37"
        }
        result.append(
            InputRecord(
                source=source,
                source_row=index + 2,
                work_order_id=_text(value(row, "work_order_id")),
                received_at=_text(value(row, "received_at")),
                title=_text(value(row, "title")),
                category=_text(value(row, "category")),
                category_level_1=_text(value(row, "category_level_1")),
                category_level_2=_text(value(row, "category_level_2")),
                category_level_3=_text(value(row, "category_level_3")),
                category_level_4=_text(value(row, "category_level_4")),
                appeal_text=_text(value(row, "appeal_text")),
                raw_fields=raw,
            )
        )
    return result


def _text(value) -> str | None:
    return None if value is None else str(value)
