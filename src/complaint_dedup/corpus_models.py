from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class InputRecord:
    source_row: int
    work_order_id: str | None
    title: str | None
    category: str | None
    appeal_text: str | None
    received_at: str | None = None
    completed_at: str | None = None
    location: str | None = None
    processing_department: str | None = None
    category_level_1: str | None = None
    category_level_2: str | None = None
    category_level_3: str | None = None
    category_level_4: str | None = None
    raw_fields: dict[str, Any] = field(default_factory=dict)
