import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from complaint_dedup.candidates import ExtractedRecord, generate_candidate_pairs
from complaint_dedup.database import connect_database
from complaint_dedup.llm_models import ExtractionBatchResponse, JudgementBatchResponse
from complaint_dedup.prompts import build_extraction_messages, build_judgement_messages


@dataclass(frozen=True)
class InputRecord:
    source: str
    source_row: int
    work_order_id: str | None
    title: str | None
    category: str | None
    appeal_text: str | None
    received_at: str | None = None
    raw_fields: dict[str, Any] = field(default_factory=dict)


class JobProcessor:
    def __init__(
        self,
        database_path: str | Path,
        llm_client: Any,
        extraction_batch_size: int,
        judgement_batch_size: int,
        max_candidates_per_record: int,
        broad_key_max_matches: int,
    ) -> None:
        self.database_path = Path(database_path)
        self.llm_client = llm_client
        self.extraction_batch_size = extraction_batch_size
        self.judgement_batch_size = judgement_batch_size
        self.max_candidates_per_record = max_candidates_per_record
        self.broad_key_max_matches = broad_key_max_matches

    def create_job(
        self,
        name: str,
        records_a: list[InputRecord],
        records_b: list[InputRecord],
        match_preset: str = "balanced",
        time_window_days: int = 0,
    ) -> str:
        job_id = uuid.uuid4().hex
        with connect_database(self.database_path) as connection:
            connection.execute(
                """
                INSERT INTO jobs (
                    id, name, status, stage, match_preset, time_window_days,
                    source_a_name, source_b_name, total_records
                ) VALUES (?, ?, 'queued', 'queued', ?, ?, 'A', 'B', ?)
                """,
                (job_id, name, match_preset, time_window_days, len(records_a) + len(records_b)),
            )
            for record in [*records_a, *records_b]:
                raw = record.raw_fields or {
                    "工单编号": record.work_order_id,
                    "受理时间": record.received_at,
                    "诉求标题": record.title,
                    "事项分类": record.category,
                    "市民诉求": record.appeal_text,
                }
                connection.execute(
                    """
                    INSERT INTO records (
                        job_id, source, source_row, work_order_id, received_at,
                        title, category, appeal_text, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        record.source,
                        record.source_row,
                        record.work_order_id,
                        record.received_at,
                        record.title,
                        record.category,
                        record.appeal_text,
                        json.dumps(raw, ensure_ascii=False, default=str),
                    ),
                )
        return job_id

    async def process(self, job_id: str) -> None:
        self._set_job(job_id, status="running", stage="extracting")
        await self._extract_pending(job_id)
        if self._pause_requested(job_id):
            self._set_job(job_id, status="paused", stage="extracting")
            return

        self._set_job(job_id, status="running", stage="generating_candidates")
        self._generate_candidates(job_id)
        self._set_job(job_id, status="running", stage="judging")
        await self._judge_pending(job_id)
        if self._pause_requested(job_id):
            self._set_job(job_id, status="paused", stage="judging")
            return

        failures = self.get_job(job_id)["extraction_failure_count"] + self.get_job(job_id)[
            "judgement_failure_count"
        ]
        self._set_job(
            job_id,
            status="completed_with_warnings" if failures else "review_ready",
            stage="review",
        )

    def get_job(self, job_id: str) -> dict[str, Any]:
        with connect_database(self.database_path) as connection:
            row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        return dict(row)

    def list_pairs(
        self, job_id: str, *, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        with connect_database(self.database_path) as connection:
            rows = connection.execute(
                """
                SELECT p.*, r.decision AS review_decision, r.note AS review_note,
                       a.work_order_id AS a_work_order_id, a.title AS a_title,
                       a.appeal_text AS a_appeal_text, a.extraction_json AS a_extraction_json,
                       b.work_order_id AS b_work_order_id, b.title AS b_title,
                       b.appeal_text AS b_appeal_text, b.extraction_json AS b_extraction_json
                FROM candidate_pairs p
                LEFT JOIN reviews r ON r.candidate_pair_id = p.id
                JOIN records a ON a.id = p.record_a_id
                JOIN records b ON b.id = p.record_b_id
                WHERE p.job_id = ?
                ORDER BY p.id
                LIMIT ? OFFSET ?
                """,
                (job_id, limit, offset),
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["hard_conflicts"] = _json_value(item.get("hard_conflicts_json"), [])
            item["evidence"] = _json_value(item.get("evidence_json"), {})
            result.append(item)
        return result

    def count_pairs(self, job_id: str) -> int:
        with connect_database(self.database_path) as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS total FROM candidate_pairs WHERE job_id = ?", (job_id,)
            ).fetchone()
        return int(row["total"])

    def list_groups(self, job_id: str) -> list[dict[str, Any]]:
        with connect_database(self.database_path) as connection:
            rows = connection.execute(
                """
                SELECT g.id, g.name, COUNT(m.record_id) AS member_count
                FROM event_groups g
                JOIN event_members m ON m.event_group_id = g.id
                WHERE g.job_id = ?
                GROUP BY g.id, g.name
                ORDER BY g.id
                """,
                (job_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def review_pair(
        self,
        pair_id: int,
        decision: str,
        note: str | None = None,
        *,
        job_id: str | None = None,
    ) -> None:
        if decision not in {"duplicate", "not_duplicate"}:
            raise ValueError("invalid review decision")
        with connect_database(self.database_path) as connection:
            pair = connection.execute(
                "SELECT job_id FROM candidate_pairs WHERE id = ?", (pair_id,)
            ).fetchone()
            if pair is None:
                raise KeyError(pair_id)
            if job_id is not None and pair["job_id"] != job_id:
                raise KeyError(pair_id)
            if decision == "duplicate":
                conflicts = connection.execute(
                    "SELECT hard_conflicts_json FROM candidate_pairs WHERE id = ?", (pair_id,)
                ).fetchone()
                if _json_value(conflicts["hard_conflicts_json"], []):
                    raise ValueError("存在硬冲突，不能确认重复")
            connection.execute(
                """
                INSERT INTO reviews (candidate_pair_id, decision, note)
                VALUES (?, ?, ?)
                ON CONFLICT(candidate_pair_id) DO UPDATE SET
                    decision = excluded.decision,
                    note = excluded.note,
                    reviewed_at = CURRENT_TIMESTAMP
                """,
                (pair_id, decision, note),
            )
            self._rebuild_groups(connection, pair["job_id"])

    def request_pause(self, job_id: str, pause: bool) -> None:
        with connect_database(self.database_path) as connection:
            connection.execute(
                "UPDATE jobs SET pause_requested = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (1 if pause else 0, job_id),
            )

    async def _extract_pending(self, job_id: str) -> None:
        with connect_database(self.database_path) as connection:
            rows = connection.execute(
                """
                SELECT * FROM records
                WHERE job_id = ?
                ORDER BY id
                """,
                (job_id,),
            ).fetchall()
        for batch_index, full_batch in enumerate(_chunks(rows, self.extraction_batch_size)):
            batch = [row for row in full_batch if row["extraction_status"] != "succeeded"]
            if not batch:
                continue
            if self._pause_requested(job_id):
                break
            batch_index = self._resume_batch_index(job_id, "extraction", batch_index)
            self._start_batch(job_id, "extraction", batch_index, [row["id"] for row in batch])
            payload = [
                {
                    "record_id": f"{row['source']}-{row['source_row']}",
                    "title": row["title"],
                    "category": row["category"],
                    "appeal_text": row["appeal_text"],
                    "received_at": row["received_at"],
                }
                for row in batch
            ]
            try:
                response = await self.llm_client.chat_json(
                    build_extraction_messages(payload), ExtractionBatchResponse
                )
                returned = {item.record_id: item for item in response.records}
                with connect_database(self.database_path) as connection:
                    for row in batch:
                        public_id = f"{row['source']}-{row['source_row']}"
                        item = returned.get(public_id)
                        if item is None:
                            connection.execute(
                                "UPDATE records SET extraction_status = 'failed', extraction_error = ? WHERE id = ?",
                                ("模型未返回该记录", row["id"]),
                            )
                        else:
                            connection.execute(
                                "UPDATE records SET extraction_status = 'succeeded', extraction_json = ?, extraction_error = NULL WHERE id = ?",
                                (item.model_dump_json(), row["id"]),
                            )
                self._finish_batch(job_id, "extraction", batch_index, response.model_dump_json())
            except Exception as exc:
                self._fail_batch(
                    job_id,
                    "extraction",
                    batch_index,
                    str(exc),
                    getattr(exc, "raw_response", None),
                )
                with connect_database(self.database_path) as connection:
                    connection.executemany(
                        "UPDATE records SET extraction_status = 'failed', extraction_error = ? WHERE id = ?",
                        [(str(exc), row["id"]) for row in batch],
                    )
        self._refresh_job_counts(job_id)

    def _generate_candidates(self, job_id: str) -> None:
        records_a, records_b = self._load_extracted_records(job_id)
        job = self.get_job(job_id)
        pairs = generate_candidate_pairs(
            records_a,
            records_b,
            self.max_candidates_per_record,
            self.broad_key_max_matches,
            preset=job["match_preset"],
            time_window_days=job["time_window_days"],
        )
        with connect_database(self.database_path) as connection:
            for pair in pairs:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO candidate_pairs (
                        job_id, record_a_id, record_b_id, candidate_key, rule_status
                    ) VALUES (?, ?, ?, ?, 'candidate')
                    """,
                    (job_id, pair.record_a_id, pair.record_b_id, pair.reason),
                )
            connection.execute(
                """
                UPDATE jobs SET candidate_count = (
                    SELECT COUNT(*) FROM candidate_pairs WHERE job_id = ?
                ), updated_at = CURRENT_TIMESTAMP WHERE id = ?
                """,
                (job_id, job_id),
            )

    async def _judge_pending(self, job_id: str) -> None:
        with connect_database(self.database_path) as connection:
            rows = connection.execute(
                """
                SELECT p.*, a.title AS a_title, a.appeal_text AS a_appeal,
                       a.extraction_json AS a_extraction,
                       b.title AS b_title, b.appeal_text AS b_appeal,
                       b.extraction_json AS b_extraction
                FROM candidate_pairs p
                JOIN records a ON a.id = p.record_a_id
                JOIN records b ON b.id = p.record_b_id
                WHERE p.job_id = ?
                ORDER BY p.id
                """,
                (job_id,),
            ).fetchall()
        for batch_index, full_batch in enumerate(_chunks(rows, self.judgement_batch_size)):
            batch = [row for row in full_batch if row["judgement_status"] != "succeeded"]
            if not batch:
                continue
            if self._pause_requested(job_id):
                break
            batch_index = self._resume_batch_index(job_id, "judgement", batch_index)
            self._start_batch(job_id, "judgement", batch_index, [row["id"] for row in batch])
            payload = [
                {
                    "pair_id": f"{row['record_a_id']}|{row['record_b_id']}",
                    "candidate_reason": row["candidate_key"],
                    "a": {
                        "title": row["a_title"],
                        "appeal_text": row["a_appeal"],
                        "extraction": json.loads(row["a_extraction"]),
                    },
                    "b": {
                        "title": row["b_title"],
                        "appeal_text": row["b_appeal"],
                        "extraction": json.loads(row["b_extraction"]),
                    },
                }
                for row in batch
            ]
            try:
                response = await self.llm_client.chat_json(
                    build_judgement_messages(payload), JudgementBatchResponse
                )
                returned = {item.pair_id: item for item in response.pairs}
                with connect_database(self.database_path) as connection:
                    for row in batch:
                        public_id = f"{row['record_a_id']}|{row['record_b_id']}"
                        item = returned.get(public_id)
                        if item is None:
                            connection.execute(
                                "UPDATE candidate_pairs SET judgement_status = 'failed', judgement_error = ? WHERE id = ?",
                                ("模型未返回该候选对", row["id"]),
                            )
                        else:
                            connection.execute(
                                """
                                UPDATE candidate_pairs SET
                                    judgement_status = 'succeeded', llm_decision = ?, confidence = ?,
                                    evidence_json = ?, hard_conflicts_json = ?, event_name_suggestion = ?,
                                    judgement_error = NULL, updated_at = CURRENT_TIMESTAMP
                                WHERE id = ?
                                """,
                                (
                                    item.decision,
                                    item.confidence,
                                    json.dumps(
                                        {
                                            "evidence_a": item.evidence_a,
                                            "evidence_b": item.evidence_b,
                                            "reason": item.reason,
                                            "subject_relation": item.subject_relation,
                                            "address_relation": item.address_relation,
                                            "issue_relation": item.issue_relation,
                                        },
                                        ensure_ascii=False,
                                    ),
                                    json.dumps(item.hard_conflicts, ensure_ascii=False),
                                    item.event_name,
                                    row["id"],
                                ),
                            )
                self._finish_batch(job_id, "judgement", batch_index, response.model_dump_json())
            except Exception as exc:
                self._fail_batch(
                    job_id,
                    "judgement",
                    batch_index,
                    str(exc),
                    getattr(exc, "raw_response", None),
                )
                with connect_database(self.database_path) as connection:
                    connection.executemany(
                        "UPDATE candidate_pairs SET judgement_status = 'failed', judgement_error = ? WHERE id = ?",
                        [(str(exc), row["id"]) for row in batch],
                    )
        self._refresh_job_counts(job_id)

    def _load_extracted_records(
        self, job_id: str
    ) -> tuple[list[ExtractedRecord], list[ExtractedRecord]]:
        with connect_database(self.database_path) as connection:
            rows = connection.execute(
                "SELECT id, source, received_at, extraction_json FROM records WHERE job_id = ? AND extraction_status = 'succeeded'",
                (job_id,),
            ).fetchall()
        result = {"A": [], "B": []}
        for row in rows:
            data = json.loads(row["extraction_json"])
            result[row["source"]].append(
                ExtractedRecord(
                    record_id=row["id"],
                    source=row["source"],
                    subject_keys=tuple(data.get("subject", {}).get("keys", [])),
                    exact_address_keys=tuple(data.get("address", {}).get("exact_keys", [])),
                    coarse_address_keys=tuple(data.get("address", {}).get("coarse_keys", [])),
                    primary_issue=data.get("issues", {}).get("primary"),
                    received_at=row["received_at"],
                )
            )
        return result["A"], result["B"]

    def _rebuild_groups(self, connection, job_id: str) -> None:
        connection.execute(
            "DELETE FROM event_groups WHERE job_id = ?", (job_id,)
        )
        rows = connection.execute(
            """
            SELECT p.record_a_id, p.record_b_id, p.event_name_suggestion
            FROM candidate_pairs p
            JOIN reviews r ON r.candidate_pair_id = p.id
            WHERE p.job_id = ? AND r.decision = 'duplicate'
            ORDER BY p.id
            """,
            (job_id,),
        ).fetchall()
        parent: dict[int, int] = {}

        def find(value: int) -> int:
            parent.setdefault(value, value)
            if parent[value] != value:
                parent[value] = find(parent[value])
            return parent[value]

        def union(left: int, right: int) -> None:
            left_root, right_root = find(left), find(right)
            if left_root != right_root:
                parent[right_root] = left_root

        names: dict[int, str] = {}
        for row in rows:
            union(row["record_a_id"], row["record_b_id"])
            names.setdefault(row["record_a_id"], row["event_name_suggestion"] or "待命名事件")
        groups: dict[int, set[int]] = {}
        for member in parent:
            groups.setdefault(find(member), set()).add(member)
        for root, members in groups.items():
            cursor = connection.execute(
                "INSERT INTO event_groups (job_id, name) VALUES (?, ?)",
                (job_id, names.get(root, "待命名事件")),
            )
            connection.executemany(
                "INSERT INTO event_members (event_group_id, record_id) VALUES (?, ?)",
                [(cursor.lastrowid, member) for member in sorted(members)],
            )

    def _start_batch(self, job_id: str, batch_type: str, batch_index: int, ids: list[int]) -> None:
        with connect_database(self.database_path) as connection:
            connection.execute(
                """
                INSERT INTO llm_batches (job_id, batch_type, batch_index, status, attempts, request_json)
                VALUES (?, ?, ?, 'running', 1, ?)
                ON CONFLICT(job_id, batch_type, batch_index) DO UPDATE SET
                    status = 'running', attempts = attempts + 1,
                    request_json = excluded.request_json, updated_at = CURRENT_TIMESTAMP
                """,
                (job_id, batch_type, batch_index, json.dumps(ids)),
            )

    def _resume_batch_index(self, job_id: str, batch_type: str, preferred: int) -> int:
        with connect_database(self.database_path) as connection:
            existing = connection.execute(
                "SELECT status FROM llm_batches WHERE job_id = ? AND batch_type = ? AND batch_index = ?",
                (job_id, batch_type, preferred),
            ).fetchone()
            if existing is None or existing["status"] != "succeeded":
                return preferred
            row = connection.execute(
                "SELECT COALESCE(MAX(batch_index), -1) + 1 AS next_index FROM llm_batches WHERE job_id = ? AND batch_type = ?",
                (job_id, batch_type),
            ).fetchone()
        return int(row["next_index"])

    def _finish_batch(self, job_id: str, batch_type: str, batch_index: int, response: str) -> None:
        with connect_database(self.database_path) as connection:
            connection.execute(
                """
                UPDATE llm_batches SET status = 'succeeded', response_json = ?, error_message = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE job_id = ? AND batch_type = ? AND batch_index = ?
                """,
                (response, job_id, batch_type, batch_index),
            )

    def _fail_batch(
        self,
        job_id: str,
        batch_type: str,
        batch_index: int,
        error: str,
        raw_response: str | None = None,
    ) -> None:
        with connect_database(self.database_path) as connection:
            connection.execute(
                """
                UPDATE llm_batches SET status = 'failed', response_json = ?, error_message = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE job_id = ? AND batch_type = ? AND batch_index = ?
                """,
                (raw_response, error, job_id, batch_type, batch_index),
            )

    def _refresh_job_counts(self, job_id: str) -> None:
        with connect_database(self.database_path) as connection:
            connection.execute(
                """
                UPDATE jobs SET
                    extracted_records = (SELECT COUNT(*) FROM records WHERE job_id = ? AND extraction_status = 'succeeded'),
                    extraction_failure_count = (SELECT COUNT(*) FROM records WHERE job_id = ? AND extraction_status = 'failed'),
                    judged_count = (SELECT COUNT(*) FROM candidate_pairs WHERE job_id = ? AND judgement_status = 'succeeded'),
                    judgement_failure_count = (SELECT COUNT(*) FROM candidate_pairs WHERE job_id = ? AND judgement_status = 'failed'),
                    retry_count = (SELECT COALESCE(SUM(MAX(attempts - 1, 0)), 0) FROM llm_batches WHERE job_id = ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (job_id, job_id, job_id, job_id, job_id, job_id),
            )

    def _pause_requested(self, job_id: str) -> bool:
        return bool(self.get_job(job_id)["pause_requested"])

    def _set_job(self, job_id: str, *, status: str, stage: str) -> None:
        with connect_database(self.database_path) as connection:
            connection.execute(
                "UPDATE jobs SET status = ?, stage = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, stage, job_id),
            )


def _chunks(rows: list[Any], size: int) -> list[list[Any]]:
    return [rows[index : index + size] for index in range(0, len(rows), size)]


def _json_value(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default
