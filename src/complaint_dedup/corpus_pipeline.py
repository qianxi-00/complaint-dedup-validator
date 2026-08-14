from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from complaint_dedup.corpus_parser import normalize_phone, parse_complaint
from complaint_dedup.corpus_normalizer import (
    anchor_location_signature,
    canonicalize_anchor_candidates,
    fuzzy_alias_match,
)
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.pipeline import InputRecord


PARSER_VERSION = "rules-v1"
EVENT_KEY_VERSION = "event-key-v1"


@dataclass(frozen=True)
class StagedCorpusBatch:
    batch_id: str
    dictionary_version_id: int


class CorpusProcessor:
    def __init__(
        self,
        repository: CorpusRepository,
        *,
        dictionary_review_required: bool = False,
    ) -> None:
        self.repository = repository
        self.dictionary_review_required = dictionary_review_required

    async def stage_records(
        self,
        *,
        name: str,
        batch_type: str,
        file_name: str,
        file_hash: str,
        records: list[InputRecord],
        batch_id: str | None = None,
    ) -> StagedCorpusBatch:
        existing_batch = None
        if batch_id is not None:
            existing_batch = await self.repository.get_batch(batch_id)
            if existing_batch["batch_type"] != batch_type:
                raise ValueError("批次类型与上传任务不一致")
        if batch_type in {"bootstrap_history", "bootstrap_compare"}:
            if existing_batch and existing_batch.get("dictionary_version_id"):
                dictionary_version_id = int(existing_batch["dictionary_version_id"])
            else:
                dictionary_version_id = await self.repository.create_dictionary_version(
                    f"dict-{file_hash[:12]}",
                    status="candidate",
                    source_record_count=len(records),
                )
            source_type = "history"
            candidate_status = "candidate"
        else:
            active = await self.repository.active_dictionary_version()
            if active is None:
                raise ValueError("尚未发布词典，不能执行每日增量")
            dictionary_version_id = int(active["id"])
            source_type = "correction" if batch_type == "correction" else "daily"
            candidate_status = "approved"
            if batch_type == "daily_increment":
                latest_history = await self.repository.max_committed_received_at()
                received_values = [
                    value
                    for record in records
                    if (value := _received_at(record.received_at)) is not None
                ]
                if (
                    latest_history is not None
                    and received_values
                    and min(received_values) <= _ensure_utc(latest_history)
                ):
                    raise ValueError("每日新增与已提交历史库的时间范围重叠，请使用补录或更正模式")

        source_id = await self.repository.create_source(
            file_name=file_name,
            file_hash=file_hash,
            source_type=source_type,
            business_columns=_business_columns(records),
            row_count=len(records),
        )
        extraction_run_id = None
        if batch_type in {"bootstrap_history", "bootstrap_compare"}:
            extraction_run_id = await self.repository.start_dictionary_extraction_run(
                source_id=source_id,
                dictionary_version_id=dictionary_version_id,
                source_file_hash=file_hash,
                parser_version=PARSER_VERSION,
            )
        if batch_id is None:
            batch_id = await self.repository.create_batch(
                name,
                batch_type,
                total_records=len(records),
                dictionary_version_id=dictionary_version_id,
            )
        else:
            await self.repository.update_batch_setup(
                batch_id,
                total_records=len(records),
                dictionary_version_id=dictionary_version_id,
            )
        await self.repository.set_batch_stage(batch_id, "parsing", status="parsing")

        parsed_rows: list[dict[str, Any]] = []
        for record in records:
            parsed = parse_complaint(
                title=record.title,
                appeal=record.appeal_text,
                location=_raw_value(record.raw_fields, "事发地点"),
            )
            issue_name = _issue_name(record, parsed.normalized_title)
            phone = normalize_phone(
                _raw_value(record.raw_fields, "联系电话")
                or _raw_value(record.raw_fields, "来电号码")
            )
            raw_json = record.raw_fields or _record_fallback_raw(record)
            parsed_rows.append(
                {
                    "record": record,
                    "parsed": parsed,
                    "issue_name": issue_name,
                    "phone": phone,
                    "raw_json": raw_json,
                }
            )

        if batch_type in {"bootstrap_history", "bootstrap_compare"}:
            dictionary_maps = await self.repository.ensure_dictionary_items(
                dictionary_version_id=dictionary_version_id,
                streets=[
                    (item["parsed"].region, item["parsed"].street)
                    for item in parsed_rows
                    if item["parsed"].street
                ],
                anchors=canonicalize_anchor_candidates(
                    [
                        {
                            "street_key": (
                                item["parsed"].region,
                                item["parsed"].street,
                            ),
                            "canonical_name": item["parsed"].anchor_raw,
                            "anchor_type": item["parsed"].anchor_type,
                            "location_signature": anchor_location_signature(
                                item["parsed"].road,
                                item["parsed"].house_no,
                                item["parsed"].building,
                                item["parsed"].direction,
                                item["parsed"].shop_no,
                                item["parsed"].floor,
                            ),
                            "road": item["parsed"].road,
                            "house_no": item["parsed"].house_no,
                            "building": item["parsed"].building,
                            "shop_no": item["parsed"].shop_no,
                            "floor": item["parsed"].floor,
                            "direction": item["parsed"].direction,
                        }
                        for item in parsed_rows
                        if item["parsed"].street and item["parsed"].anchor_raw
                    ]
                ),
                issues=[
                    {
                        "canonical_name": item["issue_name"],
                        "category_level_1": item["record"].category_level_1,
                        "category_level_2": item["record"].category_level_2,
                        "category_level_3": item["record"].category_level_3,
                        "category_level_4": item["record"].category_level_4,
                    }
                    for item in parsed_rows
                    if item["issue_name"]
                ],
                review_status=candidate_status,
            )
        else:
            dictionary_maps = await self.repository.dictionary_maps(dictionary_version_id)

        record_values: list[dict[str, Any]] = []
        resolved_ids: list[tuple[int | None, int | None, int | None]] = []
        for item in parsed_rows:
            record = item["record"]
            parsed = item["parsed"]
            issue_name = item["issue_name"]
            street_id = dictionary_maps["streets"].get((parsed.region, parsed.street))
            anchor_status = "unknown"
            anchor_id = None
            location_signature = anchor_location_signature(
                parsed.road,
                parsed.house_no,
                parsed.building,
                parsed.direction,
                parsed.shop_no,
                parsed.floor,
            )
            if street_id and parsed.anchor_raw:
                anchor_id = dictionary_maps["anchors"].get(
                    (
                        street_id,
                        parsed.anchor_raw,
                        parsed.anchor_type,
                        location_signature,
                    )
                )
                anchor_status = "exact" if anchor_id else "unknown"
                if anchor_id is None and batch_type not in {
                    "bootstrap_history",
                    "bootstrap_compare",
                }:
                    candidate_keys = [
                        key
                        for key in dictionary_maps["anchors"]
                        if key[0] == street_id
                        and key[2] == parsed.anchor_type
                        and key[3] == location_signature
                    ]
                    aliases = [key[1] for key in candidate_keys]
                    matched_alias = fuzzy_alias_match(
                        parsed.anchor_raw, aliases, threshold=0.9
                    )
                    if matched_alias:
                        matched_key = next(
                            key for key in candidate_keys if key[1] == matched_alias
                        )
                        anchor_id = dictionary_maps["anchors"][matched_key]
                        anchor_status = "fuzzy"
            issue_status = "unknown"
            issue_id = dictionary_maps["issues"].get(issue_name) if issue_name else None
            if issue_id:
                issue_status = "exact"
            elif issue_name and batch_type not in {
                "bootstrap_history",
                "bootstrap_compare",
            }:
                matched_issue = fuzzy_alias_match(
                    issue_name, list(dictionary_maps["issues"]), threshold=0.88
                )
                if matched_issue:
                    issue_id = dictionary_maps["issues"][matched_issue]
                    issue_status = "fuzzy"
            resolved_ids.append((street_id, anchor_id, issue_id))
            raw_json = item["raw_json"]
            record_values.append(
                {
                    "source_id": source_id,
                    "source_batch_id": batch_id,
                    "source_file_hash": file_hash,
                    "source_row": record.source_row,
                    "row_hash": _row_hash(record),
                    "data_source": source_type,
                    "work_order_id": record.work_order_id,
                    "received_at": _received_at(record.received_at),
                    "title_raw": record.title,
                    "title_normalized": parsed.normalized_title,
                    "title_noise_tokens": [],
                    "appeal_text": record.appeal_text,
                    "category_level_1": record.category_level_1,
                    "category_level_2": record.category_level_2,
                    "category_level_3": record.category_level_3,
                    "category_level_4": record.category_level_4,
                    "final_category": issue_name,
                    "department": _raw_value(raw_json, "所属部门")
                    or _raw_value(raw_json, "处理部门"),
                    "processing_region": _raw_value(raw_json, "处理部门所属区域"),
                    **item["phone"],
                    "address_raw": parsed.address_raw,
                    "address_line": parsed.address_line,
                    "region": parsed.region,
                    "street_id": street_id,
                    "street_raw": parsed.street,
                    "road": parsed.road,
                    "house_no": parsed.house_no,
                    "building": parsed.building,
                    "unit": parsed.unit,
                    "room": parsed.room,
                    "shop_no": parsed.shop_no,
                    "floor": parsed.floor,
                    "direction": parsed.direction,
                    "anchor_raw": parsed.anchor_raw,
                    "anchor_type": parsed.anchor_type,
                    "anchor_id": anchor_id,
                    "issue_id": issue_id,
                    "parse_source": parsed.parse_source,
                    "parse_confidence": parsed.parse_confidence,
                    "anchor_resolution_status": anchor_status,
                    "issue_resolution_status": issue_status,
                    "dictionary_version_id": dictionary_version_id,
                    "parser_version": PARSER_VERSION,
                    "raw_json": raw_json,
                }
            )

        record_ids = await self.repository.upsert_records(record_values)
        mentions: list[dict[str, Any]] = []
        for record_id, item, ids in zip(record_ids, parsed_rows, resolved_ids, strict=True):
            issue_id = ids[2]
            mentions.extend(
                [
                    {
                        "record_id": record_id,
                        "segment_no": segment.segment_no,
                        "text": segment.text,
                        "issue_id": issue_id if segment.segment_no == 1 else None,
                        "is_primary": segment.segment_no == 1,
                        "is_independent": segment.is_independent,
                    }
                    for segment in item["parsed"].issue_segments
                ]
            )
            await self.repository.add_previous_work_order_links(
                record_id, item["parsed"].previous_work_order_ids
            )
        await self.repository.add_issue_mentions_bulk(mentions)
        if extraction_run_id is not None:
            await self.repository.complete_dictionary_extraction_run(
                extraction_run_id,
                candidate_counts={
                    "streets": len(dictionary_maps["streets"]),
                    "anchors": len(dictionary_maps["anchors"]),
                    "issues": len(dictionary_maps["issues"]),
                    "unparsed_streets": sum(
                        1 for item in parsed_rows if not item["parsed"].street
                    ),
                    "unparsed_anchors": sum(
                        1 for item in parsed_rows if not item["parsed"].anchor_raw
                    ),
                },
            )

        await self.repository.set_batch_stage(batch_id, "reviewing", status="reviewing")
        return StagedCorpusBatch(batch_id, dictionary_version_id)

    async def approve_bootstrap(
        self, batch_id: str, dictionary_version_id: int, *, approved_by: str
    ) -> None:
        batch = await self.repository.get_batch(batch_id)
        if batch["batch_type"] not in {"bootstrap_history", "bootstrap_compare"}:
            raise ValueError("只有历史冷启动批次可以发布词典")
        if self.dictionary_review_required:
            await self.repository.publish_dictionary_version(
                dictionary_version_id, approved_by=approved_by
            )
        else:
            await self.repository.approve_dictionary_version(
                dictionary_version_id, approved_by=approved_by
            )
        records = await self.repository.records_for_batch(batch_id)
        await self._assign_exact_events(batch_id, frozen=True)
        await self.repository.assign_linked_records(batch_id)
        await self.repository.assign_strong_signal_records(batch_id)
        await self._assign_safe_singletons(records)
        await self.repository.commit_batch(batch_id)

    async def commit_increment(self, batch_id: str) -> None:
        batch = await self.repository.get_batch(batch_id)
        if batch["batch_type"] not in {"daily_increment", "correction"}:
            raise ValueError("该批次不是每日增量或补录批次")
        records = await self.repository.records_for_batch(batch_id)
        await self._assign_exact_events(batch_id, frozen=True)
        await self.repository.assign_linked_records(batch_id)
        await self.repository.assign_strong_signal_records(batch_id)
        await self._assign_safe_singletons(records)
        await self.repository.commit_batch(batch_id)

    async def _assign_exact_events(self, batch_id: str, *, frozen: bool) -> None:
        approved_ids = (
            await self.repository.approved_record_ids_for_batch(batch_id)
            if self.dictionary_review_required
            else None
        )
        rows = [
            {
                "record_id": int(row["id"]),
                "street_id": int(row["street_id"]),
                "anchor_id": int(row["anchor_id"]),
                "issue_id": int(row["issue_id"]),
                "event_name": _event_name(row),
                "received_at": row.get("received_at"),
            }
            for row in await self.repository.records_for_batch(batch_id)
            if (approved_ids is None or int(row["id"]) in approved_ids)
            and row["street_id"]
            and row["anchor_id"]
            and row["issue_id"]
            and row["anchor_resolution_status"] == "exact"
            and row["issue_resolution_status"] == "exact"
        ]
        await self.repository.bulk_assign_exact_events(
            rows, event_key_version=EVENT_KEY_VERSION, frozen=frozen
        )

    async def _assign_safe_singletons(self, records: list[dict[str, Any]]) -> None:
        draft_versions: dict[str, int] = {}
        assigned_ids = await self.repository.assigned_record_ids(
            [int(row["id"]) for row in records]
        )
        for row in records:
            if int(row["id"]) in assigned_ids:
                continue
            batch_id = str(row.get("source_batch_id") or "manual")
            version_id = draft_versions.get(batch_id)
            if version_id is None:
                version_id = await self.repository.create_dictionary_version(
                    f"delta-{batch_id}",
                    status="candidate",
                    source_record_count=sum(
                        1
                        for item in records
                        if str(item.get("source_batch_id") or "manual") == batch_id
                    ),
                )
                draft_versions[batch_id] = version_id
            street_name = row.get("street_raw") or "未知街道"
            region = row.get("region") or "未知地区"
            street_id = int(row["street_id"]) if row.get("street_id") else await self.repository.create_street(
                region,
                street_name,
                version_id,
                review_status="candidate",
            )
            anchor_name = (
                row.get("anchor_raw")
                or row.get("work_order_id")
                or row.get("title_normalized")
                or f"记录{row['id']}"
            )
            anchor_id = int(row["anchor_id"]) if row.get("anchor_id") else None
            if anchor_id is None:
                anchor_id = await self.repository.create_anchor(
                    street_id,
                    str(anchor_name),
                    row.get("anchor_type") or "unknown",
                    version_id,
                    review_status="candidate",
                    road=row.get("road"),
                    house_no=row.get("house_no"),
                    building=row.get("building"),
                    shop_no=row.get("shop_no"),
                    floor=row.get("floor"),
                    direction=row.get("direction"),
                )
            issue_name = row.get("final_category") or "未识别事项"
            issue_id = int(row["issue_id"]) if row.get("issue_id") else None
            if issue_id is None:
                issue_id = await self.repository.create_issue(
                    str(issue_name),
                    version_id,
                    review_status="candidate",
                    category_level_1=row.get("category_level_1"),
                    category_level_2=row.get("category_level_2"),
                    category_level_3=row.get("category_level_3"),
                    category_level_4=row.get("category_level_4"),
                )
            await self.repository.update_record_normalization(
                int(row["id"]),
                street_id=street_id,
                anchor_id=anchor_id,
                issue_id=issue_id,
                anchor_status="manual_singleton",
                issue_status="manual_singleton",
            )
            event_id = await self.repository.get_or_create_event(
                street_id=street_id,
                anchor_id=anchor_id,
                issue_id=issue_id,
                event_key_version=f"manual-{uuid.uuid4().hex}",
                event_name=_event_name(
                    {
                        **row,
                        "region": region,
                        "street_raw": street_name,
                        "anchor_raw": anchor_name,
                        "final_category": issue_name,
                    }
                ),
                is_frozen=True,
            )
            await self.repository.assign_record(
                event_id, int(row["id"]), source="manual_singleton"
            )


def _issue_name(record: InputRecord, normalized_title: str) -> str | None:
    context = " ".join(
        value for value in (normalized_title, record.appeal_text or "") if value
    )
    patterns = (
        (r"(?:物业费|物业收费|质价不符|违规收费|物业.*?价格)", "物业收费纠纷"),
        (r"(?:地下车库|停车场|车位).{0,12}(?:停车|收费|管理|无法进入)|乱停车", "停车场管理"),
        (r"(?:电梯|门禁|水管|漏水|渗水|公共设施).{0,12}(?:维修|故障|损坏)|物业维修", "物业维修"),
        (r"(?:拖欠工资|欠薪|工资未发|不发工资|克扣工资)", "拖欠工资"),
        (r"(?:路面积水|道路积水|雨天水浸|路面水浸)", "道路积水"),
        (r"(?:路面破损|道路破损|坑洼|路面不平)", "道路破损"),
        (r"(?:路灯不亮|路灯故障|照明灯不亮)", "路灯故障"),
        (r"(?:流动小贩|流动摊贩|违规摆卖|占道经营)", "流动摊贩占道"),
        (r"(?:施工噪声|施工扰民)", "施工噪声"),
        (r"(?:经营噪声|商铺噪声|店铺噪声|促销音响)", "经营噪声"),
        (r"(?:生活噪声|唱歌扰民|音响扰民)", "生活噪声"),
    )
    for pattern, issue_name in patterns:
        if re.search(pattern, context):
            return issue_name
    value = (
        record.category_level_4
        or record.category_level_3
        or record.category
        or normalized_title
    )
    text = str(value or "").strip()
    replacements = {
        "拖欠、克扣工资": "拖欠工资",
        "流动小贩": "流动摊贩占道",
        "路面积水": "道路积水",
    }
    return replacements.get(text, text) or None


def _event_name(row: dict[str, Any]) -> str:
    location_parts = [row.get("road"), row.get("house_no"), row.get("building")]
    location = "".join(
        str(value).strip()
        for value in location_parts
        if value and str(value).strip()
    )
    if row.get("shop_no"):
        location += f"商铺{str(row['shop_no']).strip()}"
    if row.get("floor"):
        location += str(row["floor"]).strip()
    anchor = str(row.get("anchor_raw") or "").strip()
    if location and location not in anchor:
        anchor = f"{location}{anchor}"
    parts = [
        row.get("region"),
        row.get("street_raw"),
        anchor,
        row.get("final_category"),
    ]
    return "｜".join(str(value).strip() for value in parts if value and str(value).strip())


def _row_hash(record: InputRecord) -> str:
    value = json.dumps(
        {
            "source_row": record.source_row,
            "work_order_id": record.work_order_id,
            "received_at": record.received_at,
            "title": record.title,
            "category": record.category,
            "appeal_text": record.appeal_text,
            "raw_fields": record.raw_fields,
        },
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _business_columns(records: list[InputRecord]) -> list[str]:
    for record in records:
        if record.raw_fields:
            return [str(value) for value in record.raw_fields.keys() if str(value) != "Unnamed: 37"]
    return []


def _raw_value(raw: dict[str, Any], name: str) -> str | None:
    normalized = name.replace(" ", "")
    for key, value in raw.items():
        if str(key).replace(" ", "") == normalized and value not in (None, ""):
            return str(value)
    return None


def _record_fallback_raw(record: InputRecord) -> dict[str, Any]:
    return {
        "工单编号": record.work_order_id,
        "受理时间": record.received_at,
        "诉求标题": record.title,
        "事项分类": record.category,
        "市民诉求": record.appeal_text,
    }


def _received_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        result = datetime.fromisoformat(str(value).strip())
    except ValueError:
        return None
    return result.replace(tzinfo=UTC) if result.tzinfo is None else result


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
