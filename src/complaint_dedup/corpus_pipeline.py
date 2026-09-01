from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo
from difflib import SequenceMatcher
from typing import Any

from complaint_dedup.corpus_dictionary import apply_audited_anchor_dictionary
from complaint_dedup.corpus_parser import (
    extract_organization_subject,
    extract_occurrence_identifiers,
    normalize_organization_name,
    normalize_phone,
    parse_complaint,
)
from complaint_dedup.corpus_normalizer import (
    anchor_location_signature,
    canonicalize_anchor_candidates,
    fuzzy_alias_match,
)
from complaint_dedup.corpus_models import InputRecord
from complaint_dedup.corpus_repository import CorpusRepository
from complaint_dedup.corpus_prompts import build_normalization_messages
from complaint_dedup.llm_models import NormalizationBatchResponse


PARSER_VERSION = "rules-v2"
EVENT_KEY_VERSION = "event-key-v2"
ENTERPRISE_EVENT_KEY_VERSION = "event-key-v3"


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
        normalization_llm_client: Any | None = None,
        normalization_llm_enabled: bool = False,
        normalization_llm_min_confidence: float = 0.9,
        normalization_llm_batch_size: int = 10,
    ) -> None:
        self.repository = repository
        self.dictionary_review_required = dictionary_review_required
        self.normalization_llm_client = normalization_llm_client
        self.normalization_llm_enabled = normalization_llm_enabled
        self.normalization_llm_min_confidence = normalization_llm_min_confidence
        self.normalization_llm_batch_size = normalization_llm_batch_size
        self._dictionary_update_lock = asyncio.Lock()

    async def stage_records(
        self,
        *,
        name: str,
        batch_type: str,
        file_name: str,
        file_hash: str,
        records: list[InputRecord],
        batch_id: str | None = None,
        source_type_override: str | None = None,
        dictionary_version_id_override: int | None = None,
    ) -> StagedCorpusBatch:
        if batch_type not in {
            "bootstrap_history",
            "bootstrap_compare",
            "daily_increment",
        }:
            raise ValueError("批次类型无效")
        existing_batch = None
        if batch_id is not None:
            existing_batch = await self.repository.get_batch(batch_id)
            if existing_batch["batch_type"] != batch_type:
                raise ValueError("批次类型与上传任务不一致")
        history_phase = (
            batch_type in {"bootstrap_history", "bootstrap_compare"}
            and source_type_override != "daily"
        )
        if history_phase:
            if existing_batch and existing_batch.get("dictionary_version_id"):
                dictionary_version_id = int(existing_batch["dictionary_version_id"])
            else:
                dictionary_version_id = await self.repository.create_dictionary_version(
                    f"dict-{file_hash[:12]}",
                    status="candidate",
                    source_record_count=len(records),
                )
            source_type = source_type_override or "history"
            candidate_status = "candidate"
            active_generation = None
        else:
            active_generation = await self.repository.active_generation()
            if active_generation is None:
                raise ValueError("尚未建立活动历史库")
            active_dictionary_id = active_generation.get("dictionary_version_id")
            active = (
                await self.repository.get_dictionary_version(int(active_dictionary_id))
                if active_dictionary_id is not None
                else None
            )
            if active is None:
                raise ValueError("活动历史库缺少标准项")
            dictionary_version_id = dictionary_version_id_override or int(active["id"])
            source_type = source_type_override or "daily"
            candidate_status = "approved"
            if batch_type == "daily_increment" and source_type_override is None:
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
                    raise ValueError(
                        "每日新增与活动历史库时间范围重叠，请重新建立历史库"
                    )

        if batch_id is None:
            batch_id = await self.repository.create_batch(
                name,
                batch_type,
                total_records=len(records),
                dictionary_version_id=dictionary_version_id,
            )
        generation = await self.repository.generation_for_batch(batch_id)
        if history_phase:
            if generation is None:
                generation_id = await self.repository.create_generation(
                    batch_id, dictionary_version_id
                )
            else:
                generation_id = int(generation["id"])
        else:
            generation_id = int(active_generation["id"])

        source_id = await self.repository.create_source(
            file_name=file_name,
            file_hash=file_hash,
            source_type=source_type,
            business_columns=_business_columns(records),
            row_count=len(records),
            generation_id=generation_id,
        )
        extraction_run_id = None
        if history_phase:
            extraction_run_id = await self.repository.start_dictionary_extraction_run(
                source_id=source_id,
                dictionary_version_id=dictionary_version_id,
                source_file_hash=file_hash,
                parser_version=PARSER_VERSION,
            )
        await self.repository.update_batch_setup(
            batch_id,
            total_records=len(records),
            dictionary_version_id=dictionary_version_id,
            generation_id=generation_id,
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
            occurrence_identifiers = extract_occurrence_identifiers(
                record.appeal_text
            )
            context = " ".join((
                value for value in (record.title, record.appeal_text) if value
            ))
            enterprise_family = _enterprise_issue_family(issue_name, context)
            organization_subject = parsed.organization_subject
            if enterprise_family and organization_subject:
                issue_name = enterprise_family
                occurrence_key = ""
            else:
                occurrence_key = _occurrence_key(
                    record,
                    issue_name=issue_name,
                    occurrence_identifiers=occurrence_identifiers,
                )
            parsed_rows.append(
                {
                    "record": record,
                    "parsed": parsed,
                    "issue_name": issue_name,
                    "enterprise_family": enterprise_family,
                    "organization_subject": organization_subject,
                    "occurrence_identifiers": occurrence_identifiers,
                    "occurrence_key": occurrence_key,
                    "phone": phone,
                    "raw_json": raw_json,
                }
            )

        if history_phase:
            dictionary_maps = await self.repository.ensure_dictionary_items(
                dictionary_version_id=dictionary_version_id,
                streets=[
                    (item["parsed"].region, item["parsed"].street)
                    for item in parsed_rows
                    if item["parsed"].street
                ],
                anchors=apply_audited_anchor_dictionary(
                    canonicalize_anchor_candidates(
                        _anchor_dictionary_item(item)
                        for item in parsed_rows
                        if item["parsed"].street and item["parsed"].anchor_raw
                    )
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
        for item in parsed_rows:
            record = item["record"]
            parsed = item["parsed"]
            issue_name = item["issue_name"]
            street_id = dictionary_maps["streets"].get((parsed.region, parsed.street))
            anchor_status = "unknown"
            anchor_id = None
            audited_anchor = apply_audited_anchor_dictionary(
                canonicalize_anchor_candidates([_anchor_dictionary_item(item)])
            )[0]
            location_signature = str(audited_anchor["location_signature"])
            lookup_anchor_type = str(audited_anchor["anchor_type"])
            lookup_names = (
                str(audited_anchor["alias"]),
                str(audited_anchor["canonical_name"]),
            )
            if street_id and parsed.anchor_raw:
                for lookup_name in dict.fromkeys(lookup_names):
                    anchor_id = dictionary_maps["anchors"].get(
                        (
                            street_id,
                            lookup_name,
                            lookup_anchor_type,
                            location_signature,
                        )
                    )
                    if anchor_id is not None:
                        break
                anchor_status = "exact" if anchor_id else "unknown"
                if anchor_id is None and not history_phase:
                    candidate_keys = [
                        key
                        for key in dictionary_maps["anchors"]
                        if key[0] == street_id
                        and key[2] == lookup_anchor_type
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
            elif issue_name and not history_phase:
                matched_issue = fuzzy_alias_match(
                    issue_name, list(dictionary_maps["issues"]), threshold=0.88
                )
                if matched_issue:
                    issue_id = dictionary_maps["issues"][matched_issue]
                    issue_status = "fuzzy"
            raw_json = item["raw_json"]
            record_values.append(
                {
                    "source_id": source_id,
                    "source_batch_id": batch_id,
                    "source_file_hash": file_hash,
                    "generation_id": generation_id,
                    "source_row": record.source_row,
                    "row_hash": _row_hash(record),
                    "occurrence_key": item["occurrence_key"],
                    "occurrence_identifiers": item["occurrence_identifiers"],
                    "data_source": record.data_source or source_type,
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
                    "processing_department": _raw_value(raw_json, "处理部门")
                    or _raw_value(raw_json, "所属部门"),
                    "completed_at": _completed_at(_raw_value(raw_json, "办结时间")),
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
                    "anchor_type": lookup_anchor_type,
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
        await self.repository.update_batch_record_count(
            batch_id, await self.repository.count_records_for_batch(batch_id)
        )
        if not history_phase:
            await self._apply_llm_normalization(
                parsed_rows=parsed_rows,
                record_values=record_values,
                record_ids=record_ids,
                dictionary_maps=dictionary_maps,
                dictionary_version_id=dictionary_version_id,
            )
            await self._register_new_exact_keys(
                parsed_rows=parsed_rows,
                record_values=record_values,
                record_ids=record_ids,
                dictionary_version_id=dictionary_version_id,
            )
        mentions: list[dict[str, Any]] = []
        previous_work_order_links: list[dict[str, Any]] = []
        for record_id, item, row in zip(
            record_ids, parsed_rows, record_values, strict=True
        ):
            issue_id = row.get("issue_id")
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
                ],
            )
            previous_work_order_links.append(
                {
                    "source_record_id": record_id,
                    "work_order_ids": item["parsed"].previous_work_order_ids,
                }
            )
        await self.repository.add_previous_work_order_links_bulk(previous_work_order_links)
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

    async def _register_new_exact_keys(
        self,
        *,
        parsed_rows: list[dict[str, Any]],
        record_values: list[dict[str, Any]],
        record_ids: list[int],
        dictionary_version_id: int,
    ) -> None:
        pending = [
            (record_id, item, row)
            for record_id, item, row in zip(
                record_ids, parsed_rows, record_values, strict=True
            )
            if item["parsed"].street
            and item["parsed"].anchor_raw
            and item.get("issue_name")
            and not (row.get("street_id") and row.get("anchor_id") and row.get("issue_id"))
        ]
        if not pending:
            return

        async with self._dictionary_update_lock:
            dictionary_maps = await self.repository.ensure_dictionary_items(
                dictionary_version_id=dictionary_version_id,
                streets=[
                    (item["parsed"].region, item["parsed"].street)
                    for _, item, _ in pending
                ],
                anchors=apply_audited_anchor_dictionary(
                    canonicalize_anchor_candidates(
                        _anchor_dictionary_item(item)
                        for _, item, _ in pending
                    )
                ),
                issues=[
                    {
                        "canonical_name": item["issue_name"],
                        "category_level_1": item["record"].category_level_1,
                        "category_level_2": item["record"].category_level_2,
                        "category_level_3": item["record"].category_level_3,
                        "category_level_4": item["record"].category_level_4,
                    }
                    for _, item, _ in pending
                ],
                review_status="approved",
                update_existing_evidence=False,
            )

        decisions: list[dict[str, Any]] = []
        for record_id, item, row in pending:
            parsed = item["parsed"]
            street_id = row.get("street_id") or dictionary_maps["streets"].get(
                (parsed.region, parsed.street)
            )
            if street_id is None:
                continue
            anchor_item = _anchor_dictionary_item({"parsed": parsed, **item})
            location_signature = str(anchor_item["location_signature"])
            lookup_anchor_type = str(anchor_item["anchor_type"])
            anchor_id = row.get("anchor_id")
            if anchor_id is None:
                for lookup_name in dict.fromkeys(
                    (
                        str(anchor_item.get("alias") or anchor_item["canonical_name"]),
                        str(anchor_item["canonical_name"]),
                    )
                ):
                    anchor_id = dictionary_maps["anchors"].get(
                        (
                            int(street_id),
                            lookup_name,
                            lookup_anchor_type,
                            location_signature,
                        )
                    )
                    if anchor_id is not None:
                        break
            issue_id = row.get("issue_id") or dictionary_maps["issues"].get(
                item["issue_name"]
            )
            if anchor_id is None or issue_id is None:
                continue
            decision: dict[str, Any] = {
                "record_id": int(record_id),
                "street_id": int(street_id),
                "reason": "当前批次新增标准项",
            }
            if row.get("anchor_id") is None:
                decision.update(
                    {
                        "anchor_id": int(anchor_id),
                        "anchor_status": "new_standard",
                        "anchor_attempted": True,
                        "anchor_raw": parsed.anchor_raw,
                        "anchor_method": "new_standard",
                        "anchor_confidence": 1.0,
                    }
                )
            if row.get("issue_id") is None:
                decision.update(
                    {
                        "issue_id": int(issue_id),
                        "issue_status": "new_standard",
                        "issue_attempted": True,
                        "issue_raw": item["issue_name"],
                        "issue_name": item["issue_name"],
                        "issue_method": "new_standard",
                        "issue_confidence": 1.0,
                    }
                )
            decisions.append(decision)
            row["street_id"] = int(street_id)
            row["anchor_id"] = int(anchor_id)
            row["issue_id"] = int(issue_id)
            if decision.get("anchor_id") is not None:
                row["anchor_resolution_status"] = "new_standard"
            if decision.get("issue_id") is not None:
                row["issue_resolution_status"] = "new_standard"

        await self.repository.apply_normalization_decisions(
            decisions,
            dictionary_version_id=dictionary_version_id,
        )

    async def _apply_llm_normalization(
        self,
        *,
        parsed_rows: list[dict[str, Any]],
        record_values: list[dict[str, Any]],
        record_ids: list[int],
        dictionary_maps: dict[str, dict],
        dictionary_version_id: int,
    ) -> None:
        if not self.normalization_llm_enabled or self.normalization_llm_client is None:
            return
        requests: list[dict[str, Any]] = []
        for record_id, item, row in zip(record_ids, parsed_rows, record_values, strict=True):
            parsed = item["parsed"]
            anchor_candidates = _normalization_anchor_candidates(
                dictionary_maps.get("anchors", {}),
                street_id=row.get("street_id"),
                anchor_type=parsed.anchor_type,
                location_signature=anchor_location_signature(
                    parsed.road,
                    parsed.house_no,
                    parsed.building,
                    parsed.direction,
                    parsed.shop_no,
                    parsed.floor,
                ),
                raw_value=parsed.anchor_raw,
            )
            issue_candidates = _normalization_issue_candidates(
                dictionary_maps.get("issues", {}), item.get("issue_name")
            )
            anchor_attempted = row.get("anchor_resolution_status") == "unknown" and bool(
                anchor_candidates
            )
            issue_attempted = row.get("issue_resolution_status") == "unknown" and bool(
                issue_candidates
            )
            if not anchor_attempted and not issue_attempted:
                continue
            requests.append(
                {
                    "record_id": str(record_id),
                    "anchor_raw": parsed.anchor_raw,
                    "anchor_candidates": anchor_candidates if anchor_attempted else [],
                    "issue_raw": item.get("issue_name"),
                    "issue_candidates": issue_candidates if issue_attempted else [],
                    "title": item["record"].title,
                    "address": parsed.address_line or parsed.address_raw,
                    "appeal": (item["record"].appeal_text or "")[:3000],
                    "category": item["record"].category,
                    "anchor_attempted": anchor_attempted,
                    "issue_attempted": issue_attempted,
                }
            )
        if not requests:
            return

        chunks = [
            requests[index : index + self.normalization_llm_batch_size]
            for index in range(0, len(requests), self.normalization_llm_batch_size)
        ]
        results = await asyncio.gather(
            *(self._normalize_chunk(chunk) for chunk in chunks),
            return_exceptions=True,
        )
        by_record = {str(value["record_id"]): value for value in requests}
        decisions: list[dict[str, Any]] = []
        for chunk, result in zip(chunks, results, strict=True):
            if isinstance(result, Exception):
                for request in chunk:
                    decisions.append(
                        _failed_normalization_decision(request, str(result))
                    )
                continue
            for decision in result.decisions:
                request = by_record.get(str(decision.record_id))
                if request is None:
                    continue
                decisions.append(
                    _accepted_normalization_decision(
                        request,
                        decision,
                        min_confidence=self.normalization_llm_min_confidence,
                    )
                )
            returned = {str(decision.record_id) for decision in result.decisions}
            for request in chunk:
                if str(request["record_id"]) not in returned:
                    decisions.append(
                        _failed_normalization_decision(request, "模型未返回该工单的归一化结果")
                    )
        unique_decisions = list(
            {int(value["record_id"]): value for value in decisions}.values()
        )
        await self.repository.apply_normalization_decisions(
            unique_decisions,
            dictionary_version_id=dictionary_version_id,
        )
        row_by_record_id = {
            int(record_id): row
            for record_id, row in zip(record_ids, record_values, strict=True)
        }
        for decision in unique_decisions:
            row = row_by_record_id[int(decision["record_id"])]
            if decision.get("anchor_id") is not None:
                row["anchor_id"] = int(decision["anchor_id"])
                row["anchor_resolution_status"] = "llm"
            if decision.get("issue_id") is not None:
                row["issue_id"] = int(decision["issue_id"])
                row["issue_resolution_status"] = "llm"
                row["final_category"] = decision.get("issue_name")

    async def _normalize_chunk(self, chunk: list[dict[str, Any]]) -> NormalizationBatchResponse:
        return await self.normalization_llm_client.chat_json(
            build_normalization_messages(chunk), NormalizationBatchResponse
        )

    async def approve_bootstrap(
        self,
        batch_id: str,
        dictionary_version_id: int,
        *,
        approved_by: str,
        defer_final_commit: bool = False,
    ) -> None:
        batch = await self.repository.get_batch(batch_id)
        if batch["batch_type"] not in {"bootstrap_history", "bootstrap_compare"}:
            raise ValueError("只有历史冷启动批次可以发布词典")
        await self.repository.approve_dictionary_version(
            dictionary_version_id, approved_by=approved_by
        )
        await self._assign_exact_events(batch_id, frozen=True)
        await self.repository.assign_linked_records(batch_id)
        await self.repository.assign_strong_signal_records(batch_id)
        await self._assign_safe_singletons(batch_id)
        await self.repository.commit_batch(batch_id)
        generation = await self.repository.generation_for_batch(batch_id)
        if generation is not None:
            await self.repository.activate_generation(int(generation["id"]))
        if defer_final_commit:
            await self.repository.set_batch_stage(
                batch_id, "awaiting_daily", status="awaiting_daily"
            )

    async def commit_increment(self, batch_id: str) -> None:
        batch = await self.repository.get_batch(batch_id)
        if batch["batch_type"] not in {"daily_increment", "bootstrap_compare"}:
            raise ValueError("该批次不是可提交的新增批次")
        await self._assign_exact_events(
            batch_id, frozen=True, data_source="daily"
        )
        await self.repository.assign_linked_records(
            batch_id, data_source="daily"
        )
        await self.repository.assign_strong_signal_records(
            batch_id, data_source="daily"
        )
        await self._assign_safe_singletons(batch_id, data_source="daily")
        await self.repository.commit_batch(batch_id)

    async def _assign_exact_events(
        self,
        batch_id: str,
        *,
        frozen: bool,
        data_source: str | None = None,
    ) -> None:
        batch = await self.repository.get_batch(batch_id)
        rows = [
            {
                "record_id": int(row["id"]),
                "street_id": int(row["street_id"]),
                "anchor_id": int(row["anchor_id"]),
                "issue_id": int(row["issue_id"]),
                "occurrence_key": str(row.get("occurrence_key") or ""),
                "anchor_type": str(row.get("anchor_type") or ""),
                "final_category": row.get("final_category"),
                "event_name": _event_name(row),
                "received_at": row.get("received_at"),
            }
            for row in await self.repository.records_for_exact_assignment(
                batch_id,
                data_source=data_source,
                approved_only=self.dictionary_review_required,
            )
        ]
        generation_id = (
            int(batch["generation_id"])
            if batch.get("generation_id") is not None
            else None
        )
        enterprise_rows = [
            row
            for row in rows
            if row["anchor_type"] == "organization"
            and _enterprise_issue_family(row.get("final_category"), "") is not None
        ]
        enterprise_ids = {int(row["record_id"]) for row in enterprise_rows}
        ordinary_rows = [
            row for row in rows if int(row["record_id"]) not in enterprise_ids
        ]
        for version, version_rows in (
            (ENTERPRISE_EVENT_KEY_VERSION, enterprise_rows),
            (EVENT_KEY_VERSION, ordinary_rows),
        ):
            await self.repository.bulk_assign_exact_events(
                version_rows,
                event_key_version=version,
                frozen=frozen,
                generation_id=generation_id,
            )

    async def _assign_safe_singletons(
        self, batch_id: str, *, data_source: str | None = None
    ) -> None:
        unresolved = await self.repository.unassigned_records_for_batch(
            batch_id, data_source=data_source
        )
        if not unresolved:
            return

        # A batch belongs to one generation. Keep the guard explicit so a
        # malformed input cannot mix singleton events across generations.
        generation_ids = {
            int(row["generation_id"])
            for row in unresolved
            if row.get("generation_id") is not None
        }
        if len(generation_ids) > 1:
            raise ValueError("单例事件不能跨历史代次批量创建")
        generation_id = next(iter(generation_ids), None)
        batch_id = str(unresolved[0].get("source_batch_id") or "manual")
        version_id = await self.repository.create_dictionary_version(
            f"delta-{batch_id}",
            status="candidate",
            source_record_count=len(unresolved),
        )
        singleton_rows = []
        for row in unresolved:
            street_name = row.get("street_raw") or "未知街道"
            region = row.get("region") or "未知地区"
            anchor_name = (
                row.get("anchor_raw")
                or row.get("work_order_id")
                or row.get("title_normalized")
                or f"记录{row['id']}"
            )
            issue_name = row.get("final_category") or "未识别事项"
            singleton_rows.append(
                {
                    "record_id": int(row["id"]),
                    "region": region,
                    "street_name": street_name,
                    "street_raw": street_name,
                    "street_id": int(row["street_id"]) if row.get("street_id") else None,
                    "anchor_name": str(anchor_name),
                    "anchor_raw": str(anchor_name),
                    "anchor_id": int(row["anchor_id"]) if row.get("anchor_id") else None,
                    "anchor_type": row.get("anchor_type") or "unknown",
                    "road": row.get("road"),
                    "house_no": row.get("house_no"),
                    "building": row.get("building"),
                    "shop_no": row.get("shop_no"),
                    "floor": row.get("floor"),
                    "direction": row.get("direction"),
                    "issue_name": str(issue_name),
                    "final_category": str(issue_name),
                    "issue_id": int(row["issue_id"]) if row.get("issue_id") else None,
                    "category_level_1": row.get("category_level_1"),
                    "category_level_2": row.get("category_level_2"),
                    "category_level_3": row.get("category_level_3"),
                    "category_level_4": row.get("category_level_4"),
                    "event_name": _event_name(
                        {
                            **row,
                            "region": region,
                            "street_raw": street_name,
                            "anchor_raw": anchor_name,
                            "final_category": issue_name,
                        }
                    ),
                    "received_at": row.get("received_at"),
                }
            )
        await self.repository.bulk_assign_safe_singletons(
            singleton_rows,
            dictionary_version_id=version_id,
            generation_id=generation_id,
        )


def _anchor_dictionary_item(item: dict[str, Any]) -> dict[str, Any]:
    parsed = item["parsed"]
    if item.get("enterprise_family") and item.get("organization_subject"):
        return {
            "street_key": (parsed.region, parsed.street),
            "canonical_name": normalize_organization_name(item["organization_subject"]),
            "anchor_type": "organization",
            "location_signature": "organization",
            "road": parsed.road,
            "house_no": parsed.house_no,
            "building": parsed.building,
            "shop_no": parsed.shop_no,
            "floor": parsed.floor,
            "direction": parsed.direction,
        }
    return {
        "street_key": (parsed.region, parsed.street),
        "canonical_name": parsed.anchor_raw,
        "anchor_type": parsed.anchor_type,
        "location_signature": anchor_location_signature(
            parsed.road,
            parsed.house_no,
            parsed.building,
            parsed.direction,
            parsed.shop_no,
            parsed.floor,
        ),
        "road": parsed.road,
        "house_no": parsed.house_no,
        "building": parsed.building,
        "shop_no": parsed.shop_no,
        "floor": parsed.floor,
        "direction": parsed.direction,
    }


def _occurrence_key(
    record: InputRecord,
    *,
    issue_name: str | None,
    occurrence_identifiers: list[str],
) -> str:
    if occurrence_identifiers:
        return occurrence_identifiers[0]
    context = " ".join(
        value for value in (record.title or "", record.appeal_text or "") if value
    )
    if not _requires_occurrence_scope(issue_name, context):
        return ""
    match = re.search(
        r"事项(?:[一二三四1-4])?\s*[:：]\s*(?P<facts>.*?)(?:\n\s*备注|$)",
        record.appeal_text or "",
        re.S,
    )
    facts = _compact_text(match.group("facts") if match else context)
    if len(facts) < 16:
        return ""
    return f"fact:{hashlib.sha256(facts.encode('utf-8')).hexdigest()[:32]}"


def _requires_occurrence_scope(issue_name: str | None, context: str) -> bool:
    if issue_name in {
        "停车收费争议",
        "停车场余位与入场",
        "停车场出入口管理",
        "停车场消防通道",
    }:
        return False
    return bool(
        re.search(
            r"(?:消费|订单|购买|退款|退费|售后|课程|会员|预付款|工资|欠薪|劳动纠纷)",
            f"{issue_name or ''} {context}",
        )
    )


def _issue_name(record: InputRecord, normalized_title: str) -> str | None:
    context = " ".join(
        value for value in (normalized_title, record.appeal_text or "") if value
    )
    patterns = (
        (
            r"(?:物业费|物业管理费|物业收费|质价不符|违规收费|物业.*?价格)",
            "物业收费纠纷",
        ),
        (
            r"(?:地下车库|停车场).{0,20}(?:消防疏散出口|消防出口|消防通道)",
            "停车场消防通道",
        ),
        (
            r"(?:停车场|车位|余位).{0,24}(?:剩余车位|余位|显示为?0|显示已满|无法入场|不能入场|无法进入|道闸)|"
            r"(?:剩余车位|余位|显示为?0|显示已满).{0,24}(?:停车场|无法入场|不能入场)",
            "停车场余位与入场",
        ),
        (
            r"(?:停车场|停车费|停车|停放).{0,24}(?:收费|被收|公示价格|多收|退回费用)|"
            r"(?:收费|公示价格|多收).{0,24}(?:停车场|停车费)",
            "停车收费争议",
        ),
        (
            r"(?:地下车库|停车场).{0,20}(?:出入口|入口|出口).{0,12}(?:开放|封闭|管理)|"
            r"(?:只开放|仅开放).{0,8}(?:一个|一处)(?:出入口|入口|出口)",
            "停车场出入口管理",
        ),
        (r"(?:斑马线|人行横道)", "人行横道设施"),
        (r"(?:地下车库|停车场|车位).{0,12}(?:停车|管理)|乱停车", "停车场管理"),
        (r"(?:电梯|门禁|水管|漏水|渗水|公共设施).{0,12}(?:维修|故障|损坏)|物业维修", "物业维修"),
        (
            r"(?:拖欠工资|欠薪|工资未发|不发工资|拒绝发放工资|不予发放工资|"
            r"未结算工资|拒发工资|克扣工资)",
            "拖欠工资",
        ),
        (
            r"(?:(?:路面|道路|路段|辅路|辅道|匝道|高架桥|桥底|门口|门前|上桥位)"
            r".{0,24}(?:积水|水浸)|(?:积水|水浸).{0,24}"
            r"(?:路面|道路|路段|辅路|辅道|匝道|高架桥|桥底|门口|门前|上桥位)|"
            r"雨天水浸|下雨.{0,8}水浸)",
            "道路积水",
        ),
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
            return [
                str(value)
                for value in record.raw_fields.keys()
                if str(value).strip() != "Unnamed: 37"
            ]
    return []


def _normalization_anchor_candidates(
    mapping: dict[tuple[Any, ...], int],
    *,
    street_id: int | None,
    anchor_type: str | None,
    location_signature: str,
    raw_value: str | None,
) -> list[dict[str, Any]]:
    if not street_id:
        return []
    rows: dict[int, dict[str, Any]] = {}
    scoped = [
        (key, value)
        for key, value in mapping.items()
        if key[0] == street_id
        and (not anchor_type or key[2] == anchor_type)
    ]
    exact_scope = [item for item in scoped if item[0][3] == location_signature]
    pool = exact_scope or scoped
    for key, value in pool:
        rows.setdefault(
            int(value),
            {
                "id": int(value),
                "name": str(key[1]),
                "anchor_type": str(key[2]),
                "location_signature": str(key[3]),
            },
        )
    return _rank_candidates(raw_value, list(rows.values()), key="name")[:12]


def _normalization_issue_candidates(
    mapping: dict[str, int], raw_value: str | None
) -> list[dict[str, Any]]:
    rows: dict[int, dict[str, Any]] = {}
    for alias, value in mapping.items():
        rows.setdefault(int(value), {"id": int(value), "name": str(alias)})
    return _rank_candidates(raw_value, list(rows.values()), key="name")[:16]


def _rank_candidates(
    raw_value: str | None,
    candidates: list[dict[str, Any]],
    *,
    key: str,
) -> list[dict[str, Any]]:
    query = _compact_text(raw_value)
    if not query:
        return candidates[:]
    return sorted(
        candidates,
        key=lambda value: SequenceMatcher(
            None, query, _compact_text(str(value.get(key) or ""))
        ).ratio(),
        reverse=True,
    )


def _accepted_normalization_decision(
    request: dict[str, Any],
    decision: Any,
    *,
    min_confidence: float,
) -> dict[str, Any]:
    anchor_by_id = {
        int(value["id"]): value for value in request.get("anchor_candidates", [])
    }
    issue_by_id = {
        int(value["id"]): value for value in request.get("issue_candidates", [])
    }
    anchor_id = (
        int(decision.anchor_id)
        if decision.anchor_id in anchor_by_id
        and decision.anchor_confidence >= min_confidence
        else None
    )
    issue_id = (
        int(decision.issue_id)
        if decision.issue_id in issue_by_id
        and decision.issue_confidence >= min_confidence
        else None
    )
    return {
        "record_id": int(request["record_id"]),
        "anchor_id": anchor_id,
        "issue_id": issue_id,
        "issue_name": issue_by_id.get(issue_id, {}).get("name") if issue_id else None,
        "anchor_raw": request.get("anchor_raw"),
        "issue_raw": request.get("issue_raw"),
        "anchor_attempted": request.get("anchor_attempted", False),
        "issue_attempted": request.get("issue_attempted", False),
        "anchor_confidence": float(decision.anchor_confidence),
        "issue_confidence": float(decision.issue_confidence),
        "anchor_method": "llm" if anchor_id is not None else "llm_rejected",
        "issue_method": "llm" if issue_id is not None else "llm_rejected",
        "reason": str(decision.reason or ""),
    }


def _failed_normalization_decision(
    request: dict[str, Any], reason: str
) -> dict[str, Any]:
    return {
        "record_id": int(request["record_id"]),
        "anchor_raw": request.get("anchor_raw"),
        "issue_raw": request.get("issue_raw"),
        "anchor_attempted": request.get("anchor_attempted", False),
        "issue_attempted": request.get("issue_attempted", False),
        "anchor_confidence": 0,
        "issue_confidence": 0,
        "anchor_method": "llm_failed",
        "issue_method": "llm_failed",
        "reason": reason,
    }


def _compact_text(value: str | None) -> str:
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]", "", str(value or "")).casefold()


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


def _completed_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        result = datetime.fromisoformat(str(value).strip())
    except ValueError:
        return None
    if result.tzinfo is None:
        return result.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return result

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
_ENTERPRISE_ISSUE_FAMILIES = (
    ("食品安全", (r"食品", r"餐饮", r"变质", r"异物", r"过期")),
    ("拖欠工资", (r"欠薪", r"拖欠工资", r"劳动报酬", r"工资")),
    ("产品质量", (r"产品质量", r"产品开裂", r"产品故障", r"质量")),
)
def _enterprise_issue_family(issue_name: str | None, context: str | None) -> str | None:
    if issue_name:
        for family, patterns in _ENTERPRISE_ISSUE_FAMILIES:
            if any(re.search(pattern, issue_name, re.I) for pattern in patterns):
                return family
    text = f"{issue_name or ''} {context or ''}"
    for family, patterns in _ENTERPRISE_ISSUE_FAMILIES:
        if any(re.search(pattern, text, re.I) for pattern in patterns):
            return family
    return None


_extract_organization_subject = extract_organization_subject
_normalize_organization_name = normalize_organization_name
