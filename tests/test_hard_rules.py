from complaint_dedup.hard_rules import detect_hard_conflicts
from complaint_dedup.async_database import AsyncDatabase
from complaint_dedup.async_pipeline import apply_hard_rules_to_pairs
import pytest


def test_different_transaction_ids_are_hard_conflict() -> None:
    conflicts = detect_hard_conflicts(
        {"event": {"transaction_id": "ORDER-001"}},
        {"event": {"transaction_id": "ORDER-002"}},
    )

    assert conflicts == ["different_transaction"]


def test_different_exact_house_numbers_are_hard_conflict() -> None:
    conflicts = detect_hard_conflicts(
        {
            "address": {
                "precision": "exact",
                "road": "金瓯路",
                "house_no": "188号",
            }
        },
        {
            "address": {
                "precision": "exact",
                "road": "金瓯路",
                "house_no": "288号",
            }
        },
    )

    assert conflicts == ["different_house_no"]


def test_missing_or_coarse_addresses_do_not_create_hard_conflict() -> None:
    conflicts = detect_hard_conflicts(
        {"address": {"precision": "coarse", "road": "金瓯路"}},
        {"address": {"precision": "exact", "road": "江海路", "house_no": "1号"}},
    )

    assert conflicts == []


def test_different_high_confidence_legal_subjects_are_hard_conflict() -> None:
    conflicts = detect_hard_conflicts(
        {"subject": {"full_name": "甲建设有限公司", "confidence": 0.98}},
        {"subject": {"full_name": "乙餐饮有限公司", "confidence": 0.97}},
    )

    assert conflicts == ["different_subject"]


def test_same_brand_with_different_explicit_branches_is_hard_conflict() -> None:
    conflicts = detect_hard_conflicts(
        {"subject": {"brand": "甲连锁", "branch": "江海店", "confidence": 0.96}},
        {"subject": {"brand": "甲连锁", "branch": "蓬江店", "confidence": 0.95}},
    )

    assert conflicts == ["different_branch"]


def test_different_numbered_objects_and_unrelated_issue_families_are_hard_conflicts() -> None:
    conflicts = detect_hard_conflicts(
        {"event": {"object": "1号电梯"}, "issues": {"primary": "拖欠工资"}},
        {"event": {"object": "2号电梯"}, "issues": {"primary": "夜间施工噪音"}},
    )

    assert conflicts == ["different_object", "different_primary_issue"]


def test_ambiguous_subject_object_and_related_issue_wording_are_not_hard_conflicts() -> None:
    conflicts = detect_hard_conflicts(
        {
            "subject": {"full_name": "甲公司", "confidence": 0.6},
            "event": {"object": "车辆"},
            "issues": {"primary": "拖欠工资"},
        },
        {
            "subject": {"full_name": "甲有限公司", "confidence": 0.99},
            "event": {"object": "摩托车辆"},
            "issues": {"primary": "欠薪"},
        },
    )

    assert conflicts == []


def test_shared_subject_alias_prevents_high_confidence_subject_conflict() -> None:
    conflicts = detect_hard_conflicts(
        {
            "subject": {
                "full_name": "甲城市建设有限公司",
                "keys": ["甲城建"],
                "confidence": 0.99,
            }
        },
        {
            "subject": {
                "full_name": "甲城建集团有限公司",
                "keys": ["甲城建"],
                "confidence": 0.99,
            }
        },
    )

    assert conflicts == []


@pytest.mark.asyncio
async def test_pipeline_marks_hard_conflict_without_sending_to_llm(tmp_path) -> None:
    database = AsyncDatabase(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    await database.initialize()
    await database.enqueue_job("job-1", "硬规则", mode="single", total_records=2)
    left, right = await database.add_records(
        "job-1",
        [
            {"source": "S", "source_row": 2, "title": "甲"},
            {"source": "S", "source_row": 3, "title": "乙"},
        ],
    )
    await database.upsert_candidate_pairs(
        "job-1",
        [{"record_a_id": left, "record_b_id": right, "recall_reason": "vector"}],
    )
    await database.save_extractions(
        "job-1",
        {
            left: {"address": {"precision": "exact", "road": "金瓯路", "house_no": "188号"}},
            right: {"address": {"precision": "exact", "road": "金瓯路", "house_no": "288号"}},
        },
    )

    await apply_hard_rules_to_pairs(database, "job-1")
    pair = (await database.list_candidate_pairs("job-1"))[0]

    assert pair["rule_status"] == "excluded"
    assert pair["judgement_status"] == "succeeded"
    assert pair["llm_decision"] == "not_duplicate"
    assert pair["hard_conflicts_json"] == ["different_house_no"]
    await database.close()
