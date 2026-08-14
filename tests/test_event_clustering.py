import pytest

from complaint_dedup.event_clustering import (
    CandidateEdge,
    can_auto_merge_event,
    build_constrained_components,
    build_initial_event_signature,
    normalize_event_title,
)
from complaint_dedup.llm_models import (
    EventClusterResponse,
    validate_event_cluster_coverage,
)


def test_group_response_accepts_events_members_and_outliers() -> None:
    response = EventClusterResponse.model_validate(
        {
            "events": [
                {
                    "temporary_id": "E1",
                    "name": "江海区｜江南街道｜保利大都汇南门｜流动摊贩占道经营",
                    "confidence": 0.96,
                    "evidence": ["地点和核心问题一致"],
                    "members": [
                        {"record_id": "1", "confidence": 0.97, "role": "anchor"},
                        {"record_id": "2", "confidence": 0.95, "role": "member"},
                    ],
                }
            ],
            "outliers": ["3"],
        }
    )

    assert response.events[0].members[0].role == "anchor"
    assert response.outliers == ["3"]


def test_group_response_coverage_requires_every_input_exactly_once() -> None:
    response = EventClusterResponse.model_validate(
        {
            "events": [
                {
                    "temporary_id": "E1",
                    "name": "事件一",
                    "confidence": 0.8,
                    "evidence": [],
                    "members": [
                        {"record_id": "1", "confidence": 0.8, "role": "member"},
                        {"record_id": "1", "confidence": 0.7, "role": "member"},
                    ],
                }
            ],
            "outliers": ["4"],
        }
    )

    with pytest.raises(ValueError, match="duplicate.*1"):
        validate_event_cluster_coverage(response, ["1", "2", "3"])


def test_group_response_coverage_rejects_missing_and_unknown_ids() -> None:
    response = EventClusterResponse.model_validate(
        {
            "events": [
                {
                    "temporary_id": "E1",
                    "name": "事件一",
                    "confidence": 0.8,
                    "evidence": [],
                    "members": [{"record_id": "1", "confidence": 0.8, "role": "member"}],
                }
            ],
            "outliers": ["9"],
        }
    )

    with pytest.raises(ValueError, match="missing.*2.*unknown.*9"):
        validate_event_cluster_coverage(response, ["1", "2"])


def test_group_response_coverage_rejects_duplicate_expected_ids() -> None:
    response = EventClusterResponse.model_validate(
        {
            "events": [],
            "outliers": ["1"],
        }
    )

    with pytest.raises(ValueError, match="expected record ids contain duplicates.*1"):
        validate_event_cluster_coverage(response, ["1", "1"])


def test_constrained_components_merge_edges_by_descending_weight() -> None:
    edges = [
        CandidateEdge(1, 2, weight=0.95),
        CandidateEdge(2, 3, weight=0.90),
        CandidateEdge(3, 4, weight=0.80),
    ]

    components = build_constrained_components(
        [1, 2, 3, 4],
        edges,
        cannot_links={(1, 3)},
        max_size=20,
    )

    assert components == [[1, 2], [3, 4]]


def test_hard_conflict_edge_becomes_cannot_link() -> None:
    edges = [
        CandidateEdge(1, 2, weight=0.99, hard_conflicts=("different_house_no",)),
        CandidateEdge(2, 3, weight=0.90),
        CandidateEdge(1, 3, weight=0.91),
    ]

    components = build_constrained_components([1, 2, 3], edges, max_size=20)

    assert components == [[1, 3], [2]]


def test_component_size_limit_prevents_oversized_merge() -> None:
    edges = [
        CandidateEdge(1, 2, weight=0.99),
        CandidateEdge(3, 4, weight=0.98),
        CandidateEdge(2, 3, weight=0.97),
    ]

    components = build_constrained_components([1, 2, 3, 4], edges, max_size=3)

    assert components == [[1, 2], [3, 4]]


def test_explicit_cannot_link_blocks_transitive_merge_after_components_form() -> None:
    edges = [
        CandidateEdge(1, 2, weight=0.99),
        CandidateEdge(3, 4, weight=0.98),
        CandidateEdge(2, 3, weight=0.97),
    ]

    components = build_constrained_components(
        [1, 2, 3, 4],
        edges,
        cannot_links={(1, 4)},
        max_size=20,
    )

    assert components == [[1, 2], [3, 4]]


def test_event_title_normalization_removes_scope_and_generic_prefixes() -> None:
    title = "【江海区】江南街道关于保利大都汇南门流动摊贩占道经营的投诉"

    assert normalize_event_title(title, region="江海区", street="江南街道") == "保利大都汇南门流动摊贩占道经营"


def test_initial_event_signature_uses_location_fallback_when_subject_missing() -> None:
    signature = build_initial_event_signature(
        region="江海区",
        street="江南街道",
        subject=None,
        location="保利大都汇南门",
        core_issue="流动摊贩占道经营",
    )

    assert signature == "江海区｜江南街道｜保利大都汇南门｜流动摊贩占道经营"


def test_auto_merge_requires_closed_evidence_and_high_member_confidence() -> None:
    event = EventClusterResponse.model_validate(
        {
            "events": [{
                "temporary_id": "E1",
                "name": "江海区｜江南街道｜保利大都汇南门｜流动摊贩占道经营",
                "confidence": 0.97,
                "evidence": ["主体、实际地点和核心问题一致"],
                "members": [
                    {"record_id": "1", "confidence": 0.97, "role": "anchor"},
                    {"record_id": "2", "confidence": 0.96, "role": "member"},
                ],
            }],
            "outliers": [],
        }
    ).events[0]

    assert can_auto_merge_event(
        event,
        threshold=0.95,
        max_members=20,
        cannot_links=set(),
        extractions={"1": {"issues": {"secondary": []}}, "2": {"issues": {"secondary": []}}},
        supporting_edges={("1", "2")},
    )


@pytest.mark.parametrize(
    "override",
    [
        {"event_confidence": 0.94},
        {"member_confidence": 0.94},
        {"cannot_links": {frozenset(("1", "2"))}},
        {"secondary_issue": True},
        {"supporting_edges": set()},
    ],
)
def test_auto_merge_rejects_any_failed_gate(override: dict) -> None:
    event = EventClusterResponse.model_validate(
        {
            "events": [{
                "temporary_id": "E1",
                "name": "江海区｜江南街道｜甲公司｜拖欠工资",
                "confidence": override.get("event_confidence", 0.97),
                "evidence": ["主体、实际地点和核心问题一致"],
                "members": [
                    {"record_id": "1", "confidence": 0.97, "role": "anchor"},
                    {"record_id": "2", "confidence": override.get("member_confidence", 0.96), "role": "member"},
                ],
            }],
            "outliers": [],
        }
    ).events[0]
    secondary = ["新增独立问题"] if override.get("secondary_issue") else []

    assert not can_auto_merge_event(
        event,
        threshold=0.95,
        max_members=20,
        cannot_links=override.get("cannot_links", set()),
        extractions={"1": {"issues": {"secondary": secondary}}, "2": {"issues": {"secondary": []}}},
        supporting_edges=override.get("supporting_edges", {("1", "2")}),
    )
