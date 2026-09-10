"""Add persisted dedup features and automated decision audit."""

import json

import sqlalchemy as sa
from alembic import op

from complaint_dedup.async_database import metadata
from complaint_dedup import corpus_schema  # noqa: F401

revision = "20260910_0003"
down_revision = "20260909_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    work_order_columns = {
        column["name"] for column in inspector.get_columns("work_orders")
    }
    features = (
        (
            "canonical_work_order_id",
            sa.Column(
                "canonical_work_order_id",
                sa.String(255),
                comment="去除 HBD 等转派后缀后的基础工单编号",
            ),
        ),
        (
            "complaint_fingerprint",
            sa.Column(
                "complaint_fingerprint",
                sa.String(64),
                comment="标题、诉求和地点的完整内容指纹",
            ),
        ),
        (
            "feature_json",
            sa.Column(
                "feature_json",
                sa.JSON(),
                comment="主体、地址、事项、发生对象等判重特征 JSON",
            ),
        ),
        (
            "feature_version",
            sa.Column(
                "feature_version",
                sa.String(32),
                comment="判重特征规则版本",
            ),
        ),
    )
    for name, column in features:
        if name not in work_order_columns:
            op.add_column("work_orders", column)

    comparison_columns = {
        column["name"] for column in inspector.get_columns("comparison_runs")
    }
    comparison_fields = (
        (
            "algorithm_version",
            sa.Column(
                "algorithm_version",
                sa.String(64),
                nullable=False,
                server_default="event-key-v3",
                comment="本次任务使用的判重算法版本",
            ),
        ),
        (
            "feature_version",
            sa.Column(
                "feature_version",
                sa.String(32),
                nullable=False,
                server_default="feature-v2",
                comment="本次任务使用的判重特征版本",
            ),
        ),
        (
            "prompt_version",
            sa.Column(
                "prompt_version",
                sa.String(64),
                nullable=False,
                server_default="event-card-v1",
                comment="事件卡提示词版本",
            ),
        ),
        (
            "model_id",
            sa.Column(
                "model_id",
                sa.String(255),
                comment="本次任务配置的事件卡模型标识",
            ),
        ),
        (
            "llm_coverage",
            sa.Column(
                "llm_coverage",
                sa.Float(),
                nullable=False,
                server_default="0",
                comment="进入事件卡模型裁决的工单覆盖率",
            ),
        ),
        (
            "fallback_count",
            sa.Column(
                "fallback_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
                comment="模型失败或未覆盖时的规则降级数",
            ),
        ),
        (
            "decision_count",
            sa.Column(
                "decision_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
                comment="持久化的自动合并决策数",
            ),
        ),
    )
    for name, column in comparison_fields:
        if name not in comparison_columns:
            op.add_column("comparison_runs", column)

    _backfill_work_order_features(bind)
    metadata.tables["comparison_decisions"].create(bind, checkfirst=True)
    _ensure_index("work_orders", "ix_work_orders_canonical_id", ["canonical_work_order_id"])
    _ensure_index(
        "work_orders",
        "ix_work_orders_complaint_fingerprint",
        ["complaint_fingerprint"],
    )
    _ensure_index(
        "work_orders",
        "ix_work_orders_feature_version",
        ["feature_version"],
    )
    if bind.dialect.name == "postgresql":
        _apply_comments()


def downgrade() -> None:
    bind = op.get_bind()
    metadata.tables["comparison_decisions"].drop(bind, checkfirst=True)
    inspector = sa.inspect(bind)
    indexes = {
        index["name"] for index in inspector.get_indexes("work_orders")
    }
    for name in (
        "ix_work_orders_feature_version",
        "ix_work_orders_complaint_fingerprint",
        "ix_work_orders_canonical_id",
    ):
        if name in indexes:
            op.drop_index(name, table_name="work_orders")
    inspector = sa.inspect(bind)
    comparison_columns = {
        column["name"] for column in inspector.get_columns("comparison_runs")
    }
    for name in (
        "decision_count",
        "fallback_count",
        "llm_coverage",
        "model_id",
        "prompt_version",
        "feature_version",
        "algorithm_version",
    ):
        if name in comparison_columns:
            op.drop_column("comparison_runs", name)
    work_order_columns = {
        column["name"] for column in inspector.get_columns("work_orders")
    }
    for name in ("feature_version", "feature_json", "complaint_fingerprint", "canonical_work_order_id"):
        if name in work_order_columns:
            op.drop_column("work_orders", name)


def _ensure_index(table: str, name: str, columns: list[str]) -> None:
    bind = op.get_bind()
    indexes = {index["name"] for index in sa.inspect(bind).get_indexes(table)}
    if name not in indexes:
        op.create_index(name, table, columns)


def _backfill_work_order_features(bind) -> None:
    from complaint_dedup.corpus_models import InputRecord
    from complaint_dedup.corpus_schema import work_orders
    from complaint_dedup.full_corpus import _normalize_record

    rows = (
        bind.execute(
            sa.select(work_orders).where(
                sa.or_(
                    work_orders.c.canonical_work_order_id.is_(None),
                    work_orders.c.complaint_fingerprint.is_(None),
                    work_orders.c.feature_json.is_(None),
                    work_orders.c.feature_version.is_(None),
                )
            )
        )
        .mappings()
        .all()
    )
    for row in rows:
        raw_json = row.get("raw_json") or {}
        if isinstance(raw_json, str):
            try:
                raw_json = json.loads(raw_json)
            except json.JSONDecodeError:
                raw_json = {}
        payload = _normalize_record(
            InputRecord(
                source_row=int(row.get("source_row") or 0),
                work_order_id=row.get("work_order_id"),
                title=row.get("title_raw"),
                category=row.get("category"),
                appeal_text=row.get("appeal_text"),
                received_at=row.get("received_at"),
                completed_at=row.get("completed_at"),
                location=row.get("location"),
                processing_department=row.get("processing_department"),
                raw_fields=raw_json,
            )
        )
        bind.execute(
            sa.update(work_orders)
            .where(work_orders.c.record_key == row["record_key"])
            .values(
                canonical_work_order_id=payload["canonical_work_order_id"],
                complaint_fingerprint=payload["complaint_fingerprint"],
                feature_json=payload["feature_json"],
                feature_version=payload["feature_version"],
            )
        )


def _apply_comments() -> None:
    comments = {
        ("work_orders", "canonical_work_order_id"): "去除 HBD 等转派后缀后的基础工单编号",
        ("work_orders", "complaint_fingerprint"): "标题、诉求和地点的完整内容指纹",
        ("work_orders", "feature_json"): "主体、地址、事项、发生对象等判重特征 JSON",
        ("work_orders", "feature_version"): "判重特征规则版本",
        ("comparison_runs", "algorithm_version"): "本次任务使用的判重算法版本",
        ("comparison_runs", "feature_version"): "本次任务使用的判重特征版本",
        ("comparison_runs", "prompt_version"): "事件卡提示词版本",
        ("comparison_runs", "model_id"): "本次任务配置的事件卡模型标识",
        ("comparison_runs", "llm_coverage"): "进入事件卡模型裁决的工单覆盖率",
        ("comparison_runs", "fallback_count"): "模型失败或未覆盖时的规则降级数",
        ("comparison_runs", "decision_count"): "持久化的自动合并决策数",
    }
    for (table, column), comment in comments.items():
        op.execute(
            sa.text(
                f'COMMENT ON COLUMN "{table}"."{column}" IS {_sql_literal(comment)}'
            )
        )
    table = metadata.tables["comparison_decisions"]
    op.execute(
        sa.text(
            f'COMMENT ON TABLE "comparison_decisions" IS {_sql_literal(table.comment)}'
        )
    )
    for column in table.columns:
        if column.comment:
            op.execute(
                sa.text(
                    f'COMMENT ON COLUMN "comparison_decisions"."{column.name}" '
                    f"IS {_sql_literal(column.comment)}"
                )
            )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
