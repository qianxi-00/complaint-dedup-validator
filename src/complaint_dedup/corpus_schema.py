from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
)

from complaint_dedup.async_database import metadata


license_state = Table(
    "license_state",
    metadata,
    Column("id", Integer, primary_key=True, comment="单行状态记录主键，固定为 1"),
    Column("first_started_at", DateTime(timezone=True), nullable=False, comment="首次启动时间（上海时区）"),
    Column("expire_date", Date, comment="构建时烧录的授权到期日期"),
    Column("updated_at", DateTime(timezone=True), nullable=False, comment="授权状态最后更新时间"),
    Column("note", Text, comment="授权状态备注"),
    CheckConstraint("id = 1", name="ck_license_state_single_row"),
    comment="服务授权状态与首次启动时间记录",
)


work_orders = Table(
    "work_orders",
    metadata,
    Column("record_key", String(128), primary_key=True, comment="工单稳定键，有编号时为工单编号键，否则为复合指纹键"),
    Column("work_order_id", String(255), comment="原始工单编号"),
    Column("received_at", DateTime(timezone=True), comment="受理时间，按 UTC 存储并按上海时区展示"),
    Column("completed_at", DateTime(timezone=True), comment="办结时间，按 UTC 存储并按上海时区展示"),
    Column("title_raw", Text, comment="原始诉求标题"),
    Column("title_normalized", Text, comment="标准化后的诉求标题"),
    Column("appeal_text", Text, comment="市民诉求原文"),
    Column("category", Text, comment="事项分类展示值"),
    Column("processing_department", Text, comment="处理部门"),
    Column("department", Text, comment="所属部门"),
    Column("location", Text, comment="事发地点原文"),
    Column("region", String(255), comment="标准化地区"),
    Column("street", String(255), comment="标准化街道"),
    Column("anchor", Text, comment="标准化地点锚点"),
    Column("organization_subject", Text, comment="标准化企业或机构主体"),
    Column("issue_family", String(64), comment="企业主体问题族标识"),
    Column("event_key", String(1024), comment="确定性判重事件键"),
    Column("raw_json", JSON, nullable=False, default=dict, comment="本行原始字段 JSON"),
    Column("source_row", Integer, comment="原文件中的数据行号"),
    Column("missing_in_latest_upload", Boolean, nullable=False, default=False, comment="是否未出现在最近一次全量上传"),
    Column("last_sync_id", String(64), comment="最近一次处理该工单的同步记录 ID"),
    Column("created_at", DateTime(timezone=True), nullable=False, comment="工单首次进入全量库的时间"),
    Column("updated_at", DateTime(timezone=True), nullable=False, comment="工单结构化数据最后变更时间"),
    UniqueConstraint("work_order_id", name="uq_work_orders_work_order_id"),
    Index("ix_work_orders_received_at", "received_at"),
    Index("ix_work_orders_completed_at", "completed_at"),
    Index("ix_work_orders_missing", "missing_in_latest_upload"),
    comment="当前全量工单库，保存每个工单的最新结构化数据",
)


sync_runs = Table(
    "sync_runs",
    metadata,
    Column("id", String(64), primary_key=True, comment="同步记录 ID"),
    Column("file_name", String(255), nullable=False, comment="上传文件名"),
    Column("file_hash", String(64), nullable=False, comment="上传文件 SHA-256 哈希"),
    Column("status", String(32), nullable=False, default="running", comment="同步状态：running、completed 或 failed"),
    Column("total_rows", Integer, nullable=False, default=0, comment="文件总数据行数"),
    Column("inserted_rows", Integer, nullable=False, default=0, comment="新增工单数"),
    Column("updated_rows", Integer, nullable=False, default=0, comment="更新工单数"),
    Column("missing_rows", Integer, nullable=False, default=0, comment="本次新标记为缺失的工单数"),
    Column("error_message", Text, comment="同步失败错误信息"),
    Column("created_at", DateTime(timezone=True), nullable=False, comment="同步开始时间"),
    Column("completed_at", DateTime(timezone=True), comment="同步完成或失败时间"),
    comment="全量文件同步记录与统计",
)


work_order_versions = Table(
    "work_order_versions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True, comment="版本快照主键"),
    Column("sync_id", String(64), ForeignKey("sync_runs.id", ondelete="CASCADE"), nullable=False, comment="所属同步记录 ID"),
    Column("record_key", String(128), ForeignKey("work_orders.record_key", ondelete="CASCADE"), nullable=False, comment="工单稳定键"),
    Column("action", String(32), nullable=False, comment="变更动作：inserted、updated 或 missing"),
    Column("before_json", JSON, comment="变更前工单快照"),
    Column("after_json", JSON, nullable=False, comment="变更后工单快照"),
    Column("created_at", DateTime(timezone=True), nullable=False, comment="快照创建时间"),
    comment="工单同步前后版本审计快照",
)


comparison_runs = Table(
    "comparison_runs",
    metadata,
    Column("id", String(64), primary_key=True, comment="比对任务 ID"),
    Column("sync_id", String(64), ForeignKey("sync_runs.id", ondelete="RESTRICT"), nullable=False, comment="任务使用的全量同步记录 ID"),
    Column("time_field", String(32), nullable=False, comment="窗口字段：completed_at 或 received_at"),
    Column("target_from", DateTime(timezone=True), nullable=False, comment="待比对窗口开始时间"),
    Column("target_to", DateTime(timezone=True), nullable=False, comment="待比对窗口结束时间（开区间结束）"),
    Column("reference_from", DateTime(timezone=True), comment="手动被比对窗口开始时间"),
    Column("reference_to", DateTime(timezone=True), comment="手动被比对窗口结束时间（开区间结束）"),
    Column("reference_mode", String(32), nullable=False, default="complement", comment="被比对窗口模式：complement 或 manual"),
    Column("status", String(32), nullable=False, default="completed", comment="比对任务状态"),
    Column("target_count", Integer, nullable=False, default=0, comment="待比对工单数"),
    Column("reference_count", Integer, nullable=False, default=0, comment="被比对工单数"),
    Column("event_count", Integer, nullable=False, default=0, comment="事件总数"),
    Column("singleton_count", Integer, nullable=False, default=0, comment="单例事件数"),
    Column("missing_time_count", Integer, nullable=False, default=0, comment="窗口字段为空的工单数"),
    Column("created_at", DateTime(timezone=True), nullable=False, comment="比对任务创建时间"),
    comment="时间窗口比对任务及统计快照",
)


comparison_record_members = Table(
    "comparison_record_members",
    metadata,
    Column("comparison_id", String(64), ForeignKey("comparison_runs.id", ondelete="CASCADE"), primary_key=True, comment="所属比对任务 ID"),
    Column("record_key", String(128), primary_key=True, comment="工单稳定键"),
    Column("side", String(16), nullable=False, comment="任务侧：target 或 reference"),
    Column("snapshot_json", JSON, nullable=False, comment="任务创建时的工单快照"),
    Column("event_key", String(1024), comment="任务内判重事件键"),
    comment="比对任务内的工单成员和快照",
)


comparison_events = Table(
    "comparison_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True, comment="任务事件主键"),
    Column("comparison_id", String(64), ForeignKey("comparison_runs.id", ondelete="CASCADE"), nullable=False, comment="所属比对任务 ID"),
    Column("event_key", String(1024), nullable=False, comment="确定性事件键"),
    Column("event_name", Text, nullable=False, comment="事件展示名称"),
    Column("status", String(32), nullable=False, default="active", comment="事件状态"),
    Column("created_at", DateTime(timezone=True), nullable=False, comment="事件创建时间"),
    UniqueConstraint("comparison_id", "event_key", name="uq_comparison_event_key"),
    comment="比对任务内的判重事件",
)


comparison_event_members = Table(
    "comparison_event_members",
    metadata,
    Column("event_id", Integer, ForeignKey("comparison_events.id", ondelete="CASCADE"), primary_key=True, comment="所属事件 ID"),
    Column("record_key", String(128), primary_key=True, comment="工单稳定键"),
    Column("side", String(16), nullable=False, comment="任务侧：target 或 reference"),
    comment="判重事件与工单的成员关系",
)


work_order_cannot_links = Table(
    "work_order_cannot_links",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True, comment="禁止关系主键"),
    Column("left_record_key", String(128), ForeignKey("work_orders.record_key", ondelete="CASCADE"), nullable=False, comment="禁止共现关系左端工单键"),
    Column("right_record_key", String(128), ForeignKey("work_orders.record_key", ondelete="CASCADE"), nullable=False, comment="禁止共现关系右端工单键"),
    Column("reason", Text, nullable=False, comment="人工确认原因"),
    Column("created_at", DateTime(timezone=True), nullable=False, comment="禁止关系创建时间"),
    UniqueConstraint("left_record_key", "right_record_key", name="uq_work_order_cannot_link"),
    comment="人工确认的全局禁止共现关系",
)
