# 全量工单库与时间窗口比对系统设计

## 目标

系统改为“单份全量 Excel 同步 + 可选时间窗口比对”。不再上传历史表和每日新增表，也不再使用账号隔离、历史代次滚动或批次审批流程。

## 数据模型

- `work_orders` 保存当前全量工单的最新结构化字段、原始字段、标准化结果和 `missing_in_latest_upload` 标记。
- `sync_runs` 保存每次全量文件同步的摘要、文件哈希、状态和错误信息。
- `work_order_versions` 保存同步前后字段快照，支持审计和回滚。
- `comparison_runs` 保存字段选择、待比对窗口、被比对窗口、状态和统计快照。
- `comparison_record_members` 保存工单属于哪一侧及其任务内快照。
- `comparison_events` 与 `comparison_event_members` 保存任务内独立的判重结果。
- `work_order_cannot_links` 保持为全局人工禁止关系，所有端点必须属于当前任务快照才可应用。

旧 `daily_batches`、`corpus_generations`、`batch_records`、历史滚动、审批状态和 `history_rebuild` 不再参与运行。

## 同步流程

上传一份全量文件后先落盘并创建 `sync_runs`。解析每行：有工单编号时按编号更新；无编号时按标准化复合指纹更新；同一文件重复键按最后一行生效。新增记录插入，已存在记录覆盖可变字段，文件缺失的旧工单保留并标记缺失。同步完成后才允许启动比对。

## 时间窗口

用户选择“办结时间”或“受理时间”，默认办结时间。待比对窗口默认取上传文件该字段的最大自然日；被比对窗口默认取全量有效工单中该窗口的补集。手动被比对窗口必须与待比对窗口不重叠。空日期记录默认进入待比对侧并单独统计。两侧联合执行确定性判重，禁止传递合并。

## 服务与部署

继续使用 FastAPI、SQLAlchemy Async、Jinja2/HTMX、XlsxWriter、PostgreSQL 和 Docker。授权到期检查、上海时区、时钟回拨保护、PyArmor 和外部 OpenAI 兼容客户端配置保留。当前确定性判重不依赖 LLM；后续如启用，LLM 只能做候选归一化，不能直接决定合并。

## 验证

覆盖同步新增/覆盖/缺失、窗口补集与重叠拒绝、空日期、跨任务快照、`cannot_links` 隔离、导出和 Web 界面。以 SQLite 回归后再做 PostgreSQL/Docker 自测。
