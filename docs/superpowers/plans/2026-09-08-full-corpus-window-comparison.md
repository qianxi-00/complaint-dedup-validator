# 全量工单库与时间窗口比对系统实施计划

> **For agentic workers:** 按任务顺序执行，先写失败测试，再写最小实现。

**Goal:** 将旧历史/每日新增判重流程重构为全量工单库同步与独立时间窗口比对任务。

**Architecture:** 以 `work_orders` 为当前库，以 `sync_runs`/`work_order_versions` 做同步审计，以 `comparison_runs`/`comparison_events` 做任务快照。保留现有解析、标准化、企业主体规则和 Excel 导出；删除账号和旧代次流程。

**Tech Stack:** Python 3.12、FastAPI、SQLAlchemy Async、Alembic、SQLite/PostgreSQL、Pytest、XlsxWriter、Docker。

---

### Task 1: 新领域模型与迁移

- [x] 重写 `corpus_schema.py`，移除账号、daily、generation 和旧事件表运行依赖，增加同步与比对表。
- [x] 新增 Alembic head，支持空库初始化并保留 `license_state`。
- [x] 更新测试 fixture 和 schema 断言。

### Task 2: 全量文件解析与同步服务

- [x] 扩展 `InputRecord`/`corpus_io` 读取办结时间、处理部门和全部原始字段。
- [x] 新建同步服务，按工单编号优先、复合指纹回退，支持重复行最后覆盖、字段更新、缺失标记和版本审计。
- [x] 增加同步状态、统计、错误和回滚测试。

### Task 3: 时间窗口与任务快照

- [x] 实现受理/办结字段切换、最大日期默认待比对窗口、补集被比对窗口、重叠校验和空日期规则。
- [x] 将现有确定性分组逻辑适配为任务内事件和成员快照。
- [x] 保证全局 `cannot_links` 只作用于当前任务成员。

### Task 4: Web、导出与运行入口

- [x] 删除登录和账号路由，新增单文件同步、窗口比对、任务列表和任务详情路由。
- [x] 保留授权启动/中间件检查及 Docker/外部 LLM 配置。
- [x] 适配现有两工作表导出，并让导出严格读取任务快照。

### Task 5: 清理旧流程与回归

- [x] 删除旧 worker 批次、history rebuild、账号模块和相关模板/测试。
- [x] 清理 Milvus/Embedding/Reranker 残留文档和依赖。
- [x] 运行 SQLite、迁移、完整 pytest、Docker 构建和启动 smoke test。
