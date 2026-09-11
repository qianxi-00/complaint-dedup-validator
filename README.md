# 投诉全量工单比对系统

系统采用“全量工单库 + 时间窗口比对”架构：维护一份持续更新的全量工单库，并从中选择一个时间窗口作为“待比对”数据，与另一时间范围（或全量补集）做确定性判重，形成可查询、可导出、可审计的历史比对任务。

## 核心流程

1. 每次上传一份全量工单表（`.xlsx`、`.xls`、`.csv`），系统按工单编号或复合指纹识别稳定工单。
2. 新工单插入，已有工单按最新文件内容更新；本次文件缺失的旧工单保留并标记为“未出现在最新上传”，后续比对默认排除。
3. 用户选择受理时间或办结时间，设置待比对时间段；被比对时间段可手动指定，不填写时自动取全量有效工单的补集。
4. 后台执行事件分组与判重，保存本次任务的工单和事件快照，历史结果不受后续全量同步影响。
5. 在历史比对与事件库中筛选、搜索、查看详情、修改事件名称或成员归属，并导出结果。

## 功能概览

- **全量同步**：解析上传文件并保留原始字段 JSON 与来源行号；同一文件内稳定键重复时以后出现的行覆盖前一行（较长文本、较晚时间优先）。每次同步记录文件哈希、行数、新增/更新/缺失数，并在 `work_order_versions` 保存变更前后快照。
- **时间窗口比对**：支持受理时间（默认）与办结时间两个窗口字段；日期按 `Asia/Shanghai` 自然日解释，窗口为闭区间。未填写待比对日期时使用全量库该字段的最新本地日期；未填写被比对窗口时取补集，保证两侧互斥并覆盖全量有效工单。
- **事件分组**：不依赖 Milvus、Embedding、Reranker 或无约束相似度传递合并。先做硬合并，再做灰区候选召回与外部 LLM 事件卡裁决，模型异常或超预算时自动回退保守规则。
- **任务与并发**：同步和比对都通过 `processing_jobs` 后台执行，状态为待处理/执行中/已完成/失败，界面只展示中文状态。系统采用单任务互斥：已有待处理或执行中的任务时，新任务直接拒绝并提示已有任务信息。
- **历史比对与事件库**：入口 `/comparisons`，默认打开最近一次比对任务；支持地区、街道、处理部门、办结时间范围、仅看无办结时间、仅看含待比对工单、隐藏单例、排序和关键字搜索（默认在当前筛选内搜索，可切换“全量搜索”）。
- **事件详情**：分页查看工单，展示受理/办结时间、所属部门、处理部门、比对侧、工单编号、诉求标题和市民诉求；市民诉求默认折叠，可逐行展开；支持重命名事件、剔除成员并将其并入已有事件或单例事件。
- **导出**：支持全量导出和按当前筛选导出。按筛选导出会先确定事件集合，再导出这些事件的全部成员，保留完整事件上下文。工作簿包含“重复项”和“孤立工单”两个工作表，保留业务列顺序、事件名称、事件编号、比对侧、事件着色、冻结首行、自动筛选及公式注入防护。

## 项目结构

```text
complaint-dedup-validator/
├─ src/complaint_dedup/
│  ├─ main.py                    # 应用入口（uvicorn complaint_dedup.main:app）
│  ├─ full_corpus.py             # 同步、窗口划分、事件快照、筛选与导出查询
│  ├─ full_corpus_web.py         # FastAPI 路由、模板渲染、上传落盘、授权中间件
│  ├─ full_corpus_worker.py      # 后台任务循环（独立进程）
│  ├─ dedup_engine.py            # 硬合并、候选召回、事件卡 LLM 裁决与回退
│  ├─ dedup_features.py          # 判重特征归一化与 PII 脱敏
│  ├─ corpus_parser.py           # 诉求文本解析（地区/街道/地址/主体/发生对象）
│  ├─ corpus_io.py               # Excel/CSV 读取为 InputRecord
│  ├─ file_inspection.py         # 文件格式、表头映射、行数限制校验
│  ├─ full_corpus_exporter.py    # xlsxwriter 导出
│  ├─ corpus_schema.py           # 10 张业务表定义（中文注释）
│  ├─ async_database.py          # 建表、schema 校验、PostgreSQL 注释
│  ├─ corpus_database.py         # 数据库连接串
│  ├─ llm_client.py / llm_models.py  # OpenAI 兼容客户端与响应模型
│  ├─ licensing.py               # 授权与时钟回拨检查
│  └─ logging_setup.py           # loguru 日志
├─ templates/full_*.html         # 4 个服务端模板（工作台、事件库、事件详情、基础布局）
├─ static/                       # app.css、export.js、vendor
├─ alembic/versions/             # 4 个迁移，head 为 20260910_0004
├─ tests/                        # pytest 用例 + 导出按钮 Node 测试
├─ deploy/                       # compose.intranet.yaml、env.intranet.example
├─ scripts/                      # reset_database.py、内网离线构建与混淆脚本
├─ Dockerfile / Dockerfile.intranet
└─ runtime/                      # 本地运行数据（上传、导出、日志），不提交
```

## 本地启动

```powershell
uv sync
Copy-Item .env.example .env   # 填写 DB_* 与百炼 LLM_API_KEY
uv run uvicorn complaint_dedup.main:app --host 127.0.0.1 --port 8765
# 另开一个终端启动后台 worker（PostgreSQL 模式下 API 不内嵌 worker）
uv run python -m complaint_dedup.full_corpus_worker
```

打开 `http://127.0.0.1:8765`。运行时统一使用 PostgreSQL；API 启动会自动创建并校验当前表结构。SQLite 仅在单元测试的临时目录中使用。

## 时间窗口规则

- 默认比对字段：`受理时间`（`received_at`），可切换为 `办结时间`（`completed_at`）。
- 待比对时间段：用户选择；未填写时默认取全量库该字段的最新本地日期当天。
- 被比对时间段：默认是全量库中待比对时间段的补集；手动指定时必须同时填写起止日期，且不能与待比对时间段重叠。手动窗口之外的有效工单不参与本次比对。
- 所选日期为空的工单进入待比对侧，并在任务统计中单独显示数量。
- 每次比对保存独立快照，之后再次同步全量库不会改变旧任务的展示和导出结果。

## 判重规则

- 同步时持久化基础工单号、完整内容指纹、主体、地址、事项、订单号和文本特征。
- 基础工单号相同（包括 `HBDn` 转派后缀）、长文本完整重复或发生对象明确相同时执行硬合并。
- 灰区工单先按主体、地址、事项、订单号和字符 n-gram 多路召回，形成事件卡后批量调用外部 LLM 做互斥分组。
- 程序继续校验硬冲突、模型置信度、支持证据和卡片合法性；模型异常、达到请求/耗时预算或证据不足时自动回退硬匹配和保守规则，不阻塞任务。
- 不同街道、不同问题族不会合并；不会仅因相同电话、街道或事项大类合并，也不使用无约束相似度传递合并。
- 事件名称由地区、街道、主体/地点和问题族或事项组合生成，可在详情页人工修改。

## 数据模型

当前业务数据库由 10 张表组成：

| 表 | 用途 |
| --- | --- |
| `license_state` | 授权状态与首次启动时间，用于时钟回拨检测 |
| `work_orders` | 当前全量工单，保存最新结构化数据与判重特征 |
| `sync_runs` | 每次全量同步的文件哈希、行数与新增/更新/缺失统计 |
| `processing_jobs` | 同步和比对后台任务队列 |
| `work_order_versions` | 工单同步前后版本审计快照 |
| `comparison_runs` | 比对任务及窗口、计数、算法/模型版本统计 |
| `comparison_record_members` | 任务内工单快照（target/reference 两侧） |
| `comparison_events` | 任务内事件 |
| `comparison_event_members` | 事件与工单成员关系 |
| `comparison_decisions` | 自动判重决策审计（含硬合并、模型分组、回退） |

时间字段按 UTC 存储、按上海时区解释和展示；表和字段在 PostgreSQL 中写入中文注释。`comparison_*` 表保存任务级快照，因此历史结果不依赖当前全量库。

## 页面与接口

- `GET /`：工作台、全量上传、创建时间窗口比对、最近任务。
- `POST /sync`：上传全量文件并创建同步任务（支持 `.xlsx`、`.xls`、`.csv`）。
- `POST /comparisons`：创建时间窗口比对任务。
- `GET /comparisons`：历史比对与事件库，支持筛选、搜索、分页和导出入口。
- `GET /events/{event_id}`：事件详情和工单分页。
- `POST /events/{event_id}/name`：保存事件名称。
- `POST /events/{event_id}/members/{record_key}/exclude`：移出事件成员。
- `GET /comparisons/{comparison_id}/export`：全量或按筛选导出；`GET /exports/corpus` 为兼容入口。
- `GET /jobs/{job_id}`：查询任务状态；`GET /healthz`：健康检查。

## 配置

配置通过 `.env`（本地）或服务器 `config/.env`（Docker）提供，主要分组：

- 服务：`APP_HOST`、`APP_PORT`、`APP_TIMEZONE`。
- 数据库：`DATABASE_MODE`、`DATABASE_PATH`、`DB_HOST`、`DB_PORT`、`DB_USER`、`DB_PASSWORD`、`DB_NAME`、`DB_POOL_SIZE`、`DB_MAX_OVERFLOW`。
- 导入与日志：`MAX_TOTAL_ROWS`、`LOG_LEVEL`、`LOG_DIR`、`LOG_RETENTION_DAYS`。
- HTTP 连接池：`HTTP_MAX_CONNECTIONS`、`HTTP_MAX_KEEPALIVE_CONNECTIONS`。
- 大模型（OpenAI 兼容）：`LLM_BASE_URL`、`LLM_API_KEY`、`LLM_MODEL`、`LLM_CONCURRENCY`、`LLM_TIMEOUT_SECONDS`、`LLM_MAX_RETRIES`、`LLM_TEMPERATURE`、`LLM_MAX_TOKENS`、`LLM_ENABLE_THINKING`、`LLM_SEND_ENABLE_THINKING`。
- 自动判重：`DEDUP_LLM_ENABLED`、`DEDUP_MAX_CANDIDATES`、`DEDUP_CARDS_PER_BATCH`、`DEDUP_MAX_REQUESTS`、`DEDUP_MAX_CONCURRENCY`、`DEDUP_MAX_SECONDS`、`DEDUP_MIN_CONFIDENCE`。

未配置 `LLM_MODEL` 或关闭 `DEDUP_LLM_ENABLED` 时，判重自动使用保守规则，不调用模型。

## 测试

```powershell
uv run pytest -q
node --test tests/export_ui.test.cjs
```

当前共 100+ 个 pytest 用例，覆盖解析、特征回填、硬合并、候选召回、事件卡模型校验与降级、全量同步、窗口补集与空日期、任务快照、事件筛选与分页、导出、授权和 Web 上传；另有导出按钮的前端交互测试。

## 评估与回归

判重算法改动必须先跑评估集，确认指标不退化：

```powershell
# 纯规则快速评估（不调用模型，用于 CI/回归）
uv run python scripts/eval_dedup.py --dataset tests/fixtures/dedup_eval --no-llm

# 完整评估（调用 .env 中配置的模型），并与上一次结果对比
uv run python scripts/eval_dedup.py --dataset tests/fixtures/dedup_eval --compare runtime/eval/eval_ruleonly_final.json
```

评估集由 `scripts/build_eval_set.py` 从本地全量库分层抽样生成，冻结在 `tests/fixtures/dedup_eval/`（文本已掩码姓名/手机号/身份证，保留工单编号）；基线报告见 `docs/判重评估报告-基线-v3.0.md`。`tests/test_dedup_eval.py` 在缺少冻结集时自动跳过。

复核不确定样本并写回冻结集：

```powershell
uv run python scripts/review_eval_queue.py --apply
# 仅规则复核（不调用模型）/ 使用已有复核结果 / 业务护栏修订
uv run python scripts/review_eval_queue.py --no-llm
uv run python scripts/review_eval_queue.py --apply --from-results runtime/eval/review_results_final.jsonl
uv run python scripts/review_eval_queue.py --refine runtime/eval/review_results.jsonl --results runtime/eval/review_results_refined.jsonl
```

## Docker 部署

内网部署使用 `deploy/compose.intranet.yaml`，包含 PostgreSQL、API 和独立 worker，默认端口 `28765`。数据库表会在 API 启动时自动创建并校验，实施人员只需配置模型连接：

```bash
mkdir -p config
cp deploy/env.intranet.example config/.env
# 修改 config/.env 中的 LLM_BASE_URL、LLM_API_KEY、LLM_MODEL
docker compose --project-directory . -f deploy/compose.intranet.yaml up -d
```

正常 Docker 启动会自动创建并校验当前表，并拒绝旧表或字段不完整的数据库。只有明确需要破坏性清空旧库时，才执行：

```powershell
uv run python scripts/reset_database.py --confirm-reset
```

该命令会删除旧表和旧 `alembic_version`，然后初始化当前使用的 10 张表。生产环境必须先停止服务并完成数据库备份。

生产凭据只保存在服务器 `config/.env`，不得提交 Git。

## 授权与交付

授权日期通过构建期生成的 `licensing_conf.py` 注入，使用上海时区判断；API 启动、请求中间件和 worker 启动/执行前均检查授权，并通过 `license_state` 检测系统时间回拨。内网离线构建、镜像导出、授权续期和回滚步骤见《内网离线交付与续期部署手册》。

## 相关文档

- [系统需求说明](docs/系统需求说明.md)
- [投诉判重系统运行与算法说明](docs/投诉判重系统运行与算法说明.md)
- [内网离线交付与续期部署手册](docs/内网离线交付与续期部署手册.md)
- [内网部署与授权说明](docs/内网部署与授权说明.md)
- [判重算法自动化协同优化方案-v2](docs/判重算法自动化协同优化方案-v2.md)
- [判重算法现状与 LLM 协同优化方案](docs/判重算法现状与LLM协同优化方案.md)
- [部署验收记录-20260910](docs/部署验收记录-20260910.md)
